package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/grvbrk/nazrein_publisher/internal/config"
	service "github.com/grvbrk/nazrein_publisher/internal/db"
	applogger "github.com/grvbrk/nazrein_publisher/internal/logger"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/redis/go-redis/v9"
)

type RedisVideo struct {
	Id         string `json:"id"`
	Link       string `json:"link"`
	Youtube_ID string `json:"youtube_id"`
}

func main() {
	logger := applogger.New("publisher")
	slog.SetDefault(logger)
	cfg := config.NewConfig(logger)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	db, err := service.ConnectPGDB()
	if err != nil {
		logger.Error("error connecting to db", "err", err)
		os.Exit(1)
	}

	defer func() {
		if err := db.Close(); err != nil {
			logger.Warn("Error closing db", "err", err)
		}
	}()

	client := service.ConnectRedis()

	defer func() {
		if err := client.Close(); err != nil {
			logger.Warn("Error closing redis client", "err", err)
		}
	}()

	logger.Info("Starting publisher", "interval", cfg.Interval, "stream", cfg.StreamName)

	Publish(ctx, logger, cfg, db, client)
	TickerInterval(ctx, logger, cfg, db, client)

	logger.Info("Shutdown complete")
}

func TickerInterval(ctx context.Context, logger *slog.Logger, cfg *config.Config, db *sql.DB, client *redis.Client) {
	ticker := time.NewTicker(cfg.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			Publish(ctx, logger, cfg, db, client)
		case <-ctx.Done():
			logger.Info("Signal received, stopping data sync")
			return
		}
	}
}

func Publish(ctx context.Context, logger *slog.Logger, cfg *config.Config, db *sql.DB, client *redis.Client) {
	start := time.Now()

	videos, err := fetchVideos(ctx, db)
	if err != nil {
		logger.Error("error reading videos", "err", err)
		return
	}

	if len(videos) == 0 {
		logger.Info("No videos to publish")
		return
	}

	published, err := publishToStream(ctx, cfg, client, videos)
	if err != nil {
		logger.Error("error publishing to stream", "err", err, "published", published, "total", len(videos))
		return
	}

	logger.Info("Published videos to stream",
		"published", published,
		"total", len(videos),
		"stream", cfg.StreamName,
		"duration", time.Since(start).Round(time.Millisecond),
	)
}

func fetchVideos(ctx context.Context, db *sql.DB) ([]RedisVideo, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT id, link, youtube_id
		FROM videos
	`)
	if err != nil {
		return nil, fmt.Errorf("querying videos: %w", err)
	}
	defer func() {
		if err := rows.Close(); err != nil {
			fmt.Println("error closing rows:", err)
		}
	}()

	var videos []RedisVideo
	for rows.Next() {
		var video RedisVideo
		if err := rows.Scan(&video.Id, &video.Link, &video.Youtube_ID); err != nil {
			return nil, fmt.Errorf("scanning video row: %w", err)
		}
		videos = append(videos, video)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating video rows: %w", err)
	}

	return videos, nil
}

func publishToStream(ctx context.Context, cfg *config.Config, client *redis.Client, videos []RedisVideo) (int, error) {
	maxLen := streamMaxLen(len(videos), cfg.MaxLenBuffer)

	pipe := client.Pipeline()
	cmds := make([]*redis.StringCmd, 0, len(videos))

	for _, v := range videos {
		if err := ctx.Err(); err != nil {
			return 0, fmt.Errorf("cancelled while queueing: %w", err)
		}

		cmds = append(cmds, pipe.XAdd(ctx, &redis.XAddArgs{
			Stream: cfg.StreamName,
			MaxLen: maxLen,
			Approx: true,
			Values: map[string]interface{}{
				"id":         v.Id,
				"link":       v.Link,
				"youtube_id": v.Youtube_ID,
			},
		}))
	}

	_, execErr := pipe.Exec(ctx)

	published := 0
	var failures []error
	for i, cmd := range cmds {
		if err := cmd.Err(); err != nil {
			failures = append(failures, fmt.Errorf("video %s: %w", videos[i].Id, err))
			continue
		}
		published++
	}

	if len(failures) > 0 {
		return published, fmt.Errorf("%d/%d writes failed: %w", len(failures), len(videos), errors.Join(failures...))
	}
	if execErr != nil && !errors.Is(execErr, redis.Nil) {
		return published, fmt.Errorf("pipeline exec: %w", execErr)
	}

	return published, nil
}

func streamMaxLen(videoCount int, buffer int64) int64 {
	if buffer < 0 {
		buffer = 0
	}
	return int64(videoCount) + buffer
}
