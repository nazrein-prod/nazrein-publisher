package main

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	service "github.com/grvbrk/nazrein_publisher/internal/db"
	_ "github.com/jackc/pgx/v4/stdlib"
	"github.com/redis/go-redis/v9"
)

const (
	interval  = 1 * time.Hour
	streamKey = "nazrein"
)

type Video struct {
	Id            string    `json:"id"`
	Link          string    `json:"link"`
	Published_At  string    `json:"published_at"`
	Title         string    `json:"title"`
	Description   string    `json:"description"`
	Thumbnail     string    `json:"thumbnail"`
	Youtube_ID    string    `json:"youtube_id"`
	Channel_Title string    `json:"channel_title"`
	Channel_ID    string    `json:"channel_id"`
	User_ID       string    `json:"user_id"`
	Is_Active     bool      `json:"is_active"`
	Visits        int       `json:"visits"`
	Created_At    time.Time `json:"created_at"`
	Updated_At    time.Time `json:"updated_at"`
}

type RedisVideo struct {
	Id         string `json:"id"`
	Link       string `json:"link"`
	Youtube_ID string `json:"youtube_id"`
}

func main() {

	ctx := context.Background()

	db, err := service.ConnectPGDB()
	if err != nil {
		panic(fmt.Errorf("error connecting to db. %w", err))
	}

	defer func() {
		if err := db.Close(); err != nil {
			fmt.Println("Error closing db:", err)
		}
	}()

	client := service.ConnectRedis()

	defer func() {
		if err := client.Close(); err != nil {
			fmt.Println("Error closing redis client", err)
		}

	}()

	FetchDataFromPostgres(ctx, db, client)
	TickerInterval(ctx, db, client)

}

func TickerInterval(ctx context.Context, db *sql.DB, client *redis.Client) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			FetchDataFromPostgres(ctx, db, client)
		case <-ctx.Done():
			fmt.Println("Context cancelled, stopping data sync")
			return
		}
	}
}

func FetchDataFromPostgres(ctx context.Context, db *sql.DB, client *redis.Client) {
	var videoArr []RedisVideo

	query := `
        SELECT id, link, youtube_id
        FROM videos
    `

	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		fmt.Println("error getting data from db:", err)
		return
	}
	defer rows.Close()

	for rows.Next() {
		var video RedisVideo
		if err := rows.Scan(&video.Id, &video.Link, &video.Youtube_ID); err != nil {
			fmt.Println("error scanning rows:", err)
			continue
		}
		videoArr = append(videoArr, video)
	}

	if err := rows.Err(); err != nil {
		fmt.Printf("error iterating rows: %v\n", err)
		return
	}

	for _, v := range videoArr {
		msgID, err := client.XAdd(ctx, &redis.XAddArgs{
			Stream: streamKey,
			MaxLen: 1000,
			Values: map[string]interface{}{
				"id":         v.Id,
				"link":       v.Link,
				"youtube_id": v.Youtube_ID,
			},
		}).Result()

		if err != nil {
			fmt.Println("error adding to redis stream:", err)
		}
		fmt.Printf("Added to redis stream. Time: %s\n msgID: %s\n", time.Now().Format("2006-01-02 15:04:05"), msgID)
	}

}
