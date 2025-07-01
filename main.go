package main

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	_ "github.com/jackc/pgx/v4/stdlib"
	"github.com/redis/go-redis/v9"
)

const (
	interval  = 10 * time.Minute
	streamKey = "trackyt"
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
	Created_At    time.Time `json:"created_at"`
	Updated_At    time.Time `json:"updated_at"`
}

func main() {

	ctx := context.Background()

	db, err := sql.Open("pgx", "host=localhost user=postgres password=postgres dbname=postgres port=5432 sslmode=disable")
	if err != nil {
		panic(fmt.Errorf("error connecting to db. %w", err))
	}

	defer db.Close()

	client := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
		Protocol: 2,
	})
	defer client.Close()

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

	var videoArr []Video

	query := `
		SELECT id, link, youtube_id
		FROM videos
	`

	rows, err := db.Query(query)
	if err != nil {
		fmt.Println("error getting data from db %w", err)
	}

	defer rows.Close()

	for rows.Next() {
		var video Video
		err = rows.Scan(&video.Id, &video.Link, &video.Youtube_ID)
		if err != nil {
			fmt.Println("error scanning row of rows.Next() %w", err)
			continue
		}

		videoArr = append(videoArr, video)
	}

	fmt.Printf("Length of videoArr: %d Time: %s\n", len(videoArr), time.Now().Format("2006-01-02 15:04:05"))

	for _, v := range videoArr {
		values := map[string]interface{}{
			"id":         v.Id,
			"link":       v.Link,
			"youtube_id": v.Youtube_ID,
		}

		_, err = client.XAdd(ctx, &redis.XAddArgs{
			Stream: streamKey,
			Values: values,
			ID:     "*",
		}).Result()

		if err != nil {
			fmt.Println("error adding values of redis streams %w", err)
		}

	}

	fmt.Printf("Added to redis stream. Time: %s\n", time.Now().Format("2006-01-02 15:04:05"))

}
