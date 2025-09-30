package main

import (
	"fmt"
	"os"

	"github.com/redis/go-redis/v9"
)

func connectRedis() *redis.Client {

	url := os.Getenv("UPSTASH_REDIS_URL")
	if url == "" {
		panic("missing UPSTASH_REDIS_URL env variable")
	}

	opt, err := redis.ParseURL(url)
	if err != nil {
		panic(fmt.Errorf("failed to parse redis URL: %w", err))
	}

	client := redis.NewClient(opt)

	return client
}
