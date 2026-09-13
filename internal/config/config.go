package config

import (
	"log/slog"
	"os"
	"strconv"
	"time"
)

const (
	DefaultStreamName   = "nazrein"
	DefaultInterval     = 1 * time.Hour
	DefaultMaxLenBuffer = 500
)

type Config struct {
	StreamName   string
	Interval     time.Duration
	MaxLenBuffer int64
}

func NewConfig(logger *slog.Logger) *Config {
	cfg := &Config{
		StreamName:   DefaultStreamName,
		Interval:     DefaultInterval,
		MaxLenBuffer: DefaultMaxLenBuffer,
	}

	if v := os.Getenv("REDIS_STREAM_NAME"); v != "" {
		cfg.StreamName = v
	}

	if v := os.Getenv("PUBLISH_INTERVAL"); v != "" {
		parsed, err := time.ParseDuration(v)
		if err != nil {
			logger.Warn("invalid PUBLISH_INTERVAL, using default", "value", v, "default", DefaultInterval)
		} else {
			cfg.Interval = parsed
		}
	}

	if v := os.Getenv("STREAM_MAXLEN_BUFFER"); v != "" {
		parsed, err := strconv.ParseInt(v, 10, 64)
		if err != nil || parsed < 0 {
			logger.Warn("invalid STREAM_MAXLEN_BUFFER, using default", "value", v, "default", DefaultMaxLenBuffer)
		} else {
			cfg.MaxLenBuffer = parsed
		}
	}

	return cfg
}
