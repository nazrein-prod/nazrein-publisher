package config

import (
	"io"
	"log/slog"
	"testing"
	"time"
)

func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// With no env set the service must behave exactly as it did when these values
// were hardcoded in main.go.
func TestNewConfig_DefaultsMatchThePreviousHardcodedValues(t *testing.T) {
	cfg := NewConfig(quietLogger())

	if cfg.StreamName != "nazrein" {
		t.Errorf("StreamName = %q, want %q", cfg.StreamName, "nazrein")
	}
	if cfg.Interval != time.Hour {
		t.Errorf("Interval = %s, want 1h", cfg.Interval)
	}
	if cfg.MaxLenBuffer != DefaultMaxLenBuffer {
		t.Errorf("MaxLenBuffer = %d, want %d", cfg.MaxLenBuffer, DefaultMaxLenBuffer)
	}
}

func TestNewConfig_ReadsEnv(t *testing.T) {
	t.Setenv("REDIS_STREAM_NAME", "nazrein_staging")
	t.Setenv("PUBLISH_INTERVAL", "15m")
	t.Setenv("STREAM_MAXLEN_BUFFER", "2000")

	cfg := NewConfig(quietLogger())

	if cfg.StreamName != "nazrein_staging" {
		t.Errorf("StreamName = %q, want %q", cfg.StreamName, "nazrein_staging")
	}
	if cfg.Interval != 15*time.Minute {
		t.Errorf("Interval = %s, want 15m", cfg.Interval)
	}
	if cfg.MaxLenBuffer != 2000 {
		t.Errorf("MaxLenBuffer = %d, want 2000", cfg.MaxLenBuffer)
	}
}

// Bad config must not take the service down — it falls back and logs.
func TestNewConfig_InvalidValuesFallBack(t *testing.T) {
	t.Setenv("PUBLISH_INTERVAL", "not-a-duration")
	t.Setenv("STREAM_MAXLEN_BUFFER", "-1")

	cfg := NewConfig(quietLogger())

	if cfg.Interval != DefaultInterval {
		t.Errorf("Interval = %s, want the %s default", cfg.Interval, DefaultInterval)
	}
	if cfg.MaxLenBuffer != DefaultMaxLenBuffer {
		t.Errorf("MaxLenBuffer = %d, want the %d default", cfg.MaxLenBuffer, DefaultMaxLenBuffer)
	}
}
