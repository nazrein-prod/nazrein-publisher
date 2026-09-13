package main

import (
	"testing"

	"github.com/grvbrk/nazrein_publisher/internal/config"
)

// The bug this guards: the stream used to be capped at a fixed MaxLen of 1000,
// so once more than a thousand videos were tracked, Redis trimmed entries off
// the stream before the worker could read them — silently dropping videos from
// tracking with nothing in the logs.
func TestStreamMaxLen_NeverTrimsTheCurrentRun(t *testing.T) {
	counts := []int{0, 1, 999, 1000, 1001, 10_000, 250_000}

	for _, n := range counts {
		got := streamMaxLen(n, config.DefaultMaxLenBuffer)
		if got <= int64(n) && n > 0 {
			t.Errorf("streamMaxLen(%d) = %d, which would trim into the run itself", n, got)
		}
		if got < int64(n) {
			t.Errorf("streamMaxLen(%d) = %d, below the number being published", n, got)
		}
	}

	// The specific regression: 1001 videos must not be capped at the old 1000.
	if got := streamMaxLen(1001, config.DefaultMaxLenBuffer); got <= 1000 {
		t.Errorf("streamMaxLen(1001) = %d; the old fixed 1000 cap is back", got)
	}
}

func TestStreamMaxLen_KeepsHeadroomForNewVideos(t *testing.T) {
	// Videos added between two runs must survive until the next publish.
	if got, want := streamMaxLen(100, 500), int64(600); got != want {
		t.Errorf("streamMaxLen(100, 500) = %d, want %d", got, want)
	}
}

func TestStreamMaxLen_NegativeBufferIsFloored(t *testing.T) {
	if got, want := streamMaxLen(10, -50), int64(10); got != want {
		t.Errorf("streamMaxLen(10, -50) = %d, want %d — a negative buffer must not shrink the cap", got, want)
	}
}
