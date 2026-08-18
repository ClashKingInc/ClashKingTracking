package platform

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"
)

func TestAsyncBatchWriterRetainsFailedBatchForRetry(t *testing.T) {
	var attempts atomic.Int32
	written := make(chan []int, 1)
	app := &App{
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		Stats:  NewTracker(),
	}
	writer := NewAsyncBatchWriter(app, AsyncBatchWriterConfig[int]{
		Domain:        "test",
		BatchSize:     1,
		QueueSize:     1,
		FlushInterval: time.Hour,
		RetryInterval: 10 * time.Millisecond,
		WriteBatch: func(_ context.Context, batch []int) error {
			if attempts.Add(1) == 1 {
				return errors.New("temporary write failure")
			}
			written <- append([]int(nil), batch...)
			return nil
		},
	})

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		defer close(done)
		writer.Run(ctx)
	}()
	if err := writer.Enqueue(ctx, 42); err != nil {
		t.Fatal(err)
	}

	select {
	case got := <-written:
		if len(got) != 1 || got[0] != 42 {
			t.Fatalf("retried batch = %v, want [42]", got)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for failed batch retry")
	}
	cancel()
	<-done

	if got := attempts.Load(); got != 2 {
		t.Fatalf("write attempts = %d, want 2", got)
	}
}

func TestAsyncBatchWriterDrainsQueuedJobsOnShutdown(t *testing.T) {
	var written atomic.Int32
	app := &App{
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		Stats:  NewTracker(),
	}
	writer := NewAsyncBatchWriter(app, AsyncBatchWriterConfig[int]{
		Domain:        "test",
		BatchSize:     1,
		QueueSize:     3,
		FlushInterval: time.Hour,
		WriteBatch: func(_ context.Context, batch []int) error {
			written.Add(int32(len(batch)))
			return nil
		},
	})
	for _, value := range []int{1, 2, 3} {
		if err := writer.Enqueue(t.Context(), value); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	writer.Run(ctx)

	if got := written.Load(); got != 3 {
		t.Fatalf("written jobs = %d, want 3", got)
	}
}
