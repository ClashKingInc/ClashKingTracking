package platform

import (
	"context"
	"errors"
	"time"
)

const asyncBatchRetryInterval = time.Second
const asyncBatchShutdownTimeout = 10 * time.Second

type AsyncBatchWriterConfig[T any] struct {
	Domain        string
	BatchSize     int
	QueueSize     int
	FlushInterval time.Duration
	RetryInterval time.Duration
	WriteBatch    func(context.Context, []T) error
}

type AsyncBatchWriter[T any] struct {
	app           *App
	domain        string
	batchSize     int
	flushInterval time.Duration
	retryInterval time.Duration
	jobs          chan T
	writeBatch    func(context.Context, []T) error
}

func NewAsyncBatchWriter[T any](app *App, cfg AsyncBatchWriterConfig[T]) *AsyncBatchWriter[T] {
	retryInterval := cfg.RetryInterval
	if retryInterval <= 0 {
		retryInterval = asyncBatchRetryInterval
	}
	return &AsyncBatchWriter[T]{
		app:           app,
		domain:        cfg.Domain,
		batchSize:     cfg.BatchSize,
		flushInterval: cfg.FlushInterval,
		retryInterval: retryInterval,
		jobs:          make(chan T, cfg.QueueSize),
		writeBatch:    cfg.WriteBatch,
	}
}

func (w *AsyncBatchWriter[T]) Enqueue(ctx context.Context, job T) error {
	if w == nil || w.jobs == nil {
		return errors.New("async batch writer is not configured")
	}
	select {
	case w.jobs <- job:
		w.recordQueueDepth(0)
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (w *AsyncBatchWriter[T]) Run(ctx context.Context) {
	if w == nil || w.writeBatch == nil {
		return
	}
	timer := time.NewTimer(w.flushInterval)
	defer timer.Stop()
	batch := make([]T, 0, w.batchSize)
	flush := func(flushCtx context.Context) bool {
		if len(batch) == 0 {
			w.recordQueueDepth(0)
			return true
		}
		if err := w.writeBatch(flushCtx, batch); err != nil {
			w.app.Logger.Error("async store batch failed", "domain", w.domain, "jobs", len(batch), "err", err)
			w.app.Stats.SetReady(w.domain, false, err.Error())
			w.recordQueueDepth(len(batch))
			return false
		}
		batch = batch[:0]
		w.recordQueueDepth(0)
		return true
	}
	resetTimer := func() {
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timer.Reset(w.flushInterval)
	}
	flushBeforeExit := func(parent context.Context) {
		flushCtx, cancel := context.WithTimeout(context.WithoutCancel(parent), asyncBatchShutdownTimeout)
		defer cancel()
		for {
		drainQueue:
			for len(batch) < w.batchSize {
				select {
				case job := <-w.jobs:
					batch = append(batch, job)
				default:
					break drainQueue
				}
			}
			if len(batch) == 0 {
				return
			}
			if flush(flushCtx) {
				continue
			}
			retry := time.NewTimer(w.retryInterval)
			select {
			case <-flushCtx.Done():
				retry.Stop()
				return
			case <-retry.C:
			}
		}
	}
	for {
		if len(batch) >= w.batchSize {
			if flush(ctx) {
				resetTimer()
				continue
			}
			select {
			case <-ctx.Done():
				flushBeforeExit(ctx)
				return
			case <-time.After(w.retryInterval):
			}
			continue
		}
		select {
		case <-ctx.Done():
			flushBeforeExit(ctx)
			return
		case job := <-w.jobs:
			batch = append(batch, job)
			w.recordQueueDepth(len(batch))
		case <-timer.C:
			if flush(ctx) {
				timer.Reset(w.flushInterval)
			} else {
				timer.Reset(w.retryInterval)
			}
		}
	}
}

func (w *AsyncBatchWriter[T]) recordQueueDepth(inBatch int) {
	if w == nil || w.app == nil || w.jobs == nil {
		return
	}
	w.app.Stats.SetQueueDepth(w.domain, len(w.jobs)+inBatch)
}
