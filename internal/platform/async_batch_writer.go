package platform

import (
	"context"
	"errors"
	"time"
)

type AsyncBatchWriterConfig[T any] struct {
	Domain        string
	BatchSize     int
	QueueSize     int
	FlushInterval time.Duration
	WriteBatch    func(context.Context, []T) error
}

type AsyncBatchWriter[T any] struct {
	app           *App
	domain        string
	batchSize     int
	flushInterval time.Duration
	jobs          chan T
	writeBatch    func(context.Context, []T) error
}

func NewAsyncBatchWriter[T any](app *App, cfg AsyncBatchWriterConfig[T]) *AsyncBatchWriter[T] {
	return &AsyncBatchWriter[T]{
		app:           app,
		domain:        cfg.Domain,
		batchSize:     cfg.BatchSize,
		flushInterval: cfg.FlushInterval,
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
	flush := func(flushCtx context.Context) {
		if len(batch) == 0 {
			w.recordQueueDepth(0)
			return
		}
		if err := w.writeBatch(flushCtx, batch); err != nil {
			w.app.Logger.Error("async store batch failed", "domain", w.domain, "jobs", len(batch), "err", err)
			w.app.Stats.SetReady(w.domain, false, err.Error())
		}
		batch = batch[:0]
		w.recordQueueDepth(0)
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
	for {
		select {
		case <-ctx.Done():
			flush(context.WithoutCancel(ctx))
			return
		case job := <-w.jobs:
			batch = append(batch, job)
			w.recordQueueDepth(len(batch))
			if len(batch) >= w.batchSize {
				flush(ctx)
				resetTimer()
			}
		case <-timer.C:
			flush(ctx)
			timer.Reset(w.flushInterval)
		}
	}
}

func (w *AsyncBatchWriter[T]) recordQueueDepth(inBatch int) {
	if w == nil || w.app == nil || w.jobs == nil {
		return
	}
	w.app.Stats.SetQueueDepth(w.domain, len(w.jobs)+inBatch)
}
