package platform

import (
	"context"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type TimescaleStatsWriter struct {
	pool     *pgxpool.Pool
	tracker  *Tracker
	script   string
	interval time.Duration
	runID    int64
	previous *RuntimeStats
}

func NewTimescaleStatsWriter(ctx context.Context, dsn string, tracker *Tracker, script string, interval time.Duration) (*TimescaleStatsWriter, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, err
	}
	var runID int64
	if err := pool.QueryRow(ctx, `SELECT nextval('tracking_stats_run_id_seq')`).Scan(&runID); err != nil {
		pool.Close()
		return nil, err
	}
	return &TimescaleStatsWriter{
		pool:     pool,
		tracker:  tracker,
		script:   script,
		interval: interval,
		runID:    runID,
	}, nil
}

func (w *TimescaleStatsWriter) Run(ctx context.Context, logger *slog.Logger) {
	if w == nil || w.tracker == nil || w.pool == nil || w.interval <= 0 {
		return
	}
	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			w.flush(context.WithoutCancel(ctx), logger)
			return
		case <-ticker.C:
			w.flush(ctx, logger)
		}
	}
}

func (w *TimescaleStatsWriter) Close() {
	if w != nil && w.pool != nil {
		w.pool.Close()
	}
}

func (w *TimescaleStatsWriter) flush(ctx context.Context, logger *slog.Logger) {
	if err := w.StoreSnapshot(ctx, w.tracker.Snapshot()); err != nil && logger != nil {
		logger.Error("timescale stats flush failed", "err", err)
	}
}

func (w *TimescaleStatsWriter) StoreSnapshot(ctx context.Context, snapshot RuntimeStats) error {
	if w == nil || w.pool == nil {
		return nil
	}
	intervalStart := snapshot.StartedAt
	if w.previous != nil {
		intervalStart = w.previous.ObservedAt
	}
	if !intervalStart.Before(snapshot.ObservedAt) {
		intervalStart = snapshot.ObservedAt.Add(-w.interval)
	}
	batch := &pgx.Batch{}
	batch.Queue(`
		INSERT INTO tracking_process_stats (
			interval_start, interval_end, run_id, script, process_started_at,
			uptime_ms, goroutines, alloc_bytes, heap_objects, gc_cycles
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
	`, intervalStart, snapshot.ObservedAt, w.runID, w.script, snapshot.StartedAt, durationMs(snapshot.Uptime), snapshot.Goroutines, int64(snapshot.AllocBytes), int64(snapshot.HeapObjects), int64(snapshot.GCCycles))
	previousDomains := previousDomainMap(w.previous)
	for _, domain := range snapshot.Domains {
		previous := previousDomains[domain.Name]
		batch.Queue(`
			INSERT INTO tracking_domain_stats (
				interval_start, interval_end, run_id, script, name,
				last_success, last_error, requests, writes, errors,
				request_latency_ms, queue_depth, healthy, last_ready_change,
				processing_count, total_process_time_ms, store_batches,
				store_rows_requested, store_rows_affected, store_duration_ms,
				target_count, target_cycle, target_processed
			)
			VALUES (
				$1, $2, $3, $4, $5,
				$6, NULLIF($7, ''), $8, $9, $10,
				$11, $12, $13, $14,
				$15, $16, $17,
				$18, $19, $20,
				$21, $22, $23
			)
		`,
			intervalStart,
			snapshot.ObservedAt,
			w.runID,
			w.script,
			domain.Name,
			nullableTime(domain.LastSuccess),
			domain.LastError,
			counterDelta(domain.Requests, previous.Requests),
			counterDelta(domain.Writes, previous.Writes),
			counterDelta(domain.Errors, previous.Errors),
			durationDeltaMs(domain.RequestLatencyTotal, previous.RequestLatencyTotal),
			domain.QueueDepth,
			domain.Healthy,
			nullableTime(domain.LastReadyChange),
			counterDelta(domain.ProcessingCount, previous.ProcessingCount),
			durationDeltaMs(domain.TotalProcessTime, previous.TotalProcessTime),
			counterDelta(domain.StoreBatches, previous.StoreBatches),
			counterDelta(domain.StoreRowsRequested, previous.StoreRowsRequested),
			counterDelta(domain.StoreRowsAffected, previous.StoreRowsAffected),
			durationDeltaMs(domain.StoreDurationTotal, previous.StoreDurationTotal),
			domain.TargetCount,
			domain.TargetCycle,
			domain.TargetProcessed,
		)
	}
	results := w.pool.SendBatch(ctx, batch)
	defer results.Close()
	for i := 0; i < batch.Len(); i++ {
		if _, err := results.Exec(); err != nil {
			return err
		}
	}
	w.previous = cloneRuntimeStats(snapshot)
	return nil
}

func durationMs(value time.Duration) float64 {
	return float64(value) / float64(time.Millisecond)
}

func nullableTime(value time.Time) any {
	if value.IsZero() {
		return nil
	}
	return value
}

func previousDomainMap(snapshot *RuntimeStats) map[string]DomainStats {
	if snapshot == nil {
		return nil
	}
	out := make(map[string]DomainStats, len(snapshot.Domains))
	for _, domain := range snapshot.Domains {
		out[domain.Name] = domain
	}
	return out
}

func cloneRuntimeStats(snapshot RuntimeStats) *RuntimeStats {
	out := snapshot
	out.Domains = append([]DomainStats(nil), snapshot.Domains...)
	return &out
}

func counterDelta(current, previous int64) int64 {
	if current < previous {
		return current
	}
	return current - previous
}

func durationDeltaMs(current, previous time.Duration) float64 {
	if current < previous {
		return durationMs(current)
	}
	return durationMs(current - previous)
}
