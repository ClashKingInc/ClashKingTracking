package platform

import (
	"runtime"
	"sync"
	"time"
)

type DomainStats struct {
	Name                string        `json:"name"`
	LastSuccess         time.Time     `json:"last_success,omitempty"`
	LastError           string        `json:"last_error,omitempty"`
	Requests            int64         `json:"requests"`
	Writes              int64         `json:"writes"`
	Errors              int64         `json:"errors"`
	RequestLatencyTotal time.Duration `json:"request_latency_total"`
	QueueDepth          int           `json:"queue_depth"`
	Healthy             bool          `json:"healthy"`
	LastReadyChange     time.Time     `json:"last_ready_change,omitempty"`
	ProcessingCount     int64         `json:"processing_count"`
	TotalProcessTime    time.Duration `json:"total_process_time"`
	StoreBatches        int64         `json:"store_batches"`
	StoreRowsRequested  int64         `json:"store_rows_requested"`
	StoreRowsAffected   int64         `json:"store_rows_affected"`
	StoreDurationTotal  time.Duration `json:"store_duration_total"`
	TargetCount         int           `json:"target_count"`
	TargetCycle         int64         `json:"target_cycle"`
	TargetProcessed     int           `json:"target_processed"`
}

type RuntimeStats struct {
	ObservedAt  time.Time     `json:"observed_at"`
	StartedAt   time.Time     `json:"started_at"`
	Uptime      time.Duration `json:"uptime"`
	Goroutines  int           `json:"goroutines"`
	AllocBytes  uint64        `json:"alloc_bytes"`
	HeapObjects uint64        `json:"heap_objects"`
	GCCycles    uint32        `json:"gc_cycles"`
	Domains     []DomainStats `json:"domains"`
}

type Tracker struct {
	started time.Time
	mu      sync.RWMutex
	domains map[string]*DomainStats
}

func NewTracker() *Tracker {
	return &Tracker{
		started: time.Now().UTC(),
		domains: make(map[string]*DomainStats),
	}
}

func (t *Tracker) Domain(name string) *DomainStats {
	t.mu.Lock()
	defer t.mu.Unlock()
	stats, ok := t.domains[name]
	if !ok {
		stats = &DomainStats{Name: name, Healthy: true, LastReadyChange: time.Now().UTC()}
		t.domains[name] = stats
	}
	return stats
}

func (t *Tracker) RecordRequest(name string, latency time.Duration, err error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	stats := t.domainLocked(name)
	stats.Requests++
	stats.RequestLatencyTotal += latency
	if err != nil {
		stats.Errors++
		stats.Healthy = false
		stats.LastReadyChange = time.Now().UTC()
		stats.LastError = err.Error()
	}
}

func (t *Tracker) RecordProcess(name string, duration time.Duration) {
	t.mu.Lock()
	defer t.mu.Unlock()
	stats := t.domainLocked(name)
	stats.ProcessingCount++
	stats.TotalProcessTime += duration
	stats.LastSuccess = time.Now().UTC()
	stats.Healthy = true
}

func (t *Tracker) RecordWrite(name string, count int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.domainLocked(name).Writes += int64(count)
}

func (t *Tracker) RecordStore(name string, duration time.Duration, requestedRows, affectedRows int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	stats := t.domainLocked(name)
	stats.StoreBatches++
	stats.StoreRowsRequested += int64(requestedRows)
	stats.StoreRowsAffected += int64(affectedRows)
	stats.StoreDurationTotal += duration
}

func (t *Tracker) SetTrackingTargets(name string, count int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	stats := t.domainLocked(name)
	stats.TargetCount = count
	if stats.TargetCycle == 0 && count > 0 {
		stats.TargetCycle = 1
	}
	if stats.TargetProcessed > count {
		stats.TargetProcessed = count
	}
}

func (t *Tracker) RecordTrackedTarget(name string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	stats := t.domainLocked(name)
	if stats.TargetCycle == 0 {
		stats.TargetCycle = 1
	}
	stats.TargetProcessed++
	if stats.TargetCount > 0 && stats.TargetProcessed >= stats.TargetCount {
		stats.TargetCycle++
		stats.TargetProcessed = 0
	}
}

func (t *Tracker) SetReady(name string, healthy bool, detail string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	stats := t.domainLocked(name)
	if stats.Healthy != healthy {
		stats.LastReadyChange = time.Now().UTC()
	}
	stats.Healthy = healthy
	if healthy {
		stats.LastError = ""
	} else if detail != "" {
		stats.LastError = detail
	}
}

func (t *Tracker) SetQueueDepth(name string, depth int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.domainLocked(name).QueueDepth = depth
}

func (t *Tracker) Snapshot() RuntimeStats {
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	return RuntimeStats{
		ObservedAt:  time.Now().UTC(),
		StartedAt:   t.started,
		Uptime:      time.Since(t.started),
		Goroutines:  runtime.NumGoroutine(),
		AllocBytes:  mem.Alloc,
		HeapObjects: mem.HeapObjects,
		GCCycles:    mem.NumGC,
		Domains:     t.snapshotDomains(),
	}
}

func (t *Tracker) snapshotDomains() []DomainStats {
	t.mu.RLock()
	defer t.mu.RUnlock()
	out := make([]DomainStats, 0, len(t.domains))
	for _, stats := range t.domains {
		out = append(out, *stats)
	}
	return out
}

func (t *Tracker) domainLocked(name string) *DomainStats {
	stats := t.domains[name]
	if stats == nil {
		// Domains are created on first observation so callers do not need setup code.
		stats = &DomainStats{Name: name, Healthy: true, LastReadyChange: time.Now().UTC()}
		t.domains[name] = stats
	}
	return stats
}
