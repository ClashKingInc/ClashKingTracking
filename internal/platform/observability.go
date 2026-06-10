package platform

import (
	"encoding/json"
	"net/http"
	"runtime"
	"sync"
	"time"
)

type DomainStats struct {
	Name             string        `json:"name"`
	LastSuccess      time.Time     `json:"last_success,omitempty"`
	LastError        string        `json:"last_error,omitempty"`
	Requests         int64         `json:"requests"`
	Writes           int64         `json:"writes"`
	Errors           int64         `json:"errors"`
	LastCycle        time.Duration `json:"last_cycle"`
	LastLatency      time.Duration `json:"last_latency"`
	AvgLatency       time.Duration `json:"avg_latency"`
	QueueDepth       int           `json:"queue_depth"`
	Healthy          bool          `json:"healthy"`
	LastReadyChange  time.Time     `json:"last_ready_change,omitempty"`
	ProcessingCount  int64         `json:"processing_count"`
	TotalProcessTime time.Duration `json:"total_process_time"`
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
	stats.LastLatency = latency
	if stats.Requests == 1 {
		stats.AvgLatency = latency
	} else {
		stats.AvgLatency = time.Duration((int64(stats.AvgLatency)*(stats.Requests-1) + int64(latency)) / stats.Requests)
	}
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
	stats.LastCycle = duration
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

func (t *Tracker) SetReady(name string, healthy bool, detail string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	stats := t.domainLocked(name)
	if stats.Healthy != healthy {
		stats.LastReadyChange = time.Now().UTC()
	}
	stats.Healthy = healthy
	if detail != "" {
		stats.LastError = detail
	}
}

func (t *Tracker) SetQueueDepth(name string, depth int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.domainLocked(name).QueueDepth = depth
}

func (t *Tracker) HTTPMux() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("/health/live", t.handleLive)
	mux.HandleFunc("/health/ready", t.handleReady)
	mux.HandleFunc("/stats", t.handleStats)
	mux.HandleFunc("/stats/domains", t.handleDomains)
	mux.HandleFunc("/stats/queues", t.handleQueues)
	return mux
}

func (t *Tracker) handleLive(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"status": "ok",
		"uptime": time.Since(t.started).String(),
	})
}

func (t *Tracker) handleReady(w http.ResponseWriter, _ *http.Request) {
	domains := t.snapshotDomains()
	ready := true
	for _, stats := range domains {
		if !stats.Healthy {
			ready = false
			break
		}
	}
	status := http.StatusOK
	if !ready {
		status = http.StatusServiceUnavailable
	}
	writeJSON(w, status, map[string]any{"ready": ready, "domains": domains})
}

func (t *Tracker) handleStats(w http.ResponseWriter, _ *http.Request) {
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	writeJSON(w, http.StatusOK, map[string]any{
		"uptime":       time.Since(t.started).String(),
		"goroutines":   runtime.NumGoroutine(),
		"alloc_bytes":  mem.Alloc,
		"heap_objects": mem.HeapObjects,
		"gc_cycles":    mem.NumGC,
		"domains":      t.snapshotDomains(),
		"queues":       t.snapshotQueues(),
	})
}

func (t *Tracker) handleDomains(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, t.snapshotDomains())
}

func (t *Tracker) handleQueues(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, t.snapshotQueues())
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

func (t *Tracker) snapshotQueues() map[string]int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	out := make(map[string]int, len(t.domains))
	for name, stats := range t.domains {
		out[name] = stats.QueueDepth
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

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
