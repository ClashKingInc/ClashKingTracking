package platform

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"time"

	clashy "github.com/clashkinginc/clashy.go"
	valkey "github.com/valkey-io/valkey-go"
)

const AvailabilityStateKey = "tracking:clash_availability"

type AvailabilityState struct {
	Available bool      `json:"available"`
	Official  bool      `json:"official_maintenance"`
	ChangedAt time.Time `json:"changed_at"`
}

// AvailabilityGate is process-local. The first unavailable response closes it
// immediately, while the controller's Valkey heartbeat is the only thing that
// opens it again. This prevents a tracker from exiting during the controller's
// next 15-second probe window.
type AvailabilityGate struct {
	client valkey.Client

	mu         sync.RWMutex
	paused     bool
	observedAt time.Time
}

func NewAvailabilityGate(client valkey.Client) *AvailabilityGate {
	return &AvailabilityGate{client: client}
}

func (g *AvailabilityGate) Run(ctx context.Context) {
	if g == nil || g.client == nil {
		return
	}
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		_ = g.refresh(ctx)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (g *AvailabilityGate) Wait(ctx context.Context) error {
	if g == nil {
		return nil
	}
	for {
		g.mu.RLock()
		paused := g.paused
		g.mu.RUnlock()
		if !paused {
			return nil
		}
		timer := time.NewTimer(time.Second)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func (g *AvailabilityGate) Observe(err error) bool {
	if g == nil || !IsClashUnavailable(err) {
		return false
	}
	g.mu.Lock()
	g.paused = true
	g.observedAt = time.Now().UTC()
	g.mu.Unlock()
	return true
}

func (g *AvailabilityGate) refresh(ctx context.Context) error {
	raw, err := g.client.Do(ctx, g.client.B().Get().Key(AvailabilityStateKey).Build()).ToString()
	if err != nil {
		return err
	}
	var state AvailabilityState
	if err := json.Unmarshal([]byte(raw), &state); err != nil {
		return err
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if !state.Available {
		g.paused = true
		return nil
	}
	if g.observedAt.IsZero() || state.ChangedAt.After(g.observedAt) {
		g.paused = false
	}
	return nil
}

func IsClashUnavailable(err error) bool {
	var maintenance *clashy.Maintenance
	if errors.As(err, &maintenance) {
		return true
	}
	var gateway *clashy.GatewayError
	if !errors.As(err, &gateway) || gateway.HTTPException == nil {
		return false
	}
	switch gateway.Status {
	case 0, 500, 502, 503:
		return true
	default:
		return false
	}
}
