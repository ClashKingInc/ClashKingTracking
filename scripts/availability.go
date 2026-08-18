package scripts

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"clashking_tracking/internal/platform"

	clashy "github.com/clashkinginc/clashy.go"
	"github.com/jackc/pgx/v5/pgxpool"
)

const availabilityDomainName = "availability"

type availabilityDomain struct{}

func NewAvailabilityDomain() platform.Domain { return &availabilityDomain{} }
func (d *availabilityDomain) Name() string   { return availabilityDomainName }

func (d *availabilityDomain) Run(ctx context.Context, app *platform.App) error {
	if app.Clash == nil || app.Valkey == nil || app.Config.TimescaleURL == "" {
		return errors.New("availability requires the Clash API, Valkey, and Timescale")
	}
	pool, err := pgxpool.New(ctx, app.Config.TimescaleURL)
	if err != nil {
		return err
	}
	defer pool.Close()

	var previous platform.AvailabilityState
	if raw, readErr := app.Valkey.Do(ctx, app.Valkey.B().Get().Key(platform.AvailabilityStateKey).Build()).ToString(); readErr == nil {
		_ = json.Unmarshal([]byte(raw), &previous)
	}
	for {
		state := d.probe(ctx, app)
		if !state.Available && !previous.Available && state.Official == previous.Official && !previous.ChangedAt.IsZero() {
			state.ChangedAt = previous.ChangedAt
		}
		if state.Available && !previous.Available && previous.Official && !previous.ChangedAt.IsZero() {
			duration := state.ChangedAt.Sub(previous.ChangedAt)
			if duration > 0 {
				if err := shiftOfficialMaintenance(ctx, pool, duration); err != nil {
					app.Logger.Error("official maintenance time shift failed", "err", err)
					if err := sleepOrDone(ctx, 15*time.Second); err != nil {
						return err
					}
					continue
				}
				_ = app.PublishEvent(ctx, platform.Event{Topic: "maintenance", Value: map[string]any{
					"status": "recovered", "duration_seconds": int(duration.Seconds()),
				}})
			}
		}
		if state.Available != previous.Available || state.Official != previous.Official || state.Available {
			if err := writeAvailabilityState(ctx, app, state); err != nil {
				app.Logger.Error("availability state write failed", "err", err)
			}
		}
		previous = state
		if err := sleepOrDone(ctx, 15*time.Second); err != nil {
			return err
		}
	}
}

func (d *availabilityDomain) probe(ctx context.Context, app *platform.App) platform.AvailabilityState {
	now := time.Now().UTC()
	_, err := app.Clash.SearchLocations(ctx, clashy.PageOptions{Limit: 1})
	if err == nil {
		return platform.AvailabilityState{Available: true, ChangedAt: now}
	}
	official := isOfficialMaintenance(err)
	return platform.AvailabilityState{Available: false, Official: official, ChangedAt: now}
}

func writeAvailabilityState(ctx context.Context, app *platform.App, state platform.AvailabilityState) error {
	raw, err := json.Marshal(state)
	if err != nil {
		return err
	}
	return app.Valkey.Do(ctx, app.Valkey.B().Set().Key(platform.AvailabilityStateKey).Value(string(raw)).Ex(45*time.Second).Build()).Error()
}

func isOfficialMaintenance(err error) bool {
	var maintenance *clashy.Maintenance
	if errors.As(err, &maintenance) {
		return true
	}
	var gateway *clashy.GatewayError
	if !errors.As(err, &gateway) || gateway.HTTPException == nil || gateway.Status != 500 {
		return false
	}
	message := strings.ToLower(gateway.Reason + " " + gateway.Message + " " + string(gateway.Body))
	return strings.Contains(message, "maintenance")
}

func shiftOfficialMaintenance(ctx context.Context, pool *pgxpool.Pool, duration time.Duration) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	_, err = tx.Exec(ctx, `
		WITH shifted_wars AS (
			UPDATE war_schedule
			SET end_time = end_time + $1,
			    next_run_at = next_run_at + $1
			WHERE end_time > now()
			RETURNING schedule_key
		), shifted_timers AS (
			UPDATE player_timers timer
			SET expires_at = expires_at + $1
			WHERE event_type = 'war'
			  AND event_key IN (SELECT schedule_key FROM shifted_wars)
		)
		UPDATE war_reminder_jobs job
		SET run_at = run_at + $1
		WHERE schedule_key IN (SELECT schedule_key FROM shifted_wars)
	`, duration)
	if err != nil {
		return err
	}
	return tx.Commit(ctx)
}
