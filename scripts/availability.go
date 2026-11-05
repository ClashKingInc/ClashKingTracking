package scripts

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"time"

	"clashking_tracking/internal/platform"
	clashy "github.com/clashkinginc/clashy.go"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	valkey "github.com/valkey-io/valkey-go"
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
	// Resume the persisted state without announcing an existing outage again.
	var lastOfficial bool
	if err := pool.QueryRow(ctx, `SELECT official_started_at IS NOT NULL FROM tracking_availability WHERE singleton`).Scan(&lastOfficial); err != nil {
		return err
	}
	for {
		probe := d.probe(ctx, app)
		if err := ctx.Err(); err != nil {
			return err
		}
		state, shifted, err := recordAvailabilityProbe(ctx, pool, probe)
		if err == nil {
			// Publish only after the SQL recovery and state transition commit.
			// If Valkey fails, the next probe republishes SQL state without shifting again.
			err = writeAvailabilityState(ctx, app, state)
			if state.Official && !lastOfficial {
				_ = app.PublishEvent(ctx, platform.Event{Topic: "maintenance", Value: map[string]any{"status": "started"}})
			}
			lastOfficial = state.Official
			if shifted > 0 {
				_ = app.PublishEvent(ctx, platform.Event{Topic: "maintenance", Value: map[string]any{
					"status": "recovered", "duration_seconds": int(shifted.Seconds()),
				}})
			}
		}
		if err != nil {
			app.Logger.Error("availability update failed", "err", err)
		}
		if app.Config.RunOnce {
			return err
		}
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
	return platform.AvailabilityState{Available: false, Official: isOfficialMaintenance(err), ChangedAt: now}
}

// SQL owns the current outage and recovery, not the expiring Valkey heartbeat.
// Locking this single row orders concurrent probes and makes clock correction
// atomic with clearing official_started_at. It intentionally stores no history.
func recordAvailabilityProbe(ctx context.Context, pool *pgxpool.Pool, probe platform.AvailabilityState) (platform.AvailabilityState, time.Duration, error) {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return platform.AvailabilityState{}, 0, err
	}
	defer tx.Rollback(ctx)
	var previous platform.AvailabilityState
	var officialStarted *time.Time
	var observedAt time.Time
	err = tx.QueryRow(ctx, `SELECT available, official_started_at, changed_at, observed_at, revision
        FROM tracking_availability WHERE singleton FOR UPDATE`).Scan(
		&previous.Available, &officialStarted, &previous.ChangedAt, &observedAt, &previous.Revision)
	if err != nil {
		return platform.AvailabilityState{}, 0, err
	}
	previous.Official = officialStarted != nil
	// A slow older probe must not replace a newer controller observation.
	if !probe.ChangedAt.After(observedAt) {
		return previous, 0, nil
	}
	state := probe
	if !probe.Available {
		if !previous.Available {
			state.ChangedAt = previous.ChangedAt
		}
		if probe.Official && officialStarted == nil {
			started := probe.ChangedAt
			officialStarted = &started
		}
		// A proxy failure after confirmed maintenance does not erase that evidence.
		state.Official = officialStarted != nil
	}
	var shifted time.Duration
	if probe.Available && officialStarted != nil {
		shifted = probe.ChangedAt.Sub(*officialStarted)
		if shifted > 0 {
			_, err = tx.Exec(ctx, `WITH shifted_wars AS (
                UPDATE war_schedule
                SET end_time = end_time + $1, next_run_at = next_run_at + $1
                WHERE end_time > $2
                RETURNING schedule_key
            ), shifted_timers AS (
                UPDATE player_timers SET expires_at = expires_at + $1
                WHERE event_type = 'war' AND event_key IN (SELECT schedule_key FROM shifted_wars)
            )
            UPDATE war_reminder_jobs SET run_at = run_at + $1
            WHERE schedule_key IN (SELECT schedule_key FROM shifted_wars)`, pgtype.Interval{Microseconds: shifted.Microseconds(), Valid: true}, *officialStarted)
			if err != nil {
				return platform.AvailabilityState{}, 0, err
			}
		}
		officialStarted = nil
		state.Official = false
	}
	state.Revision = previous.Revision + 1
	_, err = tx.Exec(ctx, `UPDATE tracking_availability SET available=$1, official_started_at=$2,
        changed_at=$3, observed_at=$4, revision=$5 WHERE singleton`,
		state.Available, officialStarted, state.ChangedAt, probe.ChangedAt, state.Revision)
	if err != nil {
		return platform.AvailabilityState{}, 0, err
	}
	if err = tx.Commit(ctx); err != nil {
		return platform.AvailabilityState{}, 0, err
	}
	return state, shifted, nil
}

var writeAvailabilityStateScript = valkey.NewLuaScript(`
    local current = redis.call('GET', KEYS[1])
    if current then
        local ok, state = pcall(cjson.decode, current)
        if ok and tonumber(state.revision or 0) > tonumber(ARGV[2]) then return 0 end
    end
    redis.call('SET', KEYS[1], ARGV[1], 'EX', 45)
    return 1
`)

func writeAvailabilityState(ctx context.Context, app *platform.App, state platform.AvailabilityState) error {
	raw, err := json.Marshal(state)
	if err != nil {
		return err
	}
	// Refresh even unchanged unavailable states. Revision fencing prevents a
	// delayed controller publish from replacing a newer committed heartbeat.
	return writeAvailabilityStateScript.Exec(ctx, app.Valkey, []string{platform.AvailabilityStateKey},
		[]string{string(raw), strconv.FormatInt(state.Revision, 10)}).Error()
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
