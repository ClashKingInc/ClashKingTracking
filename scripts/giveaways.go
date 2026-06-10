package scripts

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"clashking_tracking/internal/platform"
	"clashking_tracking/models"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel/attribute"
)

const giveawaysDomainName = "giveaways"

type giveawaysDomain struct {
	store giveawayStore
}

type giveawayStore interface {
	Close()
	DueTransitions(context.Context, time.Time) ([]models.GiveawayTransition, error)
	MarkTransition(context.Context, models.GiveawayTransition) (models.GiveawayTransition, bool, error)
	ClearPendingEvent(context.Context, string) error
}

func NewGiveawaysDomain() platform.Domain { return &giveawaysDomain{} }

func (d *giveawaysDomain) Name() string { return giveawaysDomainName }

func (d *giveawaysDomain) Run(ctx context.Context, app *platform.App) error {
	if err := validateGiveawaysConfig(app.Config); err != nil {
		return err
	}
	store, err := newGiveawayStore(ctx, app)
	if err != nil {
		return err
	}
	defer store.Close()
	d.store = store

	interval := time.Duration(app.Config.GiveawayScanSeconds) * time.Second
	for {
		start := time.Now()
		err = d.runCycle(ctx, app)
		app.Stats.RecordProcess(giveawaysDomainName, time.Since(start))
		if err != nil {
			if app.Config.RunOnce {
				return err
			}
			app.Stats.SetReady(giveawaysDomainName, false, err.Error())
		}
		if app.Config.RunOnce {
			return nil
		}
		if err := sleepOrDone(ctx, interval); err != nil {
			return err
		}
	}
}

func validateGiveawaysConfig(cfg platform.Config) error {
	if cfg.GiveawayScanSeconds <= 0 {
		return errors.New("giveaways.scan_seconds must be greater than zero")
	}
	if !cfg.DryRun && !cfg.MockDB && cfg.TimescaleURL == "" {
		return errors.New("TIMESCALE_URL is required for giveaways")
	}
	return nil
}

func newGiveawayStore(ctx context.Context, app *platform.App) (giveawayStore, error) {
	if app.Config.MockDB || app.Config.DryRun || app.Config.TimescaleURL == "" {
		return &memoryGiveawayStore{}, nil
	}
	return newTimescaleGiveawayStore(ctx, app.Config.TimescaleURL)
}

func (d *giveawaysDomain) runCycle(ctx context.Context, app *platform.App) error {
	ctx, span := platform.StartSpan(ctx, "giveaways.cycle",
		attribute.String("domain", giveawaysDomainName),
		attribute.String("operation", "scan"),
	)
	defer span.End()

	transitions, err := d.doDueTransitions(ctx, time.Now().UTC())
	if err != nil {
		platform.RecordSpanError(span, err)
		span.SetAttributes(platform.SpanErrorStatus(err))
		return err
	}
	span.SetAttributes(attribute.Int("target.count", len(transitions)))
	for _, transition := range transitions {
		stored, ok, err := d.store.MarkTransition(ctx, transition)
		if err != nil {
			return err
		}
		if !ok {
			continue
		}
		if err := app.PublishEvent(ctx, platform.Event{
			Topic:   stored.Event.Topic,
			ClanTag: stored.Event.Key,
			Value:   stored.Event.Value,
		}); err != nil {
			return err
		}
		if err := d.store.ClearPendingEvent(ctx, stored.Row.ID); err != nil {
			return err
		}
		app.Stats.RecordWrite(giveawaysDomainName, 1)
	}
	app.Stats.SetReady(giveawaysDomainName, true, "")
	span.SetAttributes(platform.SpanErrorStatus(nil))
	return nil
}

func (d *giveawaysDomain) doDueTransitions(
	ctx context.Context,
	now time.Time,
) ([]models.GiveawayTransition, error) {
	return d.store.DueTransitions(ctx, now)
}

type timescaleGiveawayStore struct {
	pool *pgxpool.Pool
}

func newTimescaleGiveawayStore(ctx context.Context, dsn string) (*timescaleGiveawayStore, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, err
	}
	return &timescaleGiveawayStore{pool: pool}, nil
}

func (s *timescaleGiveawayStore) Close() {
	if s.pool != nil {
		s.pool.Close()
	}
}

func (s *timescaleGiveawayStore) DueTransitions(
	ctx context.Context,
	now time.Time,
) ([]models.GiveawayTransition, error) {
	rows, err := s.pool.Query(ctx, dueGiveawaysSQL, now)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []models.GiveawayTransition
	for rows.Next() {
		transition, err := scanGiveawayTransition(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, transition)
	}
	return out, rows.Err()
}

func (s *timescaleGiveawayStore) MarkTransition(
	ctx context.Context,
	transition models.GiveawayTransition,
) (models.GiveawayTransition, bool, error) {
	if transition.EventDue {
		return transition, true, nil
	}
	row := s.pool.QueryRow(ctx, markGiveawayTransitionSQL,
		transition.Row.ID,
		transition.From,
		transition.To,
		transition.Kind,
	)
	stored, err := scanGiveawayTransition(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return models.GiveawayTransition{}, false, nil
	}
	if err != nil {
		return models.GiveawayTransition{}, false, err
	}
	return stored, true, nil
}

func (s *timescaleGiveawayStore) ClearPendingEvent(ctx context.Context, id string) error {
	_, err := s.pool.Exec(ctx, clearGiveawayPendingSQL, id)
	return err
}

const dueGiveawaysSQL = `
	WITH pending AS (
		SELECT event_pending AS kind, status AS from_status, status AS to_status,
			id, server_id, status, updated, start_time, end_time, to_jsonb(giveaways) AS data,
			true AS event_due
		FROM giveaways
		WHERE event_pending IS NOT NULL
	),
	due AS (
		SELECT 'giveaway_start' AS kind, 'scheduled' AS from_status, 'ongoing' AS to_status,
			id, server_id, status, updated, start_time, end_time, to_jsonb(giveaways) AS data,
			false AS event_due
		FROM giveaways
		WHERE start_time <= $1 AND status = 'scheduled' AND event_pending IS NULL
		UNION ALL
		SELECT 'giveaway_end' AS kind, 'ongoing' AS from_status, 'ended' AS to_status,
			id, server_id, status, updated, start_time, end_time, to_jsonb(giveaways) AS data,
			false AS event_due
		FROM giveaways
		WHERE end_time <= $1 AND status = 'ongoing' AND event_pending IS NULL
		UNION ALL
		SELECT 'giveaway_update' AS kind, 'ongoing' AS from_status, 'ongoing' AS to_status,
			id, server_id, status, updated, start_time, end_time, to_jsonb(giveaways) AS data,
			false AS event_due
		FROM giveaways
		WHERE status = 'ongoing' AND updated = true AND event_pending IS NULL
	)
	SELECT * FROM pending
	UNION ALL
	SELECT * FROM due
	ORDER BY start_time, id
`

const markGiveawayTransitionSQL = `
	UPDATE giveaways
	SET
		status = $3,
		updated = CASE WHEN $4 = 'giveaway_update' THEN false ELSE updated END,
		event_pending = $4,
		event_pending_at = now(),
		updated_at = now()
	WHERE id = $1 AND status = $2 AND event_pending IS NULL
	RETURNING $4 AS kind, $2 AS from_status, $3 AS to_status,
		id, server_id, status, updated, start_time, end_time, to_jsonb(giveaways) AS data,
		true AS event_due
`

const clearGiveawayPendingSQL = `
	UPDATE giveaways
	SET event_pending = NULL, event_pending_at = NULL, updated_at = now()
	WHERE id = $1
`

type giveawayRowScanner interface {
	Scan(dest ...any) error
}

func scanGiveawayTransition(scanner giveawayRowScanner) (models.GiveawayTransition, error) {
	var transition models.GiveawayTransition
	var raw []byte
	if err := scanner.Scan(
		&transition.Kind,
		&transition.From,
		&transition.To,
		&transition.Row.ID,
		&transition.Row.ServerID,
		&transition.Row.Status,
		&transition.Row.Updated,
		&transition.Row.StartTime,
		&transition.Row.EndTime,
		&raw,
		&transition.EventDue,
	); err != nil {
		return models.GiveawayTransition{}, err
	}
	var data map[string]any
	_ = json.Unmarshal(raw, &data)
	transition.Row.Data = data
	transition.Event = models.Event{
		Topic: "giveaway",
		Type:  transition.Kind,
		Value: map[string]any{
			"type":     transition.Kind,
			"giveaway": data,
		},
		CreatedAt: time.Now().UTC(),
	}
	return transition, nil
}

type memoryGiveawayStore struct{}

func (s *memoryGiveawayStore) Close() {}

func (s *memoryGiveawayStore) DueTransitions(
	context.Context,
	time.Time,
) ([]models.GiveawayTransition, error) {
	return nil, nil
}

func (s *memoryGiveawayStore) MarkTransition(
	_ context.Context,
	transition models.GiveawayTransition,
) (models.GiveawayTransition, bool, error) {
	return transition, true, nil
}

func (s *memoryGiveawayStore) ClearPendingEvent(context.Context, string) error {
	return nil
}
