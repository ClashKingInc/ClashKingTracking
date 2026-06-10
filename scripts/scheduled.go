package scripts

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strconv"
	"time"

	"clashking_tracking/internal/platform"
	"clashking_tracking/internal/utils"
	"clashking_tracking/models"

	clashy "github.com/clashkinginc/clashy.go"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel/attribute"
)

const scheduledDomainName = "scheduled"
const leaderboardKindCapital = "capital"

type leaderboardLoader func(context.Context, *clashy.Client, string) (any, error)

var leaderboardPaths = []struct {
	Kind string
	Load leaderboardLoader
}{
	{Kind: "clan_trophies", Load: func(ctx context.Context, client *clashy.Client, locationID string) (any, error) {
		return client.GetLocationClansByLocationID(ctx, locationID, 0, "", "")
	}},
	{Kind: "clan_versus_trophies", Load: func(ctx context.Context, client *clashy.Client, locationID string) (any, error) {
		return client.GetLocationClansBuilderBaseByLocationID(ctx, locationID, 0, "", "")
	}},
	{Kind: leaderboardKindCapital, Load: func(ctx context.Context, client *clashy.Client, locationID string) (any, error) {
		return client.GetLocationClansCapitalByLocationID(ctx, locationID, 0, "", "")
	}},
	{Kind: "player_trophies", Load: func(ctx context.Context, client *clashy.Client, locationID string) (any, error) {
		return client.GetLocationPlayersByLocationID(ctx, locationID, 0, "", "")
	}},
	{Kind: "player_versus_trophies", Load: func(ctx context.Context, client *clashy.Client, locationID string) (any, error) {
		return client.GetLocationPlayersBuilderBaseByLocationID(ctx, locationID, 0, "", "")
	}},
}

type scheduledDomain struct {
	store scheduledStore
}

type scheduledStore interface {
	Close()
	StoreSnapshots(context.Context, []models.LeaderboardSnapshotItemRow) error
}

func NewScheduledDomain() platform.Domain { return &scheduledDomain{} }

func (d *scheduledDomain) Name() string { return scheduledDomainName }

func (d *scheduledDomain) Run(ctx context.Context, app *platform.App) error {
	if err := validateScheduledConfig(app.Config); err != nil {
		return err
	}
	store, err := newScheduledStore(ctx, app)
	if err != nil {
		return err
	}
	defer store.Close()
	d.store = store

	interval := time.Duration(app.Config.ScheduledIntervalSeconds) * time.Second
	for {
		start := time.Now()
		err = d.runCycle(ctx, app)
		app.Stats.RecordProcess(scheduledDomainName, time.Since(start))
		if err != nil {
			if app.Config.RunOnce {
				return err
			}
			app.Stats.SetReady(scheduledDomainName, false, err.Error())
		}
		if app.Config.RunOnce {
			return nil
		}
		if err := sleepOrDone(ctx, interval); err != nil {
			return err
		}
	}
}

func validateScheduledConfig(cfg platform.Config) error {
	if cfg.ScheduledIntervalSeconds <= 0 {
		return errors.New("scheduled.interval_seconds must be greater than zero")
	}
	if !cfg.DryRun && !cfg.MockDB && cfg.TimescaleURL == "" {
		return errors.New("TIMESCALE_URL is required for scheduled")
	}
	return nil
}

func newScheduledStore(ctx context.Context, app *platform.App) (scheduledStore, error) {
	if app.Config.MockDB || app.Config.DryRun || app.Config.TimescaleURL == "" {
		return memoryScheduledStore{}, nil
	}
	return newTimescaleScheduledStore(ctx, app.Config.TimescaleURL)
}

func (d *scheduledDomain) runCycle(ctx context.Context, app *platform.App) error {
	items, err := d.doLeaderboardSnapshots(ctx, app)
	if err != nil {
		return err
	}
	if err := d.store.StoreSnapshots(ctx, items); err != nil {
		return err
	}
	app.Stats.RecordWrite(scheduledDomainName, len(items))
	app.Stats.SetReady(scheduledDomainName, true, "")
	return nil
}

func (d *scheduledDomain) doLeaderboardSnapshots(
	ctx context.Context,
	app *platform.App,
) ([]models.LeaderboardSnapshotItemRow, error) {
	start := time.Now()
	locations, err := app.Clash.SearchLocations(ctx, 0, "", "")
	app.Stats.RecordRequest(scheduledDomainName, time.Since(start), err)
	if err != nil {
		return nil, err
	}
	now := time.Now().UTC()
	date := dayStart(now)
	locationIDs := leaderboardLocationIDs(locations)
	itemRows := make([]models.LeaderboardSnapshotItemRow, 0, len(leaderboardPaths)*len(locationIDs)*200)
	for _, locationID := range locationIDs {
		for _, item := range leaderboardPaths {
			if !shouldStoreLeaderboardKind(item.Kind, now) {
				continue
			}
			start := time.Now()
			payload, err := item.Load(ctx, app.Clash, locationID)
			app.Stats.RecordRequest(scheduledDomainName, time.Since(start), err)
			if err != nil {
				continue
			}
			if !leaderboardPayloadHasItems(payload) {
				continue
			}
			itemRows = append(itemRows, leaderboardSnapshotItems(item.Kind, locationID, date, payload)...)
		}
	}
	return itemRows, nil
}

func leaderboardLocationIDs(locations []clashy.Location) []string {
	out := make([]string, 0, len(locations)+1)
	for _, location := range locations {
		if location.ID == 0 {
			continue
		}
		out = append(out, strconv.Itoa(location.ID))
	}
	return append(out, "global")
}

func leaderboardPayloadHasItems(payload any) bool {
	value := reflect.ValueOf(payload)
	for value.Kind() == reflect.Pointer || value.Kind() == reflect.Interface {
		if value.IsNil() {
			return false
		}
		value = value.Elem()
	}
	switch value.Kind() {
	case reflect.Array, reflect.Map, reflect.Slice:
		return value.Len() > 0
	default:
		return true
	}
}

func shouldStoreLeaderboardKind(kind string, now time.Time) bool {
	if kind != leaderboardKindCapital {
		return true
	}
	return now.UTC().Weekday() == time.Tuesday
}

func leaderboardSnapshotItems(
	kind string,
	locationID string,
	date time.Time,
	payload any,
) []models.LeaderboardSnapshotItemRow {
	switch items := payload.(type) {
	case []clashy.RankedClan:
		out := make([]models.LeaderboardSnapshotItemRow, 0, len(items))
		for _, item := range items {
			out = append(out, models.LeaderboardSnapshotItemRow{
				Kind:       kind,
				LocationID: locationID,
				Date:       date,
				Tag:        item.Tag,
				Name:       item.Name,
				Rank:       item.Rank,
				Data:       jsonAny(item),
			})
		}
		return out
	case []clashy.RankedPlayer:
		out := make([]models.LeaderboardSnapshotItemRow, 0, len(items))
		for _, item := range items {
			out = append(out, models.LeaderboardSnapshotItemRow{
				Kind:       kind,
				LocationID: locationID,
				Date:       date,
				Tag:        item.Tag,
				Name:       item.Name,
				Rank:       item.Rank,
				Data:       jsonAny(item),
			})
		}
		return out
	default:
		return nil
	}
}

type timescaleScheduledStore struct {
	pool *pgxpool.Pool
}

func newTimescaleScheduledStore(ctx context.Context, dsn string) (*timescaleScheduledStore, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, err
	}
	return &timescaleScheduledStore{pool: pool}, nil
}

func (s *timescaleScheduledStore) Close() {
	if s.pool != nil {
		s.pool.Close()
	}
}

func (s *timescaleScheduledStore) StoreSnapshots(
	ctx context.Context,
	items []models.LeaderboardSnapshotItemRow,
) error {
	if len(items) == 0 {
		return nil
	}
	ctx, span := platform.StartSpan(ctx, "timescale.leaderboard_snapshot_items.upsert",
		attribute.String("domain", scheduledDomainName),
		attribute.String("operation", "upsert"),
		attribute.Int("item.count", len(items)),
	)
	defer span.End()
	batch := &pgx.Batch{}
	for _, item := range items {
		raw, _ := json.Marshal(item.Data)
		batch.Queue(`
			INSERT INTO leaderboard_snapshot_items (
				kind, location_id, date, tag, name, rank, data
			)
			VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb)
			ON CONFLICT (kind, location_id, date, tag) DO UPDATE SET
				name = EXCLUDED.name,
				rank = EXCLUDED.rank,
				data = EXCLUDED.data
			WHERE
				leaderboard_snapshot_items.name IS DISTINCT FROM EXCLUDED.name OR
				leaderboard_snapshot_items.rank IS DISTINCT FROM EXCLUDED.rank OR
				leaderboard_snapshot_items.data IS DISTINCT FROM EXCLUDED.data
		`, item.Kind, item.LocationID, item.Date, item.Tag, item.Name, item.Rank, string(raw))
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	err = utils.SendBatch(ctx, tx, batch)
	if err == nil {
		err = tx.Commit(ctx)
	}
	platform.RecordSpanError(span, err)
	span.SetAttributes(platform.SpanErrorStatus(err))
	return err
}

type memoryScheduledStore struct{}

func (memoryScheduledStore) Close() {}

func (memoryScheduledStore) StoreSnapshots(
	context.Context,
	[]models.LeaderboardSnapshotItemRow,
) error {
	return nil
}

func jsonAny(value any) any {
	raw, _ := json.Marshal(value)
	var out any
	_ = json.Unmarshal(raw, &out)
	return out
}
