package scripts

import (
	"context"
	"errors"
	"time"

	"clashking_tracking/internal/platform"
	"clashking_tracking/internal/utils"
	"clashking_tracking/models"

	clashy "github.com/clashkinginc/clashy.go"
	clashtracker "github.com/clashkinginc/clashy.go/tracker"
	"github.com/jackc/pgx/v5/pgxpool"
)

const basicPlayersDomainName = "basicplayers"

const (
	basicPlayerAsyncWriteBatchSize     = 1000
	basicPlayerAsyncWriteQueueSize     = 4000
	basicPlayerAsyncWriteFlushInterval = 500 * time.Millisecond
)

const basicPlayerTargetSQL = `
	SELECT tag
	FROM basic_player
	WHERE tag > $1
	  AND tag <> ''
	ORDER BY tag
	LIMIT $2
`

const basicPlayerTargetCountSQL = `
	SELECT count(*)
	FROM basic_player
	WHERE tag <> ''
`

type basicPlayersDomain struct {
	store basicPlayerStore
}

type basicPlayerStore interface {
	NextTargetPage(context.Context, string, int) (clashtracker.TargetPage, error)
	CountTargets(context.Context) (int, error)
	Store(context.Context, []basicPlayerIngest) (requestedRows int, affectedRows int, err error)
	Close() error
}

type basicPlayerIngest struct {
	Profile *models.PlayerProfileIngest
	Delete  string
}

type timescaleBasicPlayerStore struct {
	pool *pgxpool.Pool
}

type basicPlayerTargetPager struct {
	store basicPlayerStore
}

func NewBasicPlayersDomain() platform.Domain {
	return &basicPlayersDomain{}
}

func (d *basicPlayersDomain) Name() string { return basicPlayersDomainName }

func (d *basicPlayersDomain) Run(ctx context.Context, app *platform.App) error {
	if app.Config.BasicPlayerRequestsPerSecond <= 0 {
		return errors.New("basicplayers.requests_per_second must be greater than zero when basicplayers is enabled")
	}
	if !app.Config.DryRun && !app.Config.MockDB && app.Config.TimescaleURL == "" {
		return errors.New("TIMESCALE_URL is required when basicplayers is enabled")
	}
	if app.Config.TimescaleURL != "" && !app.Config.DryRun && !app.Config.MockDB {
		store, err := newTimescaleBasicPlayerStore(ctx, app.Config.TimescaleURL)
		if err != nil {
			return err
		}
		d.store = store
		defer store.Close()
	}
	if d.store == nil {
		app.Stats.SetReady(basicPlayersDomainName, true, "")
		return nil
	}

	if count, err := d.store.CountTargets(ctx); err == nil {
		app.Stats.SetTrackingTargets(basicPlayersDomainName, count)
	} else {
		app.Logger.Error("basic player target count failed", "err", err)
	}

	writer := platform.NewAsyncBatchWriter[basicPlayerIngest](
		app,
		platform.AsyncBatchWriterConfig[basicPlayerIngest]{
			Domain:        basicPlayersDomainName,
			BatchSize:     basicPlayerAsyncWriteBatchSize,
			QueueSize:     basicPlayerAsyncWriteQueueSize,
			FlushInterval: basicPlayerAsyncWriteFlushInterval,
			WriteBatch: func(writeCtx context.Context, values []basicPlayerIngest) error {
				start := time.Now()
				requestedRows, affectedRows, err := d.store.Store(writeCtx, values)
				if err != nil {
					return err
				}
				app.Stats.RecordStore(basicPlayersDomainName, time.Since(start), requestedRows, affectedRows)
				app.Stats.RecordWrite(basicPlayersDomainName, requestedRows)
				app.Stats.SetReady(basicPlayersDomainName, true, "")
				app.Stats.RecordProcess(basicPlayersDomainName, time.Since(start))
				return nil
			},
		},
	)
	go writer.Run(ctx)

	runner, err := clashtracker.NewRunner[basicPlayerIngest](clashtracker.Config[basicPlayerIngest]{
		TargetPager:       basicPlayerTargetPager{store: d.store},
		TargetPageSize:    app.Config.BasicPlayerRequestsPerSecond * app.Config.TargetPageMultiplier,
		ResultBatchSize:   basicPlayerAsyncWriteBatchSize,
		RequestsPerSecond: app.Config.BasicPlayerRequestsPerSecond,
		MaxInFlight:       app.Config.BasicPlayerRequestsPerSecond,
		EmitInitial:       true,
		Fetch: func(fetchCtx context.Context, target clashtracker.Target) (clashtracker.FetchResult[basicPlayerIngest], error) {
			ingest, err := d.do(fetchCtx, app, target.Key)
			return clashtracker.FetchResult[basicPlayerIngest]{Value: ingest}, err
		},
		Handle: func(handleCtx context.Context, _ clashtracker.Target, _ *basicPlayerIngest, ingest basicPlayerIngest) error {
			if err := writer.Enqueue(handleCtx, ingest); err != nil {
				return err
			}
			app.Stats.RecordTrackedTarget(basicPlayersDomainName)
			return nil
		},
		OnFetchError: func(_ context.Context, target clashtracker.Target, err error) (clashtracker.FetchErrorDecision, error) {
			app.Logger.Error("basic player processing failed", "tag", target.Key, "err", err)
			app.Stats.SetReady(basicPlayersDomainName, false, err.Error())
			if decision, ok := platform.ClashFetchErrorDecision(err); ok {
				return decision, nil
			}
			return clashtracker.FetchErrorDecision{Action: clashtracker.FetchErrorStop}, nil
		},
	})
	if err != nil {
		return err
	}
	return runner.Run(ctx)
}

func (d *basicPlayersDomain) do(ctx context.Context, app *platform.App, tag string) (basicPlayerIngest, error) {
	start := time.Now()
	player, err := app.Clash.GetPlayer(ctx, tag)
	app.Stats.RecordRequest(basicPlayersDomainName, time.Since(start), err)
	if isClashNotFound(err) {
		return basicPlayerIngest{Delete: tag}, nil
	}
	if err != nil {
		return basicPlayerIngest{}, err
	}
	profile := utils.PlayerProfileFromClashy(*player)
	return basicPlayerIngest{Profile: &profile}, nil
}

func (p basicPlayerTargetPager) NextPage(ctx context.Context, cursor string, limit int) (clashtracker.TargetPage, error) {
	return p.store.NextTargetPage(ctx, cursor, limit)
}

func (p basicPlayerTargetPager) Count(ctx context.Context) (int, error) {
	return p.store.CountTargets(ctx)
}

func newTimescaleBasicPlayerStore(ctx context.Context, dsn string) (*timescaleBasicPlayerStore, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, err
	}
	return &timescaleBasicPlayerStore{pool: pool}, nil
}

func (s *timescaleBasicPlayerStore) Close() error {
	if s == nil || s.pool == nil {
		return nil
	}
	s.pool.Close()
	return nil
}

func (s *timescaleBasicPlayerStore) NextTargetPage(ctx context.Context, cursor string, limit int) (clashtracker.TargetPage, error) {
	if limit <= 0 {
		limit = 1000
	}
	rows, err := s.pool.Query(ctx, basicPlayerTargetSQL, cursor, limit+1)
	if err != nil {
		return clashtracker.TargetPage{}, err
	}
	defer rows.Close()
	targets := make([]clashtracker.Target, 0, limit)
	nextCursor := ""
	for rows.Next() {
		var tag string
		if err := rows.Scan(&tag); err != nil {
			return clashtracker.TargetPage{}, err
		}
		if tag != "" {
			if len(targets) == limit {
				nextCursor = tag
				break
			}
			targets = append(targets, clashtracker.Target{Key: tag, Value: tag})
		}
	}
	err = rows.Err()
	if err != nil {
		return clashtracker.TargetPage{}, err
	}
	if nextCursor != "" && len(targets) > 0 {
		nextCursor = targets[len(targets)-1].Key
	}
	return clashtracker.TargetPage{Targets: targets, NextCursor: nextCursor}, nil
}

func (s *timescaleBasicPlayerStore) CountTargets(ctx context.Context) (int, error) {
	var count int
	if err := s.pool.QueryRow(ctx, basicPlayerTargetCountSQL).Scan(&count); err != nil {
		return 0, err
	}
	return count, nil
}

func (s *timescaleBasicPlayerStore) Store(ctx context.Context, ingests []basicPlayerIngest) (int, int, error) {
	if len(ingests) == 0 {
		return 0, 0, nil
	}
	requestedRows := 0
	profiles := make([]models.PlayerProfileIngest, 0, len(ingests))
	deletes := make([]string, 0)
	for _, ingest := range ingests {
		if ingest.Profile != nil {
			requestedRows++
			profiles = append(profiles, *ingest.Profile)
		}
		if ingest.Delete != "" {
			requestedRows++
			deletes = append(deletes, ingest.Delete)
		}
	}
	if requestedRows == 0 {
		return 0, 0, nil
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return requestedRows, 0, err
	}
	defer tx.Rollback(ctx)

	affectedRows, err := utils.UpsertPlayerProfiles(ctx, tx, profiles, basicPlayersDomainName, nil)
	if err != nil {
		return requestedRows, affectedRows, err
	}
	if err := utils.DeletePlayers(ctx, tx, deletes); err != nil {
		return requestedRows, affectedRows, err
	}
	if err := tx.Commit(ctx); err != nil {
		return requestedRows, affectedRows, err
	}
	return requestedRows, affectedRows, nil
}

func isClashNotFound(err error) bool {
	var notFound *clashy.NotFound
	return errors.As(err, &notFound)
}
