package scripts

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"clashking_tracking/internal/platform"
	"clashking_tracking/internal/utils"
	"clashking_tracking/models"

	clashy "github.com/clashkinginc/clashy.go"
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
	NextTargetPage(context.Context, string, int) (basicPlayerTargetPage, error)
	CountTargets(context.Context) (int, error)
	Store(context.Context, []basicPlayerIngest) (requestedRows int, affectedRows int, err error)
	Close() error
}

type basicPlayerTargetPage struct {
	Tags       []string
	NextCursor string
}

type basicPlayerIngest struct {
	Profile *models.PlayerProfileIngest
	Delete  string
}

type timescaleBasicPlayerStore struct {
	pool *pgxpool.Pool
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
		return errors.New("TIMESCALE_* connection variables are required when basicplayers is enabled")
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
	writerCtx, stopWriter := context.WithCancel(ctx)
	writerDone := make(chan struct{})
	go func() {
		defer close(writerDone)
		writer.Run(writerCtx)
	}()
	defer func() {
		stopWriter()
		<-writerDone
	}()

	limiter, err := newTrackingLimiter(app.Config.BasicPlayerRequestsPerSecond)
	if err != nil {
		return err
	}

	scanCtx, cancelScan := context.WithCancel(ctx)
	jobs := make(chan string)
	errCh := make(chan error, 1)
	var reportOnce sync.Once
	reportError := func(err error) {
		if err == nil {
			return
		}
		reportOnce.Do(func() {
			errCh <- err
			cancelScan()
		})
	}
	var workers sync.WaitGroup
	for range bulkFetchWorkerCount(
		app.Config.BasicPlayerRequestsPerSecond,
		platform.RequestConcurrency(app.Config.BasicPlayerRequestsPerSecond),
	) {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for {
				var tag string
				select {
				case <-scanCtx.Done():
					return
				case tag = <-jobs:
				}
				ingest, err := retryLimitedClashFetch(scanCtx, app, limiter, func(fetchCtx context.Context) (basicPlayerIngest, error) {
					return d.do(fetchCtx, app, tag)
				})
				app.Stats.RecordTrackedTarget(basicPlayersDomainName)
				if err != nil {
					if scanCtx.Err() != nil {
						return
					}
					if isDeferredBulkFetch(err) {
						continue
					}
					reportError(fmt.Errorf("basic player processing %s: %w", tag, err))
					return
				}
				if err := writer.Enqueue(scanCtx, ingest); err != nil {
					if scanCtx.Err() == nil {
						reportError(err)
					}
					return
				}
			}
		}()
	}
	defer func() {
		cancelScan()
		workers.Wait()
	}()

	pageSize := app.Config.BasicPlayerRequestsPerSecond * app.Config.TargetPageMultiplier
	cursor := ""
	for {
		select {
		case err := <-errCh:
			return err
		default:
		}
		page, err := d.store.NextTargetPage(scanCtx, cursor, pageSize)
		if err != nil {
			return err
		}
		if len(page.Tags) == 0 {
			cursor = ""
			if err := sleepOrDone(scanCtx, time.Second); err != nil {
				return err
			}
			continue
		}
		for _, tag := range page.Tags {
			select {
			case err := <-errCh:
				return err
			case <-scanCtx.Done():
				return scanCtx.Err()
			case jobs <- tag:
			}
		}
		cursor = page.NextCursor
	}
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
	if player == nil {
		return basicPlayerIngest{}, errors.New("player fetch returned no player")
	}
	profile := utils.PlayerProfileFromClashy(*player)
	return basicPlayerIngest{Profile: &profile}, nil
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

func (s *timescaleBasicPlayerStore) NextTargetPage(ctx context.Context, cursor string, limit int) (basicPlayerTargetPage, error) {
	if limit <= 0 {
		limit = 1000
	}
	rows, err := s.pool.Query(ctx, basicPlayerTargetSQL, cursor, limit+1)
	if err != nil {
		return basicPlayerTargetPage{}, err
	}
	defer rows.Close()
	tags := make([]string, 0, limit)
	nextCursor := ""
	for rows.Next() {
		var tag string
		if err := rows.Scan(&tag); err != nil {
			return basicPlayerTargetPage{}, err
		}
		if tag != "" {
			if len(tags) == limit {
				nextCursor = tag
				break
			}
			tags = append(tags, tag)
		}
	}
	err = rows.Err()
	if err != nil {
		return basicPlayerTargetPage{}, err
	}
	if nextCursor != "" && len(tags) > 0 {
		nextCursor = tags[len(tags)-1]
	}
	return basicPlayerTargetPage{Tags: tags, NextCursor: nextCursor}, nil
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

	affectedRows, err := utils.UpsertPlayerProfiles(ctx, tx, profiles, basicPlayersDomainName)
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
