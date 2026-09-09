package scripts

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"clashking_tracking/internal/platform"
	"clashking_tracking/internal/wararchive"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	warArchiverDomainName = "war-archiver"
)

type warArchiverDomain struct{}

type pendingArchiveWar struct {
	ID          int32
	EndTime     time.Time
	WarType     string
	CWLLeagueID int
	War         wararchive.War
}

func NewWarArchiverDomain() platform.Domain { return &warArchiverDomain{} }
func (d *warArchiverDomain) Name() string   { return warArchiverDomainName }

func (d *warArchiverDomain) Run(ctx context.Context, app *platform.App) error {
	if app.Config.TimescaleURL == "" {
		return errors.New("timescale database is required for war archiving")
	}
	if app.Config.WarArchiveEndpoint == "" || app.Config.WarArchiveBucket == "" || app.Config.WarArchiveAccessKeyID == "" || app.Config.WarArchiveSecretAccessKey == "" {
		return errors.New("R2 endpoint, bucket, access key, and secret are required for war archiving")
	}
	if app.Config.WarArchiveScanSeconds <= 0 {
		return errors.New("war_archiver.scan_seconds must be greater than zero")
	}
	if app.Config.WarArchivePackSize <= 0 {
		return errors.New("war_archiver.pack_size must be greater than zero")
	}
	pool, err := pgxpool.New(ctx, app.Config.TimescaleURL)
	if err != nil {
		return err
	}
	defer pool.Close()
	client := newWarArchiveS3(app.Config)

	for {
		started := time.Now()
		pending := -1
		if count, countErr := countPendingArchiveWars(ctx, pool); countErr != nil {
			app.Logger.Error("count pending war archives failed", "error", countErr)
		} else {
			pending = count
			app.Stats.SetQueueDepth(d.Name(), pending)
		}
		archived, err := d.archiveOne(ctx, app, pool, client)
		app.Stats.RecordProcess(d.Name(), time.Since(started))
		if err != nil {
			app.Logger.Error("war archive pass failed", "error", err)
			app.Stats.SetReady(d.Name(), false, err.Error())
		} else if archived > 0 {
			app.Stats.RecordWrite(d.Name(), archived)
			if pending >= 0 {
				app.Stats.SetQueueDepth(d.Name(), max(0, pending-archived))
			}
			app.Stats.SetReady(d.Name(), true, "")
		} else {
			app.Stats.SetReady(d.Name(), true, "")
		}
		if app.Config.RunOnce {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Duration(app.Config.WarArchiveScanSeconds) * time.Second):
		}
	}
}

func countPendingArchiveWars(ctx context.Context, pool *pgxpool.Pool) (int, error) {
	var count int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM war_archive_pending`).Scan(&count); err != nil {
		return 0, err
	}
	return count, nil
}

func newWarArchiveS3(cfg platform.Config) *s3.Client {
	loaded := aws.Config{
		Region: "auto", Credentials: credentials.NewStaticCredentialsProvider(cfg.WarArchiveAccessKeyID, cfg.WarArchiveSecretAccessKey, ""),
		HTTPClient: &http.Client{Timeout: 2 * time.Minute},
	}
	return s3.NewFromConfig(loaded, func(options *s3.Options) {
		options.BaseEndpoint = aws.String(cfg.WarArchiveEndpoint)
		options.UsePathStyle = true
	})
}

func (d *warArchiverDomain) archiveOne(ctx context.Context, app *platform.App, pool *pgxpool.Pool, client *s3.Client) (int, error) {
	packID, wars, err := claimArchivePack(ctx, pool, app.Config.WarArchivePackSize)
	if err != nil || len(wars) == 0 {
		return 0, err
	}
	builder, err := wararchive.NewPackBuilder(packID)
	if err != nil {
		return 0, err
	}
	defer builder.Close()
	stats := wararchive.NewPackStats()
	stats.CWL = wararchive.NewCWLStats()
	var rawBytes int64
	firstEnd, lastEnd := wars[0].EndTime, wars[0].EndTime
	for _, pending := range wars {
		locator, err := builder.Add(pending.ID, pending.War)
		if err != nil {
			return 0, err
		}
		rawBytes += int64(locator.RawBytes)
		if err := addPendingArchiveWarStats(&stats, pending); err != nil {
			app.Logger.Error("CWL archive statistics frame invalid", "war_id", pending.ID, "error", err)
		}
		if pending.EndTime.Before(firstEnd) {
			firstEnd = pending.EndTime
		}
		if pending.EndTime.After(lastEnd) {
			lastEnd = pending.EndTime
		}
	}
	key := wararchive.ObjectKey(packID)
	if _, err := client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(app.Config.WarArchiveBucket), Key: aws.String(key),
		Body: bytes.NewReader(builder.Bytes()), ContentType: aws.String("application/octet-stream"),
		CacheControl: aws.String("public,max-age=31536000,immutable"),
	}); err != nil {
		return 0, fmt.Errorf("upload %s: %w", key, err)
	}
	if err := primeWarArchiveCache(ctx, app.Config.WarArchiveOrigin, key); err != nil {
		app.Logger.Warn("war archive cache prime failed", "pack_id", packID, "error", err)
	}
	if err := finishArchivePack(ctx, pool, packID, wars, builder.Locators(), stats, rawBytes, int64(len(builder.Bytes())), firstEnd, lastEnd); err != nil {
		return 0, err
	}
	app.Logger.Info("war archive pack uploaded", "pack_id", packID, "wars", len(wars), "bytes", len(builder.Bytes()))
	return len(wars), nil
}

func addPendingArchiveWarStats(stats *wararchive.PackStats, pending pendingArchiveWar) error {
	stats.Add(pending.WarType, pending.War)
	if pending.WarType != "cwl" {
		return nil
	}
	if stats.CWL == nil {
		stats.CWL = wararchive.NewCWLStats()
	}
	return stats.CWL.AddWar(pending.War, pending.CWLLeagueID)
}

func primeWarArchiveCache(ctx context.Context, origin, key string) error {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, origin+"/"+key, nil)
	if err != nil {
		return err
	}
	request.Header.Set("Range", "bytes=0-0")
	client := &http.Client{Timeout: 2 * time.Minute}
	response, err := client.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusPartialContent {
		return fmt.Errorf("cache prime returned HTTP %d", response.StatusCode)
	}
	bytesRead, err := io.Copy(io.Discard, response.Body)
	if err != nil {
		return fmt.Errorf("read cache-prime byte: %w", err)
	}
	if bytesRead != 1 {
		return fmt.Errorf("cache prime returned %d bytes, want 1", bytesRead)
	}
	if status := strings.ToUpper(strings.TrimSpace(response.Header.Get("CF-Cache-Status"))); status == "BYPASS" || status == "DYNAMIC" {
		return fmt.Errorf("cache prime was not eligible for caching: %s", status)
	}
	return nil
}

func claimArchivePack(ctx context.Context, pool *pgxpool.Pool, packSize int) (int64, []pendingArchiveWar, error) {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return 0, nil, err
	}
	defer tx.Rollback(ctx)
	var packID int64
	resuming := true
	err = tx.QueryRow(ctx, `
		SELECT pack_id FROM war_archive_packs
		WHERE source = 'live' AND status = 'building'
		ORDER BY pack_id LIMIT 1 FOR UPDATE
	`).Scan(&packID)
	if errors.Is(err, pgx.ErrNoRows) {
		resuming = false
		var ready int
		if err := tx.QueryRow(ctx, `SELECT count(*) FROM (SELECT 1 FROM war_archive_pending WHERE pack_id IS NULL LIMIT $1) ready`, packSize).Scan(&ready); err != nil {
			return 0, nil, err
		}
		if ready < packSize {
			return 0, nil, nil
		}
		if err := tx.QueryRow(ctx, `INSERT INTO war_archive_packs (source, status) VALUES ('live', 'building') RETURNING pack_id`).Scan(&packID); err != nil {
			return 0, nil, err
		}
		if _, err := tx.Exec(ctx, `
			WITH selected AS (
				SELECT war_id, end_time FROM war_archive_pending
				WHERE pack_id IS NULL ORDER BY created_at, war_id
				LIMIT $2 FOR UPDATE SKIP LOCKED
			)
			UPDATE war_archive_pending pending SET pack_id = $1
			FROM selected WHERE pending.war_id = selected.war_id AND pending.end_time = selected.end_time
		`, packID, packSize); err != nil {
			return 0, nil, err
		}
	} else if err != nil {
		return 0, nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, nil, err
	}

	rows, err := pool.Query(ctx, claimArchivePackWarsSQL,
		packID)
	if err != nil {
		return 0, nil, err
	}
	defer rows.Close()
	claimed := make([]pendingArchiveWar, 0, packSize)
	for rows.Next() {
		var pending pendingArchiveWar
		var payload []byte
		var cwlLeagueID *int
		if err := rows.Scan(&pending.ID, &pending.EndTime, &pending.WarType, &payload, &cwlLeagueID); err != nil {
			return 0, nil, err
		}
		if cwlLeagueID != nil {
			pending.CWLLeagueID = *cwlLeagueID
		}
		pending.War, err = wararchive.Unmarshal(payload)
		if err != nil {
			return 0, nil, fmt.Errorf("decode pending war %d: %w", pending.ID, err)
		}
		claimed = append(claimed, pending)
	}
	if err := rows.Err(); err != nil {
		return 0, nil, err
	}
	if resuming && len(claimed) == 0 {
		return 0, nil, fmt.Errorf("live archive pack %d is building but owns no pending wars", packID)
	}
	return packID, claimed, nil
}

// A war tag belongs to exactly one stored CWL group. Ambiguous or missing group
// matches remain unknown; the archiver never substitutes a clan's current league.
const claimArchivePackWarsSQL = `
		WITH cwl_attribution AS (
			SELECT tag.value AS war_tag,
			CASE
				WHEN count(DISTINCT groups.cwl_id) = 1 THEN min(groups.cwl_league_id)
				ELSE NULL
			END AS cwl_league_id
			FROM cwl_groups AS groups
			CROSS JOIN LATERAL jsonb_array_elements(groups.rounds) AS round(value)
			CROSS JOIN LATERAL jsonb_array_elements_text(
				CASE jsonb_typeof(round.value)
					WHEN 'array' THEN round.value
					WHEN 'object' THEN COALESCE(round.value -> 'warTags', '[]'::jsonb)
					ELSE '[]'::jsonb
				END
			) AS tag(value)
			GROUP BY tag.value
		)
		SELECT pending.war_id, pending.end_time, wars.war_type, pending.payload,
			cwl_attribution.cwl_league_id
		FROM war_archive_pending AS pending
		JOIN wars ON wars.war_id = pending.war_id AND wars.end_time = pending.end_time
		LEFT JOIN cwl_attribution
		  ON wars.war_type = 'cwl'
		 AND cwl_attribution.war_tag = wars.war_tag
		WHERE pending.pack_id = $1
		ORDER BY pending.created_at, pending.war_id
	`

func finishArchivePack(ctx context.Context, pool *pgxpool.Pool, packID int64, wars []pendingArchiveWar, locators []wararchive.Locator, stats wararchive.PackStats, rawBytes, compressedBytes int64, firstEnd, lastEnd time.Time) error {
	if len(wars) != len(locators) {
		return errors.New("war archive locator count does not match claimed wars")
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, `CREATE TEMP TABLE war_archive_locator_stage (war_id integer, end_time timestamptz, pack_id bigint, archive_offset bigint, compressed_bytes integer) ON COMMIT DROP`); err != nil {
		return err
	}
	copyRows := make([][]any, 0, len(wars))
	for index, pending := range wars {
		locator := locators[index]
		copyRows = append(copyRows, []any{pending.ID, pending.EndTime, packID, locator.Offset, locator.CompressedBytes})
	}
	if _, err := tx.CopyFrom(ctx, pgx.Identifier{"war_archive_locator_stage"}, []string{"war_id", "end_time", "pack_id", "archive_offset", "compressed_bytes"}, pgx.CopyFromRows(copyRows)); err != nil {
		return err
	}
	result, err := tx.Exec(ctx, `
		UPDATE wars war SET archive_pack_id = stage.pack_id, archive_offset = stage.archive_offset, archive_compressed_bytes = stage.compressed_bytes
		FROM war_archive_locator_stage stage
		WHERE war.war_id = stage.war_id AND war.end_time = stage.end_time
	`)
	if err != nil {
		return err
	}
	if result.RowsAffected() != int64(len(wars)) {
		return fmt.Errorf("updated %d war locators, expected %d", result.RowsAffected(), len(wars))
	}
	statsJSON, err := json.Marshal(stats)
	if err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `DELETE FROM war_archive_pending WHERE pack_id = $1`, packID); err != nil {
		return err
	}
	result, err = tx.Exec(ctx, `
		UPDATE war_archive_packs SET status = 'uploaded', war_count = $2, attack_count = $3,
			raw_bytes = $4, compressed_bytes = $5, first_end_time = $6, last_end_time = $7,
			stats = $8::jsonb, uploaded_at = now()
		WHERE pack_id = $1 AND source = 'live' AND status = 'building'
	`, packID, len(wars), stats.TotalAttacks(), rawBytes, compressedBytes, firstEnd, lastEnd, statsJSON)
	if err != nil {
		return err
	}
	if result.RowsAffected() != 1 {
		return fmt.Errorf("finalized %d archive pack rows, expected 1", result.RowsAffected())
	}
	return tx.Commit(ctx)
}
