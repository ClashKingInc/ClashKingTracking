package scripts

import (
	"context"
	"encoding/json"
	"errors"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	"clashking_tracking/internal/platform"
	"clashking_tracking/models"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type mobilePushStore interface {
	Close()
	RecordDeliveryAttempt(ctx context.Context, attempt models.AdminPostDeliveryAttempt) (models.AdminPostDeliveryAttempt, error)
	DuePushRetries(ctx context.Context, now time.Time) ([]models.AdminPost, error)
	ClaimDueCampaigns(ctx context.Context, now time.Time) ([]models.NotificationCampaign, error)
	RecordCampaignDelivery(ctx context.Context, campaign models.NotificationCampaign, now time.Time, eligible, sent, skipped int, status string) error
	DuePosts(ctx context.Context, now time.Time) ([]models.AdminPost, error)
	MarkPublished(ctx context.Context, id string) (models.AdminPost, error)
	DueExpirations(ctx context.Context, now time.Time) ([]models.AdminPost, error)
	MarkExpired(ctx context.Context, id string) error
	MarkPushSent(ctx context.Context, id string) error
	DevicesForPlatforms(ctx context.Context, platforms []string, locales []string) ([]models.PushDevice, error)
}

const featureFlagColumns = `flag_key, name, description, enabled, rollout_percentage, min_app_version,
	platforms, owner_name, public_exposure, starts_at, ends_at, created_at, updated_at`

func scanFeatureFlag(scanner interface{ Scan(...any) error }) (models.AdminFeatureFlag, error) {
	var flag models.AdminFeatureFlag
	err := scanner.Scan(&flag.Key, &flag.Name, &flag.Description, &flag.Enabled, &flag.RolloutPercentage,
		&flag.MinAppVersion, &flag.Platforms, &flag.Owner, &flag.PublicExposure, &flag.StartsAt,
		&flag.EndsAt, &flag.CreatedAt, &flag.LastUpdated)
	return flag, err
}

func (s *timescaleMobilePushStore) ListFeatureFlags(ctx context.Context) ([]models.AdminFeatureFlag, error) {
	rows, err := s.pool.Query(ctx, `SELECT `+featureFlagColumns+` FROM admin_feature_flags ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	flags := []models.AdminFeatureFlag{}
	for rows.Next() {
		flag, err := scanFeatureFlag(rows)
		if err != nil {
			return nil, err
		}
		flags = append(flags, flag)
	}
	return flags, rows.Err()
}

func (s *timescaleMobilePushStore) CreateFeatureFlag(ctx context.Context, input models.AdminFeatureFlagInput) (models.AdminFeatureFlag, error) {
	key, name := "", ""
	if input.Key != nil {
		key = strings.TrimSpace(*input.Key)
	}
	if input.Name != nil {
		name = strings.TrimSpace(*input.Name)
	}
	description, owner, exposure, minVersion := "", "Product", "safe", ""
	if input.Description != nil {
		description = strings.TrimSpace(*input.Description)
	}
	if input.Owner != nil {
		owner = strings.TrimSpace(*input.Owner)
	}
	if input.PublicExposure != nil {
		exposure = *input.PublicExposure
	}
	if input.MinAppVersion != nil {
		minVersion = strings.TrimSpace(*input.MinAppVersion)
	}
	enabled, rollout := false, 0
	if input.Enabled != nil {
		enabled = *input.Enabled
	}
	if input.RolloutPercentage != nil {
		rollout = *input.RolloutPercentage
	}
	platforms := input.Platforms
	if len(platforms) == 0 {
		platforms = []string{"ios", "android"}
	}
	return scanFeatureFlag(s.pool.QueryRow(ctx, `INSERT INTO admin_feature_flags
		(flag_key, name, description, enabled, rollout_percentage, min_app_version, platforms, owner_name, public_exposure, starts_at, ends_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11) RETURNING `+featureFlagColumns,
		key, name, description, enabled, rollout, minVersion, platforms, owner, exposure, input.StartsAt, input.EndsAt))
}

func (s *timescaleMobilePushStore) UpdateFeatureFlag(ctx context.Context, key string, input models.AdminFeatureFlagInput) (models.AdminFeatureFlag, bool, error) {
	current, err := scanFeatureFlag(s.pool.QueryRow(ctx, `SELECT `+featureFlagColumns+` FROM admin_feature_flags WHERE flag_key=$1`, key))
	if errors.Is(err, pgx.ErrNoRows) {
		return models.AdminFeatureFlag{}, false, nil
	}
	if err != nil {
		return models.AdminFeatureFlag{}, false, err
	}
	if input.Name != nil {
		current.Name = strings.TrimSpace(*input.Name)
	}
	if input.Description != nil {
		current.Description = strings.TrimSpace(*input.Description)
	}
	if input.Enabled != nil {
		current.Enabled = *input.Enabled
	}
	if input.RolloutPercentage != nil {
		current.RolloutPercentage = *input.RolloutPercentage
	}
	if input.MinAppVersion != nil {
		current.MinAppVersion = strings.TrimSpace(*input.MinAppVersion)
	}
	if input.ClearMinAppVersion {
		current.MinAppVersion = ""
	}
	if input.Platforms != nil {
		current.Platforms = input.Platforms
	}
	if input.Owner != nil {
		current.Owner = strings.TrimSpace(*input.Owner)
	}
	if input.PublicExposure != nil {
		current.PublicExposure = *input.PublicExposure
	}
	if input.StartsAt != nil {
		current.StartsAt = input.StartsAt
	}
	if input.ClearStartsAt {
		current.StartsAt = nil
	}
	if input.EndsAt != nil {
		current.EndsAt = input.EndsAt
	}
	if input.ClearEndsAt {
		current.EndsAt = nil
	}
	updated, err := scanFeatureFlag(s.pool.QueryRow(ctx, `UPDATE admin_feature_flags SET
		name=$2, description=$3, enabled=$4, rollout_percentage=$5, min_app_version=$6,
		platforms=$7, owner_name=$8, public_exposure=$9, starts_at=$10, ends_at=$11, updated_at=now()
		WHERE flag_key=$1 RETURNING `+featureFlagColumns, key, current.Name, current.Description, current.Enabled,
		current.RolloutPercentage, current.MinAppVersion, current.Platforms, current.Owner, current.PublicExposure,
		current.StartsAt, current.EndsAt))
	return updated, true, err
}

func (s *timescaleMobilePushStore) AdminDashboard(ctx context.Context, days int, now time.Time) (models.AdminDashboardSnapshot, error) {
	if days < 1 {
		days = 30
	}
	if days > 365 {
		days = 365
	}
	result := models.AdminDashboardSnapshot{GeneratedAt: now, Daily: []models.AdminDeliveryDailyPoint{}, AudienceDaily: []models.AdminAudienceDailyPoint{}, AppVersions: []models.AdminDimensionCount{}, Locales: []models.AdminDimensionCount{}}

	err := s.pool.QueryRow(ctx, `SELECT
		count(*) FILTER (WHERE enabled),
		count(*) FILTER (WHERE enabled AND environment = 'production'),
		count(*) FILTER (WHERE enabled AND environment = 'sandbox'),
		count(*) FILTER (WHERE enabled AND platform = 'android'),
		count(*) FILTER (WHERE enabled AND platform = 'ios'),
		count(*) FILTER (WHERE enabled AND authorization_status IN ('authorized', 'provisional')),
		count(*) FILTER (WHERE enabled AND EXISTS (
			SELECT 1 FROM mobile_notification_preferences p
			WHERE p.user_id = mobile_push_devices.user_id
			  AND p.device_id = mobile_push_devices.device_id
			  AND p.environment = mobile_push_devices.environment
			  AND p.enabled
			  AND 'announcements' = ANY(p.enabled_types)
		)),
		count(*) FILTER (WHERE enabled AND last_seen_at >= $1::timestamptz - interval '24 hours'),
		count(*) FILTER (WHERE enabled AND last_seen_at >= $1::timestamptz - interval '7 days')
		FROM mobile_push_devices`, now).Scan(
		&result.Devices.Total, &result.Devices.Production, &result.Devices.Sandbox,
		&result.Devices.Android, &result.Devices.IOS, &result.Devices.Authorized,
		&result.Devices.OptedIn, &result.Devices.Active24H, &result.Devices.Active7D,
	)
	if err != nil {
		return result, err
	}
	_, _ = s.pool.Exec(ctx, `INSERT INTO admin_kpi_daily
		(snapshot_date, devices_total, devices_production, devices_sandbox, devices_opted_in)
		VALUES ($1::date, $2, $3, $4, $5)
		ON CONFLICT (snapshot_date) DO UPDATE SET
			devices_total=excluded.devices_total, devices_production=excluded.devices_production,
			devices_sandbox=excluded.devices_sandbox, devices_opted_in=excluded.devices_opted_in, updated_at=now()`,
		now, result.Devices.Total, result.Devices.Production, result.Devices.Sandbox, result.Devices.OptedIn)

	for _, dimension := range []struct {
		query  string
		target *[]models.AdminDimensionCount
	}{
		{`SELECT COALESCE(NULLIF(app_version, ''), 'unknown'), count(*) FROM mobile_push_devices WHERE enabled GROUP BY 1 ORDER BY 2 DESC LIMIT 8`, &result.AppVersions},
		{`SELECT lower(split_part(replace(COALESCE(NULLIF(locale, ''), 'unknown'), '_', '-'), '-', 1)), count(*) FROM mobile_push_devices WHERE enabled GROUP BY 1 ORDER BY 2 DESC LIMIT 12`, &result.Locales},
	} {
		rows, queryErr := s.pool.Query(ctx, dimension.query)
		if queryErr != nil {
			return result, queryErr
		}
		for rows.Next() {
			var item models.AdminDimensionCount
			if scanErr := rows.Scan(&item.Value, &item.Count); scanErr != nil {
				rows.Close()
				return result, scanErr
			}
			*dimension.target = append(*dimension.target, item)
		}
		rows.Close()
	}

	err = s.pool.QueryRow(ctx, `SELECT
		count(*) FILTER (WHERE status = 'live'),
		count(*) FILTER (WHERE status = 'scheduled'),
		count(*) FILTER (WHERE status = 'draft')
		FROM admin_posts`).Scan(
		&result.Content.LivePosts, &result.Content.ScheduledPosts, &result.Content.DraftPosts,
	)
	if err != nil {
		return result, err
	}

	err = s.pool.QueryRow(ctx, `SELECT
		count(*) FILTER (WHERE status = 'scheduled'),
		count(*) FILTER (WHERE trigger_type <> 'manual')
		FROM admin_notification_campaigns`).Scan(
		&result.Content.ScheduledCampaigns, &result.Content.RecurringCampaigns,
	)
	if err != nil {
		return result, err
	}

	err = s.pool.QueryRow(ctx, `WITH attempts AS (
		SELECT eligible_count, sent_count, skipped_count, status, attempted_at
		FROM admin_post_delivery_attempts
		WHERE attempted_at >= $1::timestamptz - ($2::int * interval '1 day')
		UNION ALL
		SELECT eligible_count, sent_count, skipped_count, status, attempted_at
		FROM admin_campaign_delivery_attempts
		WHERE attempted_at >= $1::timestamptz - ($2::int * interval '1 day')
	)
	SELECT count(*), COALESCE(sum(eligible_count), 0), COALESCE(sum(sent_count), 0),
		COALESCE(sum(skipped_count), 0), count(*) FILTER (WHERE status IN ('failed', 'partial')),
		max(attempted_at)
	FROM attempts`, now, days).Scan(
		&result.Delivery.Attempts, &result.Delivery.Eligible, &result.Delivery.Sent,
		&result.Delivery.Skipped, &result.Delivery.Failed, &result.Delivery.LastAttempt,
	)
	if err != nil {
		return result, err
	}
	if result.Delivery.Eligible > 0 {
		result.Delivery.SuccessRate = float64(result.Delivery.Sent) / float64(result.Delivery.Eligible) * 100
	}

	_ = s.pool.QueryRow(ctx, `SELECT min(next_send) FROM (
		SELECT send_at AS next_send
		FROM admin_notification_campaigns
		WHERE status = 'scheduled' AND trigger_type = 'manual' AND send_at > $1
		UNION ALL
		SELECT date_trunc('month', $1) + ((day_of_month - 1) * interval '1 day') + (COALESCE(send_time, '09:00')::time) AS next_send
		FROM admin_notification_campaigns
		WHERE status = 'scheduled' AND trigger_type = 'monthly'
	) due WHERE next_send > $1`, now).Scan(&result.Delivery.NextSendAt)

	rows, err := s.pool.Query(ctx, `WITH dates AS (
		SELECT generate_series(($1::date - ($2::int - 1)), $1::date, interval '1 day')::date AS day
	), attempts AS (
		SELECT attempted_at::date AS day, eligible_count, sent_count, skipped_count, status
		FROM admin_post_delivery_attempts
		WHERE attempted_at >= $1::date - ($2::int - 1)
		UNION ALL
		SELECT attempted_at::date AS day, eligible_count, sent_count, skipped_count, status
		FROM admin_campaign_delivery_attempts
		WHERE attempted_at >= $1::date - ($2::int - 1)
	)
	SELECT to_char(d.day, 'YYYY-MM-DD'), count(a.day), COALESCE(sum(a.eligible_count), 0),
		COALESCE(sum(a.sent_count), 0), COALESCE(sum(a.skipped_count), 0),
		count(a.day) FILTER (WHERE a.status IN ('failed', 'partial'))
	FROM dates d LEFT JOIN attempts a ON a.day = d.day
	GROUP BY d.day ORDER BY d.day`, now, days)
	if err != nil {
		return result, err
	}
	defer rows.Close()
	for rows.Next() {
		var point models.AdminDeliveryDailyPoint
		if err := rows.Scan(&point.Date, &point.Attempts, &point.Eligible, &point.Sent, &point.Skipped, &point.Failed); err != nil {
			return result, err
		}
		result.Daily = append(result.Daily, point)
	}
	if err := rows.Err(); err != nil {
		return result, err
	}
	audienceRows, err := s.pool.Query(ctx, `SELECT to_char(snapshot_date, 'YYYY-MM-DD'), devices_total, devices_production, devices_sandbox, devices_opted_in
		FROM admin_kpi_daily WHERE snapshot_date >= $1::date - ($2::int - 1) ORDER BY snapshot_date`, now, days)
	if err != nil {
		return result, err
	}
	defer audienceRows.Close()
	for audienceRows.Next() {
		var point models.AdminAudienceDailyPoint
		if err := audienceRows.Scan(&point.Date, &point.Total, &point.Production, &point.Sandbox, &point.OptedIn); err != nil {
			return result, err
		}
		result.AudienceDaily = append(result.AudienceDaily, point)
	}
	return result, audienceRows.Err()
}

func newMobilePushStore(ctx context.Context, app *platform.App) (mobilePushStore, error) {
	if app.Config.MockDB || app.Config.DryRun || app.Config.TimescaleURL == "" {
		return newMemoryMobilePushStore(), nil
	}
	pool, err := pgxpool.New(ctx, app.Config.TimescaleURL)
	if err != nil {
		return nil, err
	}
	return &timescaleMobilePushStore{pool: pool}, nil
}

type timescaleMobilePushStore struct {
	pool *pgxpool.Pool
}

func (s *timescaleMobilePushStore) Close() {
	if s.pool != nil {
		s.pool.Close()
	}
}

const adminPostColumns = `
	id, slug, title, summary, hero_image_url, body_blocks, translations, presentation_type,
	story_url, story_version, story_history, revision_number, show_on_home, pinned_on_home, target_route, platforms,
	dismissible, priority, status, starts_at, ends_at, also_push_on_publish,
	push_title, push_body, published_at, push_sent_at, created_by, created_at, updated_at
`

func scanAdminPost(scanner interface{ Scan(...any) error }) (models.AdminPost, error) {
	var post models.AdminPost
	var rawBlocks []byte
	var rawTranslations []byte
	if err := scanner.Scan(
		&post.ID, &post.Slug, &post.Title, &post.Summary, &post.HeroImageURL,
		&rawBlocks, &rawTranslations, &post.PresentationType, &post.StoryURL, &post.StoryVersion, &post.StoryHistory, &post.RevisionNumber, &post.ShowOnHome,
		&post.PinnedOnHome, &post.TargetRoute, &post.Platforms, &post.Dismissible,
		&post.Priority, &post.Status, &post.StartsAt, &post.EndsAt,
		&post.AlsoPushOnPublish, &post.PushTitle, &post.PushBody,
		&post.PublishedAt, &post.PushSentAt, &post.CreatedBy, &post.CreatedAt,
		&post.UpdatedAt,
	); err != nil {
		return models.AdminPost{}, err
	}
	post.BodyBlocks = []models.PostBlock{}
	_ = json.Unmarshal(rawBlocks, &post.BodyBlocks)
	post.Translations = map[string]models.AdminPostTranslation{}
	_ = json.Unmarshal(rawTranslations, &post.Translations)
	return post, nil
}

func (s *timescaleMobilePushStore) ListPosts(ctx context.Context, status string) ([]models.AdminPost, error) {
	query := "SELECT " + adminPostColumns + " FROM admin_posts"
	var rows pgx.Rows
	var err error
	if status != "" {
		query += " WHERE status = $1 ORDER BY created_at DESC"
		rows, err = s.pool.Query(ctx, query, status)
	} else {
		query += " WHERE status <> 'archived' ORDER BY created_at DESC"
		rows, err = s.pool.Query(ctx, query)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []models.AdminPost{}
	for rows.Next() {
		post, err := scanAdminPost(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, post)
	}
	return out, rows.Err()
}

func (s *timescaleMobilePushStore) GetPost(ctx context.Context, id string) (models.AdminPost, bool, error) {
	row := s.pool.QueryRow(ctx, "SELECT "+adminPostColumns+" FROM admin_posts WHERE id = $1", id)
	post, err := scanAdminPost(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return models.AdminPost{}, false, nil
	}
	if err != nil {
		return models.AdminPost{}, false, err
	}
	return post, true, nil
}

func (s *timescaleMobilePushStore) CreatePost(ctx context.Context, input models.AdminPostInput) (models.AdminPost, error) {
	title := valueOr(input.Title, "")
	status := "draft"
	if input.StartsAt != nil && input.StartsAt.After(time.Now().UTC()) {
		status = "scheduled"
	}
	blocks := input.BodyBlocks
	if blocks == nil {
		blocks = []models.PostBlock{}
	}
	rawBlocks, err := json.Marshal(blocks)
	if err != nil {
		return models.AdminPost{}, err
	}
	translations := input.Translations
	if translations == nil {
		translations = map[string]models.AdminPostTranslation{}
	}
	rawTranslations, err := json.Marshal(translations)
	if err != nil {
		return models.AdminPost{}, err
	}
	platforms := input.Platforms
	if platforms == nil {
		platforms = []string{"ios", "android", "web"}
	}

	row := s.pool.QueryRow(ctx, `
		INSERT INTO admin_posts (
			slug, title, summary, hero_image_url, body_blocks, translations, presentation_type,
			story_url, show_on_home, pinned_on_home, target_route,
			platforms, dismissible, priority, status, starts_at, ends_at,
			also_push_on_publish, push_title, push_body, created_by
		) VALUES (
			$1, $2, $3, $4, $5::jsonb, $6::jsonb, $7, $8, $9, $10, $11, $12, $13,
			$14, $15, $16, $17, $18, $19, $20, $21
		)
		RETURNING `+adminPostColumns,
		uniqueSlug(title), title, valueOr(input.Summary, ""), input.HeroImageURL,
		string(rawBlocks), string(rawTranslations), valueOr(input.PresentationType, "article"), input.StoryURL,
		valueOr(input.ShowOnHome, true), valueOr(input.PinnedOnHome, false),
		input.TargetRoute, platforms, valueOr(input.Dismissible, true),
		valueOr(input.Priority, 10), status, input.StartsAt, input.EndsAt,
		valueOr(input.AlsoPushOnPublish, false), input.PushTitle, input.PushBody,
		valueOr(input.CreatedBy, ""),
	)
	return scanAdminPost(row)
}

func (s *timescaleMobilePushStore) UpdatePost(ctx context.Context, id string, input models.AdminPostInput) (models.AdminPost, bool, error) {
	current, found, err := s.GetPost(ctx, id)
	if err != nil || !found {
		return models.AdminPost{}, found, err
	}
	snapshot, err := json.Marshal(current)
	if err != nil {
		return models.AdminPost{}, false, err
	}
	if _, err := s.pool.Exec(ctx, `
		INSERT INTO admin_post_revisions (post_id, revision_number, snapshot, created_by)
		VALUES ($1, $2, $3::jsonb, $4)
		ON CONFLICT (post_id, revision_number) DO NOTHING`,
		id, current.RevisionNumber, string(snapshot), valueOr(input.CreatedBy, current.CreatedBy),
	); err != nil {
		return models.AdminPost{}, false, err
	}
	merged := mergeAdminPost(current, input)
	merged.RevisionNumber = current.RevisionNumber + 1
	if !sameOptionalString(current.StoryURL, merged.StoryURL) && merged.StoryURL != nil {
		merged.StoryVersion = max(current.StoryVersion+1, 1)
		if current.StoryURL != nil {
			merged.StoryHistory = append(append([]string{}, current.StoryHistory...), *current.StoryURL)
		}
	}
	if merged.EndsAt != nil && merged.StartsAt != nil && !merged.EndsAt.After(*merged.StartsAt) {
		return models.AdminPost{}, false, errors.New("ends_at must be after starts_at")
	}
	if err := validatePostPresentation(
		merged.PresentationType,
		merged.StoryURL,
		merged.ShowOnHome,
		merged.PinnedOnHome,
	); err != nil {
		return models.AdminPost{}, false, err
	}
	rawBlocks, err := json.Marshal(merged.BodyBlocks)
	if err != nil {
		return models.AdminPost{}, false, err
	}
	rawTranslations, err := json.Marshal(merged.Translations)
	if err != nil {
		return models.AdminPost{}, false, err
	}
	status := merged.Status
	if merged.StartsAt == nil && current.Status == "scheduled" {
		status = "draft"
	} else if merged.StartsAt != nil && merged.StartsAt.After(time.Now().UTC()) {
		// Moving an already-live post into the future must temporarily unpublish
		// it. The scheduler will make it live again when starts_at is reached.
		status = "scheduled"
	} else if current.Status == "draft" {
		// Keep due scheduled posts eligible for the scheduler. Editing a post
		// after starts_at must not silently remove it from DuePosts.
		status = "draft"
	}
	row := s.pool.QueryRow(ctx, `
		UPDATE admin_posts SET
			title = $2, summary = $3, hero_image_url = $4, body_blocks = $5::jsonb, translations = $6::jsonb,
			presentation_type = $7, story_url = $8, story_version = $9, story_history = $10,
			revision_number = $11, show_on_home = $12, pinned_on_home = $13, target_route = $14, platforms = $15,
			dismissible = $16, priority = $17, starts_at = $18, ends_at = $19,
			also_push_on_publish = $20, push_title = $21, push_body = $22,
			status = $23, updated_at = now()
		WHERE id = $1
		RETURNING `+adminPostColumns,
		id, merged.Title, merged.Summary, merged.HeroImageURL, string(rawBlocks), string(rawTranslations),
		merged.PresentationType, merged.StoryURL, merged.StoryVersion, merged.StoryHistory, merged.RevisionNumber, merged.ShowOnHome,
		merged.PinnedOnHome, merged.TargetRoute, merged.Platforms,
		merged.Dismissible, merged.Priority, merged.StartsAt, merged.EndsAt,
		merged.AlsoPushOnPublish, merged.PushTitle, merged.PushBody, status,
	)
	post, err := scanAdminPost(row)
	if err != nil {
		return models.AdminPost{}, false, err
	}
	return post, true, nil
}

func sameOptionalString(a, b *string) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return *a == *b
}

func mergeAdminPost(current models.AdminPost, input models.AdminPostInput) models.AdminPost {
	if input.Title != nil {
		current.Title = *input.Title
	}
	if input.Summary != nil {
		current.Summary = *input.Summary
	}
	if input.ClearHeroImageURL {
		current.HeroImageURL = nil
	} else if input.HeroImageURL != nil {
		current.HeroImageURL = input.HeroImageURL
	}
	if input.BodyBlocks != nil {
		current.BodyBlocks = input.BodyBlocks
	}
	if input.Translations != nil {
		current.Translations = input.Translations
	}
	if input.PresentationType != nil {
		current.PresentationType = *input.PresentationType
	}
	if input.ClearStoryURL {
		current.StoryURL = nil
	} else if input.StoryURL != nil {
		current.StoryURL = input.StoryURL
	}
	if input.ShowOnHome != nil {
		current.ShowOnHome = *input.ShowOnHome
	}
	if input.PinnedOnHome != nil {
		current.PinnedOnHome = *input.PinnedOnHome
	}
	if input.ClearTargetRoute {
		current.TargetRoute = nil
	} else if input.TargetRoute != nil {
		current.TargetRoute = input.TargetRoute
	}
	if input.Platforms != nil {
		current.Platforms = input.Platforms
	}
	if input.Dismissible != nil {
		current.Dismissible = *input.Dismissible
	}
	if input.Priority != nil {
		current.Priority = *input.Priority
	}
	if input.ClearStartsAt {
		current.StartsAt = nil
	} else if input.StartsAt != nil {
		current.StartsAt = input.StartsAt
	}
	if input.ClearEndsAt {
		current.EndsAt = nil
	} else if input.EndsAt != nil {
		current.EndsAt = input.EndsAt
	}
	if input.AlsoPushOnPublish != nil {
		current.AlsoPushOnPublish = *input.AlsoPushOnPublish
	}
	if input.ClearPushTitle {
		current.PushTitle = nil
	} else if input.PushTitle != nil {
		current.PushTitle = input.PushTitle
	}
	if input.ClearPushBody {
		current.PushBody = nil
	} else if input.PushBody != nil {
		current.PushBody = input.PushBody
	}
	return current
}

func (s *timescaleMobilePushStore) ListPostRevisions(ctx context.Context, id string) ([]models.AdminPostRevision, error) {
	rows, err := s.pool.Query(ctx, `SELECT id, post_id, revision_number, snapshot, created_by, created_at
		FROM admin_post_revisions WHERE post_id = $1 ORDER BY revision_number DESC`, id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []models.AdminPostRevision{}
	for rows.Next() {
		var revision models.AdminPostRevision
		if err := rows.Scan(&revision.ID, &revision.PostID, &revision.RevisionNumber, &revision.Snapshot, &revision.CreatedBy, &revision.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, revision)
	}
	return out, rows.Err()
}

func (s *timescaleMobilePushStore) GetPostRevision(ctx context.Context, id string, revisionNumber int) (models.AdminPostRevision, bool, error) {
	var revision models.AdminPostRevision
	err := s.pool.QueryRow(ctx, `SELECT id, post_id, revision_number, snapshot, created_by, created_at
		FROM admin_post_revisions WHERE post_id = $1 AND revision_number = $2`, id, revisionNumber).
		Scan(&revision.ID, &revision.PostID, &revision.RevisionNumber, &revision.Snapshot, &revision.CreatedBy, &revision.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return models.AdminPostRevision{}, false, nil
	}
	return revision, err == nil, err
}

func (s *timescaleMobilePushStore) RecordDeliveryAttempt(ctx context.Context, attempt models.AdminPostDeliveryAttempt) (models.AdminPostDeliveryAttempt, error) {
	err := s.pool.QueryRow(ctx, `INSERT INTO admin_post_delivery_attempts
		(post_id, attempt_number, trigger, eligible_count, sent_count, skipped_count, status, error_summary)
		VALUES ($1, COALESCE((SELECT max(attempt_number) + 1 FROM admin_post_delivery_attempts WHERE post_id = $1), 1), $2, $3, $4, $5, $6, $7)
		RETURNING id, post_id, attempt_number, trigger, eligible_count, sent_count, skipped_count, status, error_summary, attempted_at`,
		attempt.PostID, attempt.Trigger, attempt.EligibleCount, attempt.SentCount, attempt.SkippedCount, attempt.Status, attempt.ErrorSummary).
		Scan(&attempt.ID, &attempt.PostID, &attempt.AttemptNumber, &attempt.Trigger, &attempt.EligibleCount, &attempt.SentCount, &attempt.SkippedCount, &attempt.Status, &attempt.ErrorSummary, &attempt.AttemptedAt)
	return attempt, err
}

func (s *timescaleMobilePushStore) ListDeliveryAttempts(ctx context.Context, id string) ([]models.AdminPostDeliveryAttempt, error) {
	rows, err := s.pool.Query(ctx, `SELECT id, post_id, attempt_number, trigger, eligible_count, sent_count, skipped_count, status, error_summary, attempted_at
		FROM admin_post_delivery_attempts WHERE post_id = $1 ORDER BY attempt_number DESC`, id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []models.AdminPostDeliveryAttempt{}
	for rows.Next() {
		var attempt models.AdminPostDeliveryAttempt
		if err := rows.Scan(&attempt.ID, &attempt.PostID, &attempt.AttemptNumber, &attempt.Trigger, &attempt.EligibleCount, &attempt.SentCount, &attempt.SkippedCount, &attempt.Status, &attempt.ErrorSummary, &attempt.AttemptedAt); err != nil {
			return nil, err
		}
		out = append(out, attempt)
	}
	return out, rows.Err()
}

func (s *timescaleMobilePushStore) DuePushRetries(ctx context.Context, now time.Time) ([]models.AdminPost, error) {
	rows, err := s.pool.Query(ctx, `SELECT `+adminPostColumns+` FROM admin_posts p
		WHERE p.status = 'live' AND p.also_push_on_publish = true AND p.push_sent_at IS NULL
		  AND EXISTS (
			SELECT 1 FROM admin_post_delivery_attempts a WHERE a.post_id = p.id
			  AND a.attempt_number = (SELECT max(a2.attempt_number) FROM admin_post_delivery_attempts a2 WHERE a2.post_id = p.id)
			  AND a.status IN ('failed', 'partial') AND a.attempt_number < 3 AND a.attempted_at <= $1 - interval '2 minutes'
		  )`, now)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []models.AdminPost{}
	for rows.Next() {
		post, err := scanAdminPost(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, post)
	}
	return out, rows.Err()
}

const campaignColumns = `id, campaign_key, title, body, target_route, platforms, target_locales, translations, status, trigger_type, day_of_month, send_at, send_time, last_sent_at, created_by, created_at, updated_at`

func scanCampaign(scanner interface{ Scan(...any) error }) (models.NotificationCampaign, error) {
	var campaign models.NotificationCampaign
	var rawTranslations []byte
	err := scanner.Scan(&campaign.ID, &campaign.Key, &campaign.Title, &campaign.Body, &campaign.TargetRoute, &campaign.Platforms, &campaign.TargetLocales, &rawTranslations, &campaign.Status, &campaign.TriggerType, &campaign.DayOfMonth, &campaign.SendAt, &campaign.SendTime, &campaign.LastSentAt, &campaign.CreatedBy, &campaign.CreatedAt, &campaign.UpdatedAt)
	campaign.Translations = map[string]models.NotificationCampaignTranslation{}
	_ = json.Unmarshal(rawTranslations, &campaign.Translations)
	return campaign, err
}

func (s *timescaleMobilePushStore) ListCampaigns(ctx context.Context) ([]models.NotificationCampaign, error) {
	rows, err := s.pool.Query(ctx, `SELECT `+campaignColumns+` FROM admin_notification_campaigns ORDER BY created_at DESC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []models.NotificationCampaign{}
	for rows.Next() {
		campaign, err := scanCampaign(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, campaign)
	}
	return out, rows.Err()
}

func (s *timescaleMobilePushStore) CreateCampaign(ctx context.Context, input models.NotificationCampaignInput) (models.NotificationCampaign, error) {
	campaign := mergeCampaign(models.NotificationCampaign{
		Key:          uniqueSlug(valueOr(input.Title, "campaign")),
		Status:       "draft",
		TriggerType:  "manual",
		Platforms:    []string{"ios", "android", "web"},
		CreatedBy:    valueOr(input.CreatedBy, ""),
		Translations: map[string]models.NotificationCampaignTranslation{},
	}, input)
	rawTranslations, err := json.Marshal(campaign.Translations)
	if err != nil {
		return models.NotificationCampaign{}, err
	}
	row := s.pool.QueryRow(ctx, `
		INSERT INTO admin_notification_campaigns (
			campaign_key, title, body, target_route, platforms, target_locales, translations, status,
			trigger_type, day_of_month, send_at, send_time, created_by
		) VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, $8, $9, $10, $11, $12, $13)
		RETURNING `+campaignColumns,
		campaign.Key, campaign.Title, campaign.Body, campaign.TargetRoute,
		campaign.Platforms, campaign.TargetLocales, string(rawTranslations), campaign.Status, campaign.TriggerType,
		campaign.DayOfMonth, campaign.SendAt, campaign.SendTime, campaign.CreatedBy,
	)
	return scanCampaign(row)
}

func (s *timescaleMobilePushStore) UpdateCampaign(ctx context.Context, id string, input models.NotificationCampaignInput) (models.NotificationCampaign, bool, error) {
	current, found, err := s.GetCampaign(ctx, id)
	if err != nil || !found {
		return models.NotificationCampaign{}, found, err
	}
	merged := mergeCampaign(current, input)
	rawTranslations, err := json.Marshal(merged.Translations)
	if err != nil {
		return models.NotificationCampaign{}, false, err
	}
	row := s.pool.QueryRow(ctx, `
		UPDATE admin_notification_campaigns SET
			title = $2, body = $3, target_route = $4, platforms = $5, target_locales = $6, translations = $7::jsonb,
			status = $8, trigger_type = $9, day_of_month = $10, send_at = $11, send_time = $12,
			updated_at = now()
		WHERE id = $1
		RETURNING `+campaignColumns,
		id, merged.Title, merged.Body, merged.TargetRoute, merged.Platforms,
		merged.TargetLocales, string(rawTranslations), merged.Status, merged.TriggerType, merged.DayOfMonth, merged.SendAt, merged.SendTime,
	)
	campaign, err := scanCampaign(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return models.NotificationCampaign{}, false, nil
	}
	return campaign, err == nil, err
}

func (s *timescaleMobilePushStore) GetCampaign(ctx context.Context, id string) (models.NotificationCampaign, bool, error) {
	row := s.pool.QueryRow(ctx, `SELECT `+campaignColumns+` FROM admin_notification_campaigns WHERE id = $1`, id)
	campaign, err := scanCampaign(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return models.NotificationCampaign{}, false, nil
	}
	return campaign, err == nil, err
}

func mergeCampaign(current models.NotificationCampaign, input models.NotificationCampaignInput) models.NotificationCampaign {
	if input.Title != nil {
		current.Title = *input.Title
	}
	if input.Body != nil {
		current.Body = *input.Body
	}
	if input.ClearTargetRoute {
		current.TargetRoute = nil
	} else if input.TargetRoute != nil {
		current.TargetRoute = input.TargetRoute
	}
	if input.Platforms != nil {
		current.Platforms = input.Platforms
	}
	if input.TargetLocales != nil {
		current.TargetLocales = normalizeCampaignLocales(input.TargetLocales)
	}
	if input.Translations != nil {
		current.Translations = input.Translations
	}
	if input.Status != nil {
		current.Status = *input.Status
	}
	if input.TriggerType != nil {
		current.TriggerType = *input.TriggerType
	}
	if input.ClearDayOfMonth {
		current.DayOfMonth = nil
	} else if input.DayOfMonth != nil {
		current.DayOfMonth = input.DayOfMonth
	}
	if input.ClearSendAt {
		current.SendAt = nil
	} else if input.SendAt != nil {
		current.SendAt = input.SendAt
	}
	if input.ClearSendTime {
		current.SendTime = nil
	} else if input.SendTime != nil {
		sendTime := strings.TrimSpace(*input.SendTime)
		current.SendTime = &sendTime
	}
	if input.CreatedBy != nil {
		current.CreatedBy = *input.CreatedBy
	}
	if current.TriggerType == "monthly" {
		current.SendAt = nil
		if current.DayOfMonth == nil {
			day := 1
			current.DayOfMonth = &day
		}
		if current.SendTime == nil || strings.TrimSpace(*current.SendTime) == "" {
			sendTime := "09:00"
			current.SendTime = &sendTime
		}
	} else {
		current.DayOfMonth = nil
		current.SendTime = nil
	}
	return current
}

func normalizeCampaignLocales(values []string) []string {
	out := []string{}
	seen := map[string]bool{}
	for _, value := range values {
		locale := normalizeCampaignLocale(value)
		if locale == "" || seen[locale] {
			continue
		}
		seen[locale] = true
		out = append(out, locale)
	}
	return out
}

func normalizeCampaignLocale(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	value = strings.ReplaceAll(value, "_", "-")
	if cut, _, ok := strings.Cut(value, "-"); ok {
		value = cut
	}
	if len(value) != 2 {
		return ""
	}
	return value
}

func (s *timescaleMobilePushStore) ClaimDueCampaigns(ctx context.Context, now time.Time) ([]models.NotificationCampaign, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	rows, err := tx.Query(ctx, `SELECT `+campaignColumns+` FROM admin_notification_campaigns
		WHERE status = 'scheduled' AND (
			(trigger_type = 'manual' AND send_at IS NOT NULL AND send_at <= $1) OR
			(trigger_type = 'monthly' AND day_of_month <= extract(day from $1)::integer
			 AND (last_sent_at IS NULL OR date_trunc('month', last_sent_at) < date_trunc('month', $1))
			 AND (
				day_of_month < extract(day from $1)::integer
				OR send_time IS NULL
				OR send_time <= to_char($1 AT TIME ZONE 'UTC', 'HH24:MI')
			 ))
		) FOR UPDATE SKIP LOCKED`, now)
	if err != nil {
		return nil, err
	}
	due := []models.NotificationCampaign{}
	for rows.Next() {
		campaign, err := scanCampaign(rows)
		if err != nil {
			rows.Close()
			return nil, err
		}
		due = append(due, campaign)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	rows.Close()

	claimed := make([]models.NotificationCampaign, 0, len(due))
	for _, campaign := range due {
		tag, err := tx.Exec(ctx, `INSERT INTO admin_campaign_delivery_attempts
			(campaign_id, scheduled_for, eligible_count, sent_count, skipped_count, status, attempted_at)
			VALUES ($1, $2::date, 0, 0, 0, 'processing', $3)
			ON CONFLICT (campaign_id, scheduled_for) DO UPDATE SET status='processing', attempted_at=$3
			WHERE admin_campaign_delivery_attempts.status IN ('failed', 'partial', 'processing')
			  AND admin_campaign_delivery_attempts.attempted_at <= $3 - interval '5 minutes'`,
			campaign.ID, campaignScheduledFor(campaign, now), now)
		if err != nil {
			return nil, err
		}
		if tag.RowsAffected() > 0 {
			claimed = append(claimed, campaign)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return claimed, nil
}

func (s *timescaleMobilePushStore) RecordCampaignDelivery(ctx context.Context, campaign models.NotificationCampaign, now time.Time, eligible, sent, skipped int, status string) error {
	_, err := s.pool.Exec(ctx, `INSERT INTO admin_campaign_delivery_attempts
		(campaign_id, scheduled_for, eligible_count, sent_count, skipped_count, status, attempted_at)
		VALUES ($1, $2::date, $3, $4, $5, $6, $7)
		ON CONFLICT (campaign_id, scheduled_for) DO UPDATE SET eligible_count=$3, sent_count=$4,
			skipped_count=$5, status=$6, attempted_at=$7`, campaign.ID, campaignScheduledFor(campaign, now), eligible, sent, skipped, status, now)
	if err != nil {
		return err
	}
	if status != "sent" && status != "no_audience" {
		return nil
	}
	if campaign.TriggerType == "monthly" {
		_, err = s.pool.Exec(ctx, `UPDATE admin_notification_campaigns SET last_sent_at = $2, updated_at = now() WHERE id = $1`, campaign.ID, now)
	} else {
		_, err = s.pool.Exec(ctx, `UPDATE admin_notification_campaigns SET last_sent_at = $2, status = 'sent', updated_at = now() WHERE id = $1`, campaign.ID, now)
	}
	return err
}

func campaignScheduledFor(campaign models.NotificationCampaign, now time.Time) time.Time {
	if campaign.TriggerType == "manual" && campaign.SendAt != nil {
		return campaign.SendAt.UTC()
	}
	day := now.UTC().Day()
	if campaign.DayOfMonth != nil {
		day = *campaign.DayOfMonth
	}
	return time.Date(now.UTC().Year(), now.UTC().Month(), day, 0, 0, 0, 0, time.UTC)
}

func (s *timescaleMobilePushStore) ArchivePost(ctx context.Context, id string) (bool, error) {
	tag, err := s.pool.Exec(ctx, "UPDATE admin_posts SET status = 'archived', updated_at = now() WHERE id = $1", id)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() > 0, nil
}

func (s *timescaleMobilePushStore) DuePosts(ctx context.Context, now time.Time) ([]models.AdminPost, error) {
	rows, err := s.pool.Query(ctx, "SELECT "+adminPostColumns+" FROM admin_posts WHERE status = 'scheduled' AND starts_at <= $1 AND (ends_at IS NULL OR ends_at > $1)", now)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []models.AdminPost{}
	for rows.Next() {
		post, err := scanAdminPost(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, post)
	}
	return out, rows.Err()
}

func (s *timescaleMobilePushStore) MarkPublished(ctx context.Context, id string) (models.AdminPost, error) {
	row := s.pool.QueryRow(ctx, `
		UPDATE admin_posts SET status = 'live', published_at = now(), updated_at = now()
		WHERE id = $1 AND status IN ('draft', 'scheduled')
		RETURNING `+adminPostColumns, id)
	return scanAdminPost(row)
}

func (s *timescaleMobilePushStore) DueExpirations(ctx context.Context, now time.Time) ([]models.AdminPost, error) {
	rows, err := s.pool.Query(ctx, "SELECT "+adminPostColumns+" FROM admin_posts WHERE status = 'live' AND ends_at IS NOT NULL AND ends_at <= $1", now)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []models.AdminPost{}
	for rows.Next() {
		post, err := scanAdminPost(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, post)
	}
	return out, rows.Err()
}

func (s *timescaleMobilePushStore) MarkExpired(ctx context.Context, id string) error {
	_, err := s.pool.Exec(ctx, "UPDATE admin_posts SET status = 'expired', updated_at = now() WHERE id = $1", id)
	return err
}

func (s *timescaleMobilePushStore) MarkPushSent(ctx context.Context, id string) error {
	_, err := s.pool.Exec(ctx, "UPDATE admin_posts SET push_sent_at = now(), updated_at = now() WHERE id = $1", id)
	return err
}

func (s *timescaleMobilePushStore) PublicPosts(ctx context.Context, now time.Time) ([]models.AdminPost, error) {
	rows, err := s.pool.Query(ctx, "SELECT "+adminPostColumns+` FROM admin_posts
		WHERE status = 'live' AND (ends_at IS NULL OR ends_at > $1)
		ORDER BY priority DESC, published_at DESC`, now)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []models.AdminPost{}
	for rows.Next() {
		post, err := scanAdminPost(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, post)
	}
	return out, rows.Err()
}

func (s *timescaleMobilePushStore) DevicesForPlatforms(ctx context.Context, platforms []string, locales []string) ([]models.PushDevice, error) {
	locales = normalizeCampaignLocales(locales)
	rows, err := s.pool.Query(ctx, `
		SELECT d.user_id, d.device_id, d.platform, d.provider, d.environment, d.token_ciphertext,
			lower(split_part(replace(coalesce(nullif(p.locale, ''), nullif(d.locale, ''), 'en'), '_', '-'), '-', 1))
		FROM mobile_push_devices d
		JOIN mobile_notification_preferences p
		  ON p.user_id = d.user_id
		 AND p.device_id = d.device_id
		 AND p.environment = d.environment
		WHERE d.enabled = true
		  AND d.authorization_status IN ('authorized', 'provisional')
		  AND d.platform = ANY($1)
		  AND p.enabled = true
		  AND 'announcements' = ANY(p.enabled_types)
		  AND (
			cardinality($2::text[]) = 0
			OR lower(split_part(replace(coalesce(nullif(p.locale, ''), nullif(d.locale, ''), ''), '_', '-'), '-', 1)) = ANY($2)
		  )
	`, platforms, locales)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []models.PushDevice{}
	for rows.Next() {
		var device models.PushDevice
		if err := rows.Scan(
			&device.UserID, &device.DeviceID, &device.Platform,
			&device.Provider, &device.Environment, &device.TokenCiphertext, &device.Locale,
		); err != nil {
			return nil, err
		}
		out = append(out, device)
	}
	return out, rows.Err()
}

func (s *timescaleMobilePushStore) AudienceCount(ctx context.Context, platforms []string, locales []string) (int, error) {
	locales = normalizeCampaignLocales(locales)
	var count int
	err := s.pool.QueryRow(ctx, `
		SELECT count(*)
		FROM mobile_push_devices d
		JOIN mobile_notification_preferences p
		  ON p.user_id = d.user_id
		 AND p.device_id = d.device_id
		 AND p.environment = d.environment
		WHERE d.enabled = true
		  AND d.authorization_status IN ('authorized', 'provisional')
		  AND d.platform = ANY($1)
		  AND p.enabled = true
		  AND 'announcements' = ANY(p.enabled_types)
		  AND (
			cardinality($2::text[]) = 0
			OR lower(split_part(replace(coalesce(nullif(p.locale, ''), nullif(d.locale, ''), ''), '_', '-'), '-', 1)) = ANY($2)
		  )
	`, platforms, locales).Scan(&count)
	return count, err
}

var nonSlugChars = regexp.MustCompile(`[^a-z0-9]+`)

func uniqueSlug(title string) string {
	base := strings.Trim(nonSlugChars.ReplaceAllString(strings.ToLower(title), "-"), "-")
	if base == "" {
		base = "post"
	}
	return base + "-" + strconv.FormatInt(time.Now().UnixNano()%100000, 36)
}

func valueOr[T any](value *T, fallback T) T {
	if value == nil {
		return fallback
	}
	return *value
}

func validatePostPresentation(presentationType string, storyURL *string, showOnHome, pinnedOnHome bool) error {
	if presentationType != "article" && presentationType != "story" {
		return errors.New("presentation_type must be article or story")
	}
	if pinnedOnHome && !showOnHome {
		return errors.New("a pinned post must be shown on home")
	}
	if presentationType != "story" {
		return nil
	}
	if storyURL == nil || strings.TrimSpace(*storyURL) == "" {
		return errors.New("story_url is required for a story post")
	}
	parsed, err := url.Parse(strings.TrimSpace(*storyURL))
	if err != nil || parsed.Scheme != "https" || parsed.Host == "" {
		return errors.New("story_url must be a valid HTTPS URL")
	}
	return nil
}

// memoryMobilePushStore is the MockDB/DryRun worker fallback used for local
// iteration without Postgres.
type memoryMobilePushStore struct {
	posts            map[string]models.AdminPost
	revisions        map[string][]models.AdminPostRevision
	attempts         map[string][]models.AdminPostDeliveryAttempt
	campaigns        map[string]models.NotificationCampaign
	campaignClaims   map[string]time.Time
	campaignStatuses map[string]string
}

func newMemoryMobilePushStore() *memoryMobilePushStore {
	store := &memoryMobilePushStore{
		posts: map[string]models.AdminPost{}, revisions: map[string][]models.AdminPostRevision{}, attempts: map[string][]models.AdminPostDeliveryAttempt{}, campaigns: map[string]models.NotificationCampaign{}, campaignClaims: map[string]time.Time{}, campaignStatuses: map[string]string{},
	}
	now := time.Now().UTC()
	day := 1
	sendTime := "09:00"
	route := "/settings/support"
	store.campaigns["monthly-support"] = models.NotificationCampaign{ID: "monthly-support", Key: "monthly-support", Title: "New Season is Live", Body: "A new season has started. If you're getting the Gold Pass, consider using creator code ClashKing.", TargetRoute: &route, Platforms: []string{"ios", "android", "web"}, Status: "scheduled", TriggerType: "monthly", DayOfMonth: &day, SendTime: &sendTime, LastSentAt: &now, CreatedAt: now, UpdatedAt: now}
	return store
}

func (s *memoryMobilePushStore) AdminDashboard(_ context.Context, days int, now time.Time) (models.AdminDashboardSnapshot, error) {
	if days < 1 {
		days = 30
	}
	result := models.AdminDashboardSnapshot{GeneratedAt: now, Daily: []models.AdminDeliveryDailyPoint{}, AudienceDaily: []models.AdminAudienceDailyPoint{}, AppVersions: []models.AdminDimensionCount{}, Locales: []models.AdminDimensionCount{}}
	for _, post := range s.posts {
		switch post.Status {
		case "live":
			result.Content.LivePosts++
		case "scheduled":
			result.Content.ScheduledPosts++
		case "draft":
			result.Content.DraftPosts++
		}
	}
	for _, campaign := range s.campaigns {
		if campaign.Status == "scheduled" {
			result.Content.ScheduledCampaigns++
		}
		if campaign.TriggerType != "manual" {
			result.Content.RecurringCampaigns++
		}
	}
	for offset := days - 1; offset >= 0; offset-- {
		result.Daily = append(result.Daily, models.AdminDeliveryDailyPoint{Date: now.AddDate(0, 0, -offset).Format("2006-01-02")})
	}
	return result, nil
}

func (s *memoryMobilePushStore) ListFeatureFlags(context.Context) ([]models.AdminFeatureFlag, error) {
	return []models.AdminFeatureFlag{}, nil
}
func (s *memoryMobilePushStore) CreateFeatureFlag(_ context.Context, input models.AdminFeatureFlagInput) (models.AdminFeatureFlag, error) {
	now := time.Now().UTC()
	flag := models.AdminFeatureFlag{CreatedAt: now, LastUpdated: now}
	if input.Key != nil {
		flag.Key = *input.Key
	}
	if input.Name != nil {
		flag.Name = *input.Name
	}
	return flag, nil
}
func (s *memoryMobilePushStore) UpdateFeatureFlag(context.Context, string, models.AdminFeatureFlagInput) (models.AdminFeatureFlag, bool, error) {
	return models.AdminFeatureFlag{}, false, nil
}

func (s *memoryMobilePushStore) Close() {}

func (s *memoryMobilePushStore) ListPosts(_ context.Context, status string) ([]models.AdminPost, error) {
	out := make([]models.AdminPost, 0, len(s.posts))
	for _, post := range s.posts {
		if status != "" && post.Status != status {
			continue
		}
		if status == "" && post.Status == "archived" {
			continue
		}
		out = append(out, post)
	}
	return out, nil
}

func (s *memoryMobilePushStore) GetPost(_ context.Context, id string) (models.AdminPost, bool, error) {
	post, ok := s.posts[id]
	return post, ok, nil
}

func (s *memoryMobilePushStore) CreatePost(_ context.Context, input models.AdminPostInput) (models.AdminPost, error) {
	id := uniqueSlug(valueOr(input.Title, "post"))
	now := time.Now().UTC()
	post := mergeAdminPost(models.AdminPost{
		ID: id, Slug: id, Status: "draft", Platforms: []string{"ios", "android", "web"},
		Dismissible: true, Priority: 10, BodyBlocks: []models.PostBlock{},
		PresentationType: "article", StoryVersion: 1, StoryHistory: []string{}, RevisionNumber: 1, ShowOnHome: true,
		CreatedAt: now, UpdatedAt: now,
	}, input)
	if post.StartsAt != nil && post.StartsAt.After(now) {
		post.Status = "scheduled"
	}
	s.posts[id] = post
	return post, nil
}

func (s *memoryMobilePushStore) UpdatePost(_ context.Context, id string, input models.AdminPostInput) (models.AdminPost, bool, error) {
	post, ok := s.posts[id]
	if !ok {
		return models.AdminPost{}, false, nil
	}
	snapshot, _ := json.Marshal(post)
	s.revisions[id] = append(s.revisions[id], models.AdminPostRevision{ID: uniqueSlug("revision"), PostID: id, RevisionNumber: post.RevisionNumber, Snapshot: snapshot, CreatedBy: valueOr(input.CreatedBy, post.CreatedBy), CreatedAt: time.Now().UTC()})
	post = mergeAdminPost(post, input)
	post.RevisionNumber++
	if !sameOptionalString(s.posts[id].StoryURL, post.StoryURL) && post.StoryURL != nil {
		post.StoryVersion = max(s.posts[id].StoryVersion+1, 1)
		if s.posts[id].StoryURL != nil {
			post.StoryHistory = append(append([]string{}, s.posts[id].StoryHistory...), *s.posts[id].StoryURL)
		}
	}
	if post.StartsAt == nil && s.posts[id].Status == "scheduled" {
		post.Status = "draft"
	} else if post.StartsAt != nil && post.StartsAt.After(time.Now().UTC()) {
		post.Status = "scheduled"
	} else if post.Status == "draft" {
		post.Status = "draft"
	}
	post.UpdatedAt = time.Now().UTC()
	s.posts[id] = post
	return post, true, nil
}

func (s *memoryMobilePushStore) ListPostRevisions(_ context.Context, id string) ([]models.AdminPostRevision, error) {
	revisions := append([]models.AdminPostRevision{}, s.revisions[id]...)
	for left, right := 0, len(revisions)-1; left < right; left, right = left+1, right-1 {
		revisions[left], revisions[right] = revisions[right], revisions[left]
	}
	return revisions, nil
}

func (s *memoryMobilePushStore) GetPostRevision(_ context.Context, id string, revisionNumber int) (models.AdminPostRevision, bool, error) {
	for _, revision := range s.revisions[id] {
		if revision.RevisionNumber == revisionNumber {
			return revision, true, nil
		}
	}
	return models.AdminPostRevision{}, false, nil
}

func (s *memoryMobilePushStore) RecordDeliveryAttempt(_ context.Context, attempt models.AdminPostDeliveryAttempt) (models.AdminPostDeliveryAttempt, error) {
	attempt.ID = uniqueSlug("attempt")
	attempt.AttemptNumber = len(s.attempts[attempt.PostID]) + 1
	attempt.AttemptedAt = time.Now().UTC()
	s.attempts[attempt.PostID] = append(s.attempts[attempt.PostID], attempt)
	return attempt, nil
}

func (s *memoryMobilePushStore) ListDeliveryAttempts(_ context.Context, id string) ([]models.AdminPostDeliveryAttempt, error) {
	attempts := append([]models.AdminPostDeliveryAttempt{}, s.attempts[id]...)
	for left, right := 0, len(attempts)-1; left < right; left, right = left+1, right-1 {
		attempts[left], attempts[right] = attempts[right], attempts[left]
	}
	return attempts, nil
}

func (s *memoryMobilePushStore) DuePushRetries(_ context.Context, now time.Time) ([]models.AdminPost, error) {
	out := []models.AdminPost{}
	for id, attempts := range s.attempts {
		if len(attempts) == 0 {
			continue
		}
		last := attempts[len(attempts)-1]
		post := s.posts[id]
		if post.Status == "live" && post.AlsoPushOnPublish && post.PushSentAt == nil && (last.Status == "failed" || last.Status == "partial") && last.AttemptNumber < 3 && !last.AttemptedAt.After(now.Add(-2*time.Minute)) {
			out = append(out, post)
		}
	}
	return out, nil
}

func (s *memoryMobilePushStore) ListCampaigns(context.Context) ([]models.NotificationCampaign, error) {
	out := make([]models.NotificationCampaign, 0, len(s.campaigns))
	for _, campaign := range s.campaigns {
		out = append(out, campaign)
	}
	return out, nil
}

func (s *memoryMobilePushStore) CreateCampaign(_ context.Context, input models.NotificationCampaignInput) (models.NotificationCampaign, error) {
	now := time.Now().UTC()
	campaign := mergeCampaign(models.NotificationCampaign{
		ID:          uniqueSlug(valueOr(input.Title, "campaign")),
		Key:         uniqueSlug(valueOr(input.Title, "campaign")),
		Status:      "draft",
		TriggerType: "manual",
		Platforms:   []string{"ios", "android", "web"},
		CreatedAt:   now,
		UpdatedAt:   now,
	}, input)
	s.campaigns[campaign.ID] = campaign
	return campaign, nil
}

func (s *memoryMobilePushStore) UpdateCampaign(_ context.Context, id string, input models.NotificationCampaignInput) (models.NotificationCampaign, bool, error) {
	campaign, ok := s.campaigns[id]
	if !ok {
		return models.NotificationCampaign{}, false, nil
	}
	campaign = mergeCampaign(campaign, input)
	campaign.UpdatedAt = time.Now().UTC()
	s.campaigns[id] = campaign
	return campaign, true, nil
}

func (s *memoryMobilePushStore) ClaimDueCampaigns(_ context.Context, now time.Time) ([]models.NotificationCampaign, error) {
	out := []models.NotificationCampaign{}
	for _, campaign := range s.campaigns {
		if campaign.Status != "scheduled" {
			continue
		}
		due := campaign.TriggerType == "manual" && campaign.SendAt != nil && !campaign.SendAt.After(now)
		due = due || monthlyCampaignDue(campaign, now)
		claimKey := campaign.ID + ":" + campaignScheduledFor(campaign, now).Format("2006-01-02")
		if due {
			lastClaim, claimed := s.campaignClaims[claimKey]
			lastStatus := s.campaignStatuses[claimKey]
			if !claimed || ((lastStatus == "failed" || lastStatus == "partial" || lastStatus == "processing") && !lastClaim.After(now.Add(-5*time.Minute))) {
				s.campaignClaims[claimKey] = now
				s.campaignStatuses[claimKey] = "processing"
				out = append(out, campaign)
			}
		}
	}
	return out, nil
}

func monthlyCampaignDue(campaign models.NotificationCampaign, now time.Time) bool {
	if campaign.TriggerType != "monthly" || campaign.DayOfMonth == nil || *campaign.DayOfMonth > now.Day() {
		return false
	}
	if campaign.LastSentAt != nil && campaign.LastSentAt.Month() == now.Month() && campaign.LastSentAt.Year() == now.Year() {
		return false
	}
	if *campaign.DayOfMonth < now.Day() || campaign.SendTime == nil {
		return true
	}
	return strings.TrimSpace(*campaign.SendTime) <= now.UTC().Format("15:04")
}

func (s *memoryMobilePushStore) RecordCampaignDelivery(_ context.Context, campaign models.NotificationCampaign, now time.Time, _, _, _ int, status string) error {
	claimKey := campaign.ID + ":" + campaignScheduledFor(campaign, now).Format("2006-01-02")
	s.campaignClaims[claimKey] = now
	s.campaignStatuses[claimKey] = status
	if status != "sent" && status != "no_audience" {
		return nil
	}
	campaign.LastSentAt = &now
	if campaign.TriggerType == "manual" {
		campaign.Status = "sent"
	}
	s.campaigns[campaign.ID] = campaign
	return nil
}

func (s *memoryMobilePushStore) ArchivePost(_ context.Context, id string) (bool, error) {
	post, ok := s.posts[id]
	if !ok {
		return false, nil
	}
	post.Status = "archived"
	s.posts[id] = post
	return true, nil
}

func (s *memoryMobilePushStore) DuePosts(_ context.Context, now time.Time) ([]models.AdminPost, error) {
	out := []models.AdminPost{}
	for _, post := range s.posts {
		if post.Status == "scheduled" && post.StartsAt != nil && !post.StartsAt.After(now) && (post.EndsAt == nil || post.EndsAt.After(now)) {
			out = append(out, post)
		}
	}
	return out, nil
}

func (s *memoryMobilePushStore) MarkPublished(_ context.Context, id string) (models.AdminPost, error) {
	post, ok := s.posts[id]
	if !ok || (post.Status != "draft" && post.Status != "scheduled") {
		return models.AdminPost{}, errors.New("post cannot be published")
	}
	post.Status = "live"
	now := time.Now().UTC()
	post.PublishedAt = &now
	post.UpdatedAt = now
	s.posts[id] = post
	return post, nil
}

func (s *memoryMobilePushStore) DueExpirations(_ context.Context, now time.Time) ([]models.AdminPost, error) {
	out := []models.AdminPost{}
	for _, post := range s.posts {
		if post.Status == "live" && post.EndsAt != nil && !post.EndsAt.After(now) {
			out = append(out, post)
		}
	}
	return out, nil
}

func (s *memoryMobilePushStore) MarkExpired(_ context.Context, id string) error {
	post := s.posts[id]
	post.Status = "expired"
	s.posts[id] = post
	return nil
}

func (s *memoryMobilePushStore) MarkPushSent(_ context.Context, id string) error {
	post, ok := s.posts[id]
	if !ok {
		return errors.New("post not found")
	}
	now := time.Now().UTC()
	post.PushSentAt = &now
	post.UpdatedAt = now
	s.posts[id] = post
	return nil
}

func (s *memoryMobilePushStore) PublicPosts(_ context.Context, now time.Time) ([]models.AdminPost, error) {
	out := []models.AdminPost{}
	for _, post := range s.posts {
		if post.Status == "live" && (post.EndsAt == nil || post.EndsAt.After(now)) {
			out = append(out, post)
		}
	}
	return out, nil
}

func (s *memoryMobilePushStore) DevicesForPlatforms(context.Context, []string, []string) ([]models.PushDevice, error) {
	return nil, nil
}

func (s *memoryMobilePushStore) AudienceCount(context.Context, []string, []string) (int, error) {
	return 0, nil
}
