package scripts

import (
	"context"
	"encoding/json"
	"errors"
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
	ListPosts(ctx context.Context, status string) ([]models.AdminPost, error)
	GetPost(ctx context.Context, id string) (models.AdminPost, bool, error)
	CreatePost(ctx context.Context, input models.AdminPostInput) (models.AdminPost, error)
	UpdatePost(ctx context.Context, id string, input models.AdminPostInput) (models.AdminPost, bool, error)
	ListPostRevisions(ctx context.Context, id string) ([]models.AdminPostRevision, error)
	GetPostRevision(ctx context.Context, id string, revision int) (models.AdminPostRevision, bool, error)
	RecordDeliveryAttempt(ctx context.Context, attempt models.AdminPostDeliveryAttempt) (models.AdminPostDeliveryAttempt, error)
	ListDeliveryAttempts(ctx context.Context, id string) ([]models.AdminPostDeliveryAttempt, error)
	DuePushRetries(ctx context.Context, now time.Time) ([]models.AdminPost, error)
	ListCampaigns(ctx context.Context) ([]models.NotificationCampaign, error)
	DueCampaigns(ctx context.Context, now time.Time) ([]models.NotificationCampaign, error)
	RecordCampaignDelivery(ctx context.Context, campaign models.NotificationCampaign, now time.Time, eligible, sent, skipped int, status string) error
	ArchivePost(ctx context.Context, id string) (bool, error)
	DuePosts(ctx context.Context, now time.Time) ([]models.AdminPost, error)
	MarkPublished(ctx context.Context, id string) (models.AdminPost, error)
	DueExpirations(ctx context.Context, now time.Time) ([]models.AdminPost, error)
	MarkExpired(ctx context.Context, id string) error
	MarkPushSent(ctx context.Context, id string) error
	PublicPosts(ctx context.Context, now time.Time) ([]models.AdminPost, error)
	DevicesForPlatforms(ctx context.Context, platforms []string) ([]models.PushDevice, error)
	AudienceCount(ctx context.Context, platforms []string) (int, error)
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
	id, slug, title, summary, hero_image_url, body_blocks, presentation_type,
	story_url, story_version, story_history, revision_number, show_on_home, pinned_on_home, target_route, platforms,
	dismissible, priority, status, starts_at, ends_at, also_push_on_publish,
	push_title, push_body, published_at, push_sent_at, created_by, created_at, updated_at
`

func scanAdminPost(scanner interface{ Scan(...any) error }) (models.AdminPost, error) {
	var post models.AdminPost
	var rawBlocks []byte
	if err := scanner.Scan(
		&post.ID, &post.Slug, &post.Title, &post.Summary, &post.HeroImageURL,
		&rawBlocks, &post.PresentationType, &post.StoryURL, &post.StoryVersion, &post.StoryHistory, &post.RevisionNumber, &post.ShowOnHome,
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
	platforms := input.Platforms
	if platforms == nil {
		platforms = []string{"ios", "android", "web"}
	}

	row := s.pool.QueryRow(ctx, `
		INSERT INTO admin_posts (
			slug, title, summary, hero_image_url, body_blocks, presentation_type,
			story_url, show_on_home, pinned_on_home, target_route,
			platforms, dismissible, priority, status, starts_at, ends_at,
			also_push_on_publish, push_title, push_body, created_by
		) VALUES (
			$1, $2, $3, $4, $5::jsonb, $6, $7, $8, $9, $10, $11, $12,
			$13, $14, $15, $16, $17, $18, $19, $20
		)
		RETURNING `+adminPostColumns,
		uniqueSlug(title), title, valueOr(input.Summary, ""), input.HeroImageURL,
		string(rawBlocks), valueOr(input.PresentationType, "article"), input.StoryURL,
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
		id, current.RevisionNumber, string(snapshot), current.CreatedBy,
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
	status := merged.Status
	if merged.StartsAt != nil && merged.StartsAt.After(time.Now().UTC()) {
		// Moving an already-live post into the future must temporarily unpublish
		// it. The scheduler will make it live again when starts_at is reached.
		status = "scheduled"
	} else if current.Status == "draft" || current.Status == "scheduled" {
		status = "draft"
	}
	row := s.pool.QueryRow(ctx, `
		UPDATE admin_posts SET
			title = $2, summary = $3, hero_image_url = $4, body_blocks = $5::jsonb,
			presentation_type = $6, story_url = $7, story_version = $8, story_history = $9,
			revision_number = $10, show_on_home = $11, pinned_on_home = $12, target_route = $13, platforms = $14,
			dismissible = $15, priority = $16, starts_at = $17, ends_at = $18,
			also_push_on_publish = $19, push_title = $20, push_body = $21,
			status = $22, updated_at = now()
		WHERE id = $1
		RETURNING `+adminPostColumns,
		id, merged.Title, merged.Summary, merged.HeroImageURL, string(rawBlocks),
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
			  AND a.status = 'failed' AND a.attempt_number < 3 AND a.attempted_at <= $1 - interval '2 minutes'
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

const campaignColumns = `id, campaign_key, title, body, target_route, platforms, status, trigger_type, day_of_month, send_at, last_sent_at, created_by, created_at, updated_at`

func scanCampaign(scanner interface{ Scan(...any) error }) (models.NotificationCampaign, error) {
	var campaign models.NotificationCampaign
	err := scanner.Scan(&campaign.ID, &campaign.Key, &campaign.Title, &campaign.Body, &campaign.TargetRoute, &campaign.Platforms, &campaign.Status, &campaign.TriggerType, &campaign.DayOfMonth, &campaign.SendAt, &campaign.LastSentAt, &campaign.CreatedBy, &campaign.CreatedAt, &campaign.UpdatedAt)
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

func (s *timescaleMobilePushStore) DueCampaigns(ctx context.Context, now time.Time) ([]models.NotificationCampaign, error) {
	rows, err := s.pool.Query(ctx, `SELECT `+campaignColumns+` FROM admin_notification_campaigns
		WHERE status = 'scheduled' AND (
			(trigger_type = 'manual' AND send_at IS NOT NULL AND send_at <= $1) OR
			(trigger_type = 'monthly' AND day_of_month <= extract(day from $1)::integer
			 AND (last_sent_at IS NULL OR date_trunc('month', last_sent_at) < date_trunc('month', $1)))
		)`, now)
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

func (s *timescaleMobilePushStore) RecordCampaignDelivery(ctx context.Context, campaign models.NotificationCampaign, now time.Time, eligible, sent, skipped int, status string) error {
	_, err := s.pool.Exec(ctx, `INSERT INTO admin_campaign_delivery_attempts (campaign_id, scheduled_for, eligible_count, sent_count, skipped_count, status)
		VALUES ($1, $2::date, $3, $4, $5, $6) ON CONFLICT (campaign_id, scheduled_for) DO NOTHING`, campaign.ID, now, eligible, sent, skipped, status)
	if err != nil {
		return err
	}
	if campaign.TriggerType == "monthly" {
		_, err = s.pool.Exec(ctx, `UPDATE admin_notification_campaigns SET last_sent_at = $2, updated_at = now() WHERE id = $1`, campaign.ID, now)
	} else {
		_, err = s.pool.Exec(ctx, `UPDATE admin_notification_campaigns SET last_sent_at = $2, status = 'sent', updated_at = now() WHERE id = $1`, campaign.ID, now)
	}
	return err
}

func (s *timescaleMobilePushStore) ArchivePost(ctx context.Context, id string) (bool, error) {
	tag, err := s.pool.Exec(ctx, "UPDATE admin_posts SET status = 'archived', updated_at = now() WHERE id = $1", id)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() > 0, nil
}

func (s *timescaleMobilePushStore) DuePosts(ctx context.Context, now time.Time) ([]models.AdminPost, error) {
	rows, err := s.pool.Query(ctx, "SELECT "+adminPostColumns+" FROM admin_posts WHERE status = 'scheduled' AND starts_at <= $1", now)
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

func (s *timescaleMobilePushStore) DevicesForPlatforms(ctx context.Context, platforms []string) ([]models.PushDevice, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT d.user_id, d.device_id, d.platform, d.provider, d.environment, d.token_ciphertext
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
	`, platforms)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []models.PushDevice{}
	for rows.Next() {
		var device models.PushDevice
		if err := rows.Scan(
			&device.UserID, &device.DeviceID, &device.Platform,
			&device.Provider, &device.Environment, &device.TokenCiphertext,
		); err != nil {
			return nil, err
		}
		out = append(out, device)
	}
	return out, rows.Err()
}

func (s *timescaleMobilePushStore) AudienceCount(ctx context.Context, platforms []string) (int, error) {
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
	`, platforms).Scan(&count)
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

// memoryMobilePushStore is the MockDB/DryRun fallback: same interface, no
// database, so the HTTP API and scheduler both stay runnable for quick
// local iteration without Postgres.
type memoryMobilePushStore struct {
	posts     map[string]models.AdminPost
	revisions map[string][]models.AdminPostRevision
	attempts  map[string][]models.AdminPostDeliveryAttempt
	campaigns map[string]models.NotificationCampaign
}

func newMemoryMobilePushStore() *memoryMobilePushStore {
	store := &memoryMobilePushStore{
		posts: map[string]models.AdminPost{}, revisions: map[string][]models.AdminPostRevision{}, attempts: map[string][]models.AdminPostDeliveryAttempt{}, campaigns: map[string]models.NotificationCampaign{},
	}
	now := time.Now().UTC()
	day := 1
	route := "/settings/support"
	store.campaigns["monthly-support"] = models.NotificationCampaign{ID: "monthly-support", Key: "monthly-support", Title: "Support ClashKing", Body: "Monthly support helps keep ClashKing available and improving. Thank you.", TargetRoute: &route, Platforms: []string{"ios", "android", "web"}, Status: "scheduled", TriggerType: "monthly", DayOfMonth: &day, LastSentAt: &now, CreatedAt: now, UpdatedAt: now}
	return store
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
	s.revisions[id] = append(s.revisions[id], models.AdminPostRevision{ID: uniqueSlug("revision"), PostID: id, RevisionNumber: post.RevisionNumber, Snapshot: snapshot, CreatedBy: post.CreatedBy, CreatedAt: time.Now().UTC()})
	post = mergeAdminPost(post, input)
	post.RevisionNumber++
	if !sameOptionalString(s.posts[id].StoryURL, post.StoryURL) && post.StoryURL != nil {
		post.StoryVersion = max(s.posts[id].StoryVersion+1, 1)
		if s.posts[id].StoryURL != nil {
			post.StoryHistory = append(append([]string{}, s.posts[id].StoryHistory...), *s.posts[id].StoryURL)
		}
	}
	if post.StartsAt != nil && post.StartsAt.After(time.Now().UTC()) {
		post.Status = "scheduled"
	} else if post.Status == "draft" || post.Status == "scheduled" {
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
		if post.Status == "live" && post.AlsoPushOnPublish && post.PushSentAt == nil && last.Status == "failed" && last.AttemptNumber < 3 && !last.AttemptedAt.After(now.Add(-2*time.Minute)) {
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

func (s *memoryMobilePushStore) DueCampaigns(_ context.Context, now time.Time) ([]models.NotificationCampaign, error) {
	out := []models.NotificationCampaign{}
	for _, campaign := range s.campaigns {
		if campaign.Status != "scheduled" {
			continue
		}
		if campaign.TriggerType == "manual" && campaign.SendAt != nil && !campaign.SendAt.After(now) {
			out = append(out, campaign)
		}
		if campaign.TriggerType == "monthly" && campaign.DayOfMonth != nil && *campaign.DayOfMonth <= now.Day() && (campaign.LastSentAt == nil || campaign.LastSentAt.Month() != now.Month() || campaign.LastSentAt.Year() != now.Year()) {
			out = append(out, campaign)
		}
	}
	return out, nil
}

func (s *memoryMobilePushStore) RecordCampaignDelivery(_ context.Context, campaign models.NotificationCampaign, now time.Time, _, _, _ int, _ string) error {
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
		if post.Status == "scheduled" && post.StartsAt != nil && !post.StartsAt.After(now) {
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

func (s *memoryMobilePushStore) DevicesForPlatforms(context.Context, []string) ([]models.PushDevice, error) {
	return nil, nil
}

func (s *memoryMobilePushStore) AudienceCount(context.Context, []string) (int, error) {
	return 0, nil
}
