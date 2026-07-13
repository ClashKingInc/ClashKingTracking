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
	id, slug, title, summary, hero_image_url, body_blocks, target_route, platforms,
	dismissible, priority, status, starts_at, ends_at, also_push_on_publish,
	push_title, push_body, published_at, push_sent_at, created_by, created_at, updated_at
`

func scanAdminPost(scanner interface{ Scan(...any) error }) (models.AdminPost, error) {
	var post models.AdminPost
	var rawBlocks []byte
	if err := scanner.Scan(
		&post.ID, &post.Slug, &post.Title, &post.Summary, &post.HeroImageURL,
		&rawBlocks, &post.TargetRoute, &post.Platforms, &post.Dismissible,
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
			slug, title, summary, hero_image_url, body_blocks, target_route,
			platforms, dismissible, priority, status, starts_at, ends_at,
			also_push_on_publish, push_title, push_body, created_by
		) VALUES (
			$1, $2, $3, $4, $5::jsonb, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16
		)
		RETURNING `+adminPostColumns,
		uniqueSlug(title), title, valueOr(input.Summary, ""), input.HeroImageURL,
		string(rawBlocks), input.TargetRoute, platforms, valueOr(input.Dismissible, true),
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
	merged := mergeAdminPost(current, input)
	rawBlocks, err := json.Marshal(merged.BodyBlocks)
	if err != nil {
		return models.AdminPost{}, false, err
	}
	row := s.pool.QueryRow(ctx, `
		UPDATE admin_posts SET
			title = $2, summary = $3, hero_image_url = $4, body_blocks = $5::jsonb,
			target_route = $6, platforms = $7, dismissible = $8, priority = $9,
			starts_at = $10, ends_at = $11, also_push_on_publish = $12,
			push_title = $13, push_body = $14, updated_at = now()
		WHERE id = $1
		RETURNING `+adminPostColumns,
		id, merged.Title, merged.Summary, merged.HeroImageURL, string(rawBlocks),
		merged.TargetRoute, merged.Platforms, merged.Dismissible, merged.Priority,
		merged.StartsAt, merged.EndsAt, merged.AlsoPushOnPublish, merged.PushTitle,
		merged.PushBody,
	)
	post, err := scanAdminPost(row)
	if err != nil {
		return models.AdminPost{}, false, err
	}
	return post, true, nil
}

func mergeAdminPost(current models.AdminPost, input models.AdminPostInput) models.AdminPost {
	if input.Title != nil {
		current.Title = *input.Title
	}
	if input.Summary != nil {
		current.Summary = *input.Summary
	}
	if input.HeroImageURL != nil {
		current.HeroImageURL = input.HeroImageURL
	}
	if input.BodyBlocks != nil {
		current.BodyBlocks = input.BodyBlocks
	}
	if input.TargetRoute != nil {
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
	if input.StartsAt != nil {
		current.StartsAt = input.StartsAt
	}
	if input.EndsAt != nil {
		current.EndsAt = input.EndsAt
	}
	if input.AlsoPushOnPublish != nil {
		current.AlsoPushOnPublish = *input.AlsoPushOnPublish
	}
	if input.PushTitle != nil {
		current.PushTitle = input.PushTitle
	}
	if input.PushBody != nil {
		current.PushBody = input.PushBody
	}
	return current
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
		WHERE id = $1
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
		SELECT user_id, device_id, platform, provider, environment, token_ciphertext
		FROM mobile_push_devices
		WHERE enabled = true AND platform = ANY($1)
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
		SELECT count(*) FROM mobile_push_devices WHERE enabled = true AND platform = ANY($1)
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
	posts map[string]models.AdminPost
}

func newMemoryMobilePushStore() *memoryMobilePushStore {
	return &memoryMobilePushStore{posts: map[string]models.AdminPost{}}
}

func (s *memoryMobilePushStore) Close() {}

func (s *memoryMobilePushStore) ListPosts(context.Context, string) ([]models.AdminPost, error) {
	out := make([]models.AdminPost, 0, len(s.posts))
	for _, post := range s.posts {
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
		CreatedAt: now, UpdatedAt: now,
	}, input)
	s.posts[id] = post
	return post, nil
}

func (s *memoryMobilePushStore) UpdatePost(_ context.Context, id string, input models.AdminPostInput) (models.AdminPost, bool, error) {
	post, ok := s.posts[id]
	if !ok {
		return models.AdminPost{}, false, nil
	}
	post = mergeAdminPost(post, input)
	s.posts[id] = post
	return post, true, nil
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

func (s *memoryMobilePushStore) DuePosts(context.Context, time.Time) ([]models.AdminPost, error) {
	return nil, nil
}

func (s *memoryMobilePushStore) MarkPublished(_ context.Context, id string) (models.AdminPost, error) {
	post := s.posts[id]
	post.Status = "live"
	s.posts[id] = post
	return post, nil
}

func (s *memoryMobilePushStore) DueExpirations(context.Context, time.Time) ([]models.AdminPost, error) {
	return nil, nil
}

func (s *memoryMobilePushStore) MarkExpired(_ context.Context, id string) error {
	post := s.posts[id]
	post.Status = "expired"
	s.posts[id] = post
	return nil
}

func (s *memoryMobilePushStore) MarkPushSent(context.Context, string) error { return nil }

func (s *memoryMobilePushStore) PublicPosts(context.Context, time.Time) ([]models.AdminPost, error) {
	return nil, nil
}

func (s *memoryMobilePushStore) DevicesForPlatforms(context.Context, []string) ([]models.PushDevice, error) {
	return nil, nil
}

func (s *memoryMobilePushStore) AudienceCount(context.Context, []string) (int, error) {
	return 0, nil
}
