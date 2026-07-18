package models

import (
	"encoding/json"
	"time"
)

// PostBlock is one entry of an AdminPost's body_blocks jsonb array. Type
// determines which of the other fields are populated:
//   - "heading":      Text
//   - "paragraph":    Text
//   - "bullet_list":  Items
//   - "image":        URL, Caption
type PostBlock struct {
	Type    string   `json:"type"`
	Text    string   `json:"text,omitempty"`
	Items   []string `json:"items,omitempty"`
	URL     string   `json:"url,omitempty"`
	Caption string   `json:"caption,omitempty"`
}

type AdminPostTranslation struct {
	Title      string      `json:"title"`
	Summary    string      `json:"summary"`
	BodyBlocks []PostBlock `json:"body_blocks,omitempty"`
	PushTitle  string      `json:"push_title,omitempty"`
	PushBody   string      `json:"push_body,omitempty"`
}

// AdminPost mirrors the admin_posts table (clashking-devkit/database/timescale/017_mobile_admin_operations.sql).
type AdminPost struct {
	ID                string                          `json:"id"`
	Slug              string                          `json:"slug"`
	Title             string                          `json:"title"`
	Summary           string                          `json:"summary"`
	HeroImageURL      *string                         `json:"hero_image_url,omitempty"`
	BodyBlocks        []PostBlock                     `json:"body_blocks"`
	Translations      map[string]AdminPostTranslation `json:"translations"`
	PresentationType  string                          `json:"presentation_type"`
	StoryURL          *string                         `json:"story_url,omitempty"`
	StoryVersion      int                             `json:"story_version"`
	StoryHistory      []string                        `json:"story_history"`
	RevisionNumber    int                             `json:"revision_number"`
	ShowOnHome        bool                            `json:"show_on_home"`
	PinnedOnHome      bool                            `json:"pinned_on_home"`
	TargetRoute       *string                         `json:"target_route,omitempty"`
	Platforms         []string                        `json:"platforms"`
	Dismissible       bool                            `json:"dismissible"`
	Priority          int                             `json:"priority"`
	Status            string                          `json:"status"`
	StartsAt          *time.Time                      `json:"starts_at,omitempty"`
	EndsAt            *time.Time                      `json:"ends_at,omitempty"`
	AlsoPushOnPublish bool                            `json:"also_push_on_publish"`
	PushTitle         *string                         `json:"push_title,omitempty"`
	PushBody          *string                         `json:"push_body,omitempty"`
	PublishedAt       *time.Time                      `json:"published_at,omitempty"`
	PushSentAt        *time.Time                      `json:"push_sent_at,omitempty"`
	CreatedBy         string                          `json:"created_by"`
	CreatedAt         time.Time                       `json:"created_at"`
	UpdatedAt         time.Time                       `json:"updated_at"`
}

// AdminPostRevision is an immutable snapshot written before every edit.
type AdminPostRevision struct {
	ID             string          `json:"id"`
	PostID         string          `json:"post_id"`
	RevisionNumber int             `json:"revision_number"`
	Snapshot       json.RawMessage `json:"snapshot"`
	CreatedBy      string          `json:"created_by"`
	CreatedAt      time.Time       `json:"created_at"`
}

// AdminPostDeliveryAttempt records every real post push attempt, including
// no-audience and provider failures, so the dashboard never has to guess.
type AdminPostDeliveryAttempt struct {
	ID            string    `json:"id"`
	PostID        string    `json:"post_id"`
	AttemptNumber int       `json:"attempt_number"`
	Trigger       string    `json:"trigger"`
	EligibleCount int       `json:"eligible_count"`
	SentCount     int       `json:"sent_count"`
	SkippedCount  int       `json:"skipped_count"`
	Status        string    `json:"status"`
	ErrorSummary  string    `json:"error_summary,omitempty"`
	AttemptedAt   time.Time `json:"attempted_at"`
}

type NotificationCampaign struct {
	ID            string                                     `json:"id"`
	Key           string                                     `json:"key"`
	Title         string                                     `json:"title"`
	Body          string                                     `json:"body"`
	TargetRoute   *string                                    `json:"target_route,omitempty"`
	Platforms     []string                                   `json:"platforms"`
	TargetLocales []string                                   `json:"target_locales"`
	Translations  map[string]NotificationCampaignTranslation `json:"translations"`
	Status        string                                     `json:"status"`
	TriggerType   string                                     `json:"trigger_type"`
	DayOfMonth    *int                                       `json:"day_of_month,omitempty"`
	SendAt        *time.Time                                 `json:"send_at,omitempty"`
	SendTime      *string                                    `json:"send_time,omitempty"`
	LastSentAt    *time.Time                                 `json:"last_sent_at,omitempty"`
	CreatedBy     string                                     `json:"created_by"`
	CreatedAt     time.Time                                  `json:"created_at"`
	UpdatedAt     time.Time                                  `json:"updated_at"`
}

type NotificationCampaignTranslation struct {
	Title string `json:"title"`
	Body  string `json:"body"`
}

type NotificationCampaignInput struct {
	Title         *string                                    `json:"title,omitempty"`
	Body          *string                                    `json:"body,omitempty"`
	TargetRoute   *string                                    `json:"target_route,omitempty"`
	Platforms     []string                                   `json:"platforms,omitempty"`
	TargetLocales []string                                   `json:"target_locales,omitempty"`
	Translations  map[string]NotificationCampaignTranslation `json:"translations,omitempty"`
	Status        *string                                    `json:"status,omitempty"`
	TriggerType   *string                                    `json:"trigger_type,omitempty"`
	DayOfMonth    *int                                       `json:"day_of_month,omitempty"`
	SendAt        *time.Time                                 `json:"send_at,omitempty"`
	SendTime      *string                                    `json:"send_time,omitempty"`
	CreatedBy     *string                                    `json:"created_by,omitempty"`

	ClearTargetRoute bool `json:"-"`
	ClearDayOfMonth  bool `json:"-"`
	ClearSendAt      bool `json:"-"`
	ClearSendTime    bool `json:"-"`
}

// AdminPostInput is the create/update request body. Pointer fields left nil
// are not modified by an update; Platforms/BodyBlocks nil means "leave as
// stored" on update but "use column default" on create.
type AdminPostInput struct {
	Title             *string                         `json:"title,omitempty"`
	Summary           *string                         `json:"summary,omitempty"`
	HeroImageURL      *string                         `json:"hero_image_url,omitempty"`
	BodyBlocks        []PostBlock                     `json:"body_blocks,omitempty"`
	Translations      map[string]AdminPostTranslation `json:"translations,omitempty"`
	PresentationType  *string                         `json:"presentation_type,omitempty"`
	StoryURL          *string                         `json:"story_url,omitempty"`
	ShowOnHome        *bool                           `json:"show_on_home,omitempty"`
	PinnedOnHome      *bool                           `json:"pinned_on_home,omitempty"`
	TargetRoute       *string                         `json:"target_route,omitempty"`
	Platforms         []string                        `json:"platforms,omitempty"`
	Dismissible       *bool                           `json:"dismissible,omitempty"`
	Priority          *int                            `json:"priority,omitempty"`
	StartsAt          *time.Time                      `json:"starts_at,omitempty"`
	EndsAt            *time.Time                      `json:"ends_at,omitempty"`
	AlsoPushOnPublish *bool                           `json:"also_push_on_publish,omitempty"`
	PushTitle         *string                         `json:"push_title,omitempty"`
	PushBody          *string                         `json:"push_body,omitempty"`
	CreatedBy         *string                         `json:"created_by,omitempty"`

	// Clear* is populated by the HTTP decoder when a nullable JSON field is
	// explicitly set to null. encoding/json otherwise cannot distinguish null
	// from an omitted pointer field, which made optional post values impossible
	// to remove through PATCH.
	ClearHeroImageURL bool `json:"-"`
	ClearStoryURL     bool `json:"-"`
	ClearTargetRoute  bool `json:"-"`
	ClearStartsAt     bool `json:"-"`
	ClearEndsAt       bool `json:"-"`
	ClearPushTitle    bool `json:"-"`
	ClearPushBody     bool `json:"-"`
}

// PushDevice is a row read from mobile_push_devices (owned by clashking-api;
// this service only reads it) for the columns mobile_push needs to send.
type PushDevice struct {
	UserID          string
	DeviceID        string
	Platform        string
	Provider        string
	Environment     string
	TokenCiphertext string
	Locale          string
}
