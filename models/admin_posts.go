package models

import "time"

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

// AdminPost mirrors the admin_posts table (clashking-devkit/database/timescale/012_admin_posts.sql).
type AdminPost struct {
	ID                string      `json:"id"`
	Slug              string      `json:"slug"`
	Title             string      `json:"title"`
	Summary           string      `json:"summary"`
	HeroImageURL      *string     `json:"hero_image_url,omitempty"`
	BodyBlocks        []PostBlock `json:"body_blocks"`
	TargetRoute       *string     `json:"target_route,omitempty"`
	Platforms         []string    `json:"platforms"`
	Dismissible       bool        `json:"dismissible"`
	Priority          int         `json:"priority"`
	Status            string      `json:"status"`
	StartsAt          *time.Time  `json:"starts_at,omitempty"`
	EndsAt            *time.Time  `json:"ends_at,omitempty"`
	AlsoPushOnPublish bool        `json:"also_push_on_publish"`
	PushTitle         *string     `json:"push_title,omitempty"`
	PushBody          *string     `json:"push_body,omitempty"`
	PublishedAt       *time.Time  `json:"published_at,omitempty"`
	PushSentAt        *time.Time  `json:"push_sent_at,omitempty"`
	CreatedBy         string      `json:"created_by"`
	CreatedAt         time.Time   `json:"created_at"`
	UpdatedAt         time.Time   `json:"updated_at"`
}

// AdminPostInput is the create/update request body. Pointer fields left nil
// are not modified by an update; Platforms/BodyBlocks nil means "leave as
// stored" on update but "use column default" on create.
type AdminPostInput struct {
	Title             *string     `json:"title,omitempty"`
	Summary           *string     `json:"summary,omitempty"`
	HeroImageURL      *string     `json:"hero_image_url,omitempty"`
	BodyBlocks        []PostBlock `json:"body_blocks,omitempty"`
	TargetRoute       *string     `json:"target_route,omitempty"`
	Platforms         []string    `json:"platforms,omitempty"`
	Dismissible       *bool       `json:"dismissible,omitempty"`
	Priority          *int        `json:"priority,omitempty"`
	StartsAt          *time.Time  `json:"starts_at,omitempty"`
	EndsAt            *time.Time  `json:"ends_at,omitempty"`
	AlsoPushOnPublish *bool       `json:"also_push_on_publish,omitempty"`
	PushTitle         *string     `json:"push_title,omitempty"`
	PushBody          *string     `json:"push_body,omitempty"`
	CreatedBy         *string     `json:"created_by,omitempty"`
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
}
