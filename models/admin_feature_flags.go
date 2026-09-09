package models

import "time"

type AdminFeatureFlag struct {
	Key               string     `json:"key"`
	Name              string     `json:"name"`
	Description       string     `json:"description"`
	Enabled           bool       `json:"enabled"`
	RolloutPercentage int        `json:"rolloutPercentage"`
	MinAppVersion     string     `json:"minAppVersion,omitempty"`
	Platforms         []string   `json:"platforms"`
	Owner             string     `json:"owner"`
	PublicExposure    string     `json:"publicExposure"`
	StartsAt          *time.Time `json:"startsAt,omitempty"`
	EndsAt            *time.Time `json:"endsAt,omitempty"`
	CreatedAt         time.Time  `json:"createdAt"`
	LastUpdated       time.Time  `json:"lastUpdated"`
}

type AdminFeatureFlagInput struct {
	Key               *string    `json:"key,omitempty"`
	Name              *string    `json:"name,omitempty"`
	Description       *string    `json:"description,omitempty"`
	Enabled           *bool      `json:"enabled,omitempty"`
	RolloutPercentage *int       `json:"rolloutPercentage,omitempty"`
	MinAppVersion     *string    `json:"minAppVersion,omitempty"`
	Platforms         []string   `json:"platforms,omitempty"`
	Owner             *string    `json:"owner,omitempty"`
	PublicExposure    *string    `json:"publicExposure,omitempty"`
	StartsAt          *time.Time `json:"startsAt,omitempty"`
	EndsAt            *time.Time `json:"endsAt,omitempty"`

	ClearMinAppVersion bool `json:"-"`
	ClearStartsAt      bool `json:"-"`
	ClearEndsAt        bool `json:"-"`
}
