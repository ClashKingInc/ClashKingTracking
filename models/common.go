package models

import "time"

// These lightweight transport models are shared by event and health surfaces.
type Event struct {
	Topic     string         `json:"topic"`
	Key       string         `json:"key,omitempty"`
	Type      string         `json:"type,omitempty"`
	Value     map[string]any `json:"value"`
	CreatedAt time.Time      `json:"created_at"`
}
