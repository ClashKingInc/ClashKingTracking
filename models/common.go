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

type HealthStatus struct {
	Name       string    `json:"name"`
	Healthy    bool      `json:"healthy"`
	LastOK     time.Time `json:"last_ok,omitempty"`
	LastError  string    `json:"last_error,omitempty"`
	LastUpdate time.Time `json:"last_update"`
}

type QueueStats struct {
	Name     string `json:"name"`
	Capacity int    `json:"capacity"`
	Depth    int    `json:"depth"`
	Dropped  uint64 `json:"dropped"`
}
