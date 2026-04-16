package models

import "time"

// These lightweight transport models are shared across the in-process bus,
// health endpoints, and queue reporting surfaces.
type Event struct {
	Topic     string         `json:"topic" bson:"topic"`
	Key       string         `json:"key,omitempty" bson:"key,omitempty"`
	Type      string         `json:"type,omitempty" bson:"type,omitempty"`
	Value     map[string]any `json:"value" bson:"value"`
	CreatedAt time.Time      `json:"created_at" bson:"created_at"`
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
