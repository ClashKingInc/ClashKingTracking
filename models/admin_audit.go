package models

import (
	"encoding/json"
	"time"
)

type AdminAuditEvent struct {
	ID           string          `json:"id"`
	Actor        string          `json:"actor"`
	Action       string          `json:"action"`
	ResourceType string          `json:"resource_type"`
	ResourceID   string          `json:"resource_id"`
	Summary      string          `json:"summary"`
	Metadata     json.RawMessage `json:"metadata"`
	IPAddress    string          `json:"ip_address"`
	UserAgent    string          `json:"user_agent"`
	CreatedAt    time.Time       `json:"created_at"`
}

type AdminAuditEventInput struct {
	Actor        string
	Action       string
	ResourceType string
	ResourceID   string
	Summary      string
	Metadata     map[string]any
	IPAddress    string
	UserAgent    string
}
