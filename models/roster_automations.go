package models

import "time"

// RosterAutomationExecution is one exact-time automation occurrence for one
// roster. ExecutionID is deterministic so downstream webhook delivery can
// reject duplicate Valkey deliveries safely.
type RosterAutomationExecution struct {
	ExecutionID      string
	AutomationID     string
	ServerID         string
	RosterID         string
	GroupID          string
	ActionType       string
	ScheduledAt      time.Time
	DiscordChannelID string
	PingType         string
	WebhookID        string
	MessageID        string
	RosterAlias      string
	EventStartTime   *int64
	Attempt          int
}

func (e RosterAutomationExecution) EventValue() map[string]any {
	value := map[string]any{
		"type":             e.ActionType,
		"execution_id":     e.ExecutionID,
		"automation_id":    e.AutomationID,
		"server_id":        e.ServerID,
		"roster_id":        e.RosterID,
		"action_type":      e.ActionType,
		"scheduled_at":     e.ScheduledAt.UTC().Format(time.RFC3339Nano),
		"delivery_attempt": e.Attempt,
	}
	if e.GroupID != "" {
		value["group_id"] = e.GroupID
	}
	if e.DiscordChannelID != "" {
		value["discord_channel_id"] = e.DiscordChannelID
	}
	if e.PingType != "" {
		value["options"] = map[string]any{"ping_type": e.PingType}
	}
	if e.WebhookID != "" && e.MessageID != "" {
		value["webhook_id"] = e.WebhookID
		value["message_id"] = e.MessageID
	}
	if e.RosterAlias != "" {
		value["roster_alias"] = e.RosterAlias
	}
	if e.EventStartTime != nil {
		value["event_start_time"] = *e.EventStartTime
	}
	return value
}
