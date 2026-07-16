package scripts

import (
	"context"
	"encoding/json"

	"clashking_tracking/models"
)

func (s *timescaleMobilePushStore) RecordAuditEvent(ctx context.Context, input models.AdminAuditEventInput) error {
	if input.Metadata == nil {
		input.Metadata = map[string]any{}
	}
	metadata, err := json.Marshal(input.Metadata)
	if err != nil {
		return err
	}
	_, err = s.pool.Exec(ctx, `INSERT INTO admin_audit_events
		(actor, action, resource_type, resource_id, summary, metadata, ip_address, user_agent)
		VALUES ($1,$2,$3,$4,$5,$6::jsonb,$7,$8)`, input.Actor, input.Action, input.ResourceType,
		input.ResourceID, input.Summary, string(metadata), input.IPAddress, input.UserAgent)
	return err
}

func (s *timescaleMobilePushStore) ListAuditEvents(ctx context.Context, limit int, resourceType, actor string) ([]models.AdminAuditEvent, error) {
	if limit < 1 {
		limit = 100
	}
	if limit > 500 {
		limit = 500
	}
	rows, err := s.pool.Query(ctx, `SELECT id, actor, action, resource_type, resource_id, summary, metadata,
		ip_address, user_agent, created_at FROM admin_audit_events
		WHERE ($2 = '' OR resource_type = $2) AND ($3 = '' OR actor = $3)
		ORDER BY created_at DESC LIMIT $1`, limit, resourceType, actor)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	events := []models.AdminAuditEvent{}
	for rows.Next() {
		var event models.AdminAuditEvent
		if err := rows.Scan(&event.ID, &event.Actor, &event.Action, &event.ResourceType, &event.ResourceID,
			&event.Summary, &event.Metadata, &event.IPAddress, &event.UserAgent, &event.CreatedAt); err != nil {
			return nil, err
		}
		events = append(events, event)
	}
	return events, rows.Err()
}

func (s *memoryMobilePushStore) RecordAuditEvent(_ context.Context, input models.AdminAuditEventInput) error {
	s.auditEvents = append(s.auditEvents, models.AdminAuditEvent{Actor: input.Actor, Action: input.Action,
		ResourceType: input.ResourceType, ResourceID: input.ResourceID, Summary: input.Summary})
	return nil
}

func (s *memoryMobilePushStore) ListAuditEvents(context.Context, int, string, string) ([]models.AdminAuditEvent, error) {
	return append([]models.AdminAuditEvent{}, s.auditEvents...), nil
}
