package scripts

import (
	"context"
	"errors"
	"fmt"
	"time"

	"clashking_tracking/internal/platform"
	"clashking_tracking/models"

	"github.com/jackc/pgx/v5/pgxpool"
)

const rosterAutomationsDomainName = "rosterautomations"

type rosterAutomationsDomain struct {
	store   rosterAutomationStore
	publish func(context.Context, platform.Event) error
}

type rosterAutomationStore interface {
	Close()
	ClaimDue(context.Context, time.Time, int) ([]models.RosterAutomationExecution, error)
	MarkDispatched(context.Context, models.RosterAutomationExecution, time.Time) error
	MarkRetry(context.Context, models.RosterAutomationExecution, time.Time, string) error
}

func NewRosterAutomationsDomain() platform.Domain { return &rosterAutomationsDomain{} }

func (d *rosterAutomationsDomain) Name() string { return rosterAutomationsDomainName }

func (d *rosterAutomationsDomain) Run(ctx context.Context, app *platform.App) error {
	if err := validateRosterAutomationsConfig(app.Config); err != nil {
		return err
	}
	store, err := newRosterAutomationStore(ctx, app)
	if err != nil {
		return err
	}
	defer store.Close()
	d.store = store
	d.publish = app.PublishEvent

	interval := time.Duration(app.Config.RosterAutomationScanSeconds) * time.Second
	for {
		started := time.Now()
		err := d.runCycle(ctx, app, time.Now().UTC())
		app.Stats.RecordProcess(rosterAutomationsDomainName, time.Since(started))
		readinessMessage := ""
		if err != nil {
			readinessMessage = err.Error()
		}
		app.Stats.SetReady(rosterAutomationsDomainName, err == nil, readinessMessage)
		if err != nil && app.Config.RunOnce {
			return err
		}
		if app.Config.RunOnce {
			return nil
		}
		if err := sleepOrDone(ctx, interval); err != nil {
			return err
		}
	}
}

func validateRosterAutomationsConfig(cfg platform.Config) error {
	if cfg.RosterAutomationScanSeconds <= 0 {
		return errors.New("roster_automations.scan_seconds must be greater than zero")
	}
	if cfg.RosterAutomationBatchSize <= 0 {
		return errors.New("roster_automations.batch_size must be greater than zero")
	}
	if !cfg.DryRun && !cfg.MockDB && cfg.TimescaleURL == "" {
		return errors.New("TIMESCALE_* connection variables are required for rosterautomations")
	}
	if !cfg.DryRun && !cfg.MockDB && (cfg.ValkeyAddr == "" || cfg.EventStreamName == "") {
		return errors.New("Valkey and events.stream are required for rosterautomations")
	}
	return nil
}

func newRosterAutomationStore(ctx context.Context, app *platform.App) (rosterAutomationStore, error) {
	if app.Config.DryRun || app.Config.MockDB || app.Config.TimescaleURL == "" {
		return &memoryRosterAutomationStore{}, nil
	}
	pool, err := pgxpool.New(ctx, app.Config.TimescaleURL)
	if err != nil {
		return nil, err
	}
	return &timescaleRosterAutomationStore{pool: pool}, nil
}

func (d *rosterAutomationsDomain) runCycle(ctx context.Context, app *platform.App, now time.Time) error {
	executions, err := d.store.ClaimDue(ctx, now, app.Config.RosterAutomationBatchSize)
	if err != nil {
		return err
	}
	var firstErr error
	for _, execution := range executions {
		event := platform.Event{
			Topic:     "roster_automation",
			Timestamp: now,
			Value:     execution.EventValue(),
		}
		if err := d.publish(ctx, event); err != nil {
			retryAt := now.Add(rosterAutomationRetryDelay(execution.Attempt))
			if markErr := d.store.MarkRetry(ctx, execution, retryAt, err.Error()); markErr != nil {
				err = errors.Join(err, markErr)
			}
			app.Logger.Error("roster automation dispatch failed", "execution_id", execution.ExecutionID, "err", err)
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		if err := d.store.MarkDispatched(ctx, execution, now); err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		app.Stats.RecordWrite(rosterAutomationsDomainName, 1)
	}
	return firstErr
}

func rosterAutomationRetryDelay(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	delay := time.Duration(1<<min(attempt-1, 6)) * 15 * time.Second
	return min(delay, 15*time.Minute)
}

type timescaleRosterAutomationStore struct{ pool *pgxpool.Pool }

func (s *timescaleRosterAutomationStore) Close() { s.pool.Close() }

// ClaimDue first freezes every due rule into one durable execution per target
// roster, then claims retryable executions with SKIP LOCKED. The execution
// table is the delivery ledger; roster_automation_rules remains user config.
func (s *timescaleRosterAutomationStore) ClaimDue(ctx context.Context, now time.Time, limit int) ([]models.RosterAutomationExecution, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, expandDueRosterAutomationsSQL, now); err != nil {
		return nil, err
	}
	if _, err := tx.Exec(ctx, markUntargetedRosterAutomationsSQL, now); err != nil {
		return nil, err
	}
	rows, err := tx.Query(ctx, claimDueRosterAutomationsSQL, now, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	executions := make([]models.RosterAutomationExecution, 0, limit)
	for rows.Next() {
		var execution models.RosterAutomationExecution
		if err := rows.Scan(
			&execution.ExecutionID, &execution.AutomationID, &execution.ServerID,
			&execution.RosterID, &execution.GroupID, &execution.ActionType,
			&execution.ScheduledAt, &execution.DiscordChannelID, &execution.PingType,
			&execution.WebhookID, &execution.MessageID, &execution.RosterAlias,
			&execution.EventStartTime, &execution.Attempt,
		); err != nil {
			return nil, err
		}
		executions = append(executions, execution)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return executions, nil
}

func (s *timescaleRosterAutomationStore) MarkDispatched(ctx context.Context, execution models.RosterAutomationExecution, now time.Time) error {
	_, err := s.pool.Exec(ctx, markRosterAutomationDispatchedSQL, execution.ExecutionID, execution.AutomationID, now)
	return err
}

func (s *timescaleRosterAutomationStore) MarkRetry(ctx context.Context, execution models.RosterAutomationExecution, retryAt time.Time, summary string) error {
	if len(summary) > 1000 {
		summary = summary[:1000]
	}
	_, err := s.pool.Exec(ctx, markRosterAutomationRetrySQL, execution.ExecutionID, retryAt, summary)
	return err
}

const expandDueRosterAutomationsSQL = `
	INSERT INTO roster_automation_executions (
		execution_id, automation_id, roster_id, scheduled_at, status, next_attempt_at
	)
	SELECT
		rule.automation_id || ':' || roster.id::text || ':' || extract(epoch FROM rule.scheduled_at)::bigint,
		rule.automation_id, roster.id, rule.scheduled_at, 'pending', rule.scheduled_at
	FROM roster_automation_rules rule
	JOIN rosters roster ON (
		rule.roster_id = roster.id
		OR (rule.roster_id IS NULL AND rule.group_id IS NOT NULL AND roster.group_id = rule.group_id AND roster.server_id = rule.server_id)
	)
	WHERE rule.enabled = true
	  AND rule.executed = false
	  AND rule.scheduled_at <= $1
	ON CONFLICT (execution_id) DO NOTHING
`

const markUntargetedRosterAutomationsSQL = `
	UPDATE roster_automation_rules rule
	SET executed = true, executed_at = extract(epoch FROM $1)::bigint,
		execution_status = 'missed', last_missed_at = extract(epoch FROM rule.scheduled_at)::bigint,
		updated_at = $1
	WHERE rule.enabled = true
	  AND rule.executed = false
	  AND rule.scheduled_at <= $1
	  AND NOT EXISTS (
		SELECT 1 FROM rosters roster
		WHERE rule.roster_id = roster.id
		   OR (rule.roster_id IS NULL AND rule.group_id IS NOT NULL AND roster.group_id = rule.group_id AND roster.server_id = rule.server_id)
	  )
`

const claimDueRosterAutomationsSQL = `
	WITH candidates AS (
		SELECT execution_id
		FROM roster_automation_executions
		WHERE (
			status = 'pending'
			OR (status = 'dispatching' AND claimed_at < $1 - interval '5 minutes')
		)
		  AND next_attempt_at <= $1
		ORDER BY scheduled_at, execution_id
		FOR UPDATE SKIP LOCKED
		LIMIT $2
	), claimed AS (
		UPDATE roster_automation_executions execution
		SET status = 'dispatching', claimed_at = $1, attempt_count = attempt_count + 1,
			updated_at = $1
		FROM candidates
		WHERE execution.execution_id = candidates.execution_id
		RETURNING execution.*
	)
	SELECT claimed.execution_id, rule.automation_id, rule.server_id,
		roster.id::text, coalesce(rule.group_id, ''), rule.action_type,
		claimed.scheduled_at, coalesce(rule.discord_channel_id, ''), coalesce(rule.ping_type, ''),
		coalesce(roster.webhook_id, ''), coalesce(roster.message_id, ''), roster.alias,
		roster.event_start_time, claimed.attempt_count
	FROM claimed
	JOIN roster_automation_rules rule ON rule.automation_id = claimed.automation_id
	JOIN rosters roster ON roster.id = claimed.roster_id
	ORDER BY claimed.scheduled_at, claimed.execution_id
`

const markRosterAutomationDispatchedSQL = `
	WITH marked AS (
		UPDATE roster_automation_executions
		SET status = 'dispatched', dispatched_at = $3, last_error = NULL, updated_at = $3
		WHERE execution_id = $1 AND status = 'dispatching'
		RETURNING automation_id
	)
	UPDATE roster_automation_rules rule
	SET executed = true, executed_at = extract(epoch FROM $3)::bigint,
		execution_status = 'dispatched', updated_at = $3
	WHERE rule.automation_id = $2
	  AND EXISTS (SELECT 1 FROM marked)
	  AND NOT EXISTS (
		SELECT 1 FROM roster_automation_executions execution
		WHERE execution.automation_id = rule.automation_id
		  AND execution.status IN ('pending', 'dispatching')
	  )
`

const markRosterAutomationRetrySQL = `
	UPDATE roster_automation_executions
	SET status = 'pending', next_attempt_at = $2, last_error = $3,
		claimed_at = NULL, updated_at = now()
	WHERE execution_id = $1 AND status = 'dispatching'
`

type memoryRosterAutomationStore struct {
	due        []models.RosterAutomationExecution
	dispatched []string
	retried    []string
}

func (s *memoryRosterAutomationStore) Close() {}

func (s *memoryRosterAutomationStore) ClaimDue(context.Context, time.Time, int) ([]models.RosterAutomationExecution, error) {
	return append([]models.RosterAutomationExecution(nil), s.due...), nil
}

func (s *memoryRosterAutomationStore) MarkDispatched(_ context.Context, execution models.RosterAutomationExecution, _ time.Time) error {
	s.dispatched = append(s.dispatched, execution.ExecutionID)
	return nil
}

func (s *memoryRosterAutomationStore) MarkRetry(_ context.Context, execution models.RosterAutomationExecution, _ time.Time, _ string) error {
	s.retried = append(s.retried, execution.ExecutionID)
	return nil
}

func rosterAutomationExecutionID(automationID, rosterID string, scheduledAt time.Time) string {
	return fmt.Sprintf("%s:%s:%d", automationID, rosterID, scheduledAt.UTC().Unix())
}
