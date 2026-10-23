package scripts

import (
	"clashking_tracking/models"
	"context"
	"fmt"
	clashy "github.com/clashkinginc/clashy.go"
	"github.com/jackc/pgx/v5/pgxpool"
)

type roleRefreshSink interface {
	QueueRoleRefresh(context.Context, string, string, string) error
}

// Coalesce event bursts by server/member. A pending leave opens one grace
// window; other changes cannot shorten it or repeatedly restart the timer.
// Joining immediately reevaluates current accounts, cancelling stale departures.
const queueAutoRoleRefreshSQL = `
 INSERT INTO role_refresh_jobs(id,server_id,user_id,due_at,grace_until)
 SELECT 'auto:' || server.id || ':' || link.user_id,server.id,link.user_id,
   CASE WHEN $3='member_leave' THEN now()+make_interval(mins=>server.role_leave_delay_minutes) ELSE now() END,
   CASE WHEN $3='member_leave' THEN now()+make_interval(mins=>server.role_leave_delay_minutes) ELSE NULL END
 FROM servers server JOIN server_clans clan ON clan.server_id=server.id AND clan.tag=$2
 JOIN player_links link ON link.tag=$1 AND link.hidden=false AND link.user_id IS NOT NULL
 WHERE server.autoeval_enabled AND server.left_at IS NULL
 ON CONFLICT(id) DO UPDATE SET
   due_at=CASE WHEN role_refresh_jobs.completed_at IS NOT NULL THEN EXCLUDED.due_at
     WHEN $3='member_join' THEN now()
     WHEN role_refresh_jobs.grace_until IS NOT NULL THEN role_refresh_jobs.grace_until
     ELSE EXCLUDED.due_at END,
   grace_until=CASE WHEN role_refresh_jobs.completed_at IS NOT NULL THEN EXCLUDED.grace_until
     WHEN $3='member_join' THEN NULL ELSE COALESCE(role_refresh_jobs.grace_until,EXCLUDED.grace_until) END,
   revision=role_refresh_jobs.revision+1,attempts=0,completed_at=NULL,result=NULL,updated_at=now()
`

func queueAutoRoleRefresh(ctx context.Context, pool *pgxpool.Pool, tag, clanTag, trigger string) error {
	_, err := pool.Exec(ctx, queueAutoRoleRefreshSQL, tag, clanTag, trigger)
	if err != nil {
		return fmt.Errorf("queue automatic role refresh: %w", err)
	}
	return nil
}
func (s *timescaleTrackedClanStore) QueueRoleRefresh(ctx context.Context, tag, clanTag, trigger string) error {
	return queueAutoRoleRefresh(ctx, s.pool, tag, clanTag, trigger)
}
func (s *timescaleTrackedPlayerStore) QueueRoleRefresh(ctx context.Context, tag, clanTag, trigger string) error {
	return queueAutoRoleRefresh(ctx, s.pool, tag, clanTag, trigger)
}
func queuePlayerRoleRefreshes(ctx context.Context, store trackedPlayerStore, ingest models.TrackedPlayerIngest) error {
	sink, ok := store.(roleRefreshSink)
	if !ok {
		return nil
	}
	if ingest.CurrentClan == "" {
		return nil
	}
	return sink.QueueRoleRefresh(ctx, ingest.SnapshotTag, ingest.CurrentClan, "account_update")
}

func queueClanRoleRefreshes(ctx context.Context, store trackedClanStore, previous, current clashy.Clan) error {
	sink, ok := store.(roleRefreshSink)
	if !ok {
		return nil
	}
	joined, left := trackedClanMemberChanges(previous, current)
	for _, member := range joined {
		if err := sink.QueueRoleRefresh(ctx, member.Tag, current.Tag, "member_join"); err != nil {
			return err
		}
	}
	for _, member := range left {
		if err := sink.QueueRoleRefresh(ctx, member.Tag, current.Tag, "member_leave"); err != nil {
			return err
		}
	}
	old := map[string]clashy.ClanMember{}
	for _, member := range previous.Members {
		old[member.Tag] = member
	}
	for _, member := range current.Members {
		if before, exists := old[member.Tag]; exists && before.Role != member.Role {
			if err := sink.QueueRoleRefresh(ctx, member.Tag, current.Tag, "role_change"); err != nil {
				return err
			}
		}
	}
	return nil
}
