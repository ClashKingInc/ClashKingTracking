package scripts

import (
	"context"
	"sort"
	"time"

	clashy "github.com/clashkinginc/clashy.go"
	"github.com/jackc/pgx/v5/pgconn"
)

type activityExecutor interface {
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
}

// last_active is qualifying attack activity, not a last-login timestamp.
// Repeated snapshots do not write unless their source timestamp advances.
func advancePlayerActivity(ctx context.Context, db activityExecutor, activity map[string]time.Time) error {
	tags := make([]string, 0, len(activity))
	for tag, at := range activity {
		if tag != "" && !at.IsZero() && !at.After(time.Now().Add(time.Minute)) {
			tags = append(tags, tag)
		}
	}
	if len(tags) == 0 {
		return nil
	}
	sort.Strings(tags)
	times := make([]time.Time, len(tags))
	for i, tag := range tags {
		times[i] = activity[tag].UTC()
	}
	_, err := db.Exec(ctx, `UPDATE basic_player player SET last_active = observed.at
		FROM unnest($1::text[], $2::timestamptz[]) observed(tag, at)
		WHERE player.tag = observed.tag
		AND (player.last_active IS NULL OR player.last_active < observed.at)`, tags, times)
	return err
}

func warPlayerActivity(war clashy.ClanWar) map[string]time.Time {
	activity := map[string]time.Time{}
	if war.EndTime == nil || (war.State != "warEnded" && war.State != "ended") {
		return activity
	}
	for _, clan := range []*clashy.WarClan{war.Clan, war.Opponent} {
		if clan == nil {
			continue
		}
		for _, member := range clan.Members {
			if len(member.Attacks) > 0 {
				activity[member.Tag] = war.EndTime.Time
			}
		}
	}
	return activity
}
