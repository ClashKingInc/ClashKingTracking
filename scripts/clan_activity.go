package scripts

import (
	"context"
	"sort"
	"time"

	clashy "github.com/clashkinginc/clashy.go"
)

// Only a completed war stored by the global finalizer qualifies. Both known
// sides use the game end time, so discovery and repeated fetches cannot extend
// priority. Regular, friendly, and CWL wars share this contract.
func warClanActivity(war clashy.ClanWar, now time.Time) map[string]time.Time {
	if war.Clan == nil || war.Opponent == nil || war.Clan.Tag == "" ||
		war.Opponent.Tag == "" || war.Clan.Tag == war.Opponent.Tag || war.TeamSize <= 0 ||
		war.PreparationStartTime == nil || war.EndTime == nil {
		return nil
	}
	prepAt, endAt := war.PreparationStartTime.Time.UTC(), war.EndTime.Time.UTC()
	if prepAt.IsZero() || prepAt.After(now) || !endAt.After(prepAt) {
		return nil
	}
	if war.State != clashy.WarStateEnded || endAt.After(now) {
		return nil
	}
	return map[string]time.Time{war.Clan.Tag: endAt, war.Opponent.Tag: endAt}
}

func advanceClanActivity(ctx context.Context, db activityExecutor, activity map[string]time.Time) error {
	tags := make([]string, 0, len(activity))
	now := time.Now().UTC()
	for tag, at := range activity {
		if tag != "" && !at.IsZero() && !at.After(now) {
			tags = append(tags, tag)
		}
	}
	if len(tags) == 0 {
		return nil
	}
	// One batch inside the existing war transaction avoids a connection or
	// query per clan. The predicate skips duplicate/older wars
	// and preserves newer activity when discoveries or finalizers race.
	sort.Strings(tags)
	times := make([]time.Time, len(tags))
	for i, tag := range tags {
		times[i] = activity[tag].UTC()
	}
	_, err := db.Exec(ctx, `
		UPDATE basic_clan clan
		SET last_active = observed.at
		FROM unnest($1::text[], $2::timestamptz[]) observed(tag, at)
		WHERE clan.tag = observed.tag
		  AND (clan.last_active IS NULL OR clan.last_active < observed.at)
	`, tags, times)
	return err
}
