package scripts

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"clashking_tracking/internal/platform"
	clashy "github.com/clashkinginc/clashy.go"
)

// A shared API request is delivered by PostgreSQL NOTIFY to the existing
// worker. It is intentionally best effort: there is no durable request queue.
func (d *remindersDomain) processManualReminder(ctx context.Context, app *platform.App, serverID, reminderID string) error {
	var reminderRaw []byte
	var typ, clanTag, rosterID string
	err := d.pool.QueryRow(ctx, `SELECT `+discordReminderPayloadSQL+`, reminder.type_name, reminder.clan_tag, COALESCE(reminder.roster_id, '')
		FROM reminders reminder
		WHERE reminder.id=$1::uuid AND reminder.server_id=$2 AND reminder.disabled=false
          AND reminder.type<>4
          AND reminder.type_name IN ('War', 'Clan Capital', 'Clan Games', 'roster')`, reminderID, serverID).Scan(&reminderRaw, &typ, &clanTag, &rosterID)
	if err != nil {
		return fmt.Errorf("load manual reminder: %w", err)
	}
	var reminder map[string]any
	if err := json.Unmarshal(reminderRaw, &reminder); err != nil {
		return err
	}
	clan, err := fetchReminderClan(ctx, app, d.limiter, clanTag)
	if err != nil || clan == nil {
		return fmt.Errorf("fetch reminder clan: %w", err)
	}
	value := map[string]any{"manual": true, "clan": clan, "reminder": reminder}
	roles, halls := reminderStringList(reminder["roles"]), reminderIntList(reminder["town_halls"])
	switch typ {
	case "War":
		war, err := retryLimitedClashFetch(ctx, app, d.limiter, func(fetchCtx context.Context) (*clashy.ClanWar, error) {
			return app.Clash.GetCurrentWar(fetchCtx, clanTag)
		})
		if err != nil || war == nil || war.Clan == nil || war.Opponent == nil {
			return errors.New("current war unavailable")
		}
		if !warReminderAvailable(war, time.Now()) {
			return nil
		}
		if !manualWarTypeAllowed(reminderStringList(reminder["war_types"]), war.Type()) {
			return nil
		}
		limit := 2
		if war.Type() == "cwl" {
			limit = 1
		}
		threshold := intValue(reminder["trigger_threshold"])
		clanMembers := make(map[string]clashy.ClanMember, len(clan.Members))
		for _, member := range clan.Members {
			clanMembers[member.Tag] = member
		}
		var members []map[string]any
		for _, member := range war.Clan.Members {
			live, found := clanMembers[member.Tag]
			used := len(member.Attacks)
			if war.State == "preparation" {
				used = 0
			}
			if !found || !clanMemberEligible(live, roles, halls) || used >= limit-threshold {
				continue
			}
			members = append(members, map[string]any{"tag": member.Tag, "name": member.Name, "townhall": member.Townhall, "role": live.Role, "attacks": used, "total_attacks": limit})
		}
		if len(members) == 0 {
			return nil
		}
		value["type"], value["data"], value["members"] = "war", war, members
	case "Clan Capital":
		raid, ok := loadCachedRaid(ctx, app, clanTag)
		if !ok {
			raid, ok, err = fetchReminderRaid(ctx, app, d.limiter, clanTag)
		}
		if err != nil || !ok {
			return errors.New("current raid unavailable")
		}
		if raid.EndTime == nil || !raid.EndTime.Time.After(time.Now()) {
			return nil
		}
		config := raidReminder{ID: reminderID, Roles: roles, TownHalls: halls, AttackThreshold: intValue(reminder["trigger_threshold"])}
		members := raidMissingMembers(*clan, raid, config)
		if len(members) == 0 {
			return nil
		}
		value["type"], value["raid"], value["members"] = "raid", raid, members
	case "Clan Games":
		members, err := d.manualClanGamesMembers(ctx, clanTag, roles, halls, intValue(reminder["trigger_threshold"]), *clan)
		if err != nil {
			return err
		}
		if len(members) == 0 {
			return nil
		}
		value["type"], value["members"] = "clan_games", members
	case "roster":
		roster, members, err := d.manualRosterMembers(ctx, reminderID, rosterID)
		if err != nil {
			return err
		}
		if len(members) == 0 {
			return nil
		}
		value["type"], value["roster"], value["members"] = "roster", roster, members
	default:
		return fmt.Errorf("unsupported manual reminder type %q", typ)
	}
	return app.PublishEvent(ctx, platform.Event{Topic: "reminder", ClanTag: clanTag, Value: value})
}

func manualWarTypeAllowed(configured []string, actual string) bool {
	if len(configured) == 0 {
		return true
	}
	for _, value := range configured {
		if strings.EqualFold(value, actual) || strings.EqualFold(value, "regular") && actual == "random" {
			return true
		}
	}
	return false
}

func (d *remindersDomain) manualClanGamesMembers(ctx context.Context, clanTag string, roles []string, halls []int, threshold int, clan clashy.Clan) ([]map[string]any, error) {
	start := time.Date(time.Now().UTC().Year(), time.Now().UTC().Month(), 22, 8, 0, 0, 0, time.UTC)
	rows, err := d.pool.Query(ctx, `SELECT member->>'tag', COALESCE(sum(change.delta),0)::bigint FROM basic_clan clan
		CROSS JOIN LATERAL jsonb_array_elements(clan.members) member
		LEFT JOIN player_stat_changes change ON change.player_tag=member->>'tag' AND change.stat_type='clan_games' AND change.event_time >= $2
		WHERE clan.tag=$1 GROUP BY member->>'tag'`, clanTag, start)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	points := map[string]int64{}
	for rows.Next() {
		var tag string
		var count int64
		if err := rows.Scan(&tag, &count); err != nil {
			return nil, err
		}
		points[tag] = count
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	var members []map[string]any
	for _, member := range clan.Members {
		if !clanMemberEligible(member, roles, halls) || points[member.Tag] > int64(threshold) {
			continue
		}
		members = append(members, map[string]any{"tag": member.Tag, "name": member.Name, "townhall": member.TownHall, "role": member.Role, "points": points[member.Tag]})
	}
	return members, nil
}

func (d *remindersDomain) manualRosterMembers(ctx context.Context, reminderID, rosterID string) (map[string]any, []map[string]any, error) {
	var alias, clanTag string
	var start *int64
	err := d.pool.QueryRow(ctx, `SELECT roster.alias, COALESCE(roster.clan_tag,''), roster.event_start_time FROM rosters roster
		JOIN reminders reminder ON reminder.roster_id=roster.id::text AND reminder.server_id=roster.server_id
		WHERE reminder.id=$1::uuid AND roster.id=$2::uuid`, reminderID, rosterID).Scan(&alias, &clanTag, &start)
	if err != nil {
		return nil, nil, err
	}
	rows, err := d.pool.Query(ctx, `SELECT tag,name,townhall,COALESCE(discord_user_id,'') FROM roster_members WHERE roster_id=$1::uuid ORDER BY position,tag`, rosterID)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	var members []map[string]any
	for rows.Next() {
		var tag, name, user string
		var hall int
		if err := rows.Scan(&tag, &name, &hall, &user); err != nil {
			return nil, nil, err
		}
		members = append(members, map[string]any{"tag": tag, "name": name, "townhall": hall, "discord_user_id": user})
	}
	if err := rows.Err(); err != nil {
		return nil, nil, err
	}
	return map[string]any{"id": rosterID, "alias": alias, "clan_tag": clanTag, "event_start_time": start}, members, nil
}
