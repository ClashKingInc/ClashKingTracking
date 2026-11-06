package scripts

import (
	"clashking_tracking/internal/platform"
	"clashking_tracking/models"
	"context"
	"crypto/sha256"
	"fmt"
	"github.com/disgoorg/disgo/discord"
	"time"
)

type discordLegendInterest struct {
	clanTag        string
	clanName       string
	clanBadgeToken string
	playerName     string
	playerTrophies int
	types          map[string]struct{}
}

const discordLegendTargetsSQL = `
 SELECT DISTINCT player.tag, player.clan_tag, player.name, coalesce(player.trophies,0),
   coalesce(clan.name,player.clan_tag), coalesce(clan.badge_token,''), log.type
 FROM basic_player player
 JOIN server_logs log ON log.clan_tag=player.clan_tag
 JOIN servers server ON server.id=log.server_id
 LEFT JOIN basic_clan clan ON clan.tag=player.clan_tag
 WHERE player.league_id=105000036
 AND server.last_command_at >= now()-interval '90 days'
 AND log.disabled=false
 AND log.type IN ('legend_log_attacks','legend_log_defenses')
`

func (s *timescaleBattlelogStore) LoadDiscordLegendTargets(ctx context.Context) (map[string]discordLegendInterest, error) {
	rows, err := s.pool.Query(ctx, discordLegendTargetsSQL)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	targets := make(map[string]discordLegendInterest)
	for rows.Next() {
		var tag, logType string
		var interest discordLegendInterest
		if err := rows.Scan(&tag, &interest.clanTag, &interest.playerName, &interest.playerTrophies, &interest.clanName, &interest.clanBadgeToken, &logType); err != nil {
			return nil, err
		}
		interest.types = targets[tag].types
		if interest.types == nil {
			interest.types = make(map[string]struct{})
		}
		interest.types[logType] = struct{}{}
		targets[tag] = interest
	}
	return targets, rows.Err()
}

// The caller invokes this only after raw SQL commits. Legend I has no usable
// league group: route and display context comes from existing player/clan data.
func publishDiscordLegendEvent(ctx context.Context, app *platform.App, n models.DiscordLegendNotification) error {
	row := n.Battle
	kind := "legend_attack"
	delta := legendAttackTrophies(int(row.Stars), int(row.DestructionPercentage))
	if !row.Attack {
		kind = "legend_defense"
		delta = -delta
	}
	id := fmt.Sprintf("%x", sha256.Sum256([]byte(row.PlayerTag+"\x00"+kind+"\x00"+row.Timestamp.UTC().Format(time.RFC3339Nano))))
	value := map[string]any{
		"type": kind, "event_id": id, "player_tag": row.PlayerTag, "player_name": n.PlayerName,
		"clan":     map[string]any{"tag": n.ClanTag, "name": n.ClanName, "badgeUrls": map[string]any{"large": notificationClanBadgeURL(n.ClanBadgeToken)}},
		"trophies": n.PlayerTrophies, "trophy_change": delta, "battle_time": row.Timestamp.UTC().Format(time.RFC3339Nano),
		"stars": row.Stars, "destruction_percentage": row.DestructionPercentage,
	}
	// Retain the existing Discord Legend stream dedupe namespace and TTL.
	// Only the source changes from unavailable group logs to observed raw rows.
	_, err := platform.AppendEventOnce(ctx, app.Valkey, app.Config, "discord-legend:"+id, 30*24*time.Hour, platform.Event{Topic: "discord_legend", ClanTag: n.ClanTag, Timestamp: row.Timestamp, Value: value})
	return err
}

func discordLegendLogMessages(event mobileWarEvent, logType string, icons discordLogIcons) []discord.MessageCreate {
	clan, _ := mapValue(event.Value["clan"])
	badge, _ := mapValue(clan["badgeUrls"])
	delta := intValue(event.Value["trophy_change"])
	color := 0x2ecc71
	icon := icons.icon("clash_sword", "⚔️")
	if logType == "legend_log_defenses" {
		color = 0xe74c3c
		icon = icons.icon("shield", "🛡️")
		if delta > 0 {
			delta = -delta
		}
	}
	embed := discord.Embed{Description: fmt.Sprintf("%s %+d trophies | %s %d", icon, delta, icons.icon("legendleaguei", icons.icon("Legend League", "🏆")), intValue(event.Value["trophies"])), Color: color, Author: &discord.EmbedAuthor{Name: stringMapValue(event.Value, "player_name") + " | " + stringMapValue(clan, "name"), IconURL: stringMapValue(badge, "large")}}
	return []discord.MessageCreate{{Embeds: []discord.Embed{embed}}}
}
