package scripts

import (
	"clashking_tracking/internal/platform"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	clashy "github.com/clashkinginc/clashy.go"
	"github.com/disgoorg/disgo/discord"
	valkey "github.com/valkey-io/valkey-go"
	"strconv"
	"time"
)

type discordLegendSnapshot struct {
	UpdatedAt time.Time `json:"updated_at"`
	IDs       []string  `json:"ids"`
}

type discordLegendInterest struct {
	clanTag string
	types   map[string]struct{}
}

const discordLegendTargetsSQL = `
	SELECT DISTINCT player.tag, player.clan_tag, log.type
	FROM basic_player player
	JOIN server_logs log ON log.clan_tag = player.clan_tag
	JOIN servers server ON server.id = log.server_id
	WHERE player.league_id = 105000036
	  AND server.last_command_at >= now() - interval '90 days'
	  AND log.disabled = false
	  AND log.type IN ('legend_log_attacks', 'legend_log_defenses')
`

func (s *timescaleBattlelogStore) LoadDiscordLegendTargets(ctx context.Context) (map[string]discordLegendInterest, error) {
	rows, err := s.pool.Query(ctx, discordLegendTargetsSQL)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	targets := make(map[string]discordLegendInterest)
	for rows.Next() {
		var tag, clanTag, logType string
		if err := rows.Scan(&tag, &clanTag, &logType); err != nil {
			return nil, err
		}
		interest := targets[tag]
		interest.clanTag = clanTag
		if interest.types == nil {
			interest.types = make(map[string]struct{})
		}
		interest.types[logType] = struct{}{}
		targets[tag] = interest
	}
	return targets, rows.Err()
}

func discordLegendBattleID(tag, kind string, battle clashy.LeagueTierGroupBattleLogEntry) string {
	return fmt.Sprintf("%x", sha256.Sum256([]byte(tag+"\x00"+kind+"\x00"+battle.CreationTime+"\x00"+battle.OpponentPlayerTag)))
}

func discordLegendEventValue(player clashy.Player, kind, id string, battle clashy.LeagueTierGroupBattleLogEntry) map[string]any {
	return map[string]any{
		"type": kind, "event_id": id, "player_tag": player.Tag, "player_name": player.Name,
		"clan": player.Clan, "trophies": player.Trophies,
		"trophy_change": battle.Trophies, "battle_time": battle.CreationTime,
		"stars": battle.Stars, "destruction_percentage": battle.DestructionPercentage,
	}
}

// The group API gives individual battle trophy deltas. Comparing profile trophy
// totals would merge simultaneous attacks and defenses and lose zero-trophy hits.
func pollDiscordLegendLogs(ctx context.Context, app *platform.App, player clashy.Player, interests map[string]struct{}, limiter *clashy.Limiter) error {
	_, attacks := interests["legend_log_attacks"]
	_, defenses := interests["legend_log_defenses"]
	if (!attacks && !defenses) || player.Clan == nil || player.CurrentLeagueGroupTag == "" || player.CurrentLeagueSeasonID == 0 || !discordLegendTier(player.LeagueTier.ID) || app.Valkey == nil {
		return nil
	}
	key := app.Config.EventStreamName + ":discord-legend-snapshot:" + player.Tag + ":" + player.CurrentLeagueGroupTag + ":" + strconv.Itoa(player.CurrentLeagueSeasonID)
	raw, err := app.Valkey.Do(ctx, app.Valkey.B().Get().Key(key).Build()).ToString()
	if err != nil && !valkey.IsValkeyNil(err) {
		return err
	}
	var previous discordLegendSnapshot
	exists := err == nil
	if exists {
		if err := json.Unmarshal([]byte(raw), &previous); err != nil {
			return err
		}
		if time.Since(previous.UpdatedAt) < 30*time.Second {
			return nil
		}
	}
	group, err := retryLimitedClashFetch(ctx, app, limiter, func(fetchCtx context.Context) (*clashy.LeagueTierGroup, error) {
		return app.Clash.GetPlayerLeagueGroup(fetchCtx, player.Tag, player.CurrentLeagueGroupTag, strconv.Itoa(player.CurrentLeagueSeasonID))
	})
	if err != nil {
		return err
	}
	if group == nil {
		return nil
	}
	known := map[string]bool{}
	for _, id := range previous.IDs {
		known[id] = true
	}
	current := discordLegendSnapshot{UpdatedAt: time.Now().UTC()}
	for _, side := range []struct {
		kind    string
		enabled bool
		items   []clashy.LeagueTierGroupBattleLogEntry
	}{{"legend_attack", attacks, group.AttackLogs}, {"legend_defense", defenses, group.DefenseLogs}} {
		for _, battle := range side.items {
			if battle.CreationTime == "" {
				continue
			}
			id := discordLegendBattleID(player.Tag, side.kind, battle)
			current.IDs = append(current.IDs, id)
			if !exists || known[id] || !side.enabled {
				continue
			}
			// A separate topic avoids duplicating the mobile defense notification.
			value := discordLegendEventValue(player, side.kind, id, battle)
			if _, err := platform.AppendEventOnce(ctx, app.Valkey, app.Config, "discord-legend:"+id, 30*24*time.Hour, platform.Event{Topic: "discord_legend", ClanTag: player.Clan.Tag, Value: value}); err != nil {
				return err
			}
		}
	}
	encoded, err := json.Marshal(current)
	if err != nil {
		return err
	}
	return app.Valkey.Do(ctx, app.Valkey.B().Set().Key(key).Value(string(encoded)).Ex(30*24*time.Hour).Build()).Error()
}

func discordLegendTier(id int) bool { return id == 105000036 }

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
