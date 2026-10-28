package scripts

import (
	"context"
	"fmt"
	"strings"
	"time"

	clashy "github.com/clashkinginc/clashy.go"
	"github.com/disgoorg/disgo/discord"
	"github.com/disgoorg/disgo/rest"
	"github.com/disgoorg/snowflake/v2"
)

// Polling also recovers an active war after downtime. Discord is the source of
// truth for already-created events; no occurrence ledger or delivery lease.
func (w *discordDeliveryWorker) refreshWarEvents(ctx context.Context, now time.Time, wars map[string]countdownWar, onlyClan string) error {
	rows, err := w.pool.Query(ctx, `SELECT c.server_id,c.tag FROM server_clans c JOIN servers s ON s.id=c.server_id WHERE c.war_events_enabled AND s.left_at IS NULL AND ($1='' OR c.tag=$1) ORDER BY c.server_id,c.tag`, onlyClan)
	if err != nil {
		return err
	}
	type target struct{ server, clan string }
	var targets []target
	for rows.Next() {
		var t target
		if err = rows.Scan(&t.server, &t.clan); err != nil {
			rows.Close()
			return err
		}
		targets = append(targets, t)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	for _, t := range targets {
		if !w.app.Config.AllowsDiscordGuild(t.server) {
			continue
		}
		itemCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
		err = w.refreshWarEvent(itemCtx, t.server, t.clan, now, wars)
		cancel()
		if err != nil {
			w.app.Logger.Warn("war scheduled event update failed", "server_id", t.server, "clan_tag", t.clan, "err", err)
		}
	}
	return nil
}
func (w *discordDeliveryWorker) refreshWarEvent(ctx context.Context, server, clan string, now time.Time, wars map[string]countdownWar) error {
	if w.app.Clash == nil {
		return fmt.Errorf("war event requires clan provider")
	}
	result, ok := wars[clan]
	if !ok {
		result.War, result.Err = w.app.Clash.GetCurrentWar(ctx, clan)
		wars[clan] = result
	}
	if result.Err != nil {
		return result.Err
	}
	war := result.War
	if war == nil || (war.State != clashy.WarStatePreparation && war.State != clashy.WarStateInWar) {
		return nil
	}
	if war.Clan == nil || war.Opponent == nil || war.Clan.Tag != clan || war.PreparationStartTime == nil || war.StartTime == nil || war.EndTime == nil {
		return fmt.Errorf("war identity or times missing")
	}
	if war.PreparationStartTime.Time.After(now) || !war.EndTime.Time.After(now.Add(time.Minute)) {
		return nil
	}
	guild, err := snowflake.Parse(server)
	if err != nil {
		return err
	}
	marker := fmt.Sprintf("ClashKing war: %s/%d", clan, war.PreparationStartTime.Time.Unix())
	events, err := w.client.Rest.GetGuildScheduledEvents(guild, false, rest.WithCtx(ctx))
	if err != nil {
		return err
	}
	for _, event := range events {
		// Creator and guild checks protect human events even if their description
		// happens to contain our marker. Never recreate a cancelled/completed event.
		if event.GuildID == guild && event.CreatorID == w.client.ApplicationID && strings.HasSuffix(event.Description, marker) {
			if event.Status != discord.ScheduledEventStatusScheduled && event.Status != discord.ScheduledEventStatusActive {
				return nil
			}
			if event.ScheduledEndTime == nil || !event.ScheduledEndTime.Equal(war.EndTime.Time) {
				_, err = w.client.Rest.UpdateGuildScheduledEvent(guild, event.ID, discord.GuildScheduledEventUpdate{ScheduledEndTime: &war.EndTime.Time}, rest.WithCtx(ctx))
			}
			return err
		}
	}
	// Re-read the opt-in after network reads so a disabled/unlinked clan cannot
	// publish from a stale target snapshot. Delivery remains best effort.
	var enabled bool
	err = w.pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM server_clans c JOIN servers s ON s.id=c.server_id WHERE c.server_id=$1 AND c.tag=$2 AND c.war_events_enabled AND s.left_at IS NULL)`, server, clan).Scan(&enabled)
	if err != nil || !enabled {
		return err
	}
	kind := "Regular"
	if war.Type() == "cwl" {
		kind = "CWL"
	}
	cut := func(value string, n int) string {
		runes := []rune(value)
		if len(runes) > n {
			return string(runes[:n])
		}
		return value
	}
	name := cut(fmt.Sprintf("%s war · %s", kind, war.Clan.Name), 100)
	description := fmt.Sprintf("%s vs %s\nWar size: %d v %d\nBattle starts: <t:%d:F>\nWar ends: <t:%d:F>\n%s", cut(war.Clan.Name, 100), cut(war.Opponent.Name, 100), war.TeamSize, war.TeamSize, war.StartTime.Time.Unix(), war.EndTime.Time.Unix(), marker)
	_, err = w.client.Rest.CreateGuildScheduledEvent(guild, discord.GuildScheduledEventCreate{Name: name, Description: description, PrivacyLevel: discord.ScheduledEventPrivacyLevelGuildOnly, EntityType: discord.ScheduledEventEntityTypeExternal, EntityMetaData: &discord.EntityMetaData{Location: cut(war.Clan.Name+" vs "+war.Opponent.Name, 100)}, ScheduledStartTime: now.Add(30 * time.Second), ScheduledEndTime: &war.EndTime.Time}, rest.WithCtx(ctx))
	return err
}
