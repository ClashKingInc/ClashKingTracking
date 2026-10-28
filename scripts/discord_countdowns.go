package scripts

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	clashy "github.com/clashkinginc/clashy.go"
	"github.com/disgoorg/disgo/discord"
	"github.com/disgoorg/disgo/rest"
	"github.com/disgoorg/snowflake/v2"
)

type countdownChannel struct{ Server, Clan, Kind, Channel string }
type countdownWar struct {
	War *clashy.ClanWar
	Err error
}

func (w *discordDeliveryWorker) refreshCountdownChannels(ctx context.Context, now time.Time) error {
	rows, err := w.pool.Query(ctx, `SELECT c.server_id,COALESCE(c.clan_tag,''),c.type,c.channel_id
 FROM server_countdowns c JOIN servers s ON s.id=c.server_id
 WHERE s.left_at IS NULL AND (c.clan_tag IS NULL OR EXISTS(SELECT 1 FROM server_clans sc WHERE sc.server_id=c.server_id AND sc.tag=c.clan_tag))
 ORDER BY c.server_id,c.channel_id`)
	if err != nil {
		return err
	}
	var channels []countdownChannel
	for rows.Next() {
		var c countdownChannel
		if err = rows.Scan(&c.Server, &c.Clan, &c.Kind, &c.Channel); err != nil {
			rows.Close()
			return err
		}
		channels = append(channels, c)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	wars := map[string]countdownWar{}
	for _, c := range channels {
		if !w.app.Config.AllowsDiscordGuild(c.Server) {
			continue
		}
		channelCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
		target, err := w.countdownText(channelCtx, c, now, wars)
		if err == nil && target != "" {
			err = w.renameCountdown(channelCtx, c, target)
		}
		cancel()
		if err != nil {
			w.app.Logger.Warn("countdown update failed", "server_id", c.Server, "channel_id", c.Channel, "err", err)
		}
	}
	return nil
}

func (w *discordDeliveryWorker) countdownText(ctx context.Context, c countdownChannel, now time.Time, wars map[string]countdownWar) (string, error) {
	now = now.UTC()
	if c.Kind == "war_timer" || c.Kind == "war_score" {
		if c.Clan == "" || w.app.Clash == nil {
			return "", fmt.Errorf("war countdown requires clan provider")
		}
		result, ok := wars[c.Clan]
		if !ok {
			result.War, result.Err = w.app.Clash.GetCurrentWar(ctx, c.Clan)
			wars[c.Clan] = result
		}
		if result.Err != nil {
			var private *clashy.PrivateWarLog
			if errors.As(result.Err, &private) {
				return "War log private", nil
			}
			return "", result.Err
		}
		war := result.War
		if war == nil || war.State == clashy.WarStateNotInWar {
			// A private regular log with no visible CWL can also return nil.
			// Absence of a visible war does not prove the clan is idle.
			return "No visible war", nil
		}
		if war.State == clashy.WarStatePreparation {
			if war.StartTime == nil {
				return "", fmt.Errorf("war start missing")
			}
			return "Starts in " + countdownDuration(war.StartTime.Time.Sub(now)), nil
		}
		if c.Kind == "war_score" {
			if war.Clan == nil || war.Opponent == nil {
				return "", fmt.Errorf("war sides missing")
			}
			return fmt.Sprintf("%d ⭐ | %d ⭐", war.Clan.Stars, war.Opponent.Stars), nil
		}
		if war.State == clashy.WarStateEnded {
			return "War ended", nil
		}
		if war.EndTime == nil {
			return "", fmt.Errorf("war end missing")
		}
		return "Ends in " + countdownDuration(war.EndTime.Time.Sub(now)), nil
	}
	switch c.Kind {
	case "clan_games_timer":
		return monthlyCountdown(now, "CG", 22, 28), nil
	case "cwl_timer":
		return monthlyCountdown(now, "CWL signup", 1, 3), nil
	case "raid_weekend_timer":
		start := time.Date(now.Year(), now.Month(), now.Day(), 7, 0, 0, 0, time.UTC).AddDate(0, 0, -(int(now.Weekday())+2)%7)
		if now.Before(start) {
			start = start.AddDate(0, 0, -7)
		}
		end := start.AddDate(0, 0, 3)
		if now.Before(end) {
			return "Raids end in " + countdownDuration(end.Sub(now)), nil
		}
		return "Raids in " + countdownDuration(start.AddDate(0, 0, 7).Sub(now)), nil
	case "season_end_timer":
		return "EOS in " + countdownDuration(clashy.GetSeason(now, false).EndTime.Sub(now)), nil
	case "season_day_timer":
		s := clashy.GetSeason(now, false)
		return fmt.Sprintf("Day %d/%d", int(now.Sub(s.StartTime).Hours()/24)+1, int(s.EndTime.Sub(s.StartTime).Hours()/24)), nil
	default:
		return "", nil
	}
}
func monthlyCountdown(now time.Time, label string, startDay, endDay int) string {
	start := time.Date(now.Year(), now.Month(), startDay, 8, 0, 0, 0, time.UTC)
	end := time.Date(now.Year(), now.Month(), endDay, 8, 0, 0, 0, time.UTC)
	if !now.Before(start) && now.Before(end) {
		return label + " ends in " + countdownDuration(end.Sub(now))
	}
	if !now.Before(end) {
		start = start.AddDate(0, 1, 0)
	}
	return label + " in " + countdownDuration(start.Sub(now))
}
func countdownDuration(d time.Duration) string {
	minutes := max(0, int(d.Minutes()))
	if minutes >= 1440 {
		return fmt.Sprintf("%dd %dh", minutes/1440, (minutes%1440)/60)
	}
	return fmt.Sprintf("%dh %dm", minutes/60, minutes%60)
}
func (w *discordDeliveryWorker) renameCountdown(ctx context.Context, c countdownChannel, target string) error {
	id, err := snowflake.Parse(c.Channel)
	if err != nil {
		return err
	}
	channel, err := w.client.Rest.GetChannel(id, rest.WithCtx(ctx))
	if err != nil {
		return err
	}
	voice, ok := channel.(discord.GuildVoiceChannel)
	if !ok || voice.GuildID().String() != c.Server {
		return fmt.Errorf("countdown destination is not a voice channel in its server")
	}
	name := target
	// Preserve the old optional custom prefix while replacing only timer text.
	if c.Clan != "" {
		if prefix, _, found := strings.Cut(voice.Name(), ":"); found {
			name = strings.TrimSpace(prefix) + ": " + target
		}
	} else if prefix, _, found := strings.Cut(voice.Name(), "|"); found {
		name = strings.TrimSpace(prefix) + " | " + target
	}
	name = string([]rune(name)[:min(100, len([]rune(name)))])
	// Discord rate-limits channel renames tightly. Compare the live name and
	// scan no more than once per fifteen minutes; never enqueue rename retries.
	if voice.Name() == name {
		return nil
	}
	_, err = w.client.Rest.UpdateChannel(id, discord.GuildVoiceChannelUpdate{Name: &name}, rest.WithCtx(ctx))
	return err
}
