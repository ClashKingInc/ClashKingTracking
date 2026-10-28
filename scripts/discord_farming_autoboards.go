package scripts

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"time"

	clashy "github.com/clashkinginc/clashy.go"
	"github.com/disgoorg/disgo/discord"
	"github.com/disgoorg/disgo/rest"
	"github.com/disgoorg/snowflake/v2"
)

type farmingBoard struct {
	ID, ServerID, WebhookID, ThreadID, MessageID, ClanTag, Window, Zone string
	Limit, Seconds                                                      int
	Started, Revision                                                   time.Time
}

func (w *discordDeliveryWorker) refreshDueFarmingBoards(ctx context.Context) error {
	rows, err := w.pool.Query(ctx, `SELECT a.id::text,a.server_id,a.webhook_id,COALESCE(a.thread_id,''),COALESCE(a.message_id,''),
 t.target,f.window_mode,f.time_zone,a.row_limit,COALESCE(a.interval_seconds,a.interval_minutes*60,30),f.counting_from,a.updated_at
 FROM autoboards a JOIN autoboard_farming_settings f ON f.autoboard_id=a.id
 JOIN autoboard_targets t ON t.autoboard_id=a.id AND t.position=0
 WHERE a.enabled AND a.board_type='farming' AND a.delivery_mode='refresh' AND a.next_run_at<=now()
 ORDER BY a.next_run_at,a.id LIMIT 20`)
	if err != nil {
		return err
	}
	var boards []farmingBoard
	for rows.Next() {
		var b farmingBoard
		if err = rows.Scan(&b.ID, &b.ServerID, &b.WebhookID, &b.ThreadID, &b.MessageID, &b.ClanTag, &b.Window, &b.Zone, &b.Limit, &b.Seconds, &b.Started, &b.Revision); err != nil {
			rows.Close()
			return err
		}
		boards = append(boards, b)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	for _, b := range boards {
		if !w.app.Config.AllowsDiscordGuild(b.ServerID) {
			continue
		}
		if err = w.deliverFarmingBoard(ctx, b); err != nil {
			w.app.Logger.Error("farming autoboard delivery failed", "board_id", b.ID, "err", err)
			if _, updateErr := w.pool.Exec(ctx, `UPDATE autoboards SET next_run_at=now()+interval '1 minute' WHERE id=$1::uuid AND updated_at=$2`, b.ID, b.Revision); updateErr != nil {
				return updateErr
			}
		}
	}
	return nil
}

func (w *discordDeliveryWorker) deliverFarmingBoard(ctx context.Context, b farmingBoard) error {
	if err := w.refreshLogEmojis(time.Now()); err != nil {
		return err
	}
	// A current roster is essential: a stale snapshot can silently credit departed
	// members. Unlike historic clan attribution, this board ranks today's members.
	clan, err := fetchClan(w.app)(ctx, b.ClanTag)
	if err != nil {
		return err
	}
	if clan == nil {
		return fmt.Errorf("clan roster unavailable")
	}
	now := time.Now().UTC()
	start := b.Started
	zone, err := time.LoadLocation(b.Zone)
	if err != nil {
		return err
	}
	switch b.Window {
	case "calendar_month":
		local := now.In(zone)
		start = time.Date(local.Year(), local.Month(), 1, 0, 0, 0, 0, zone).UTC()
	case "game_season":
		start = clashy.GetSeason(now, false).StartTime
	}
	tags := make([]string, 0, len(clan.Members))
	names := map[string]string{}
	for _, member := range clan.Members {
		tags = append(tags, member.Tag)
		names[member.Tag] = member.Name
	}
	rows, err := w.pool.Query(ctx, `SELECT player_tag,gold::text,elixir::text,dark_elixir::text,total_loot::text,attacks,timed_attacks,attack_seconds::float8,session_seconds::float8
 FROM farming_summary($1,$2,$3) WHERE total_loot>0 ORDER BY total_loot DESC,player_tag LIMIT $4`, tags, start, now, max(1, min(5, b.Limit)))
	if err != nil {
		return err
	}
	icons := discordLogIcons(w.emojis)
	fields := []discord.EmbedField{}
	for rows.Next() {
		var tag, gold, elixir, dark, total string
		var attacks, timed int64
		var attackSeconds, sessionSeconds float64
		if err = rows.Scan(&tag, &gold, &elixir, &dark, &total, &attacks, &timed, &attackSeconds, &sessionSeconds); err != nil {
			rows.Close()
			return err
		}
		value := fmt.Sprintf("**%s** total loot · **%d** attacks\n%s **%s** · %s **%s** · %s **%s**", farmingCompact(total), attacks,
			icons.icon("full_gold", icons.icon("gold", "🟡")), farmingCompact(gold), icons.icon("full_elixir", icons.icon("elixir", "🟣")), farmingCompact(elixir), icons.icon("full_dark_elixir", icons.icon("dark_elixir", "⚫")), farmingCompact(dark))
		totalNumber, _ := new(big.Rat).SetString(total)
		if totalNumber != nil && attacks > 0 {
			perAttack := new(big.Rat).Quo(totalNumber, new(big.Rat).SetInt64(attacks))
			rate := "—"
			duration := "Unavailable"
			if timed == attacks && sessionSeconds > 0 {
				n, _ := totalNumber.Float64()
				rate = farmingCompact(fmt.Sprintf("%.0f", n*60/sessionSeconds))
				duration = farmingDuration(sessionSeconds)
			}
			average := "—"
			if timed == attacks {
				average = farmingDuration(attackSeconds / float64(attacks))
			}
			value += fmt.Sprintf("\n**%s** / farming min · **%s** / attack\nFarmed **%s** · Avg attack **%s**", rate, farmingCompact(perAttack.FloatString(0)), duration, average)
		}
		fields = append(fields, discord.EmbedField{Name: fmt.Sprintf("%d. %s", len(fields)+1, discordLabel(discordClip(names[tag], 60))), Value: value})
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	description := ""
	if len(fields) == 0 {
		description = "No recorded farming attacks for current members in this period."
	}
	// Spacing separates players on mobile without wasting space after the last row.
	for i := 0; i < len(fields)-1; i++ {
		fields[i].Value += "\n\u200b"
	}
	embeds := []discord.Embed{{Title: discordClip(clan.Name, 80) + " · Top Looters", Description: description, Color: 0xe8a524, Fields: fields,
		Footer: &discord.EmbedFooter{Text: fmt.Sprintf("Since %s · Current members · Recorded attacks · %s UTC", start.In(zone).Format("Jan 2 15:04"), now.Format("15:04"))}}}
	target, err := w.webhookTarget(ctx, b.ServerID, b.WebhookID, b.ThreadID)
	if err != nil {
		return err
	}
	webhookID, err := snowflake.Parse(target.WebhookID)
	if err != nil {
		return err
	}
	var threadID snowflake.ID
	if target.ThreadID != "" {
		threadID, err = snowflake.Parse(target.ThreadID)
		if err != nil {
			return err
		}
	}
	mentions := &discord.AllowedMentions{Parse: []discord.AllowedMentionType{}}
	messageID := b.MessageID
	if messageID != "" {
		id, parseErr := snowflake.Parse(messageID)
		if parseErr != nil {
			return parseErr
		}
		_, err = w.client.Rest.UpdateWebhookMessage(webhookID, target.Secret, id, discord.WebhookMessageUpdate{Embeds: &embeds, AllowedMentions: mentions}, rest.UpdateWebhookMessageParams{ThreadID: threadID})
		// Only a missing message permits recreation; permission/rate errors must not
		// generate a second board. Next tick retries all other failures.
		var providerErr *rest.Error
		if errors.As(err, &providerErr) && providerErr.Response != nil && providerErr.Response.StatusCode == 404 && providerErr.Code == rest.JSONErrorCodeUnknownMessage {
			messageID = ""
			err = nil
		}
	}
	if messageID == "" && err == nil {
		var message *discord.Message
		message, err = w.client.Rest.CreateWebhookMessage(webhookID, target.Secret, discord.WebhookMessageCreate{Embeds: embeds, AllowedMentions: mentions}, rest.CreateWebhookMessageParams{Wait: true, ThreadID: threadID})
		if err == nil && message != nil {
			messageID = message.ID.String()
		}
	}
	if err != nil {
		return err
	}
	if messageID == "" {
		return fmt.Errorf("Discord did not return a message")
	}
	_, err = w.pool.Exec(ctx, `UPDATE autoboards SET message_id=$2,last_run_at=$3,next_run_at=$4 WHERE id=$1::uuid AND enabled AND updated_at=$5`, b.ID, messageID, now, now.Add(time.Duration(max(30, b.Seconds))*time.Second), b.Revision)
	return err
}

func farmingCompact(value string) string {
	n, ok := new(big.Rat).SetString(value)
	if !ok {
		return "—"
	}
	for _, unit := range []struct {
		number int64
		suffix string
	}{{1000000000, "B"}, {1000000, "M"}, {1000, "K"}} {
		if n.Cmp(new(big.Rat).SetInt64(unit.number)) >= 0 {
			return strings.TrimSuffix(new(big.Rat).Quo(n, new(big.Rat).SetInt64(unit.number)).FloatString(1), ".0") + unit.suffix
		}
	}
	return n.FloatString(0)
}
func farmingDuration(seconds float64) string {
	n := int64(seconds)
	if n >= 3600 {
		return fmt.Sprintf("%dh %dm", n/3600, n%3600/60)
	}
	if n >= 60 {
		return fmt.Sprintf("%dm %ds", n/60, n%60)
	}
	return fmt.Sprintf("%ds", n)
}
