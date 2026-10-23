package scripts

import (
	"context"
	"fmt"
	clashy "github.com/clashkinginc/clashy.go"
	"sort"
	"strings"
	"time"

	"github.com/disgoorg/disgo/discord"
	"github.com/disgoorg/disgo/rest"
	"github.com/disgoorg/snowflake/v2"
)

// Donation boards reuse autoboards' schedule and last message. One delivery
// worker executes them serially; no lock is held while Discord is contacted.
func (w *discordDeliveryWorker) markDonationBoardsDirty(ctx context.Context, event mobileWarEvent) error {
	if event.Topic != "clan" || stringMapValue(event.Value, "type") != "all_member_donations" || event.ClanTag == "" {
		return nil
	}
	_, err := w.pool.Exec(ctx, `UPDATE autoboards board SET next_run_at=LEAST(next_run_at,
		GREATEST(now()+interval '5 seconds',COALESCE(last_run_at+make_interval(mins=>interval_minutes),now())))
		WHERE enabled AND board_type='donations' AND delivery_mode='refresh' AND (
		(target_scope='family' AND EXISTS (SELECT 1 FROM server_clans clan WHERE clan.server_id=board.server_id AND clan.tag=$1))
		OR (target_scope='custom' AND EXISTS (SELECT 1 FROM autoboard_targets target WHERE target.autoboard_id=board.id AND target.target=$1)))
		AND next_run_at>GREATEST(now()+interval '5 seconds',COALESCE(last_run_at+make_interval(mins=>interval_minutes),now()))`, event.ClanTag)
	return err
}

type donationBoard struct {
	ID, ServerID, WebhookID, ThreadID, MessageID, Mode, Scope string
	RowLimit, IntervalMinutes                                 int
	ScheduleKind, ScheduleTime                                string
	Weekdays                                                  []int16
	MonthDay                                                  int
	Revision                                                  time.Time
}

func (w *discordDeliveryWorker) refreshDueDonationBoards(ctx context.Context) error {
	rows, err := w.pool.Query(ctx, `SELECT id::text,server_id,webhook_id,COALESCE(thread_id,''),COALESCE(message_id,''),
		delivery_mode,target_scope,row_limit,COALESCE(interval_minutes,1),COALESCE(schedule_kind,''),
		COALESCE(to_char(schedule_time,'HH24:MI'),''),COALESCE(schedule_weekdays,'{}'),COALESCE(schedule_day_of_month,1),updated_at
		FROM autoboards WHERE enabled AND board_type='donations' AND next_run_at<=now() ORDER BY next_run_at,id LIMIT 20`)
	if err != nil {
		return err
	}
	var boards []donationBoard
	for rows.Next() {
		var board donationBoard
		if err = rows.Scan(&board.ID, &board.ServerID, &board.WebhookID, &board.ThreadID, &board.MessageID, &board.Mode, &board.Scope,
			&board.RowLimit, &board.IntervalMinutes, &board.ScheduleKind, &board.ScheduleTime, &board.Weekdays, &board.MonthDay, &board.Revision); err != nil {
			rows.Close()
			return err
		}
		boards = append(boards, board)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	for _, board := range boards {
		if !w.app.Config.AllowsDiscordGuild(board.ServerID) {
			continue
		}
		if err = w.deliverDonationBoard(ctx, board); err != nil {
			w.app.Logger.Error("donation autoboard delivery failed", "board_id", board.ID, "err", err)
			// Best effort: retain an existing message, and retry from current data
			// after one minute. A crash between send and save can create a duplicate.
			if _, updateErr := w.pool.Exec(ctx, `UPDATE autoboards SET next_run_at=now()+interval '1 minute' WHERE id=$1::uuid AND updated_at=$2`, board.ID, board.Revision); updateErr != nil {
				return updateErr
			}
		}
	}
	return nil
}

func (w *discordDeliveryWorker) deliverDonationBoard(ctx context.Context, board donationBoard) error {
	if err := w.refreshLogEmojis(time.Now()); err != nil {
		return err
	}
	var tags []string
	query := `SELECT tag FROM server_clans WHERE server_id=$1 ORDER BY tag`
	if board.Scope == "custom" {
		query = `SELECT target FROM autoboard_targets WHERE autoboard_id=$1::uuid ORDER BY position`
	}
	key := board.ServerID
	if board.Scope == "custom" {
		key = board.ID
	}
	rows, err := w.pool.Query(ctx, query, key)
	if err != nil {
		return err
	}
	for rows.Next() {
		var tag string
		if err = rows.Scan(&tag); err != nil {
			rows.Close()
			return err
		}
		tags = append(tags, tag)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	now := time.Now().UTC()
	players := map[string]clashy.ClanMember{}
	store := newTrackedClanSnapshotStore(w.app)
	header := "Clan donations"
	if board.Scope == "family" {
		if err = w.pool.QueryRow(ctx, `SELECT COALESCE(NULLIF(family_label,''),name,'Family') FROM servers WHERE id=$1`, board.ServerID).Scan(&header); err != nil {
			return err
		}
	}
	for _, tag := range tags {
		clan, _, ok, loadErr := loadTrackedClanSnapshot[clashy.Clan](ctx, store, w.app.Config.TrackedClanSnapshotPrefix, "clan", tag)
		if loadErr != nil {
			return loadErr
		}
		if !ok || clan == nil {
			clan, err = fetchClan(w.app)(ctx, tag)
			if err != nil {
				return err
			}
		}
		if clan == nil {
			return fmt.Errorf("clan %s data unavailable", tag)
		}
		if board.Scope == "custom" && len(tags) == 1 {
			header = clan.Name
		}
		for _, member := range clan.Members {
			// A moving player can briefly occur in two snapshots; keep the
			// largest current counters rather than counting that player twice.
			previous, exists := players[member.Tag]
			if !exists || member.Donations > previous.Donations || member.Donations == previous.Donations && member.Received > previous.Received {
				players[member.Tag] = member
			}
		}
	}
	ordered := make([]clashy.ClanMember, 0, len(players))
	var lines []string
	var totalDonated, totalReceived int64
	icons := discordLogIcons(w.emojis)
	for _, member := range players {
		ordered = append(ordered, member)
		totalDonated += int64(member.Donations)
		totalReceived += int64(member.Received)
	}
	sort.Slice(ordered, func(i, j int) bool {
		a, b := ordered[i], ordered[j]
		if a.Donations != b.Donations {
			return a.Donations > b.Donations
		}
		if a.Received != b.Received {
			return a.Received > b.Received
		}
		return a.Tag < b.Tag
	})
	for _, member := range ordered[:min(board.RowLimit, len(ordered))] {
		lines = append(lines, fmt.Sprintf("`%2d` %s **%s** · %s / %s", len(lines)+1, icons.th(member.TownHall), discordLabel(discordClip(member.Name, 16)), donationCompact(int64(member.Donations)), donationCompact(int64(member.Received))))
	}
	if len(lines) == 0 {
		lines = []string{"No tracked donation data for these clans."}
	}
	var embeds []discord.Embed
	for offset := 0; offset < len(lines); offset += 25 {
		stop := min(offset+25, len(lines))
		embeds = append(embeds, discord.Embed{Title: fmt.Sprintf("%s · Top %d donators", discordClip(header, 80), min(board.RowLimit, len(ordered))), Description: icons.icon("up_green_arrow", "↑") + " Donated / " + icons.icon("down_red_arrow", "↓") + " Received\n" + strings.Join(lines[offset:stop], "\n"), Color: 0x14a37f,
			Footer: &discord.EmbedFooter{Text: fmt.Sprintf("↑%s donated · ↓%s received · Current season · %s UTC", donationCompact(totalDonated), donationCompact(totalReceived), now.Format("Jan 2 15:04"))}})
	}
	target, err := w.webhookTarget(ctx, board.ServerID, board.WebhookID, board.ThreadID)
	if err != nil {
		return err
	}
	webhookID, err := snowflake.Parse(target.WebhookID)
	if err != nil {
		return err
	}
	mentions := &discord.AllowedMentions{Parse: []discord.AllowedMentionType{}}
	var threadID snowflake.ID
	if target.ThreadID != "" {
		threadID, err = snowflake.Parse(target.ThreadID)
		if err != nil {
			return err
		}
	}
	messageID := board.MessageID
	if board.Mode == "refresh" && messageID != "" {
		id, parseErr := snowflake.Parse(messageID)
		if parseErr != nil {
			return parseErr
		}
		_, err = w.client.Rest.UpdateWebhookMessage(webhookID, target.Secret, id, discord.WebhookMessageUpdate{Embeds: &embeds, AllowedMentions: mentions}, rest.UpdateWebhookMessageParams{ThreadID: threadID})
	} else {
		var message *discord.Message
		message, err = w.client.Rest.CreateWebhookMessage(webhookID, target.Secret, discord.WebhookMessageCreate{Embeds: embeds, AllowedMentions: mentions}, rest.CreateWebhookMessageParams{Wait: true, ThreadID: threadID})
		if err == nil && message != nil {
			messageID = message.ID.String()
		}
	}
	if err != nil {
		return err
	}
	next := time.Date(9999, 12, 31, 0, 0, 0, 0, time.UTC)
	var storedMessage any = messageID
	if board.Mode == "send" {
		next = nextDonationBoardRun(board, now)
		storedMessage = nil
	}
	_, err = w.pool.Exec(ctx, `UPDATE autoboards SET message_id=$2,last_run_at=$3,next_run_at=$4 WHERE id=$1::uuid AND enabled AND updated_at=$5`, board.ID, storedMessage, now, next, board.Revision)
	return err
}

func donationCompact(value int64) string {
	for _, unit := range []struct {
		n int64
		s string
	}{{1_000_000_000, "B"}, {1_000_000, "M"}, {1_000, "K"}} {
		if value >= unit.n {
			return strings.TrimSuffix(fmt.Sprintf("%.1f", float64(value)/float64(unit.n)), ".0") + unit.s
		}
	}
	return fmt.Sprint(value)
}

func nextDonationBoardRun(board donationBoard, now time.Time) time.Time {
	var hour, minute int
	_, _ = fmt.Sscanf(board.ScheduleTime, "%d:%d", &hour, &minute)
	for offset := 0; offset < 370; offset++ {
		day := now.AddDate(0, 0, offset)
		candidate := time.Date(day.Year(), day.Month(), day.Day(), hour, minute, 0, 0, time.UTC)
		if !candidate.After(now) {
			continue
		}
		if board.ScheduleKind == "day_of_month" && candidate.Day() != board.MonthDay {
			continue
		}
		if board.ScheduleKind == "weekdays" {
			weekday := int16(candidate.Weekday())
			if weekday == 0 {
				weekday = 7
			}
			found := false
			for _, allowed := range board.Weekdays {
				if allowed == weekday {
					found = true
				}
			}
			if !found {
				continue
			}
		}
		return candidate
	}
	return now.Add(24 * time.Hour)
}
