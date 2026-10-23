package scripts

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/disgoorg/disgo/discord"
	"github.com/disgoorg/disgo/rest"
	"github.com/disgoorg/snowflake/v2"
	"github.com/jackc/pgx/v5"
)

type giveawayPublication struct {
	Version     int       `json:"version"`
	OccurredAt  time.Time `json:"occurred_at"`
	ReplacedIDs []string  `json:"replaced_user_ids"`
	ActorLabel  string    `json:"actor_label"`
	Giveaway    struct {
		ID         string    `json:"id"`
		ServerID   string    `json:"server_id"`
		ChannelID  string    `json:"channel_id"`
		Prize      string    `json:"prize"`
		EndTime    time.Time `json:"end_time"`
		Winners    int       `json:"winners"`
		Mentions   []string  `json:"mentions"`
		TextAbove  string    `json:"text_above_embed"`
		TextIn     string    `json:"text_in_embed"`
		TextEnd    string    `json:"text_on_end"`
		ImageURL   string    `json:"image_url"`
		EntryCount int       `json:"entry_count"`
	} `json:"giveaway"`
	WinnerIDs []string `json:"winner_ids"`
}
type giveawayClaim struct {
	Outcome    string `json:"outcome"`
	ClaimToken string `json:"claimToken"`
	Effect     struct {
		EffectID        string              `json:"effectId"`
		Kind            string              `json:"kind"`
		SourceMessageID string              `json:"sourceMessageId"`
		Payload         giveawayPublication `json:"payload"`
	} `json:"effect"`
}

func giveawayMessage(payload giveawayPublication, kind string) discord.MessageCreate {
	g := payload.Giveaway
	noun := "Winner"
	if g.Winners != 1 {
		noun = "Winners"
	}
	embed := discord.Embed{Title: discordClip(fmt.Sprintf("🎉 %s - %d %s 🎉", g.Prize, g.Winners, noun), 256),
		Description: discordClip(g.TextIn, 4096), Color: 0x5865f2, Timestamp: &g.EndTime, Footer: &discord.EmbedFooter{Text: "Ends"}}
	if g.ImageURL != "" {
		embed.Image = &discord.EmbedResource{URL: g.ImageURL}
	}
	mentions := &discord.AllowedMentions{Parse: []discord.AllowedMentionType{}}
	prefix := make([]string, 0, len(g.Mentions))
	for _, role := range g.Mentions {
		// Stored Dashboard values are role IDs. Never interpret arbitrary mention text.
		role = strings.TrimSuffix(strings.TrimPrefix(role, "<@&"), ">")
		if id, err := snowflake.Parse(role); err == nil {
			prefix = append(prefix, "<@&"+id.String()+">")
			mentions.Roles = append(mentions.Roles, id)
		}
	}
	message := discord.MessageCreate{Content: discordClip(strings.TrimSpace(strings.Join(prefix, " ")+"\n"+g.TextAbove), 2000),
		Embeds: []discord.Embed{embed}, AllowedMentions: mentions}
	if kind == "end" || kind == "reroll" {
		embed.Color = 0xe74c3c
		embed.Description = fmt.Sprintf("**Total Participants:** %d", g.EntryCount)
		embed.Timestamp, embed.Footer = nil, nil
		message.Embeds = []discord.Embed{embed}
		winners := make([]string, 0, len(payload.WinnerIDs))
		mentions.Roles = nil
		for _, user := range payload.WinnerIDs {
			if id, err := snowflake.Parse(user); err == nil {
				winners = append(winners, "<@"+id.String()+">")
				mentions.Users = append(mentions.Users, id)
			}
		}
		if len(winners) == 0 {
			winners = append(winners, "No winners")
		}
		message.Content = discordClip(strings.TrimSpace(strings.Join(winners, " ")+"\n"+g.TextEnd), 2000)
		if kind == "reroll" {
			oldWinners := make([]string, 0, len(payload.ReplacedIDs))
			for _, id := range payload.ReplacedIDs {
				oldWinners = append(oldWinners, "<@"+id+">")
			}
			embed.Title = discordClip("🎉 Reroll Winner(s) for "+g.Prize+" 🎉", 256)
			embed.Description = discordClip(fmt.Sprintf("**Replaced Winner(s):** %s\n**New Winner(s):** %s\n\n**Total Participants:** %d", strings.Join(oldWinners, ", "), strings.Join(winners, ", "), g.EntryCount), 4096)
			embed.Color = 0xe67e22
			embed.Image = nil
			embed.Timestamp = &payload.OccurredAt
			embed.Footer = &discord.EmbedFooter{Text: "Rerolled by " + payload.ActorLabel}
			message.Embeds = []discord.Embed{embed}
			message.Content = discordClip(strings.Join(winners, ", "), 2000)
		}
	} else {
		message.Components = []discord.LayoutComponent{discord.NewActionRow(discord.NewPrimaryButton(
			"🎟️ Participate", "ck:giveaway:enter:"+g.ID))}
	}
	return message
}

func (w *discordDeliveryWorker) giveawayAPI(ctx context.Context, path string, input, out any) error {
	if w.app.Config.ClashKingAPIURL == "" || w.app.Config.ClashKingAPIToken == "" {
		return errors.New("giveaway delivery requires CLASHKING_API_ORIGIN and CLASHKING_API_TOKEN")
	}
	body, err := json.Marshal(input)
	if err != nil {
		return err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(w.app.Config.ClashKingAPIURL, "/")+path, bytes.NewReader(body))
	if err != nil {
		return err
	}
	request.Header.Set("Authorization", "Bearer "+w.app.Config.ClashKingAPIToken)
	request.Header.Set("Content-Type", "application/json")
	response, err := (&http.Client{Timeout: 30 * time.Second}).Do(request)
	if err != nil {
		return errors.New("giveaway runtime API transport failed")
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return fmt.Errorf("giveaway runtime API returned HTTP %d", response.StatusCode)
	}
	raw, err := io.ReadAll(io.LimitReader(response.Body, 300_001))
	if err != nil {
		return err
	}
	if len(raw) > 300_000 {
		return errors.New("giveaway runtime response exceeds limit")
	}
	if out == nil {
		return nil
	}
	return json.Unmarshal(raw, out)
}

func (w *discordDeliveryWorker) deliverGiveaway(ctx context.Context, event mobileWarEvent) error {
	g, ok := mapValue(event.Value["giveaway"])
	if !ok {
		return errors.New("giveaway event is missing giveaway")
	}
	if !w.app.Config.AllowsDiscordGuild(stringMapValue(g, "server_id")) {
		return nil
	}
	kind := strings.TrimPrefix(stringMapValue(event.Value, "type"), "giveaway_")
	if kind != "start" && kind != "update" && kind != "end" {
		return nil
	}
	return w.prepareGiveaway(ctx, stringMapValue(g, "id"), kind)
}

func (w *discordDeliveryWorker) prepareGiveaway(ctx context.Context, id, kind string) error {
	var server string
	if err := w.pool.QueryRow(ctx, `SELECT server_id FROM giveaways WHERE id=$1 AND NOT disabled`, id).Scan(&server); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		return err
	}
	if !w.app.Config.AllowsDiscordGuild(server) {
		return nil
	}
	var prepared struct {
		Outcome  string `json:"outcome"`
		EffectID string `json:"effectId"`
	}
	if err := w.giveawayAPI(ctx, "/v2/runtime/giveaways/"+id+"/publications/prepare", map[string]string{"kind": kind}, &prepared); err != nil {
		return err
	}
	if prepared.Outcome != "ready" {
		return nil
	}
	return w.executeGiveaway(ctx, id, prepared.EffectID)
}

// Both unprocessed stream hints and pending journals survive delivery restarts.
func (w *discordDeliveryWorker) recoverGiveawayPublications(ctx context.Context) error {
	if w.app.Config.ClashKingAPIURL == "" || w.app.Config.ClashKingAPIToken == "" {
		return nil
	}
	guilds := make([]string, 0, len(w.app.Config.DiscordGuildAllowlist))
	for id := range w.app.Config.DiscordGuildAllowlist {
		guilds = append(guilds, id)
	}
	rows, err := w.pool.Query(ctx, `SELECT g.id,g.server_id,CASE WHEN g.message_id IS NULL THEN 'start' ELSE 'end' END
  FROM giveaways g WHERE NOT g.disabled AND (cardinality($1::text[])=0 OR g.server_id=ANY($1::text[])) AND ((g.status='ongoing' AND g.message_id IS NULL) OR
  (g.status='ended' AND g.message_id IS NOT NULL)) AND NOT EXISTS(SELECT 1 FROM giveaway_publication_effects p
   WHERE p.giveaway_id=g.id AND p.kind=CASE WHEN g.message_id IS NULL THEN 'start' ELSE 'end' END)
  ORDER BY g.start_time LIMIT 10`, guilds)
	if err != nil {
		return err
	}
	type candidate struct{ id, server, kind string }
	var candidates []candidate
	for rows.Next() {
		var c candidate
		if err := rows.Scan(&c.id, &c.server, &c.kind); err != nil {
			rows.Close()
			return err
		}
		candidates = append(candidates, c)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	for _, c := range candidates {
		if w.app.Config.AllowsDiscordGuild(c.server) {
			if err := w.prepareGiveaway(ctx, c.id, c.kind); err != nil {
				return err
			}
		}
	}
	pendingRows, err := w.pool.Query(ctx, `SELECT p.giveaway_id,p.effect_id FROM giveaway_publication_effects p
    JOIN giveaways g ON g.id=p.giveaway_id WHERE NOT g.disabled
    AND (cardinality($1::text[])=0 OR p.server_id=ANY($1::text[]))
    AND ((p.state='pending' AND p.next_attempt_at<=clock_timestamp()) OR (p.state='sending' AND p.lease_expires_at<=clock_timestamp()))
    ORDER BY p.updated_at,p.effect_id LIMIT 10`, guilds)
	if err != nil {
		return err
	}
	var pending [][2]string
	for pendingRows.Next() {
		var item [2]string
		if err := pendingRows.Scan(&item[0], &item[1]); err != nil {
			pendingRows.Close()
			return err
		}
		pending = append(pending, item)
	}
	err = pendingRows.Err()
	pendingRows.Close()
	if err != nil {
		return err
	}
	for _, item := range pending {
		if err := w.executeGiveaway(ctx, item[0], item[1]); err != nil {
			return err
		}
	}
	return nil
}

func (w *discordDeliveryWorker) giveawayWebhook(ctx context.Context, id string) (discordDeliveryTarget, error) {
	tx, err := w.pool.Begin(ctx)
	if err != nil {
		return discordDeliveryTarget{}, err
	}
	defer tx.Rollback(ctx)
	var target discordDeliveryTarget
	target.Config, target.ConfigID = "giveaway", id
	err = tx.QueryRow(ctx, `SELECT server_id,channel_id,COALESCE(webhook_id,''),updated_at FROM giveaways WHERE id=$1 AND NOT disabled FOR UPDATE`, id).
		Scan(&target.ServerID, &target.ChannelID, &target.WebhookID, &target.Revision)
	if err != nil {
		return target, err
	}
	if !w.app.Config.AllowsDiscordGuild(target.ServerID) {
		return target, errors.New("giveaway server is outside delivery allowlist")
	}
	if target.WebhookID == "" {
		// Serialize webhook selection across giveaways sharing one destination.
		if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, "giveaway-webhook:"+target.ChannelID); err != nil {
			return target, err
		}
		channelID, err := snowflake.Parse(target.ChannelID)
		if err != nil {
			return target, err
		}
		channel, err := w.client.Rest.GetChannel(channelID, rest.WithCtx(ctx))
		if err != nil {
			return target, err
		}
		guildChannel, ok := channel.(discord.GuildChannel)
		if !ok || guildChannel.GuildID().String() != target.ServerID || (channel.Type() != discord.ChannelTypeGuildText && channel.Type() != discord.ChannelTypeGuildNews) {
			return target, errors.New("giveaway destination must be a text channel in its server")
		}
		hooks, err := w.client.Rest.GetWebhooks(channelID, rest.WithCtx(ctx))
		if err != nil {
			return target, err
		}
		for _, hook := range hooks {
			var incoming *discord.IncomingWebhook
			switch h := hook.(type) {
			case discord.IncomingWebhook:
				incoming = &h
			case *discord.IncomingWebhook:
				incoming = h
			}
			if incoming != nil && incoming.ApplicationID != nil && *incoming.ApplicationID == w.client.ApplicationID && incoming.GuildID.String() == target.ServerID && incoming.Token != "" {
				target.WebhookID = incoming.ID().String()
				break
			}
		}
		if target.WebhookID == "" {
			created, err := w.client.Rest.CreateWebhook(channelID, discord.WebhookCreate{Name: "ClashKing Giveaways"}, rest.WithCtx(ctx))
			if err != nil {
				return target, err
			}
			if created == nil || created.ApplicationID == nil || *created.ApplicationID != w.client.ApplicationID {
				return target, errors.New("giveaway webhook is not owned by this application")
			}
			target.WebhookID = created.ID().String()
		}
		if _, err = tx.Exec(ctx, `UPDATE giveaways SET webhook_id=$2 WHERE id=$1`, id, target.WebhookID); err != nil {
			return target, err
		}
	}
	if err = tx.Commit(ctx); err != nil {
		return target, err
	}
	resolved, err := w.webhookTarget(ctx, target.ServerID, target.WebhookID, "")
	if err != nil {
		return target, err
	}
	if resolved.ChannelID != target.ChannelID {
		return target, errors.New("giveaway webhook channel changed")
	}
	target.Secret = resolved.Secret
	return target, nil
}

func (w *discordDeliveryWorker) executeGiveaway(ctx context.Context, id, effectID string) error {
	target, err := w.giveawayWebhook(ctx, id)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	var claim giveawayClaim
	base := "/v2/runtime/giveaways/" + id + "/publications/" + effectID
	if err = w.giveawayAPI(ctx, base+"/claim", map[string]any{}, &claim); err != nil {
		return err
	}
	if claim.Outcome != "claimed" {
		return nil
	}
	payload := claim.Effect.Payload
	complete := map[string]any{"claimToken": claim.ClaimToken}
	if payload.Version != 1 || payload.Giveaway.ID != id || payload.Giveaway.ServerID != target.ServerID || payload.Giveaway.ChannelID != target.ChannelID {
		complete["outcome"] = "failed"
		complete["failureReason"] = "payload_validation"
		return w.giveawayAPI(ctx, base+"/complete", complete, nil)
	}
	message := giveawayMessage(payload, claim.Effect.Kind)
	webhookID := snowflake.MustParse(target.WebhookID)
	var sent *discord.Message
	sendCtx, cancel := context.WithTimeout(ctx, 45*time.Second)
	defer cancel()
	if claim.Effect.Kind == "update" {
		messageID, parseErr := snowflake.Parse(claim.Effect.SourceMessageID)
		if parseErr != nil {
			err = parseErr
		} else {
			sent, err = w.client.Rest.UpdateWebhookMessage(webhookID, target.Secret, messageID,
				discord.WebhookMessageUpdate{Content: &message.Content, Embeds: &message.Embeds, Components: &message.Components, AllowedMentions: message.AllowedMentions}, rest.UpdateWebhookMessageParams{WithComponents: true}, rest.WithCtx(sendCtx))
		}
	} else {
		if claim.Effect.Kind == "end" {
			messageID, parseErr := snowflake.Parse(claim.Effect.SourceMessageID)
			if parseErr != nil {
				err = parseErr
			} else {
				components := []discord.LayoutComponent{discord.NewActionRow(discord.NewPrimaryButton("🎟️ Participate", "ck:giveaway:enter:"+id).WithDisabled(true))}
				_, err = w.client.Rest.UpdateWebhookMessage(webhookID, target.Secret, messageID, discord.WebhookMessageUpdate{Components: &components}, rest.UpdateWebhookMessageParams{WithComponents: true}, rest.WithCtx(sendCtx))
			}
			if err != nil {
				complete["outcome"] = "retry"
				complete["failureReason"] = "source_update_uncertain"
				return w.giveawayAPI(ctx, base+"/complete", complete, nil)
			}
		}
		sent, err = w.client.Rest.CreateWebhookMessage(webhookID, target.Secret, discord.WebhookMessageCreate{
			Content: message.Content, Embeds: message.Embeds, Components: message.Components, AllowedMentions: message.AllowedMentions}, rest.CreateWebhookMessageParams{Wait: true, WithComponents: true}, rest.WithCtx(sendCtx))
	}
	if err != nil || sent == nil {
		outcome, reason := "ambiguous", "transport_uncertain"
		if claim.Effect.Kind == "update" {
			outcome, reason = "retry", "source_update_uncertain"
		}
		var providerErr *rest.Error
		if errors.As(err, &providerErr) && providerErr.Response != nil && providerErr.Response.StatusCode >= 400 && providerErr.Response.StatusCode < 500 {
			outcome, reason = "failed", "http_rejected"
			if providerErr.Response.StatusCode == 429 {
				outcome, reason = "retry", "rate_limited"
			}
		}
		complete["outcome"], complete["failureReason"] = outcome, reason
	} else {
		complete["outcome"], complete["messageId"] = "succeeded", sent.ID.String()
	}
	return w.giveawayAPI(ctx, base+"/complete", complete, nil)
}
