package scripts

import (
	"context"
	"errors"
	"fmt"
	"github.com/disgoorg/disgo/discord"
	"github.com/disgoorg/disgo/rest"
	"github.com/disgoorg/snowflake/v2"
	"time"
)

func (w *discordDeliveryWorker) deliverMemberWarning(ctx context.Context, event mobileWarEvent) error {
	if _, ok := event.Value["previous_count"]; !ok {
		return nil
	}
	if _, ok := event.Value["current_count"]; !ok {
		return nil
	}
	oldCount, newCount := intValue(event.Value["previous_count"]), intValue(event.Value["current_count"])
	if oldCount < 0 || oldCount > 50 || newCount < 0 || newCount > 50 || oldCount == newCount {
		return nil
	}
	rows, err := w.pool.Query(ctx, `SELECT a.server_id,a.channel_id,a.below_count,a.above_count,COALESCE(a.role_id,''),a.updated_at FROM server_clan_member_warnings a JOIN servers s ON s.id=a.server_id WHERE a.clan_tag=$1 AND s.left_at IS NULL`, event.ClanTag)
	if err != nil {
		return err
	}
	type target struct {
		server, channel, role string
		below, above          *int
		updated               time.Time
	}
	var targets []target
	for rows.Next() {
		var t target
		if err = rows.Scan(&t.server, &t.channel, &t.below, &t.above, &t.role, &t.updated); err != nil {
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
	var failures []error
	for _, t := range targets {
		if !w.app.Config.AllowsDiscordGuild(t.server) || event.Timestamp.IsZero() || event.Timestamp.Before(t.updated) {
			continue
		}
		direction, limit, color := "", 0, 0x2ecc71
		// Strict crossings match the old bot. Equality itself is not outside a limit.
		if t.below != nil && oldCount >= *t.below && newCount < *t.below {
			direction, limit, color = "below", *t.below, 0xe74c3c
		}
		if t.above != nil && oldCount <= *t.above && newCount > *t.above {
			direction, limit = "above", *t.above
		}
		if direction == "" {
			continue
		}
		id, err := snowflake.Parse(t.channel)
		if err != nil {
			failures = append(failures, err)
			continue
		}
		channel, err := w.client.Rest.GetChannel(id, rest.WithCtx(ctx))
		if err != nil {
			failures = append(failures, err)
			continue
		}
		c, ok := channel.(discord.GuildChannel)
		if !ok || c.GuildID().String() != t.server || (channel.Type() != discord.ChannelTypeGuildText && channel.Type() != discord.ChannelTypeGuildNews) {
			failures = append(failures, fmt.Errorf("member warning channel is not in its server"))
			continue
		}
		clan, _ := mapValue(event.Value["clan"])
		name := stringMapValue(clan, "name")
		if name == "" {
			name = event.ClanTag
		}
		embed := discord.Embed{Title: name, Description: fmt.Sprintf("Member count is %s %d: **%d → %d**", direction, limit, oldCount, newCount), Color: color}
		if badge, ok := mapValue(clan["badgeUrls"]); ok {
			if icon := stringMapValue(badge, "large"); icon != "" {
				embed.Thumbnail = &discord.EmbedResource{URL: icon}
			}
		}
		message := discord.MessageCreate{Embeds: []discord.Embed{embed}}
		if t.role != "" {
			role, err := snowflake.Parse(t.role)
			if err != nil {
				failures = append(failures, err)
				continue
			}
			message.Content = "<@&" + t.role + ">"
			message.AllowedMentions = &discord.AllowedMentions{Roles: []snowflake.ID{role}}
		}
		if err = w.sendMessage(ctx, discordDeliveryTarget{ServerID: t.server, ChannelID: t.channel}, message, nil); err != nil {
			failures = append(failures, err)
		}
	}
	return errors.Join(failures...)
}
