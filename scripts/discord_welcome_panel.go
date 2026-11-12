package scripts

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/disgoorg/disgo/discord"
	"github.com/disgoorg/disgo/rest"
	"github.com/disgoorg/snowflake/v2"
	"github.com/jackc/pgx/v5"
)

func (w *discordDeliveryWorker) deliverWelcomePanel(ctx context.Context, event mobileWarEvent) error {
	guild, user := stringMapValue(event.Value, "guild_id"), stringMapValue(event.Value, "user_id")
	if !w.app.Config.AllowsDiscordGuild(guild) || stringMapValue(event.Value, "application_id") != w.client.ApplicationID.String() {
		return nil
	}
	if _, err := snowflake.Parse(guild); err != nil {
		return nil
	}
	userID, err := snowflake.Parse(user)
	if err != nil {
		return nil
	}
	// A welcome is timely, best-effort delivery, not a backlog to replay after an outage.
	if event.Timestamp.IsZero() || time.Since(event.Timestamp) > 5*time.Minute {
		return nil
	}
	var channel, color, serverName, embedColor, guildIcon string
	var raw []byte
	err = w.pool.QueryRow(ctx, `SELECT panel.welcome_channel_id,panel.button_color,COALESCE(embed.data,'{}'::jsonb),server.name,COALESCE(server.embed_color,'2829617'),COALESCE((SELECT data->>'icon' FROM discord_cache.guilds WHERE id=panel.server_id),'')
 FROM server_welcome_panels panel JOIN servers server ON server.id=panel.server_id
 LEFT JOIN server_custom_embeds embed ON embed.server_id=panel.server_id AND embed.name=panel.embed_name
 WHERE panel.server_id=$1 AND panel.welcome_channel_id IS NOT NULL AND server.left_at IS NULL`, guild).Scan(&channel, &color, &raw, &serverName, &embedColor, &guildIcon)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	var stored struct {
		Content string          `json:"content"`
		Embeds  []discord.Embed `json:"embeds"`
		Flags   int             `json:"flags"`
	}
	if err = json.Unmarshal(raw, &stored); err != nil {
		return fmt.Errorf("decode welcome embed: %w", err)
	}
	if stored.Flags&32768 != 0 {
		return errors.New("welcome panel requires a classic saved embed")
	}
	rows, err := w.pool.Query(ctx, `SELECT button_name FROM server_welcome_panel_buttons WHERE server_id=$1 ORDER BY position,button_name`, guild)
	if err != nil {
		return err
	}
	defer rows.Close()
	if err := w.refreshLogEmojis(time.Now()); err != nil {
		w.app.Logger.Warn("welcome emoji lookup failed; using text", "err", err)
	}
	var buttons []discord.InteractiveComponent
	seen := map[string]bool{}
	for rows.Next() {
		var name string
		if err = rows.Scan(&name); err != nil {
			return err
		}
		action, label, emoji := "", "", ""
		switch name {
		case "Link Button":
			action, label, emoji = "link", "Link Account", "🔗"
		case "Link Help Button":
			action, label, emoji = "help", "Help", "❓"
		case "Refresh Button":
			action, label, emoji = "refresh", "Refresh Roles", discordLogIcons(w.emojis).icon("refresh", "🔄")
		case "To-Do Button":
			action, label, emoji = "todo", "To-Do List", discordLogIcons(w.emojis).icon("green_check", "✅")
		case "Roster Button":
			action, label, emoji = "rosters", "My Rosters", discordLogIcons(w.emojis).icon("calendar", "📅")
		}
		if action == "" || seen[action] {
			continue
		}
		seen[action] = true
		button := discord.NewSecondaryButton(label, "ck:quick:"+guild+":"+action).WithEmoji(redditButtonEmoji(emoji))
		switch color {
		case "Blue":
			button.Style = discord.ButtonStylePrimary
		case "Green":
			button.Style = discord.ButtonStyleSuccess
		case "Red":
			button.Style = discord.ButtonStyleDanger
		}
		buttons = append(buttons, button)
	}
	if err = rows.Err(); err != nil {
		return err
	}
	rows.Close()
	if len(buttons) == 0 {
		return nil
	}
	channelID, err := snowflake.Parse(channel)
	if err != nil {
		return err
	}
	destination, err := w.client.Rest.GetChannel(channelID, rest.WithCtx(ctx))
	if err != nil {
		return err
	}
	guildChannel, ok := destination.(discord.GuildChannel)
	validType := destination.Type() == discord.ChannelTypeGuildText || destination.Type() == discord.ChannelTypeGuildNews || destination.Type() == discord.ChannelTypeGuildNewsThread || destination.Type() == discord.ChannelTypeGuildPublicThread || destination.Type() == discord.ChannelTypeGuildPrivateThread
	if !ok || guildChannel.GuildID().String() != guild || !validType {
		return errors.New("welcome destination must be a text channel or thread in its server")
	}
	if thread, isThread := destination.(discord.GuildThread); isThread && (thread.ThreadMetadata.Locked || thread.ThreadMetadata.Archived) {
		return nil
	}
	if stored.Content == "" && len(stored.Embeds) == 0 {
		base := 10
		if strings.HasPrefix(embedColor, "#") {
			base = 16
			embedColor = strings.TrimPrefix(embedColor, "#")
		}
		parsed, parseErr := strconv.ParseInt(embedColor, base, 32)
		if parseErr != nil || parsed < 0 || parsed > 0xffffff {
			parsed = 2829617
		}
		stored.Embeds = []discord.Embed{{Description: "**Welcome to " + discordLabel(serverName) + "!**\nUse the buttons below to get started.", Color: int(parsed)}}
		if guildIcon != "" {
			stored.Embeds[0].Thumbnail = &discord.EmbedResource{URL: "https://cdn.discordapp.com/icons/" + guild + "/" + url.PathEscape(guildIcon) + ".png"}
		}
	}
	// Suppress mentions from saved content; only the newly joined member may ping.
	message := discord.MessageCreate{Content: discordClip("<@"+user+">\n"+stored.Content, 2000), Embeds: stored.Embeds, Components: []discord.LayoutComponent{discord.NewActionRow(buttons...)}}
	return w.sendMessage(ctx, discordDeliveryTarget{ServerID: guild, ChannelID: channel}, message, []snowflake.ID{userID})
}
