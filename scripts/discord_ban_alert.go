package scripts

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/disgoorg/disgo/discord"
	"github.com/disgoorg/snowflake/v2"
	"github.com/jackc/pgx/v5"
)

type discordBanQuery interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}

type discordBanAlert struct {
	PlayerName string
	Reason     string
	CreatedAt  time.Time
	ServerName string
	RoleID     string
}

// Only bans owned by this guild may produce its red ban alert. A join is
// checked against the current SQL row, so removed bans cannot alert later.
func loadDiscordBanAlert(ctx context.Context, db discordBanQuery, serverID, playerTag, clanTag string) (discordBanAlert, bool, error) {
	var ban discordBanAlert
	err := db.QueryRow(ctx, `
		SELECT ban.player_name, ban.reason, ban.created_at, server.name,
		       COALESCE((SELECT rule.role_id FROM server_roles rule
		                 WHERE rule.server_id = ban.server_id AND rule.clan_tag = $3
		                   AND rule.type = 'clan_role' AND rule.option = 'member'
		                 ORDER BY rule.updated_at DESC LIMIT 1), '')
		FROM server_bans ban JOIN servers server ON server.id = ban.server_id
		WHERE ban.server_id = $1 AND ban.player_tag = $2
	`, serverID, playerTag, clanTag).Scan(&ban.PlayerName, &ban.Reason, &ban.CreatedAt, &ban.ServerName, &ban.RoleID)
	if errors.Is(err, pgx.ErrNoRows) {
		return discordBanAlert{}, false, nil
	}
	if err != nil {
		return discordBanAlert{}, false, fmt.Errorf("load local server ban: %w", err)
	}
	return ban, true, nil
}

func discordBanAlertMessage(event mobileWarEvent, ban discordBanAlert) discord.MessageCreate {
	member, _ := mapValue(event.Value["member"])
	clan, _ := mapValue(event.Value["clan"])
	name := firstDiscordString(stringMapValue(member, "name"), ban.PlayerName, stringMapValue(member, "tag"))
	tag := stringMapValue(member, "tag")
	clanName := firstDiscordString(stringMapValue(clan, "name"), event.ClanTag)
	reason := strings.TrimSpace(ban.Reason)
	if reason == "" {
		reason = "No Reason Given"
	}
	link := "https://link.clashofclans.com/en?action=OpenPlayerProfile&tag=" + url.QueryEscape(tag)
	embed := discord.Embed{
		Description: fmt.Sprintf("[WARNING! BANNED PLAYER %s JOINED](%s)", discordLabel(name), link),
		Color:       0xff0000,
		Thumbnail:   &discord.EmbedResource{URL: "https://cdn.discordapp.com/attachments/843624785560993833/932701461614313562/2EdQ9Cx.png"},
		Fields: []discord.EmbedField{{Name: "Banned Player.", Value: discordClip(fmt.Sprintf("Player %s [%s] has joined %s and is on the %s BAN list!\n\nBanned on: %s\nReason: %s",
			name, tag, clanName, ban.ServerName, ban.CreatedAt.UTC().Format("2006-01-02"), reason), 1024)}},
	}
	message := discord.MessageCreate{Embeds: []discord.Embed{embed}}
	if role, err := snowflake.Parse(ban.RoleID); err == nil && ban.RoleID != "" {
		message.Content = "<@&" + role.String() + ">"
		message.AllowedMentions = &discord.AllowedMentions{Parse: []discord.AllowedMentionType{}, Roles: []snowflake.ID{role}}
	}
	return message
}
