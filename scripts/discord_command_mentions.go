package scripts

import (
	"context"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/disgoorg/disgo/discord"
	"github.com/disgoorg/disgo/rest"
)

// Like the legacy listener, only a message starting with -/ is a request.
// This is a command link, never an invocation or a parser for command arguments.
func plainSlashCommandPath(content string) string {
	if !strings.HasPrefix(content, "-/") {
		return ""
	}
	parts := strings.Fields(strings.TrimPrefix(content, "-/"))
	if len(parts) == 0 || len(parts) > 3 {
		return ""
	}
	for _, part := range parts {
		if utf8.RuneCountInString(part) > 32 {
			return ""
		}
	}
	return strings.ToLower(strings.Join(parts, " "))
}

func (w *discordDeliveryWorker) deliverCommandMention(ctx context.Context, event mobileWarEvent, path string) error {
	p := discordMessagePayload(event)
	if p.GuildID == "" || p.ChannelID == "" || !w.app.Config.AllowsDiscordGuild(p.GuildID) {
		return nil
	}
	if err := w.refreshCommandMentions(ctx, time.Now()); err != nil {
		return err
	}
	mention, exists := w.commandMentions[path]
	if !exists {
		return nil
	}
	return w.send(ctx, discordDeliveryTarget{ServerID: p.GuildID, ChannelID: p.ChannelID}, mention, nil)
}

func (w *discordDeliveryWorker) refreshCommandMentions(ctx context.Context, now time.Time) error {
	if now.Before(w.nextCommandLoad) {
		return nil
	}
	commands, err := w.client.Rest.GetGlobalCommands(w.client.ApplicationID, false, rest.WithCtx(ctx))
	if err != nil {
		// Do not emit a removed command from stale metadata or retry on every message.
		w.commandMentions = nil
		w.nextCommandLoad = now.Add(30 * time.Second)
		return fmt.Errorf("load registered slash commands: %w", err)
	}
	mentions := make(map[string]string)
	for _, command := range commands {
		slash, ok := command.(discord.SlashCommand)
		if !ok || slash.ID() == 0 {
			continue
		}
		add := func(path string) {
			mentions[strings.ToLower(path)] = fmt.Sprintf("</%s:%s>", path, slash.ID())
		}
		add(slash.Name())
		for _, option := range slash.Options {
			switch child := option.(type) {
			case discord.ApplicationCommandOptionSubCommand:
				add(slash.Name() + " " + child.Name)
			case discord.ApplicationCommandOptionSubCommandGroup:
				for _, subcommand := range child.Options {
					add(slash.Name() + " " + child.Name + " " + subcommand.Name)
				}
			}
		}
	}
	// Use Discord's registered IDs and paths, not a duplicate hard-coded command list.
	w.commandMentions = mentions
	w.nextCommandLoad = now.Add(5 * time.Minute)
	return nil
}
