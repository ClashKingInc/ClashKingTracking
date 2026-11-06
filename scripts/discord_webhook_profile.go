package scripts

import (
	"context"
	"time"

	"github.com/disgoorg/disgo/discord"
	"github.com/disgoorg/disgo/rest"
	"github.com/disgoorg/snowflake/v2"
)

type discordWebhookProfile struct {
	name, avatar string
	expires      time.Time
}

// Webhooks do not inherit the bot's avatar. Resolve the bot's actual member
// profile, including the global fallback, and override each outgoing message.
// This also repairs old hooks without editing shared webhook configuration.
func (w *discordDeliveryWorker) createWebhookMessage(ctx context.Context, guild string, id snowflake.ID, token string, message discord.WebhookMessageCreate, params rest.CreateWebhookMessageParams, opts ...rest.RequestOpt) (*discord.Message, error) {
	if w.profiles == nil {
		w.profiles = make(map[string]discordWebhookProfile)
	}
	profile, found := w.profiles[guild]
	now := time.Now()
	if !found || !now.Before(profile.expires) {
		next := discordWebhookProfile{expires: now.Add(time.Minute)}
		guildID, err := snowflake.Parse(guild)
		if err == nil {
			member, lookupErr := w.client.Rest.GetMember(guildID, w.client.ApplicationID, rest.WithCtx(ctx))
			if lookupErr == nil && member != nil && member.User.ID == w.client.ApplicationID {
				member.GuildID = guildID
				next = discordWebhookProfile{name: member.EffectiveName(), avatar: member.EffectiveAvatarURL(), expires: now.Add(5 * time.Minute)}
			} else {
				// Profile lookup must not suppress a valid reminder. Retry in one minute;
				// retain any previously observed identity while Discord is unavailable.
				next.name, next.avatar = profile.name, profile.avatar
				w.app.Logger.Warn("webhook bot profile unavailable", "server_id", guild)
			}
		}
		// Bound inactive guilds in this process-local cache; it is never a delivery
		// checkpoint or an authorization source.
		if len(w.profiles) >= 25000 {
			clear(w.profiles)
		}
		w.profiles[guild], profile = next, next
	}
	if profile.name != "" {
		message.Username = profile.name
	}
	if profile.avatar != "" {
		message.AvatarURL = profile.avatar
	}
	return w.client.Rest.CreateWebhookMessage(id, token, message, params, opts...)
}
