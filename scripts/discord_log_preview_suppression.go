package scripts

import "github.com/disgoorg/disgo/discord"

func discordSuppressAutomaticEmbeds(message discord.MessageCreate) discord.MessageCreate {
	// Suppress link unfurls in text logs, never the intentionally authored embeds.
	if len(message.Embeds) == 0 && len(message.Files) == 0 {
		return message.WithSuppressEmbeds(true)
	}
	return message
}
