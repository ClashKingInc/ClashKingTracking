package scripts

import (
	"context"
	valkey "github.com/valkey-io/valkey-go"
)

const verifiedPlayerTrackingKey = "tracking:verified_players"
const verifiedPlayerClanHashKey = "tracking:verified_player_clans"

func updateVerifiedPlayerClan(ctx context.Context, client valkey.Client, playerTag, clanTag string) error {
	if client == nil || playerTag == "" {
		return nil
	}
	current, err := client.Do(ctx, client.B().Hget().Key(verifiedPlayerClanHashKey).Field(playerTag).Build()).ToString()
	if valkey.IsValkeyNil(err) {
		current = ""
		err = nil
	}
	if err != nil || current == clanTag {
		return err
	}
	if clanTag == "" {
		return client.Do(ctx, client.B().Hdel().Key(verifiedPlayerClanHashKey).Field(playerTag).Build()).Error()
	}
	return client.Do(ctx, client.B().Hset().Key(verifiedPlayerClanHashKey).FieldValue().FieldValue(playerTag, clanTag).Build()).Error()
}
