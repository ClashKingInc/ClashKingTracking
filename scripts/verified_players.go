package scripts

import (
	"context"
	"strconv"
	"time"

	valkey "github.com/valkey-io/valkey-go"
)

const verifiedPlayerTrackingKey = "tracking:verified_players"
const verifiedPlayerClanHashKey = "tracking:verified_player_clans"

// activeVerifiedPlayerTags returns the verified accounts refreshed by the app
// within the last seven days. The sorted-set score is an expiry timestamp, which
// makes the set enumerable without key scans or user-specific cache keys.
func activeVerifiedPlayerTags(ctx context.Context, client valkey.Client) ([]string, error) {
	if client == nil {
		return nil, nil
	}
	now := strconv.FormatInt(time.Now().UTC().Unix(), 10)
	expiredValues, err := client.Do(ctx, client.B().Zrangebyscore().Key(verifiedPlayerTrackingKey).
		Min("-inf").Max(now).Build()).ToArray()
	if err != nil {
		return nil, err
	}
	if len(expiredValues) > 0 {
		expired := make([]string, 0, len(expiredValues))
		for _, value := range expiredValues {
			if tag, valueErr := value.ToString(); valueErr == nil && tag != "" {
				expired = append(expired, tag)
			}
		}
		if len(expired) > 0 {
			if err := client.Do(ctx, client.B().Hdel().Key(verifiedPlayerClanHashKey).Field(expired...).Build()).Error(); err != nil {
				return nil, err
			}
		}
	}
	if err := client.Do(ctx, client.B().Zremrangebyscore().Key(verifiedPlayerTrackingKey).
		Min("-inf").Max(now).Build()).Error(); err != nil {
		return nil, err
	}
	values, err := client.Do(ctx, client.B().Zrangebyscore().Key(verifiedPlayerTrackingKey).
		Min(now).Max("+inf").Build()).ToArray()
	if err != nil {
		return nil, err
	}
	tags := make([]string, 0, len(values))
	for _, value := range values {
		tag, err := value.ToString()
		if err == nil && tag != "" {
			tags = append(tags, tag)
		}
	}
	return tags, nil
}

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
