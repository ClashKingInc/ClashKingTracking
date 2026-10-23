package scripts

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/disgoorg/disgo/discord"
	"github.com/disgoorg/disgo/rest"
	"github.com/disgoorg/snowflake/v2"
	"github.com/jackc/pgx/v5"
	valkey "github.com/valkey-io/valkey-go"
)

func discordWarEnded(event mobileWarEvent) bool {
	if event.Topic != "war" {
		return false
	}
	if kind := stringMapValue(event.Value, "type"); kind != "war_state" && kind != "new_war" {
		return false
	}
	war, ok := mapValue(event.Value["war"])
	return ok && stringMapValue(war, "state") == "warEnded"
}

func (w *discordDeliveryWorker) deliverWarPanel(ctx context.Context, event mobileWarEvent, target, resolved discordDeliveryTarget) error {
	message, ok := discordWarPanelMessage(event, w.emojis)
	if !ok {
		return nil
	}
	identity := discordWarIdentity(event)
	if identity == "" {
		return nil
	}
	tx, err := w.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	var current discordDeliveryTarget
	var disabled bool
	err = tx.QueryRow(ctx, `SELECT webhook_id, COALESCE(thread_id,''), COALESCE(active_war_id,''),
		COALESCE(message_id,''), disabled FROM server_logs
		WHERE server_id=$1 AND clan_tag=$2 AND type='war_panel' FOR UPDATE`, target.ServerID, target.ClanTag).
		Scan(&current.WebhookID, &current.ThreadID, &current.ActiveWarID, &current.MessageID, &disabled)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	if disabled || current.WebhookID != target.WebhookID || current.ThreadID != target.ThreadID {
		return nil // Configuration changed after the destination snapshot.
	}
	if warPanelOperation(current.ActiveWarID, current.MessageID, identity) == "skip" {
		return nil
	}
	webhookID, err := snowflake.Parse(resolved.WebhookID)
	if err != nil {
		return err
	}
	params := rest.UpdateWebhookMessageParams{WithComponents: true}
	if resolved.ThreadID != "" {
		params.ThreadID, err = snowflake.Parse(resolved.ThreadID)
		if err != nil {
			return err
		}
	}
	if warPanelOperation(current.ActiveWarID, current.MessageID, identity) == "edit" {
		messageID, err := snowflake.Parse(current.MessageID)
		if err == nil {
			_, err = w.client.Rest.UpdateWebhookMessage(webhookID, resolved.Secret, messageID,
				discord.WebhookMessageUpdate{Embeds: &message.Embeds, Components: &message.Components}, params)
			if err == nil {
				if err := tx.Commit(ctx); err != nil {
					return err
				}
				return w.sendWarResultOnce(ctx, event, resolved)
			}
			var discordErr *rest.Error
			if !errors.As(err, &discordErr) || discordErr.Code != rest.JSONErrorCodeUnknownMessage {
				return err
			}
		}
	}
	createParams := rest.CreateWebhookMessageParams{Wait: true, ThreadID: params.ThreadID, WithComponents: true}
	created, err := w.client.Rest.CreateWebhookMessage(webhookID, resolved.Secret, discord.WebhookMessageCreate{
		Embeds: message.Embeds, Components: message.Components, AllowedMentions: &discord.AllowedMentions{Parse: []discord.AllowedMentionType{}},
	}, createParams)
	if err != nil {
		return err
	}
	if created == nil {
		return errors.New("war panel webhook did not return a message")
	}
	_, err = tx.Exec(ctx, `UPDATE server_logs SET active_war_id=$1,message_id=$2,updated_at=now()
		WHERE server_id=$3 AND clan_tag=$4 AND type='war_panel' AND webhook_id=$5
		AND COALESCE(thread_id,'')=$6 AND disabled=false`, identity, created.ID.String(), target.ServerID, target.ClanTag, target.WebhookID, target.ThreadID)
	if err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	return w.sendWarResultOnce(ctx, event, resolved)
}

func warPanelOperation(activeWarID, messageID, incomingWarID string) string {
	if incomingWarID == "" {
		return "skip"
	}
	if activeWarID == incomingWarID && messageID != "" {
		return "edit"
	}
	if activeWarID != "" && activeWarID != incomingWarID {
		oldPrep, oldErr := strconv.ParseInt(activeWarID[strings.LastIndex(activeWarID, "-")+1:], 10, 64)
		newPrep, newErr := strconv.ParseInt(incomingWarID[strings.LastIndex(incomingWarID, "-")+1:], 10, 64)
		if oldErr == nil && newErr == nil && oldPrep > newPrep {
			return "skip"
		}
	}
	return "create"
}

type warEndStageStore interface {
	Acquire(context.Context, string, string) (bool, error)
	Release(context.Context, string, string) error
	Done(context.Context, string, string) (bool, error)
	Mark(context.Context, string, string) error
}

type valkeyWarEndStageStore struct{ client valkey.Client }

var warEndAcquireScript = valkey.NewLuaScript(`
	if redis.call('SET', KEYS[1], ARGV[1], 'NX', 'EX', 120) then return 1 end
	return 0
`)
var warEndReleaseScript = valkey.NewLuaScript(`
	if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('DEL', KEYS[1]) end
	return 0
`)
var warEndMarkScript = valkey.NewLuaScript(`
	redis.call('SADD', KEYS[1], ARGV[1])
	redis.call('EXPIRE', KEYS[1], 2592000)
	return 1
`)

func (s valkeyWarEndStageStore) Acquire(ctx context.Context, key, token string) (bool, error) {
	value, err := warEndAcquireScript.Exec(ctx, s.client, []string{key + ":lock"}, []string{token}).AsInt64()
	return value == 1, err
}
func (s valkeyWarEndStageStore) Release(ctx context.Context, key, token string) error {
	return warEndReleaseScript.Exec(ctx, s.client, []string{key + ":lock"}, []string{token}).Error()
}
func (s valkeyWarEndStageStore) Done(ctx context.Context, key, stage string) (bool, error) {
	value, err := s.client.Do(ctx, s.client.B().Sismember().Key(key+":stages").Member(stage).Build()).AsBool()
	return value, err
}
func (s valkeyWarEndStageStore) Mark(ctx context.Context, key, stage string) error {
	return warEndMarkScript.Exec(ctx, s.client, []string{key + ":stages"}, []string{stage}).Error()
}

func discordWarEndMarkerKey(stream string, target discordDeliveryTarget, identity string) string {
	return fmt.Sprintf("%s:discord:war-end:%s:%s:%s:%s:%s", stream, target.ServerID, target.WebhookID, target.ThreadID, target.LogType, identity)
}

// Each stage is marked only after Discord accepts it. A failed image or missed
// report stays eligible on replay, while earlier successful stages are skipped.
func runWarEndStages(ctx context.Context, store warEndStageStore, key string, stages []struct {
	name string
	send func() error
}) error {
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return err
	}
	token := hex.EncodeToString(nonce[:])
	claimed, err := store.Acquire(ctx, key, token)
	if err != nil {
		return err
	}
	if !claimed {
		return nil
	} // Another owner holds the bounded lease.
	defer store.Release(context.WithoutCancel(ctx), key, token)
	for _, stage := range stages {
		done, err := store.Done(ctx, key, stage.name)
		if err != nil {
			return err
		}
		if done {
			continue
		}
		if err := stage.send(); err != nil {
			return err
		}
		if err := store.Mark(ctx, key, stage.name); err != nil {
			return err
		}
	}
	return nil
}

func (w *discordDeliveryWorker) sendWarResultOnce(ctx context.Context, event mobileWarEvent, target discordDeliveryTarget) error {
	if !discordWarEnded(event) {
		return nil
	}
	identity := discordWarIdentity(event)
	if identity == "" {
		return nil
	}
	key := discordWarEndMarkerKey(w.app.Config.EventStreamName, target, identity)
	stages := []struct {
		name string
		send func() error
	}{{name: "image", send: func() error {
		image, err := discordWarResultImage(event)
		if err != nil {
			return err
		}
		return w.sendMessage(ctx, target, image, nil)
	}}}
	for i, missed := range discordWarMissedHitsMessages(event, w.emojis) {
		message := missed
		stages = append(stages, struct {
			name string
			send func() error
		}{name: fmt.Sprintf("missed:%d", i), send: func() error { return w.sendMessage(ctx, target, message, nil) }})
	}
	return runWarEndStages(ctx, valkeyWarEndStageStore{client: w.app.Valkey}, key, stages)
}
