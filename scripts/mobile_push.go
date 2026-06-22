package scripts

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"clashking_tracking/internal/platform"

	"github.com/jackc/pgx/v5/pgxpool"
	valkey "github.com/valkey-io/valkey-go"
)

const mobilePushDomainName = "mobilepush"

type mobilePushDomain struct{}

func NewMobilePushDomain() platform.Domain { return &mobilePushDomain{} }

func (d *mobilePushDomain) Name() string { return mobilePushDomainName }

type mobileSubscription struct {
	Provider            string
	Environment         string
	TokenCiphertext     string
	WarStartEnabled     bool
	ScoreChangeEnabled  bool
	WarEndEnabled       bool
	CWLRankEnabled      bool
	LiveActivityEnabled bool
}

type mobileLiveActivity struct {
	ID              string
	Environment     string
	TokenCiphertext string
	LastPayloadHash string
}

type mobileWarEvent struct {
	Topic     string
	ClanTag   string
	Timestamp time.Time
	Value     map[string]any
}

func (d *mobilePushDomain) Run(ctx context.Context, app *platform.App) error {
	if err := validateMobilePushConfig(app.Config, app.Valkey); err != nil {
		return err
	}
	pool, err := pgxpool.New(ctx, app.Config.TimescaleURL)
	if err != nil {
		return err
	}
	defer pool.Close()

	worker := &mobilePushWorker{
		client: app.Valkey,
		cfg:    app.Config,
		pool:   pool,
		http:   http.DefaultClient,
	}
	if err := worker.ensureGroup(ctx); err != nil {
		return err
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		entries, err := worker.read(ctx)
		if err != nil {
			if valkey.IsValkeyNil(err) {
				continue
			}
			return err
		}
		if err := worker.processEntries(ctx, entries); err != nil {
			return err
		}
	}
}

func validateMobilePushConfig(cfg platform.Config, client valkey.Client) error {
	if client == nil {
		return errors.New("valkey_addr is required for mobilepush")
	}
	if cfg.EventStreamName == "" {
		return errors.New("events.stream is required for mobilepush")
	}
	if cfg.TimescaleURL == "" {
		return errors.New("TIMESCALE_URL is required for mobilepush")
	}
	if (cfg.MobilePushAPNSBearerToken != "" || cfg.MobilePushFCMBearerToken != "") && cfg.MobilePushTokenKey == "" {
		return errors.New("MOBILE_PUSH_TOKEN_KEY or ENCRYPTION_KEY is required for mobilepush delivery")
	}
	return nil
}

type mobilePushWorker struct {
	client valkey.Client
	cfg    platform.Config
	pool   *pgxpool.Pool
	http   *http.Client
}

func (w *mobilePushWorker) group() string {
	return "mobilepush"
}

func (w *mobilePushWorker) consumer() string {
	if w.cfg.EventStreamConsumer != "" {
		return w.cfg.EventStreamConsumer + ":mobilepush"
	}
	return "mobilepush"
}

func (w *mobilePushWorker) ensureGroup(ctx context.Context) error {
	err := w.client.Do(ctx, w.client.B().XgroupCreate().
		Key(w.cfg.EventStreamName).
		Group(w.group()).
		Id("0").
		Mkstream().
		Build(),
	).Error()
	if err != nil && !strings.Contains(err.Error(), "BUSYGROUP") {
		return err
	}
	return nil
}

func (w *mobilePushWorker) read(ctx context.Context) ([]valkey.XRangeEntry, error) {
	result, err := w.client.Do(ctx, w.client.B().Xreadgroup().
		Group(w.group(), w.consumer()).
		Count(50).
		Block(5000).
		Streams().
		Key(w.cfg.EventStreamName).
		Id(">").
		Build(),
	).AsXRead()
	if err != nil {
		return nil, err
	}
	return result[w.cfg.EventStreamName], nil
}

func (w *mobilePushWorker) processEntries(ctx context.Context, entries []valkey.XRangeEntry) error {
	for _, entry := range entries {
		event, ok := mobileEventFromEntry(entry)
		if !ok || !mobilePushEventType(event) {
			if err := w.ack(ctx, entry.ID); err != nil {
				return err
			}
			continue
		}
		if err := w.processEvent(ctx, event); err != nil {
			return err
		}
		if err := w.ack(ctx, entry.ID); err != nil {
			return err
		}
	}
	return nil
}

func (w *mobilePushWorker) ack(ctx context.Context, id string) error {
	return w.client.Do(ctx, w.client.B().Xack().
		Key(w.cfg.EventStreamName).
		Group(w.group()).
		Id(id).
		Build(),
	).Error()
}

func (w *mobilePushWorker) processEvent(ctx context.Context, event mobileWarEvent) error {
	subscriptions, err := w.subscriptions(ctx, event.ClanTag)
	if err != nil {
		return err
	}
	title, body := mobileNotificationText(event)
	for _, sub := range subscriptions {
		if !subscriptionWantsEvent(sub, event) {
			continue
		}
		token := decodeMobileToken(sub.TokenCiphertext, w.cfg.MobilePushTokenKey)
		if token == "" {
			continue
		}
		switch sub.Provider {
		case "apns":
			if err := w.sendAPNSNotification(ctx, sub.Environment, token, title, body); err != nil {
				return err
			}
		case "fcm":
			if err := w.sendFCMNotification(ctx, token, title, body); err != nil {
				return err
			}
		}
	}
	return w.updateLiveActivities(ctx, event)
}

func (w *mobilePushWorker) subscriptions(ctx context.Context, clanTag string) ([]mobileSubscription, error) {
	rows, err := w.pool.Query(ctx, `
		SELECT d.provider, d.environment, d.token_ciphertext,
		       s.war_start_enabled, s.score_change_enabled, s.war_end_enabled,
		       s.cwl_rank_enabled, s.live_activity_enabled
		FROM mobile_war_subscriptions s
		JOIN mobile_push_devices d
		  ON d.user_id = s.user_id
		 AND d.device_id = s.device_id
		 AND d.enabled = true
		WHERE s.clan_tag = $1
		  AND s.enabled = true
	`, clanTag)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []mobileSubscription
	for rows.Next() {
		var sub mobileSubscription
		if err := rows.Scan(&sub.Provider, &sub.Environment, &sub.TokenCiphertext, &sub.WarStartEnabled, &sub.ScoreChangeEnabled, &sub.WarEndEnabled, &sub.CWLRankEnabled, &sub.LiveActivityEnabled); err != nil {
			return nil, err
		}
		out = append(out, sub)
	}
	return out, rows.Err()
}

func (w *mobilePushWorker) updateLiveActivities(ctx context.Context, event mobileWarEvent) error {
	payload := mobileLiveActivityPayload(event)
	payloadHash := mobilePayloadHash(payload)
	rows, err := w.pool.Query(ctx, `
		SELECT id::text, environment, push_token_ciphertext, COALESCE(last_payload_hash, '')
		FROM mobile_live_activities
		WHERE clan_tag = $1
		  AND status = 'active'
	`, event.ClanTag)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var activity mobileLiveActivity
		if err := rows.Scan(&activity.ID, &activity.Environment, &activity.TokenCiphertext, &activity.LastPayloadHash); err != nil {
			return err
		}
		if activity.LastPayloadHash == payloadHash {
			continue
		}
		token := decodeMobileToken(activity.TokenCiphertext, w.cfg.MobilePushTokenKey)
		if token == "" {
			continue
		}
		if err := w.sendAPNSLiveActivity(ctx, activity.Environment, token, payload); err != nil {
			return err
		}
		if _, err := w.pool.Exec(ctx, `
			UPDATE mobile_live_activities
			SET last_payload_hash = $1, updated_at = now()
			WHERE id = $2::uuid
		`, payloadHash, activity.ID); err != nil {
			return err
		}
	}
	return rows.Err()
}

func mobileEventFromEntry(entry valkey.XRangeEntry) (mobileWarEvent, bool) {
	timestamp, _ := time.Parse(time.RFC3339Nano, entry.FieldValues["timestamp"])
	var value map[string]any
	if err := json.Unmarshal([]byte(entry.FieldValues["value"]), &value); err != nil {
		return mobileWarEvent{}, false
	}
	return mobileWarEvent{
		Topic:     entry.FieldValues["topic"],
		ClanTag:   entry.FieldValues["clan_tag"],
		Timestamp: timestamp,
		Value:     value,
	}, true
}

func mobilePushEventType(event mobileWarEvent) bool {
	eventType, _ := event.Value["type"].(string)
	switch eventType {
	case "new_war", "new_attacks", "war_state", "cwl_war_update", "cwl_new_attacks":
		return event.Topic == "war" || event.Topic == "cwl"
	default:
		return false
	}
}

func subscriptionWantsEvent(sub mobileSubscription, event mobileWarEvent) bool {
	eventType, _ := event.Value["type"].(string)
	switch eventType {
	case "new_war":
		return sub.WarStartEnabled
	case "new_attacks":
		return sub.ScoreChangeEnabled
	case "war_state":
		return sub.WarEndEnabled || sub.WarStartEnabled
	case "cwl_war_update", "cwl_new_attacks":
		return sub.CWLRankEnabled || sub.ScoreChangeEnabled
	default:
		return false
	}
}

func mobileNotificationText(event mobileWarEvent) (string, string) {
	eventType, _ := event.Value["type"].(string)
	switch eventType {
	case "new_war":
		return "Clan war started", "A new war is available for your selected clan."
	case "new_attacks", "cwl_new_attacks":
		return "War score updated", "A new attack changed the clan war score."
	case "war_state":
		return "War status changed", "Your clan war status changed."
	case "cwl_war_update":
		return "CWL updated", "Your Clan War League round has new information."
	default:
		return "ClashKing war update", "Your selected clan has a war update."
	}
}

func mobileLiveActivityPayload(event mobileWarEvent) map[string]any {
	title, body := mobileNotificationText(event)
	return map[string]any{
		"aps": map[string]any{
			"timestamp": time.Now().Unix(),
			"event":     "update",
			"alert": map[string]any{
				"title": title,
				"body":  body,
			},
			"content-state": map[string]any{
				"state":         mobileStringValue(event.Value["new_state"], "inWar"),
				"mode":          event.Topic,
				"clanName":      event.ClanTag,
				"opponentName":  "Opponent",
				"clanStars":     0,
				"opponentStars": 0,
				"timeState":     title,
			},
		},
	}
}

func (w *mobilePushWorker) sendAPNSNotification(ctx context.Context, environment, token, title, body string) error {
	if w.cfg.MobilePushAPNSBearerToken == "" || w.cfg.MobilePushAPNSBundleID == "" {
		return nil
	}
	topic := w.cfg.MobilePushAPNSBundleID
	payload := map[string]any{"aps": map[string]any{"alert": map[string]string{"title": title, "body": body}, "sound": "default"}}
	return w.postAPNS(ctx, environment, token, topic, "alert", payload)
}

func (w *mobilePushWorker) sendAPNSLiveActivity(ctx context.Context, environment, token string, payload map[string]any) error {
	if w.cfg.MobilePushAPNSBearerToken == "" || w.cfg.MobilePushAPNSBundleID == "" {
		return nil
	}
	return w.postAPNS(ctx, environment, token, w.cfg.MobilePushAPNSBundleID+".push-type.liveactivity", "liveactivity", payload)
}

func (w *mobilePushWorker) postAPNS(ctx context.Context, environment, token, topic, pushType string, payload map[string]any) error {
	host := "https://api.push.apple.com"
	if environment == "sandbox" {
		host = "https://api.sandbox.push.apple.com"
	}
	body, _ := json.Marshal(payload)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, fmt.Sprintf("%s/3/device/%s", host, token), bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("authorization", "bearer "+w.cfg.MobilePushAPNSBearerToken)
	req.Header.Set("apns-topic", topic)
	req.Header.Set("apns-push-type", pushType)
	req.Header.Set("content-type", "application/json")
	resp, err := w.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return fmt.Errorf("apns returned status %d", resp.StatusCode)
	}
	return nil
}

func (w *mobilePushWorker) sendFCMNotification(ctx context.Context, token, title, body string) error {
	if w.cfg.MobilePushFCMBearerToken == "" || w.cfg.MobilePushFCMProjectID == "" {
		return nil
	}
	payload := map[string]any{
		"message": map[string]any{
			"token": token,
			"notification": map[string]string{
				"title": title,
				"body":  body,
			},
		},
	}
	raw, _ := json.Marshal(payload)
	url := fmt.Sprintf("https://fcm.googleapis.com/v1/projects/%s/messages:send", w.cfg.MobilePushFCMProjectID)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(raw))
	if err != nil {
		return err
	}
	req.Header.Set("authorization", "Bearer "+w.cfg.MobilePushFCMBearerToken)
	req.Header.Set("content-type", "application/json")
	resp, err := w.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return fmt.Errorf("fcm returned status %d", resp.StatusCode)
	}
	return nil
}

func decodeMobileToken(ciphertext, keySource string) string {
	if strings.HasPrefix(ciphertext, "v1:") {
		return decryptMobileToken(strings.TrimPrefix(ciphertext, "v1:"), keySource)
	}
	raw, err := base64.URLEncoding.DecodeString(ciphertext)
	if err != nil {
		return ""
	}
	return string(raw)
}

func decryptMobileToken(ciphertext, keySource string) string {
	if keySource == "" {
		return ""
	}
	key := sha256.Sum256([]byte(keySource))
	block, err := aes.NewCipher(key[:])
	if err != nil {
		return ""
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return ""
	}
	raw, err := base64.RawURLEncoding.DecodeString(ciphertext)
	if err != nil || len(raw) <= gcm.NonceSize() {
		return ""
	}
	nonce := raw[:gcm.NonceSize()]
	sealed := raw[gcm.NonceSize():]
	out, err := gcm.Open(nil, nonce, sealed, nil)
	if err != nil {
		return ""
	}
	return string(out)
}

func mobilePayloadHash(payload map[string]any) string {
	raw, _ := json.Marshal(payload)
	sum := sha256.Sum256(raw)
	return fmt.Sprintf("%x", sum[:])
}

func mobileStringValue(value any, fallback string) string {
	if text, ok := value.(string); ok && text != "" {
		return text
	}
	return fallback
}
