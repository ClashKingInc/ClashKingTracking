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
	"log/slog"
	"net/http"
	"strings"
	"time"

	"clashking_tracking/internal/platform"

	"github.com/jackc/pgx/v5/pgxpool"
	valkey "github.com/valkey-io/valkey-go"
)

const mobileEventsDomainName = "mobilepush"

type mobileEventsDomain struct{}

func NewMobileEventsDomain() platform.Domain { return &mobileEventsDomain{} }

func (d *mobileEventsDomain) Name() string { return mobileEventsDomainName }

type mobileSubscription struct {
	Provider           string
	Environment        string
	TokenCiphertext    string
	WarStartEnabled    bool
	ScoreChangeEnabled bool
	WarEndEnabled      bool
	CWLRankEnabled     bool
}

type mobileWarEvent struct {
	Topic     string
	ClanTag   string
	Timestamp time.Time
	Value     map[string]any
}

func (d *mobileEventsDomain) Run(ctx context.Context, app *platform.App) error {
	if err := validateMobileEventsConfig(app.Config, app.Valkey); err != nil {
		return err
	}
	pool, err := pgxpool.New(ctx, app.Config.TimescaleURL)
	if err != nil {
		return err
	}
	defer pool.Close()

	worker := &mobileEventsWorker{
		client: app.Valkey,
		cfg:    app.Config,
		pool:   pool,
		http:   &http.Client{Timeout: 15 * time.Second},
		logger: app.Logger,
	}
	if err := worker.ensureGroup(ctx); err != nil {
		return err
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		entries, err := worker.claimPending(ctx)
		if err == nil && len(entries) == 0 {
			entries, err = worker.readPending(ctx)
		}
		if err == nil && len(entries) == 0 {
			entries, err = worker.read(ctx)
		}
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

func validateMobileEventsConfig(cfg platform.Config, client valkey.Client) error {
	if client == nil {
		return errors.New("valkey_addr is required for mobilepush")
	}
	if cfg.EventStreamName == "" {
		return errors.New("events.stream is required for mobilepush")
	}
	if cfg.EventStreamReclaimIdleSeconds <= 0 {
		return errors.New("events.reclaim_idle_seconds must be greater than zero for mobilepush")
	}
	if cfg.TimescaleURL == "" {
		return errors.New("TIMESCALE_URL is required for mobilepush")
	}
	hasAPNSToken := cfg.MobilePushAPNSBearerToken != ""
	hasAPNSBundle := cfg.MobilePushAPNSBundleID != ""
	if hasAPNSToken != hasAPNSBundle {
		return errors.New("both MOBILE_PUSH_APNS_BEARER_TOKEN and MOBILE_PUSH_APNS_BUNDLE_ID are required for APNS")
	}
	hasFCMToken := cfg.MobilePushFCMBearerToken != ""
	hasFCMProject := cfg.MobilePushFCMProjectID != ""
	if hasFCMToken != hasFCMProject {
		return errors.New("both MOBILE_PUSH_FCM_BEARER_TOKEN and MOBILE_PUSH_FCM_PROJECT_ID are required for FCM")
	}
	if !hasAPNSToken && !hasFCMToken {
		return errors.New("APNS or FCM credentials are required for mobilepush")
	}
	if cfg.MobilePushTokenKey == "" {
		return errors.New("MOBILE_PUSH_TOKEN_KEY or ENCRYPTION_KEY is required for mobilepush delivery")
	}
	return nil
}

type mobileEventsWorker struct {
	client valkey.Client
	cfg    platform.Config
	pool   *pgxpool.Pool
	http   *http.Client
	logger *slog.Logger
}

func (w *mobileEventsWorker) group() string {
	return "mobilepush"
}

func (w *mobileEventsWorker) consumer() string {
	if w.cfg.EventStreamConsumer != "" {
		return w.cfg.EventStreamConsumer + ":mobilepush"
	}
	return "mobilepush"
}

func (w *mobileEventsWorker) ensureGroup(ctx context.Context) error {
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

func (w *mobileEventsWorker) read(ctx context.Context) ([]valkey.XRangeEntry, error) {
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

func (w *mobileEventsWorker) readPending(ctx context.Context) ([]valkey.XRangeEntry, error) {
	result, err := w.client.Do(ctx, w.client.B().Xreadgroup().
		Group(w.group(), w.consumer()).
		Count(50).
		Streams().
		Key(w.cfg.EventStreamName).
		Id("0").
		Build(),
	).AsXRead()
	if err != nil {
		return nil, err
	}
	return result[w.cfg.EventStreamName], nil
}

func (w *mobileEventsWorker) claimPending(ctx context.Context) ([]valkey.XRangeEntry, error) {
	minIdle := fmt.Sprintf("%d", w.cfg.EventStreamReclaimIdleSeconds*1000)
	values, err := w.client.Do(ctx, w.client.B().Xautoclaim().
		Key(w.cfg.EventStreamName).
		Group(w.group()).
		Consumer(w.consumer()).
		MinIdleTime(minIdle).
		Start("0-0").
		Count(50).
		Build(),
	).ToArray()
	if err != nil {
		if valkey.IsValkeyNil(err) {
			return nil, nil
		}
		return nil, err
	}
	if len(values) < 2 {
		return nil, nil
	}
	return values[1].AsXRange()
}

func (w *mobileEventsWorker) processEntries(ctx context.Context, entries []valkey.XRangeEntry) error {
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

func (w *mobileEventsWorker) ack(ctx context.Context, id string) error {
	return w.client.Do(ctx, w.client.B().Xack().
		Key(w.cfg.EventStreamName).
		Group(w.group()).
		Id(id).
		Build(),
	).Error()
}

func (w *mobileEventsWorker) processEvent(ctx context.Context, event mobileWarEvent) error {
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
				w.logDeliveryError("mobile APNS delivery failed", "clan_tag", event.ClanTag, "err", err)
			}
		case "fcm":
			if err := w.sendFCMNotification(ctx, token, title, body); err != nil {
				w.logDeliveryError("mobile FCM delivery failed", "clan_tag", event.ClanTag, "err", err)
			}
		}
	}
	return nil
}

const mobileSubscriptionsSQL = `
	SELECT d.provider, d.environment, d.token_ciphertext,
	       s.war_start_enabled, s.score_change_enabled, s.war_end_enabled,
	       s.cwl_rank_enabled
	FROM mobile_war_subscriptions s
	JOIN mobile_push_devices d
	  ON d.user_id = s.user_id
	 AND d.device_id = s.device_id
	 AND d.enabled = true
	WHERE s.clan_tag = $1
	  AND s.enabled = true
`

func (w *mobileEventsWorker) subscriptions(ctx context.Context, clanTag string) ([]mobileSubscription, error) {
	rows, err := w.pool.Query(ctx, mobileSubscriptionsSQL, clanTag)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []mobileSubscription
	for rows.Next() {
		var sub mobileSubscription
		if err := rows.Scan(&sub.Provider, &sub.Environment, &sub.TokenCiphertext, &sub.WarStartEnabled, &sub.ScoreChangeEnabled, &sub.WarEndEnabled, &sub.CWLRankEnabled); err != nil {
			return nil, err
		}
		out = append(out, sub)
	}
	return out, rows.Err()
}

func (w *mobileEventsWorker) logDeliveryError(message string, args ...any) {
	if w.logger != nil {
		w.logger.Error(message, args...)
	}
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

func (w *mobileEventsWorker) sendAPNSNotification(ctx context.Context, environment, token, title, body string) error {
	if w.cfg.MobilePushAPNSBearerToken == "" || w.cfg.MobilePushAPNSBundleID == "" {
		return nil
	}
	topic := w.cfg.MobilePushAPNSBundleID
	payload := map[string]any{"aps": map[string]any{"alert": map[string]string{"title": title, "body": body}, "sound": "default"}}
	return w.postAPNS(ctx, environment, token, topic, "alert", payload)
}

func (w *mobileEventsWorker) postAPNS(ctx context.Context, environment, token, topic, pushType string, payload map[string]any) error {
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

func (w *mobileEventsWorker) sendFCMNotification(ctx context.Context, token, title, body string) error {
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
