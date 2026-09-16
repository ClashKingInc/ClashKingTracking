package scripts

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"clashking_tracking/internal/platform"
	"clashking_tracking/internal/utils"
	"clashking_tracking/models"

	clashy "github.com/clashkinginc/clashy.go"
	"github.com/jackc/pgx/v5/pgxpool"
	valkey "github.com/valkey-io/valkey-go"
)

const mobileEventsDomainName = "mobilepush"

type mobileEventsDomain struct{}

func NewMobileEventsDomain() platform.Domain { return &mobileEventsDomain{} }

func (d *mobileEventsDomain) Name() string { return mobileEventsDomainName }

type mobileSubscription struct {
	UserID             string
	DeviceID           string
	Provider           string
	Environment        string
	TokenCiphertext    string
	WarStartEnabled    bool
	ScoreChangeEnabled bool
	WarEndEnabled      bool
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
		app:    app,
		logger: app.Logger,
	}
	return worker.run(ctx, app, trackingProgressName(mobileEventsDomainName, "events"))
}

func (w *mobileEventsWorker) run(ctx context.Context, app *platform.App, statsName string) error {
	if err := w.ensureGroup(ctx); err != nil {
		return err
	}
	readFreshNext := false
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		var err error
		var entries []valkey.XRangeEntry
		if readFreshNext {
			entries, err = w.read(ctx)
			readFreshNext = false
		} else {
			entries, err = w.claimPending(ctx)
			if err == nil && len(entries) == 0 {
				entries, err = w.readPending(ctx)
			}
		}
		if err == nil && len(entries) == 0 {
			entries, err = w.read(ctx)
		}
		if err != nil {
			if valkey.IsValkeyNil(err) {
				continue
			}
			return err
		}
		app.Stats.SetQueueDepth(statsName, len(entries))
		started := time.Now()
		if err := w.processEntries(ctx, entries); err != nil {
			readFreshNext = true
			app.Stats.RecordRequest(statsName, time.Since(started), err)
			app.Stats.SetReady(statsName, false, err.Error())
			app.Logger.Error("mobile event delivery failed; leaving stream entry pending", "err", err)
			if app.Errors != nil {
				app.Errors.Capture(err, map[string]string{"domain": mobileEventsDomainName, "operation": "stream-delivery"})
			}
			if err := sleepOrDone(ctx, time.Second); err != nil {
				return err
			}
			continue
		}
		app.Stats.SetQueueDepth(statsName, 0)
		app.Stats.RecordProcess(statsName, time.Since(started))
		app.Stats.SetReady(statsName, true, "")
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
		return errors.New("TIMESCALE_* connection variables are required for mobilepush")
	}
	if cfg.MobilePushFCMProjectID == "" {
		return errors.New("MOBILE_PUSH_FCM_PROJECT_ID is required for mobilepush")
	}
	if cfg.MobilePushTokenKey == "" {
		return errors.New("DATA_ENCRYPTION_KEY is required for mobilepush delivery")
	}
	return nil
}

type mobileEventsWorker struct {
	client        valkey.Client
	cfg           platform.Config
	pool          *pgxpool.Pool
	app           *platform.App
	logger        *slog.Logger
	legendMarkers legendDeliveryMarkerStore
	processEntry  func(context.Context, string, mobileWarEvent) error
	ackEntry      func(context.Context, string) error
}

type legendDeliveryMarkerStore interface {
	Delivered(context.Context, string, string) (bool, error)
	MarkDelivered(context.Context, string, string) error
}

type valkeyLegendDeliveryMarkers struct {
	client valkey.Client
	stream string
	ttl    time.Duration
}

var markLegendDeliveryScript = valkey.NewLuaScript(`
	redis.call('SADD', KEYS[1], ARGV[1])
	redis.call('EXPIRE', KEYS[1], ARGV[2])
	return 1
`)

func legendDeliveryMarkerTTL(cfg platform.Config) time.Duration {
	// Keep per-device retry state slightly longer than the shared stream entry
	// without extending retention for every high-volume domain event.
	ttl := 2 * time.Duration(cfg.EventStreamRetentionSeconds) * time.Second
	if ttl < 10*time.Minute {
		ttl = 10 * time.Minute
	}
	if ttl > time.Hour {
		ttl = time.Hour
	}
	return ttl
}

func legendDeliveryMarkerIdentity(value string) string {
	return fmt.Sprintf("%x", sha256.Sum256([]byte(value)))
}

func (m *valkeyLegendDeliveryMarkers) key(eventID string) string {
	return m.stream + ":mobilepush:legend-delivered:" + legendDeliveryMarkerIdentity(eventID)
}

func (m *valkeyLegendDeliveryMarkers) Delivered(ctx context.Context, eventID, deviceID string) (bool, error) {
	if m == nil || m.client == nil {
		return false, errors.New("Valkey is required for Legend delivery retry markers")
	}
	return m.client.Do(ctx, m.client.B().Sismember().Key(m.key(eventID)).Member(legendDeliveryMarkerIdentity(deviceID)).Build()).AsBool()
}

func (m *valkeyLegendDeliveryMarkers) MarkDelivered(ctx context.Context, eventID, deviceID string) error {
	if m == nil || m.client == nil {
		return errors.New("Valkey is required for Legend delivery retry markers")
	}
	seconds := int64(m.ttl / time.Second)
	return markLegendDeliveryScript.Exec(ctx, m.client, []string{m.key(eventID)}, []string{
		legendDeliveryMarkerIdentity(deviceID), fmt.Sprintf("%d", seconds),
	}).Error()
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
	process := w.processEvent
	if w.processEntry != nil {
		process = w.processEntry
	}
	ack := w.ack
	if w.ackEntry != nil {
		ack = w.ackEntry
	}
	var entryErrors []error
	for _, entry := range entries {
		event, ok := mobileEventFromEntry(entry)
		if !ok || !mobilePushEventType(event) {
			if err := ack(ctx, entry.ID); err != nil {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				entryErrors = append(entryErrors, fmt.Errorf("acknowledge ignored mobile event %s: %w", entry.ID, err))
			}
			continue
		}
		if err := process(ctx, entry.ID, event); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			entryErrors = append(entryErrors, fmt.Errorf("process mobile event %s: %w", entry.ID, err))
			continue
		}
		if err := ack(ctx, entry.ID); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			entryErrors = append(entryErrors, fmt.Errorf("acknowledge mobile event %s: %w", entry.ID, err))
		}
	}
	return errors.Join(entryErrors...)
}

func (w *mobileEventsWorker) ack(ctx context.Context, id string) error {
	return w.client.Do(ctx, w.client.B().Xack().
		Key(w.cfg.EventStreamName).
		Group(w.group()).
		Id(id).
		Build(),
	).Error()
}

func (w *mobileEventsWorker) processEvent(ctx context.Context, entryID string, event mobileWarEvent) error {
	if event.Topic == "reminder" {
		switch stringValue(event.Value["type"]) {
		case "war":
			return w.processWarReminder(ctx, event)
		case "raid_mobile":
			return w.processRaidReminder(ctx, event)
		default:
			return nil
		}
	}
	if event.Topic == "legend" && stringValue(event.Value["type"]) == "legend_defense" {
		return w.processLegendDefense(ctx, entryID, event)
	}
	subscriptions, err := w.subscriptions(ctx, event.ClanTag, event.Topic)
	if err != nil {
		return err
	}
	title, body := mobileNotificationText(event)
	var deliveryErrors []error
	for _, sub := range subscriptions {
		if !subscriptionWantsEvent(sub, event) {
			continue
		}
		token, err := utils.DecryptSecret(sub.TokenCiphertext, w.cfg.MobilePushTokenKey)
		if err != nil || token == "" {
			w.logDeliveryError("mobile FCM token decrypt failed", "clan_tag", event.ClanTag, "err", err)
			continue
		}
		if sub.Provider != "fcm" {
			continue
		}
		if err := sendFCM(ctx, w.app, token, pushMessage{Title: title, Body: body, Data: map[string]string{"type": mobileNotificationRouteType(event), "target_tag": event.ClanTag}}); err != nil {
			w.logDeliveryError("mobile FCM delivery failed", "clan_tag", event.ClanTag, "err", err)
			deliveryErrors = append(deliveryErrors, fmt.Errorf("deliver live event to device %s: %w", sub.DeviceID, err))
		}
	}
	return errors.Join(deliveryErrors...)
}

const legendDefenseSubscriptionsSQL = `
	SELECT DISTINCT device.user_id,device.device_id,device.platform,device.provider,
	       device.environment,device.token_ciphertext,device.locale
	FROM mobile_notification_accounts account
	JOIN mobile_notification_preferences preference ON preference.user_id=account.user_id
	JOIN mobile_push_devices device ON device.user_id=account.user_id
	WHERE account.player_tag=$1 AND account.enabled=true
	  AND preference.legend_defenses_enabled=true
	  AND device.enabled=true AND device.provider='fcm'
`

func (w *mobileEventsWorker) processLegendDefense(ctx context.Context, entryID string, event mobileWarEvent) error {
	playerTag := stringValue(event.Value["player_tag"])
	if playerTag == "" {
		return errors.New("Legend defense event is missing player_tag")
	}
	rows, err := w.pool.Query(ctx, legendDefenseSubscriptionsSQL, playerTag)
	if err != nil {
		return err
	}
	defer rows.Close()
	var devices []models.PushDevice
	for rows.Next() {
		var device models.PushDevice
		if err := rows.Scan(&device.UserID, &device.DeviceID, &device.Platform, &device.Provider,
			&device.Environment, &device.TokenCiphertext, &device.Locale); err != nil {
			return err
		}
		devices = append(devices, device)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	data := map[string]string{"type": "legend_defense", "target_tag": playerTag}
	if eventID := stringValue(event.Value["event_id"]); eventID != "" {
		data["event_id"] = eventID
	}
	message := pushMessage{
		Title: "Legend defense",
		Body:  "A new Legend League defense is available.",
		Data:  data,
	}
	deliveryID := stringValue(event.Value["event_id"])
	if deliveryID == "" {
		deliveryID = entryID
	}
	return w.deliverLegendDefenseDevices(ctx, deliveryID, playerTag, devices, message)
}

func (w *mobileEventsWorker) deliverLegendDefenseDevices(ctx context.Context, deliveryID, playerTag string, devices []models.PushDevice, message pushMessage) error {
	markers := w.legendMarkers
	if markers == nil {
		markers = &valkeyLegendDeliveryMarkers{
			client: w.client,
			stream: w.cfg.EventStreamName,
			ttl:    legendDeliveryMarkerTTL(w.cfg),
		}
	}
	var deliveryErrors []error
	for _, device := range devices {
		deviceIdentity := strings.Join([]string{device.UserID, device.DeviceID, device.Provider, device.Environment}, "\x00")
		delivered, err := markers.Delivered(ctx, deliveryID, deviceIdentity)
		if err != nil {
			return fmt.Errorf("read Legend delivery marker for device %s: %w", device.DeviceID, err)
		}
		if delivered {
			continue
		}
		token, err := utils.DecryptSecret(device.TokenCiphertext, w.cfg.MobilePushTokenKey)
		if err != nil || token == "" {
			if err == nil {
				err = errors.New("decrypted token is empty")
			}
			w.logDeliveryError("mobile FCM token decrypt failed", "player_tag", playerTag, "err", err)
			continue
		}
		if err := sendFCM(ctx, w.app, token, message); err != nil {
			w.logDeliveryError("mobile FCM Legend defense delivery failed", "player_tag", playerTag, "err", err)
			if isRetryablePushError(err) {
				deliveryErrors = append(deliveryErrors, fmt.Errorf("deliver Legend defense to device %s: %w", device.DeviceID, err))
			}
			continue
		}
		// The provider send deliberately precedes the marker so delivery remains
		// at-least-once. A process crash between these calls can duplicate this
		// device on retry; marking first would instead lose the notification.
		if err := markers.MarkDelivered(ctx, deliveryID, deviceIdentity); err != nil {
			deliveryErrors = append(deliveryErrors, fmt.Errorf("record Legend delivery for device %s: %w", device.DeviceID, err))
		}
	}
	return errors.Join(deliveryErrors...)
}

func stringValue(value any) string { valueString, _ := value.(string); return valueString }

func (w *mobileEventsWorker) processWarReminder(ctx context.Context, event mobileWarEvent) error {
	war, minutes, err := decodeWarReminderEvent(event)
	if err != nil {
		return err
	}
	attacksPerMember := 2
	if war.WarTag != "" {
		attacksPerMember = 1
	}
	type remainingAccount struct {
		name      string
		remaining int
	}
	remaining := map[string]remainingAccount{}
	for _, side := range []*clashy.WarClan{war.Clan, war.Opponent} {
		if side == nil {
			continue
		}
		for _, member := range side.Members {
			unused := attacksPerMember - len(member.Attacks)
			if unused > 0 {
				remaining[member.Tag] = remainingAccount{name: member.Name, remaining: unused}
			}
		}
	}
	if len(remaining) == 0 {
		return nil
	}
	tags := make([]string, 0, len(remaining))
	for tag := range remaining {
		tags = append(tags, tag)
	}
	rows, err := w.pool.Query(ctx, `
		SELECT account.user_id, account.player_tag, device.device_id,
		       device.platform, device.provider, device.environment,
		       device.token_ciphertext, device.locale
		FROM mobile_notification_accounts account
		JOIN mobile_notification_preferences preference ON preference.user_id = account.user_id
		JOIN mobile_push_devices device
		  ON device.user_id = account.user_id
		 AND device.enabled = true
		 AND device.provider = 'fcm'
		WHERE account.enabled = true
		  AND preference.war_reminders_enabled = true
		  AND $2 = ANY(preference.reminder_timings)
		  AND account.player_tag = ANY($1)
	`, tags, minutes)
	if err != nil {
		return err
	}
	type recipient struct {
		userID    string
		remaining int
		devices   map[string]models.PushDevice
		players   map[string]struct{}
	}
	recipients := map[string]*recipient{}
	for rows.Next() {
		var userID, playerTag string
		var device models.PushDevice
		if err := rows.Scan(&userID, &playerTag, &device.DeviceID, &device.Platform,
			&device.Provider, &device.Environment, &device.TokenCiphertext, &device.Locale); err != nil {
			rows.Close()
			return err
		}
		device.UserID = userID
		if recipients[userID] == nil {
			recipients[userID] = &recipient{userID: userID, devices: make(map[string]models.PushDevice), players: make(map[string]struct{})}
		}
		if _, counted := recipients[userID].players[playerTag]; !counted {
			recipients[userID].remaining += remaining[playerTag].remaining
			recipients[userID].players[playerTag] = struct{}{}
		}
		deviceKey := device.DeviceID + "\x00" + device.Environment
		recipients[userID].devices[deviceKey] = device
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	var deliveryErrors []error
	for _, recipient := range recipients {
		if recipient.remaining <= 0 {
			continue
		}
		devices := make([]models.PushDevice, 0, len(recipient.devices))
		for _, device := range recipient.devices {
			devices = append(devices, device)
		}
		_, _, err := sendPushToDevices(ctx, w.app, devices, pushMessage{
			Title: "War attacks remaining",
			Body:  fmt.Sprintf("%s & %d attacks left in war!", formatReminderTime(minutes), recipient.remaining),
			Data:  map[string]string{"type": "war_reminder", "target_tag": event.ClanTag},
		})
		if err != nil {
			deliveryErrors = append(deliveryErrors, fmt.Errorf("deliver war reminder to user %s: %w", recipient.userID, err))
		}
	}
	return errors.Join(deliveryErrors...)
}

func decodeWarReminderEvent(event mobileWarEvent) (clashy.ClanWar, int, error) {
	payload, ok := event.Value["data"].(map[string]any)
	if !ok {
		return clashy.ClanWar{}, 0, errors.New("war reminder data must be a nested object")
	}
	minutes := intNumber(event.Value["minutes_remaining"])
	if minutes <= 0 {
		return clashy.ClanWar{}, 0, errors.New("war reminder minutes_remaining must be a positive integer")
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return clashy.ClanWar{}, 0, err
	}
	var war clashy.ClanWar
	if err := json.Unmarshal(raw, &war); err != nil {
		return clashy.ClanWar{}, 0, err
	}
	return war, minutes, nil
}

func (w *mobileEventsWorker) processRaidReminder(ctx context.Context, event mobileWarEvent) error {
	userID := stringValue(event.Value["user_id"])
	minutes := intNumber(event.Value["minutes_remaining"])
	remaining := intNumber(event.Value["remaining_attacks"])
	if userID == "" || minutes <= 0 || remaining <= 0 {
		return nil
	}
	rows, err := w.pool.Query(ctx, `
		SELECT device_id, platform, provider, environment, token_ciphertext, locale
		FROM mobile_push_devices device
		JOIN mobile_notification_preferences preference ON preference.user_id=device.user_id
		WHERE device.user_id = $1 AND device.enabled = true AND device.provider = 'fcm'
		  AND preference.raid_reminders_enabled = true
		  AND $2 = ANY(preference.raid_reminder_timings)
	`, userID, minutes)
	if err != nil {
		return err
	}
	var devices []models.PushDevice
	for rows.Next() {
		var device models.PushDevice
		device.UserID = userID
		if err := rows.Scan(&device.DeviceID, &device.Platform, &device.Provider, &device.Environment,
			&device.TokenCiphertext, &device.Locale); err != nil {
			rows.Close()
			return err
		}
		devices = append(devices, device)
	}
	rows.Close()
	_, _, err = sendPushToDevices(ctx, w.app, devices, pushMessage{
		Title: "Raid attacks remaining",
		Body:  fmt.Sprintf("%s & %d attacks left in Raid Weekend!", formatReminderTime(minutes), remaining),
		Data:  map[string]string{"type": "raid_reminder", "target_tag": event.ClanTag},
	})
	if err != nil {
		return fmt.Errorf("deliver raid reminder to user %s: %w", userID, err)
	}
	return nil
}

func intNumber(value any) int {
	switch typed := value.(type) {
	case float64:
		return int(typed)
	case int:
		return typed
	case json.Number:
		result, _ := typed.Int64()
		return int(result)
	default:
		return 0
	}
}

func formatReminderTime(minutes int) string {
	if minutes%60 == 0 {
		hours := minutes / 60
		if hours == 1 {
			return "1 hour"
		}
		return fmt.Sprintf("%d hours", hours)
	}
	return fmt.Sprintf("%d minutes", minutes)
}

const mobileSubscriptionsSQL = `
	SELECT DISTINCT d.user_id, d.device_id, d.provider, d.environment, d.token_ciphertext,
	       preference.war_state_enabled, preference.war_attacks_enabled, preference.war_state_enabled
	FROM mobile_notification_accounts account
	JOIN mobile_notification_preferences preference ON preference.user_id = account.user_id
	JOIN mobile_push_devices d
	  ON d.user_id = account.user_id
	 AND d.enabled = true
	 AND d.provider = 'fcm'
	JOIN player_timers timer
	  ON timer.player_tag = account.player_tag
	 AND timer.event_type = 'war'
	 AND timer.expires_at > now()
	JOIN war_schedule schedule ON schedule.schedule_key = timer.event_key
	WHERE account.enabled = true
	  AND $2 IN ('war', 'cwl')
	  AND $1 IN (schedule.source_clan_tag, schedule.opponent_tag)
`

func (w *mobileEventsWorker) subscriptions(ctx context.Context, targetTag, topic string) ([]mobileSubscription, error) {
	rows, err := w.pool.Query(ctx, mobileSubscriptionsSQL, targetTag, topic)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []mobileSubscription
	for rows.Next() {
		var sub mobileSubscription
		if err := rows.Scan(&sub.UserID, &sub.DeviceID, &sub.Provider, &sub.Environment, &sub.TokenCiphertext, &sub.WarStartEnabled, &sub.ScoreChangeEnabled, &sub.WarEndEnabled); err != nil {
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
	case "new_war", "new_attacks", "war_state":
		return event.Topic == "war"
	case "war", "raid_mobile":
		return event.Topic == "reminder"
	case "legend_defense":
		return event.Topic == "legend"
	default:
		return false
	}
}

func subscriptionWantsEvent(sub mobileSubscription, event mobileWarEvent) bool {
	if stringValue(event.Value["war_role"]) == string(cwlWarPreparation) {
		return false
	}
	eventType, _ := event.Value["type"].(string)
	switch eventType {
	case "new_war":
		return sub.WarStartEnabled
	case "new_attacks":
		return sub.ScoreChangeEnabled
	case "war_state":
		return sub.WarEndEnabled || sub.WarStartEnabled
	default:
		return false
	}
}

func mobileNotificationText(event mobileWarEvent) (string, string) {
	eventType, _ := event.Value["type"].(string)
	switch eventType {
	case "new_war":
		return "Clan war started", "A new war is available for your selected clan."
	case "new_attacks":
		return "War score updated", "A new attack changed the clan war score."
	case "war_state":
		return "War status changed", "Your clan war status changed."
	default:
		return "ClashKing war update", "Your selected clan has a war update."
	}
}

func mobileNotificationRouteType(event mobileWarEvent) string {
	if eventType, ok := event.Value["type"].(string); ok && eventType != "" {
		return eventType
	}
	return "war_update"
}
