package scripts

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"strings"
	"time"
	"unicode"

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
		if err := sendFCM(ctx, w.app, token, pushMessage{Title: title, Body: body, Data: mobileWarNotificationData(event)}); err != nil {
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
	title, body, err := legendDefenseNotificationText(event.Value)
	if err != nil {
		return err
	}
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
	data := map[string]string{"type": "legend_defense", "target_tag": playerTag, "image_url": legendNotificationImage}
	if eventID := stringValue(event.Value["event_id"]); eventID != "" {
		data["event_id"] = eventID
	}
	message := pushMessage{
		Title: title,
		Body:  body,
		Data:  data,
	}
	deliveryID := stringValue(event.Value["event_id"])
	if deliveryID == "" {
		deliveryID = entryID
	}
	return w.deliverLegendDefenseDevices(ctx, deliveryID, playerTag, devices, message)
}

func legendDefenseNotificationText(value map[string]any) (string, string, error) {
	player := strings.TrimSpace(stringValue(value["player_name"]))
	opponent := strings.TrimSpace(stringValue(value["opponent_name"]))
	number := func(key string, max int) (int, bool) {
		var n float64
		switch v := value[key].(type) {
		case int:
			n = float64(v)
		case float64:
			n = v
		default:
			return 0, false
		}
		if n < 0 || n > float64(max) || n != float64(int(n)) {
			return 0, false
		}
		return int(n), true
	}
	stars, starsOK := number("stars", 3)
	destruction, destructionOK := number("destruction_percentage", 100)
	if player == "" || opponent == "" || !starsOK || !destructionOK {
		return "", "", errors.New("Legend defense event requires player_name, opponent_name, stars and destruction_percentage")
	}
	starDisplay := strings.Repeat("★", stars) + strings.Repeat("☆", 3-stars)
	return player + " was attacked", fmt.Sprintf("%s %s (%d%%)", opponent, starDisplay, destruction), nil
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
	var live *clashy.ClanWar
	if war.WarTag != "" {
		wars, fetchErr := w.app.Clash.GetLeagueWars(ctx, []string{war.WarTag})
		if fetchErr != nil {
			return fetchErr
		}
		if len(wars) > 0 {
			live = &wars[0]
		}
	} else {
		live, err = w.app.Clash.GetClanWar(ctx, event.ClanTag)
		if err != nil {
			return err
		}
	}
	if live == nil || live.State != "inWar" || live.EndTime == nil || !live.EndTime.Time.After(time.Now()) {
		return nil
	}
	// A delayed job for a previous war must not remind for a different war.
	if war.EndTime == nil || !war.EndTime.Time.Equal(live.EndTime.Time) {
		return nil
	}
	war = *live
	remaining := unfinishedWarMembers(war, event.ClanTag)
	clanName := ""
	clanBadge := ""
	for _, side := range []*clashy.WarClan{war.Clan, war.Opponent} {
		if side == nil || side.Tag != event.ClanTag {
			continue
		}
		clanName = side.Name
		clanBadge = side.Badge.URL()
	}
	if len(remaining) == 0 {
		return nil
	}
	tags := make([]string, 0, len(remaining))
	for tag := range remaining {
		tags = append(tags, tag)
	}
	rows, err := w.pool.Query(ctx, personalWarReminderRecipientsSQL, event.ClanTag, minutes, tags)
	if err != nil {
		return err
	}
	type recipient struct {
		userID    string
		remaining int
		devices   map[string]models.PushDevice
		players   map[string]struct{}
		name      string
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
			recipients[userID].players[playerTag] = struct{}{}
			recipients[userID].remaining += remaining[playerTag].remaining
			recipients[userID].name = remaining[playerTag].name
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
		title, body, textErr := reminderNotificationText(false, isolateMobileClanName(clanName), isolateMobileClanName(recipient.name), len(recipient.players), 0, recipient.remaining, minutes)
		if textErr != nil {
			return textErr
		}
		_, _, err = sendPushToDevices(ctx, w.app, devices, pushMessage{
			Title: title,
			Body:  body,
			Data:  map[string]string{"type": "war_reminder", "target_tag": event.ClanTag, "image_url": clanBadge},
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
	if userID == "" || minutes <= 0 {
		return nil
	}
	rows, err := w.pool.Query(ctx, `
		SELECT device_id, platform, provider, environment, token_ciphertext, locale
		FROM mobile_push_devices device
		JOIN mobile_notification_preferences preference ON preference.user_id=device.user_id
		JOIN user_bookmarks bookmark ON bookmark.user_id=device.user_id AND bookmark.entity_type='clan'
		  AND bookmark.tag=$3 AND bookmark.raid_notifications_enabled
		JOIN clan_notification_tracking interest ON interest.tag=bookmark.tag AND interest.raid_tracked
		WHERE device.user_id = $1 AND device.enabled = true AND device.provider = 'fcm'
		  AND preference.raid_reminders_enabled = true
		  AND $2 = ANY(preference.raid_reminder_timings)
	`, userID, minutes, event.ClanTag)
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
	if err := rows.Err(); err != nil {
		return err
	}
	if len(devices) == 0 {
		return nil
	}
	links, err := w.pool.Query(ctx, "SELECT tag FROM player_links WHERE user_id=$1 AND is_verified", userID)
	if err != nil {
		return err
	}
	var tags []string
	for links.Next() {
		var tag string
		if err := links.Scan(&tag); err != nil {
			links.Close()
			return err
		}
		tags = append(tags, tag)
	}
	links.Close()
	if err := links.Err(); err != nil {
		return err
	}
	if len(tags) == 0 {
		return nil
	}
	// Recheck live attack counts immediately before delivery, rather than
	// trusting a queued reminder or a potentially stale capital snapshot.
	raids, err := w.app.Clash.GetRaidLog(ctx, event.ClanTag, clashy.PageOptions{Limit: 1})
	if err != nil {
		return err
	}
	if len(raids) == 0 || raids[0].EndTime == nil || !raids[0].EndTime.Time.After(time.Now()) {
		return nil
	}
	if end := stringValue(event.Value["raid_end"]); end != "" {
		expected, parseErr := time.Parse(time.RFC3339, end)
		if parseErr != nil || !raids[0].EndTime.Time.Equal(expected) {
			return nil
		}
	}
	clan, err := w.app.Clash.GetClan(ctx, event.ClanTag)
	if err != nil {
		return err
	}
	if clan == nil {
		return nil
	}
	personal := personalRaidRemaining(tags, clan.Members, raids[0].Members)
	if personal.Remaining == 0 {
		return nil
	}
	title, body, err := reminderNotificationText(true, isolateMobileClanName(clan.Name), isolateMobileClanName(personal.Name), personal.Accounts, personal.Unstarted, personal.Remaining, minutes)
	if err != nil {
		return err
	}
	_, _, err = sendPushToDevices(ctx, w.app, devices, pushMessage{
		Title: title,
		Body:  body,
		Data:  map[string]string{"type": "raid_reminder", "target_tag": event.ClanTag, "image_url": stringValue(event.Value["clan_badge_url"])},
	})
	if err != nil {
		return fmt.Errorf("deliver raid reminder to user %s: %w", userID, err)
	}
	return nil
}

func reminderNotificationText(raid bool, clan, account string, accounts, unstarted, attacks, minutes int) (string, string, error) {
	if strings.TrimSpace(clan) == "" || accounts < 1 || attacks < 1 || minutes < 1 || (accounts == 1 && strings.TrimSpace(account) == "") {
		return "", "", errors.New("reminder requires clan and account context")
	}
	kind := "War"
	if raid {
		kind = "Raid"
	}
	title := clan + ": " + kind + " reminder"
	if raid && accounts > 1 && unstarted == accounts {
		return title, fmt.Sprintf("You have %d accounts that haven't started raids. Ends in %s.", accounts, formatReminderTime(minutes)), nil
	}
	label := "attacks"
	if attacks == 1 {
		label = "attack"
	}
	body := fmt.Sprintf("%d %s %s left", attacks, strings.ToLower(kind), label)
	if accounts == 1 {
		body = account + " has " + body
	}
	body += fmt.Sprintf(". Ends in %s.", formatReminderTime(minutes))
	return title, body, nil
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
	minuteUnit := "mins"
	if minutes%60 == 1 {
		minuteUnit = "min"
	}
	if minutes < 60 {
		return fmt.Sprintf("%d %s", minutes, minuteUnit)
	}
	if minutes%60 == 0 {
		return fmt.Sprintf("%d hr", minutes/60)
	}
	return fmt.Sprintf("%d hr %d %s", minutes/60, minutes%60, minuteUnit)
}

const mobileSubscriptionsSQL = `
	SELECT DISTINCT d.user_id, d.device_id, d.provider, d.environment, d.token_ciphertext,
	       preference.war_state_enabled, preference.war_attacks_enabled, preference.war_state_enabled
	FROM user_bookmarks account
	JOIN clan_notification_tracking interest ON interest.tag=account.tag AND interest.war_tracked
	JOIN mobile_notification_preferences preference ON preference.user_id = account.user_id
	JOIN mobile_push_devices d
	  ON d.user_id = account.user_id
	 AND d.enabled = true
	 AND d.provider = 'fcm'
	WHERE account.entity_type='clan' AND account.war_notifications_enabled
	  AND $2 IN ('war', 'cwl')
	  AND account.tag=$1
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
	// Attack events still feed Discord and war tracking, but no longer produce
	// mobile pushes, even for accounts with the legacy preference enabled.
	case "new_war", "war_state":
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
	war, ok := mapValue(event.Value["war"])
	if !ok || discordWarIdentity(event) == "" {
		return false
	}
	state := stringMapValue(war, "state")
	switch eventType {
	case "new_war":
		return sub.WarStartEnabled && (state == "preparation" || state == "inWar")
	case "new_attacks":
		return false
	case "war_state":
		return sub.WarEndEnabled && state == "warEnded" || sub.WarStartEnabled && state == "inWar"
	default:
		return false
	}
}

func mobileNotificationText(event mobileWarEvent) (string, string) {
	war, _ := mapValue(event.Value["war"])
	clan, opponent := discordWarSides(war, event.ClanTag)
	name := strings.TrimSpace(stringMapValue(clan, "name"))
	if name == "" {
		name = event.ClanTag
	}
	if name == "" {
		name = "Clan"
	}
	name = isolateMobileClanName(name)
	versus := ""
	if opponentName := strings.TrimSpace(stringMapValue(opponent, "name")); opponentName != "" {
		versus = " against " + isolateMobileClanName(opponentName)
	}
	eventType, _ := event.Value["type"].(string)
	switch eventType {
	case "new_war", "war_state":
		switch stringMapValue(war, "state") {
		case "preparation":
			return name + " war preparation started", "War preparation" + versus + " started."
		case "inWar":
			body := "War" + versus + " started."
			if end := discordWarTime(stringMapValue(war, "endTime")); end.After(time.Now()) {
				minutes := int(math.Ceil(time.Until(end).Minutes()))
				body += " Ends in " + formatReminderTime(minutes) + "."
			}
			return name + " war started", body
		case "warEnded":
			outcome := "Tie"
			stars, opponentStars := intValue(clan["stars"]), intValue(opponent["stars"])
			destruction, opponentDestruction := floatValue(clan["destructionPercentage"]), floatValue(opponent["destructionPercentage"])
			if stars > opponentStars || (stars == opponentStars && destruction > opponentDestruction) {
				outcome = "Win"
			} else if stars < opponentStars || (stars == opponentStars && destruction < opponentDestruction) {
				outcome = "Loss"
			}
			return name + " war ended", fmt.Sprintf("%s%s. %d ★ - %d ★", outcome, versus, stars, opponentStars)
		default:
			if eventType == "new_war" {
				return name + " war started", "War" + versus + " started."
			}
			return name + ": War update", name + " has a war update" + versus + "."
		}
	case "new_attacks":
		return name + ": War score updated", "A new attack changed " + name + "'s war score" + versus + "."
	default:
		return name + ": War update", name + " has a war update" + versus + "."
	}
}

func mobileWarNotificationData(event mobileWarEvent) map[string]string {
	data := map[string]string{"type": mobileNotificationRouteType(event), "target_tag": event.ClanTag}
	war, _ := mapValue(event.Value["war"])
	clan, _ := discordWarSides(war, event.ClanTag)
	badge, _ := mapValue(clan["badgeUrls"])
	for _, size := range []string{"large", "medium", "small"} {
		if url := stringMapValue(badge, size); strings.HasPrefix(url, "https://") {
			data["image_url"] = url
			break
		}
	}
	return data
}

// Keep RTL names from pulling adjacent Latin scores and punctuation into their
// directional run. FSI preserves the name's natural direction; PDI ends it.
func isolateMobileClanName(name string) string {
	name = strings.Map(func(r rune) rune {
		if unicode.Is(unicode.Bidi_Control, r) {
			return -1
		}
		return r
	}, name)
	for _, r := range name {
		if unicode.Is(unicode.Arabic, r) || unicode.Is(unicode.Hebrew, r) {
			return "\u2068" + name + "\u2069"
		}
	}
	return name
}

func mobileNotificationRouteType(event mobileWarEvent) string {
	if eventType, ok := event.Value["type"].(string); ok && eventType != "" {
		return eventType
	}
	return "war_update"
}
