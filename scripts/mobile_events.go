package scripts

import (
	"context"
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
	client valkey.Client
	cfg    platform.Config
	pool   *pgxpool.Pool
	app    *platform.App
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
	if event.Topic == "legend" {
		return w.processLegendBattle(ctx, event)
	}
	if event.Topic == "reminder" {
		return w.processWarReminder(ctx, event)
	}
	subscriptions, err := w.subscriptions(ctx, event.ClanTag, event.Topic)
	if err != nil {
		return err
	}
	title, body := mobileNotificationText(event)
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
		}
	}
	return nil
}

type mobileLegendBattle struct {
	BattleID              string
	PlayerTag             string
	OpponentName          string
	Attack                bool
	Stars                 int
	DestructionPercentage int
}

type mobileLegendRecipient struct {
	userID         string
	playerTag      string
	playerName     string
	attackDevices  []models.PushDevice
	defenseDevices []models.PushDevice
}

func (w *mobileEventsWorker) processLegendBattle(ctx context.Context, event mobileWarEvent) error {
	battle := mobileLegendBattleFromEvent(event)
	if battle.BattleID == "" || battle.PlayerTag == "" {
		return nil
	}
	recipients, err := w.legendRecipients(ctx, []string{battle.PlayerTag})
	if err != nil {
		return err
	}
	for _, recipient := range recipients[battle.PlayerTag] {
		devices := recipient.defenseDevices
		if battle.Attack {
			devices = recipient.attackDevices
		}
		if len(devices) == 0 {
			continue
		}
		deliveryKey := fmt.Sprintf("legend_battle:%s:%s:%t", battle.BattleID, battle.PlayerTag, battle.Attack)
		claim, err := w.pool.Exec(ctx, `
				INSERT INTO mobile_notification_deliveries (user_id, notification_key)
				VALUES ($1, $2) ON CONFLICT DO NOTHING
			`, recipient.userID, deliveryKey)
		if err != nil {
			return err
		}
		if claim.RowsAffected() == 0 {
			continue
		}
		title := "Legend defense"
		body := fmt.Sprintf("%s attacked %s: %d stars, %d%%.", battle.OpponentName, recipient.playerName, battle.Stars, battle.DestructionPercentage)
		if battle.Attack {
			title = "Legend attack"
			body = fmt.Sprintf("%s attacked %s: %d stars, %d%%.", recipient.playerName, battle.OpponentName, battle.Stars, battle.DestructionPercentage)
		}
		sent, _ := sendPushToDevices(ctx, w.app, devices, pushMessage{
			Title: title, Body: body,
			Data: map[string]string{"type": "legend_battle", "target_tag": battle.PlayerTag, "battle_id": battle.BattleID},
		})
		if sent == 0 {
			_, _ = w.pool.Exec(ctx, `DELETE FROM mobile_notification_deliveries WHERE user_id = $1 AND notification_key = $2`, recipient.userID, deliveryKey)
		}
	}
	return nil
}

func mobileLegendBattleFromEvent(event mobileWarEvent) mobileLegendBattle {
	return mobileLegendBattle{
		BattleID: stringValue(event.Value["battle_id"]), PlayerTag: stringValue(event.Value["player_tag"]),
		OpponentName: stringValue(event.Value["opponent_name"]), Attack: boolValue(event.Value["attack"]),
		Stars: intNumber(event.Value["stars"]), DestructionPercentage: intNumber(event.Value["destruction_percentage"]),
	}
}

func (w *mobileEventsWorker) legendRecipients(ctx context.Context, tags []string) (map[string]map[string]*mobileLegendRecipient, error) {
	rows, err := w.pool.Query(ctx, legendBattleRecipientsSQL, tags)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make(map[string]map[string]*mobileLegendRecipient, len(tags))
	for rows.Next() {
		var playerTag, userID, playerName string
		var attacksEnabled, defensesEnabled bool
		var device models.PushDevice
		if err := rows.Scan(&playerTag, &userID, &playerName, &device.DeviceID, &device.Platform,
			&device.Provider, &device.Environment, &device.TokenCiphertext, &device.Locale,
			&attacksEnabled, &defensesEnabled); err != nil {
			return nil, err
		}
		device.UserID = userID
		if out[playerTag] == nil {
			out[playerTag] = make(map[string]*mobileLegendRecipient)
		}
		recipient := out[playerTag][userID]
		if recipient == nil {
			recipient = &mobileLegendRecipient{userID: userID, playerTag: playerTag, playerName: playerName}
			out[playerTag][userID] = recipient
		}
		if attacksEnabled {
			recipient.attackDevices = append(recipient.attackDevices, device)
		}
		if defensesEnabled {
			recipient.defenseDevices = append(recipient.defenseDevices, device)
		}
	}
	return out, rows.Err()
}

func stringValue(value any) string { valueString, _ := value.(string); return valueString }
func boolValue(value any) bool     { valueBool, _ := value.(bool); return valueBool }

const legendBattleRecipientsSQL = `
	SELECT account.player_tag, account.user_id, player.name, device.device_id, device.platform,
	       device.provider, device.environment, device.token_ciphertext, device.locale,
	       (account.source = 'bookmarked' AND device.legend_attacks_enabled = true) AS attacks_enabled,
	       device.legend_defenses_enabled = true AS defenses_enabled
	FROM mobile_notification_accounts account
	JOIN mobile_push_devices device
	  ON device.user_id = account.user_id
	 AND device.enabled = true
	 AND device.provider = 'fcm'
	 AND device.authorization_status IN ('authorized', 'provisional')
	 AND device.last_seen_at >= now() - interval '7 days'
	JOIN basic_player player
	  ON player.tag = account.player_tag AND player.league_id = 105000036
	WHERE account.player_tag = ANY($1)
	  AND account.active = true
	  AND ((account.source = 'bookmarked' AND device.legend_attacks_enabled = true)
	       OR device.legend_defenses_enabled = true)
`

func (w *mobileEventsWorker) processWarReminder(ctx context.Context, event mobileWarEvent) error {
	raw, _ := event.Value["data"].(string)
	minutes := intNumber(event.Value["minutes_remaining"])
	if raw == "" || minutes <= 0 {
		return nil
	}
	var war clashy.ClanWar
	if err := json.Unmarshal([]byte(raw), &war); err != nil {
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
		JOIN mobile_push_devices device
		  ON device.user_id = account.user_id
		 AND device.enabled = true
		 AND device.provider = 'fcm'
		 AND device.war_reminders_enabled = true
		 AND $2 = ANY(device.reminder_timings)
		WHERE account.active = true
		  AND account.player_tag = ANY($1)
	`, tags, minutes)
	if err != nil {
		return err
	}
	type recipient struct {
		userID    string
		playerTag string
		devices   []models.PushDevice
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
		key := userID + "\x00" + playerTag
		if recipients[key] == nil {
			recipients[key] = &recipient{userID: userID, playerTag: playerTag}
		}
		recipients[key].devices = append(recipients[key].devices, device)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	warID := war.WarTag
	if warID == "" && war.PreparationStartTime != nil {
		warID = war.PreparationStartTime.RawTime
	}
	if warID == "" {
		warID = event.ClanTag + ":" + raw
	}
	for _, recipient := range recipients {
		deliveryKey := fmt.Sprintf("war_reminder:%s:%s:%d", warID, recipient.playerTag, minutes)
		result, err := w.pool.Exec(ctx, `
			INSERT INTO mobile_notification_deliveries (user_id, notification_key)
			VALUES ($1, $2)
			ON CONFLICT DO NOTHING
		`, recipient.userID, deliveryKey)
		if err != nil {
			return err
		}
		if result.RowsAffected() == 0 {
			continue
		}
		account := remaining[recipient.playerTag]
		name := account.name
		if name == "" {
			name = recipient.playerTag
		}
		unit := "attacks"
		if account.remaining == 1 {
			unit = "attack"
		}
		sent, _ := sendPushToDevices(ctx, w.app, recipient.devices, pushMessage{
			Title: "War attacks remaining",
			Body:  fmt.Sprintf("%s still has %d %s left with %s remaining.", name, account.remaining, unit, formatReminderTime(minutes)),
			Data:  map[string]string{"type": "war_reminder", "target_tag": recipient.playerTag},
		})
		if sent == 0 {
			_, _ = w.pool.Exec(ctx, `DELETE FROM mobile_notification_deliveries WHERE user_id = $1 AND notification_key = $2`, recipient.userID, deliveryKey)
		}
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
	SELECT DISTINCT d.provider, d.environment, d.token_ciphertext,
	       d.war_state_enabled, d.war_attacks_enabled, d.war_state_enabled
	FROM mobile_notification_accounts account
	JOIN mobile_push_devices d
	  ON d.user_id = account.user_id
	 AND d.enabled = true
	 AND d.provider = 'fcm'
	JOIN basic_player player ON player.tag = account.player_tag
	LEFT JOIN current_war_timers timer ON timer.player_tag = account.player_tag AND timer.end_time > now()
	WHERE account.active = true
	  AND $2 IN ('war', 'cwl')
	  AND (
			player.clan_tag = $1
			OR timer.clan_tag = $1
			OR timer.opponent_tag = $1
	  )
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
		if err := rows.Scan(&sub.Provider, &sub.Environment, &sub.TokenCiphertext, &sub.WarStartEnabled, &sub.ScoreChangeEnabled, &sub.WarEndEnabled); err != nil {
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
	case "legend_battle":
		return event.Topic == "legend"
	case "war":
		return event.Topic == "reminder"
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
	case "cwl_war_update":
		return sub.WarStartEnabled || sub.WarEndEnabled
	case "cwl_new_attacks":
		return sub.ScoreChangeEnabled
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

func mobileNotificationRouteType(event mobileWarEvent) string {
	if eventType, ok := event.Value["type"].(string); ok && eventType != "" {
		return eventType
	}
	return "war_update"
}
