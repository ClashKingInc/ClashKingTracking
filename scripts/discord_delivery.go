package scripts

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"clashking_tracking/internal/platform"

	clashy "github.com/clashkinginc/clashy.go"
	"github.com/disgoorg/disgo"
	"github.com/disgoorg/disgo/bot"
	"github.com/disgoorg/disgo/discord"
	"github.com/disgoorg/disgo/rest"
	"github.com/disgoorg/snowflake/v2"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	valkey "github.com/valkey-io/valkey-go"
)

const discordDeliveryDomainName = "discord-delivery"

type discordDeliveryDomain struct{}

type discordDeliveryTarget struct {
	ServerID  string
	ChannelID string
	ThreadID  string
	Custom    string
	Config    string
	ConfigID  string
	WebhookID string
	LogType   string
	ClanTag   string
	Revision  time.Time
	Secret    string
}

func NewDiscordDeliveryDomain() platform.Domain { return &discordDeliveryDomain{} }
func (d *discordDeliveryDomain) Name() string   { return discordDeliveryDomainName }

func (d *discordDeliveryDomain) Run(ctx context.Context, app *platform.App) error {
	if err := validateDiscordDeliveryConfig(app.Config, app.Valkey); err != nil {
		return err
	}
	pool, err := pgxpool.New(ctx, app.Config.TimescaleURL)
	if err != nil {
		return err
	}
	defer pool.Close()
	var clientOpts []bot.ConfigOpt
	if app.Config.DiscordAPIURL != "" {
		if err := platform.ValidateLoopbackProviderURL(app.Config.DiscordAPIURL); err != nil {
			return fmt.Errorf("invalid CLASHKING_LOCAL_DISCORD_API_URL: %w", err)
		}
		clientOpts = append(clientOpts, bot.WithRestClientConfigOpts(rest.WithURL(app.Config.DiscordAPIURL)))
	}
	client, err := disgo.New(app.Config.DiscordBotToken, clientOpts...)
	if err != nil {
		return fmt.Errorf("create Discord delivery client: %w", err)
	}
	defer client.Close(context.WithoutCancel(ctx))

	worker := &discordDeliveryWorker{app: app, client: client, pool: pool}
	if err := worker.ensureGroup(ctx); err != nil {
		return err
	}
	app.Stats.SetReady(discordDeliveryDomainName, true, "")
	for {
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
		app.Stats.SetQueueDepth(discordDeliveryDomainName, len(entries))
		if err := worker.processEntries(ctx, entries); err != nil {
			return err
		}
		app.Stats.SetQueueDepth(discordDeliveryDomainName, 0)
	}
}

func validateDiscordDeliveryConfig(cfg platform.Config, client valkey.Client) error {
	if cfg.DiscordBotToken == "" {
		return errors.New("DISCORD_BOT_TOKEN is required for discord-delivery")
	}
	if cfg.TimescaleURL == "" {
		return errors.New("TIMESCALE_* connection variables are required for discord-delivery")
	}
	if client == nil || cfg.EventStreamName == "" {
		return errors.New("Valkey and events.stream are required for discord-delivery")
	}
	if cfg.DiscordDeliveryBatchSize <= 0 {
		return errors.New("discord-delivery batch size must be greater than zero")
	}
	if cfg.EventStreamReclaimIdleSeconds <= 0 {
		return errors.New("events.reclaim_idle_seconds must be greater than zero for discord-delivery")
	}
	if cfg.DiscordMessageCreateEnabled && (cfg.ClashKingAPIURL == "" || cfg.ClashKingAPIToken == "") {
		return errors.New("CLASHKING_API_ORIGIN and CLASHKING_API_TOKEN are required when Discord link parsing is enabled")
	}
	return nil
}

type discordDeliveryWorker struct {
	app            *platform.App
	client         *bot.Client
	pool           *pgxpool.Pool
	processEventFn func(context.Context, mobileWarEvent) (bool, error)
	ackFn          func(context.Context, string) error
	disableFn      func(context.Context, discordDeliveryTarget, string) (int64, error)
}

func (w *discordDeliveryWorker) group() string { return "discord-delivery" }

func (w *discordDeliveryWorker) consumer() string {
	if w.app.Config.EventStreamConsumer != "" {
		return w.app.Config.EventStreamConsumer + ":discord-delivery"
	}
	return "discord-delivery"
}

func (w *discordDeliveryWorker) ensureGroup(ctx context.Context) error {
	err := w.app.Valkey.Do(ctx, w.app.Valkey.B().XgroupCreate().
		Key(w.app.Config.EventStreamName).Group(w.group()).Id("0").Mkstream().Build()).Error()
	if err != nil && !strings.Contains(err.Error(), "BUSYGROUP") {
		return err
	}
	return nil
}

func (w *discordDeliveryWorker) read(ctx context.Context) ([]valkey.XRangeEntry, error) {
	result, err := w.app.Valkey.Do(ctx, w.app.Valkey.B().Xreadgroup().
		Group(w.group(), w.consumer()).Count(int64(w.app.Config.DiscordDeliveryBatchSize)).Block(5000).
		Streams().Key(w.app.Config.EventStreamName).Id(">").Build()).AsXRead()
	if err != nil {
		return nil, err
	}
	return result[w.app.Config.EventStreamName], nil
}

func (w *discordDeliveryWorker) readPending(ctx context.Context) ([]valkey.XRangeEntry, error) {
	result, err := w.app.Valkey.Do(ctx, w.app.Valkey.B().Xreadgroup().
		Group(w.group(), w.consumer()).Count(int64(w.app.Config.DiscordDeliveryBatchSize)).
		Streams().Key(w.app.Config.EventStreamName).Id("0").Build()).AsXRead()
	if err != nil {
		return nil, err
	}
	return result[w.app.Config.EventStreamName], nil
}

func (w *discordDeliveryWorker) claimPending(ctx context.Context) ([]valkey.XRangeEntry, error) {
	minIdle := fmt.Sprintf("%d", w.app.Config.EventStreamReclaimIdleSeconds*1000)
	values, err := w.app.Valkey.Do(ctx, w.app.Valkey.B().Xautoclaim().
		Key(w.app.Config.EventStreamName).Group(w.group()).Consumer(w.consumer()).
		MinIdleTime(minIdle).Start("0-0").Count(int64(w.app.Config.DiscordDeliveryBatchSize)).Build()).ToArray()
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

func (w *discordDeliveryWorker) processEntries(ctx context.Context, entries []valkey.XRangeEntry) error {
	for _, entry := range entries {
		event, ok := mobileEventFromEntry(entry)
		if !ok {
			w.app.Logger.Error("Discord delivery received an invalid event payload", "stream_id", entry.ID)
			w.captureUnexpected("invalid_event", errors.New("Discord delivery received an invalid event payload"))
			if err := w.ack(ctx, entry.ID); err != nil {
				return err
			}
			continue
		}
		if discordDeliveryEventExpired(event, time.Now().UTC(), w.app.Config.EventStreamRetentionSeconds) {
			w.app.Logger.Debug("Discord delivery dropped expired event", "stream_id", entry.ID, "topic", event.Topic)
			if err := w.ack(ctx, entry.ID); err != nil {
				return err
			}
			continue
		}
		handled, err := w.process(ctx, event)
		if err != nil {
			// Discord delivery is intentionally best effort. A failed provider or
			// destination attempt is recorded and dropped instead of leaving the
			// stream entry pending for another send attempt.
			w.app.Logger.Error("Discord delivery attempt failed", "stream_id", entry.ID, "topic", event.Topic, "err", err)
			if hasInvalidDiscordForm(err) {
				w.captureUnexpected("invalid_provider_request", errors.New("Discord rejected a Tracking delivery payload as invalid"))
			}
		}
		if !handled {
			w.app.Logger.Debug("Discord delivery ignored event", "topic", event.Topic)
		}
		if err := w.ack(ctx, entry.ID); err != nil {
			return err
		}
	}
	return nil
}

func (w *discordDeliveryWorker) captureUnexpected(category string, err error) {
	if w.app != nil && w.app.Errors != nil {
		w.app.Errors.Capture(err, map[string]string{"domain": discordDeliveryDomainName, "category": category})
	}
}

func hasInvalidDiscordForm(err error) bool {
	if err == nil {
		return false
	}
	var discordErr *rest.Error
	if errors.As(err, &discordErr) && discordErr.Code == rest.JSONErrorCodeInvalidFormBody {
		return true
	}
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		for _, child := range joined.Unwrap() {
			if hasInvalidDiscordForm(child) {
				return true
			}
		}
	}
	return false
}

func (w *discordDeliveryWorker) ack(ctx context.Context, id string) error {
	if w.ackFn != nil {
		return w.ackFn(ctx, id)
	}
	return w.app.Valkey.Do(ctx, w.app.Valkey.B().Xack().Key(w.app.Config.EventStreamName).
		Group(w.group()).Id(id).Build()).Error()
}

func (w *discordDeliveryWorker) process(ctx context.Context, event mobileWarEvent) (bool, error) {
	if w.processEventFn != nil {
		return w.processEventFn(ctx, event)
	}
	return w.processEvent(ctx, event)
}

func discordDeliveryEventExpired(event mobileWarEvent, now time.Time, retentionSeconds int) bool {
	return retentionSeconds > 0 && !event.Timestamp.IsZero() &&
		event.Timestamp.Before(now.Add(-time.Duration(retentionSeconds)*time.Second))
}

func (w *discordDeliveryWorker) processEvent(ctx context.Context, event mobileWarEvent) (bool, error) {
	switch event.Topic {
	case "reminder":
		return true, w.deliverReminder(ctx, event)
	case "giveaway":
		return true, w.deliverGiveaway(ctx, event)
	case "discord_message_create":
		return true, w.deliverLinkParse(ctx, event)
	case "clan", "war", "capital", "player", "reddit":
		return true, w.deliverLogEvent(ctx, event)
	default:
		return false, nil
	}
}

func (w *discordDeliveryWorker) deliverReminder(ctx context.Context, event mobileWarEvent) error {
	targets, err := w.reminderTargets(ctx, event)
	if err != nil {
		return err
	}
	tags := reminderMemberTags(event)
	return attemptDiscordDestinations(targets, func(target discordDeliveryTarget) error {
		userIDs, err := w.linkedGuildUsers(ctx, target.ServerID, tags)
		if err != nil {
			return fmt.Errorf("resolve Discord reminder destination %s: %w", discordDestinationID(target), err)
		}
		minutes := intValue(event.Value["minutes_remaining"])
		if reminder, ok := mapValue(event.Value["reminder"]); ok {
			minutes = intValue(reminder["minutes_remaining"])
		}
		content := reminderText(event, minutes, target.Custom, userIDs)
		if err := w.send(ctx, target, content, userIDs); err != nil {
			return fmt.Errorf("send Discord reminder destination %s: %w", discordDestinationID(target), w.handleDestinationFailure(ctx, target, err))
		}
		return nil
	})
}

func attemptDiscordDestinations[T any](destinations []T, attempt func(T) error) error {
	var deliveryErr error
	for _, destination := range destinations {
		deliveryErr = errors.Join(deliveryErr, attempt(destination))
	}
	return deliveryErr
}

func (w *discordDeliveryWorker) reminderTargets(ctx context.Context, event mobileWarEvent) ([]discordDeliveryTarget, error) {
	if reminder, ok := mapValue(event.Value["reminder"]); ok {
		target := discordDeliveryTarget{
			ServerID:  stringMapValue(reminder, "server_id"),
			ChannelID: stringMapValue(reminder, "channel_id"),
			ThreadID:  stringMapValue(reminder, "thread_id"),
			Custom:    stringMapValue(reminder, "custom_text"),
			Config:    "reminder",
			ConfigID:  stringMapValue(reminder, "id"),
		}
		if target.ConfigID == "" {
			return nil, nil
		}
		if err := w.pool.QueryRow(ctx, `SELECT updated_at, webhook_token FROM reminders WHERE id::text = $1 AND disabled = false AND server_id = $2 AND COALESCE(channel_id, '') = $3 AND COALESCE(thread_id, '') = $4`, target.ConfigID, target.ServerID, target.ChannelID, target.ThreadID).Scan(&target.Revision, &target.Secret); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return nil, nil
			}
			return nil, err
		}
		return []discordDeliveryTarget{target}, nil
	}
	if stringMapValue(event.Value, "type") != "war" {
		return nil, nil
	}
	rows, err := w.pool.Query(ctx, `
		SELECT server_id, COALESCE(channel_id, ''), COALESCE(thread_id, ''), custom_text, id::text, updated_at, webhook_token
		FROM reminders
		WHERE disabled = false AND type_name = 'War' AND clan_tag = $1 AND minutes_remaining = $2
		ORDER BY id
	`, event.ClanTag, intValue(event.Value["minutes_remaining"]))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var targets []discordDeliveryTarget
	for rows.Next() {
		var target discordDeliveryTarget
		if err := rows.Scan(&target.ServerID, &target.ChannelID, &target.ThreadID, &target.Custom, &target.ConfigID, &target.Revision, &target.Secret); err != nil {
			return nil, err
		}
		target.Config = "reminder"
		targets = append(targets, target)
	}
	return targets, rows.Err()
}

func (w *discordDeliveryWorker) linkedGuildUsers(ctx context.Context, guildID string, tags []string) ([]snowflake.ID, error) {
	if len(tags) == 0 || guildID == "" {
		return nil, nil
	}
	rows, err := w.pool.Query(ctx, `
		SELECT DISTINCT link.user_id
		FROM player_links link
		JOIN discord_cache.guilds guild
		  ON guild.id = $1
		 AND guild.application_id = $3
		 AND guild.available = true
		 AND guild.metadata_complete = true
		 AND guild.members_complete = true
		JOIN discord_cache.gateway_shards shard
		  ON (shard.application_id, shard.shard_id) = (guild.application_id, guild.shard_id)
		 AND shard.generation = guild.generation
		 AND shard.healthy = true
		 AND shard.heartbeat_at > clock_timestamp() - interval '45 seconds'
		JOIN discord_cache.members member
		  ON member.guild_id = $1 AND member.user_id = link.user_id
		WHERE link.tag = ANY($2) AND link.user_id IS NOT NULL
		ORDER BY link.user_id
	`, guildID, tags, w.client.ApplicationID.String())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var users []snowflake.ID
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			return nil, err
		}
		id, err := snowflake.Parse(raw)
		if err == nil {
			users = append(users, id)
		}
	}
	return users, rows.Err()
}

func (w *discordDeliveryWorker) deliverLogEvent(ctx context.Context, event mobileWarEvent) error {
	logTypes := discordEventLogTypes(event)
	if len(logTypes) == 0 {
		return nil
	}
	query := `
		SELECT server_id, COALESCE(clan_tag, ''), type, webhook_id, COALESCE(thread_id, ''), updated_at
		FROM server_logs
		WHERE disabled = false AND type = ANY($1)
	`
	args := []any{logTypes}
	if event.Topic != "reddit" {
		query += ` AND clan_tag = $2`
		args = append(args, event.ClanTag)
	}
	rows, err := w.pool.Query(ctx, query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()
	content := discordEventText(event)
	var deliveryErr error
	for rows.Next() {
		var target discordDeliveryTarget
		if err := rows.Scan(&target.ServerID, &target.ClanTag, &target.LogType, &target.WebhookID, &target.ThreadID, &target.Revision); err != nil {
			return err
		}
		target.Config = "server_log"
		resolved, err := w.webhookTarget(ctx, target.ServerID, target.WebhookID, target.ThreadID)
		if err != nil {
			deliveryErr = errors.Join(deliveryErr, fmt.Errorf("resolve Discord log webhook %s: %w", target.WebhookID, w.handleDestinationFailure(ctx, target, err)))
			continue
		}
		resolved.Config, resolved.WebhookID, resolved.LogType, resolved.ClanTag, resolved.Revision = target.Config, target.WebhookID, target.LogType, target.ClanTag, target.Revision
		if err := w.send(ctx, resolved, content, nil); err != nil {
			deliveryErr = errors.Join(deliveryErr, fmt.Errorf("send Discord log destination %s: %w", discordDestinationID(resolved), w.handleDestinationFailure(ctx, resolved, err)))
		}
	}
	return errors.Join(deliveryErr, rows.Err())
}

func (w *discordDeliveryWorker) webhookTarget(ctx context.Context, serverID, webhookID, threadID string) (discordDeliveryTarget, error) {
	id, err := snowflake.Parse(webhookID)
	if err != nil {
		return discordDeliveryTarget{}, err
	}
	webhook, err := w.client.Rest.GetWebhook(id)
	if err != nil {
		return discordDeliveryTarget{}, err
	}
	var channelID snowflake.ID
	switch typed := webhook.(type) {
	case discord.IncomingWebhook:
		channelID = typed.ChannelID
	case *discord.IncomingWebhook:
		channelID = typed.ChannelID
	default:
		return discordDeliveryTarget{}, fmt.Errorf("webhook %s is not an incoming webhook", webhookID)
	}
	return discordDeliveryTarget{ServerID: serverID, ChannelID: channelID.String(), ThreadID: threadID}, nil
}

func (w *discordDeliveryWorker) deliverGiveaway(ctx context.Context, event mobileWarEvent) error {
	giveaway, ok := mapValue(event.Value["giveaway"])
	if !ok {
		return errors.New("giveaway event is missing giveaway")
	}
	target := discordDeliveryTarget{
		ServerID:  stringMapValue(giveaway, "server_id"),
		ChannelID: stringMapValue(giveaway, "channel_id"),
		Config:    "giveaway",
		ConfigID:  stringMapValue(giveaway, "id"),
	}
	if target.ChannelID == "" || target.ConfigID == "" {
		return nil
	}
	if err := w.pool.QueryRow(ctx, `SELECT updated_at FROM giveaways WHERE id = $1 AND disabled = false AND server_id = $2 AND COALESCE(channel_id, '') = $3`, target.ConfigID, target.ServerID, target.ChannelID).Scan(&target.Revision); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		return err
	}
	prize := stringMapValue(giveaway, "prize")
	kind := stringMapValue(event.Value, "type")
	content := "🎉 Giveaway: " + prize
	if kind == "giveaway_end" {
		content = "🏆 Giveaway ended: " + prize
	}
	if err := w.send(ctx, target, content, nil); err != nil {
		return w.handleDestinationFailure(ctx, target, err)
	}
	return nil
}

func (w *discordDeliveryWorker) send(_ context.Context, target discordDeliveryTarget, content string, users []snowflake.ID) error {
	destination := discordDestinationID(target)
	id, err := snowflake.Parse(destination)
	if err != nil {
		return fmt.Errorf("invalid Discord destination %q: %w", destination, err)
	}
	if len(content) > 2000 {
		content = content[:1997] + "..."
	}
	_, err = w.client.Rest.CreateMessage(id, discord.MessageCreate{
		Content: content,
		AllowedMentions: &discord.AllowedMentions{
			Parse: []discord.AllowedMentionType{},
			Users: users,
		},
	})
	return err
}

func (w *discordDeliveryWorker) handleDestinationFailure(ctx context.Context, target discordDeliveryTarget, deliveryErr error) error {
	reason, permanent := permanentDiscordDestinationFailure(deliveryErr)
	if !permanent || target.Config == "" {
		return deliveryErr
	}
	_, err := w.disableDestination(ctx, target, reason)
	if err != nil {
		return errors.Join(deliveryErr, fmt.Errorf("disable permanently invalid Discord %s destination: %w", target.Config, err))
	}
	return deliveryErr
}

func (w *discordDeliveryWorker) disableDestination(ctx context.Context, target discordDeliveryTarget, reason string) (int64, error) {
	if w.disableFn != nil {
		return w.disableFn(ctx, target, reason)
	}
	var result pgconn.CommandTag
	var err error
	switch target.Config {
	case "reminder":
		result, err = w.pool.Exec(ctx, `UPDATE reminders SET disabled = true, disabled_reason = $2, updated_at = now() WHERE id::text = $1 AND disabled = false AND server_id = $3 AND COALESCE(channel_id, '') = $4 AND COALESCE(thread_id, '') = $5 AND updated_at = $6 AND webhook_token = $7`, target.ConfigID, reason, target.ServerID, target.ChannelID, target.ThreadID, target.Revision, target.Secret)
	case "giveaway":
		result, err = w.pool.Exec(ctx, `UPDATE giveaways SET disabled = true, disabled_reason = $2, updated_at = now() WHERE id = $1 AND disabled = false AND server_id = $3 AND COALESCE(channel_id, '') = $4 AND updated_at = $5`, target.ConfigID, reason, target.ServerID, target.ChannelID, target.Revision)
	case "server_log":
		result, err = w.pool.Exec(ctx, `UPDATE server_logs SET disabled = true, disabled_reason = $1, updated_at = now() WHERE disabled = false AND server_id = $2 AND clan_tag IS NOT DISTINCT FROM NULLIF($3, '') AND type = $4 AND webhook_id = $5 AND COALESCE(thread_id, '') = $6 AND updated_at = $7`, reason, target.ServerID, target.ClanTag, target.LogType, target.WebhookID, target.ThreadID, target.Revision)
	}
	if err != nil {
		return 0, err
	}
	return result.RowsAffected(), nil
}

func permanentDiscordDestinationFailure(err error) (string, bool) {
	var discordErr *rest.Error
	if !errors.As(err, &discordErr) {
		return "", false
	}
	switch discordErr.Code {
	case rest.JSONErrorCodeUnknownChannel:
		return "discord_unknown_channel", true
	case rest.JSONErrorCodeUnknownGuild:
		return "discord_unknown_guild", true
	case rest.JSONErrorCodeUnknownWebhook, rest.JSONErrorCodeInvalidWebhookToken:
		return "discord_invalid_webhook", true
	case rest.JSONErrorCodeMissingAccess:
		return "discord_missing_access", true
	case rest.JSONErrorCode(50013):
		return "discord_missing_permissions", true
	default:
		return "", false
	}
}

func discordDestinationID(target discordDeliveryTarget) string {
	if target.ThreadID != "" {
		return target.ThreadID
	}
	return target.ChannelID
}

func reminderMemberTags(event mobileWarEvent) []string {
	if members, ok := event.Value["members"].([]any); ok {
		var tags []string
		for _, item := range members {
			if member, ok := mapValue(item); ok {
				if tag := stringMapValue(member, "tag"); tag != "" {
					tags = append(tags, tag)
				}
			}
		}
		return tags
	}
	if stringMapValue(event.Value, "type") != "war" {
		return nil
	}
	raw, err := json.Marshal(event.Value["data"])
	if err != nil {
		return nil
	}
	var war clashy.ClanWar
	if json.Unmarshal(raw, &war) != nil {
		return nil
	}
	attacks := 2
	if war.WarTag != "" {
		attacks = 1
	}
	for _, side := range []*clashy.WarClan{war.Clan, war.Opponent} {
		if side == nil || side.Tag != event.ClanTag {
			continue
		}
		var tags []string
		for _, member := range side.Members {
			if len(member.Attacks) < attacks {
				tags = append(tags, member.Tag)
			}
		}
		return tags
	}
	return nil
}

func reminderText(event mobileWarEvent, minutes int, custom string, users []snowflake.ID) string {
	typeName := strings.ReplaceAll(stringMapValue(event.Value, "type"), "_", " ")
	if typeName == "war" {
		typeName = "war attacks"
	}
	parts := []string{fmt.Sprintf("⏰ %s reminder", strings.Title(typeName))}
	if minutes > 0 {
		parts = append(parts, fmt.Sprintf("%d minutes remaining", minutes))
	}
	if custom != "" {
		parts = append(parts, custom)
	}
	if len(users) > 0 {
		mentions := make([]string, 0, len(users))
		for _, user := range users {
			mentions = append(mentions, "<@"+user.String()+">")
		}
		parts = append(parts, strings.Join(mentions, " "))
	}
	return strings.Join(parts, "\n")
}

func discordEventLogTypes(event mobileWarEvent) []string {
	switch event.Topic {
	case "clan":
		if stringMapValue(event.Value, "type") == "member_join" {
			return []string{"join_log"}
		}
		if stringMapValue(event.Value, "type") == "member_leave" {
			return []string{"leave_log"}
		}
	case "war":
		return []string{"war_log"}
	case "capital":
		if stringMapValue(event.Value, "type") == "raid_attacks" {
			return []string{"capital_attacks"}
		}
		return []string{"capital_weekly_summary"}
	case "player":
		if values, ok := event.Value["log_types"].([]any); ok {
			var out []string
			for _, value := range values {
				if text, ok := value.(string); ok && !slices.Contains(out, text) {
					out = append(out, text)
				}
			}
			return out
		}
	case "reddit":
		return []string{"reddit_feed"}
	}
	return nil
}

func discordEventText(event mobileWarEvent) string {
	typeName := strings.ReplaceAll(stringMapValue(event.Value, "type"), "_", " ")
	switch event.Topic {
	case "clan":
		member, _ := mapValue(event.Value["member"])
		return fmt.Sprintf("**%s** `%s` — %s in `%s`", stringMapValue(member, "name"), stringMapValue(member, "tag"), typeName, event.ClanTag)
	case "player":
		changes := stringList(event.Value["changed_types"])
		return fmt.Sprintf("**%s** `%s` — %s", stringMapValue(event.Value, "name"), stringMapValue(event.Value, "tag"), strings.Join(changes, ", "))
	case "reddit":
		data, _ := mapValue(event.Value["data"])
		return fmt.Sprintf("**%s**\n%s", stringMapValue(data, "title"), firstDiscordString(stringMapValue(data, "comments_link"), stringMapValue(data, "url")))
	default:
		return fmt.Sprintf("**%s** — %s for `%s`", strings.Title(event.Topic), typeName, event.ClanTag)
	}
}

func mapValue(value any) (map[string]any, bool) {
	result, ok := value.(map[string]any)
	return result, ok
}

func stringMapValue(value map[string]any, key string) string {
	if value == nil {
		return ""
	}
	switch typed := value[key].(type) {
	case string:
		return typed
	case float64:
		return strconv.FormatInt(int64(typed), 10)
	case json.Number:
		return typed.String()
	default:
		return ""
	}
}

func intValue(value any) int {
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

func stringList(value any) []string {
	values, _ := value.([]any)
	result := make([]string, 0, len(values))
	for _, value := range values {
		if text, ok := value.(string); ok {
			result = append(result, text)
		}
	}
	return result
}

func firstDiscordString(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}
