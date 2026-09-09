package scripts

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"clashking_tracking/internal/platform"

	"github.com/disgoorg/disgo"
	"github.com/disgoorg/disgo/bot"
	"github.com/disgoorg/disgo/cache"
	"github.com/disgoorg/disgo/discord"
	"github.com/disgoorg/disgo/events"
	"github.com/disgoorg/disgo/gateway"
	"github.com/disgoorg/disgo/sharding"
	"github.com/disgoorg/snowflake/v2"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	valkey "github.com/valkey-io/valkey-go"
)

const discordGatewayDomainName = "discord-gateway"

type discordGatewayDomain struct{}

const discordGatewayHeartbeatInterval = 15 * time.Second
const discordGuildActivityReconcileInterval = 5 * time.Minute
const discordMemberChunkTimeout = 2 * time.Minute

type discordMutationMeta struct {
	ApplicationID string
	ShardID       int
	ShardCount    int
	Generation    uuid.UUID
	Sequence      int64
}

type discordCacheMutation struct {
	Meta  discordMutationMeta
	Apply func(context.Context, *pgxpool.Pool) error
}

type discordShardState struct {
	Generation uuid.UUID
	ShardCount int
	Sequence   int64
}

type discordGatewayState struct {
	mu     sync.Mutex
	appID  string
	shards map[int]discordShardState
	syncs  map[string]discordMemberSync
}

type discordMemberSync struct {
	Meta   discordMutationMeta
	Token  uuid.UUID
	Deltas []discordMemberDelta
}

type discordMemberDelta struct {
	Member *discord.Member
	UserID string
}

func NewDiscordGatewayDomain() platform.Domain { return &discordGatewayDomain{} }

func (d *discordGatewayDomain) Name() string { return discordGatewayDomainName }

func (d *discordGatewayDomain) Run(ctx context.Context, app *platform.App) error {
	if err := validateDiscordGatewayConfig(app.Config); err != nil {
		return err
	}

	pool, err := pgxpool.New(ctx, app.Config.TimescaleURL)
	if err != nil {
		return err
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		return fmt.Errorf("ping Discord cache database: %w", err)
	}

	intents := gateway.IntentGuilds | gateway.IntentGuildMembers
	if app.Config.DiscordMessageCreateEnabled {
		intents |= gateway.IntentGuildMessages | gateway.IntentMessageContent
	}

	runCtx, stopRun := context.WithCancel(ctx)
	defer stopRun()
	state := &discordGatewayState{shards: map[int]discordShardState{}, syncs: map[string]discordMemberSync{}}
	mutations := make(chan discordCacheMutation, app.Config.DiscordGatewayQueueSize)
	enqueue := func(mutation discordCacheMutation) {
		select {
		case mutations <- mutation:
		case <-runCtx.Done():
		}
	}
	listener := discordCacheListener(runCtx, app, pool, state, enqueue)
	// Discord can serialize snowflake fields as unquoted JSON integers. The
	// snowflake package parses these directly as uint64 when enabled, without a
	// float conversion; quoted canonical IDs remain unchanged.
	snowflake.AllowUnquoted = true
	client, err := disgo.New(
		app.Config.DiscordBotToken,
		bot.WithLogger(slog.New(discordLibraryLogHandler{target: app.Logger.Handler(), reporter: app.Errors})),
		bot.WithDefaultShardManager(),
		bot.WithShardManagerConfigOpts(
			sharding.WithGatewayConfigOpts(gateway.WithIntents(intents)),
		),
		bot.WithCacheConfigOpts(cache.WithCaches(
			cache.FlagGuilds,
			cache.FlagChannels,
			cache.FlagRoles,
		)),
		bot.WithEventListeners(listener),
	)
	if err != nil {
		return fmt.Errorf("create Discord gateway client: %w", err)
	}
	state.appID = client.ApplicationID.String()
	ownership, err := acquireDiscordGatewayOwnership(ctx, pool, state.appID)
	if err != nil {
		client.Close(context.WithoutCancel(ctx))
		return err
	}
	defer ownership.Release()

	writerDone := make(chan error, 1)
	go func() { writerDone <- runDiscordCacheWriter(runCtx, pool, mutations) }()
	ownershipDone := make(chan error, 1)
	go func() { ownershipDone <- monitorDiscordGatewayOwnership(runCtx, ownership) }()
	go runDiscordGatewayHeartbeats(runCtx, client, state, enqueue)
	reconcileDone := make(chan error, 1)
	go func() {
		reconcileDone <- runDiscordGuildActivityReconciler(runCtx, app, pool, client, state, enqueue)
	}()
	activityDone := make(chan error, 1)
	go func() {
		activityDone <- runDiscordGuildActivitySignals(runCtx, app, pool, client, state, enqueue)
	}()
	wakeDone := make(chan error, 1)
	go func() {
		wakeDone <- runTrackingWakeListener(runCtx, app, func(wakeCtx context.Context, event trackingWakeEvent) error {
			if event.Kind != "guild_reactivated" {
				return nil
			}
			return reconcileOneDiscordGuildActivity(wakeCtx, app, pool, client, state, enqueue, event.ServerID)
		})
	}()

	openDone := make(chan error, 1)
	go func() { openDone <- client.OpenShardManager(runCtx) }()
	opened, err := awaitDiscordGatewayOpen(ctx, openDone, writerDone, ownershipDone, reconcileDone, activityDone, wakeDone)
	if opened {
		app.Stats.SetReady(discordGatewayDomainName, true, "")
		err = awaitDiscordGatewayExit(ctx, writerDone, ownershipDone, reconcileDone, activityDone, wakeDone)
	}
	stopRun()

	closeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()
	client.Close(closeCtx)
	markCtx, markCancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
	_, _ = pool.Exec(markCtx, `UPDATE discord_cache.gateway_shards SET healthy = false, heartbeat_at = now() WHERE application_id = $1`, state.appID)
	markCancel()
	if errors.Is(err, context.Canceled) {
		return nil
	}
	return err
}

func awaitDiscordGatewayOpen(ctx context.Context, openDone, writerDone, ownershipDone, reconcileDone, activityDone, wakeDone <-chan error) (bool, error) {
	select {
	case <-ctx.Done():
		return false, ctx.Err()
	case err := <-openDone:
		if err != nil {
			return false, fmt.Errorf("open Discord shard manager: %w", err)
		}
		return true, nil
	case err := <-writerDone:
		return false, discordGatewayRuntimeError("cache writer", err)
	case err := <-ownershipDone:
		return false, discordGatewayRuntimeError("ownership", err)
	case err := <-reconcileDone:
		return false, discordGatewayRuntimeError("guild activity reconciliation", err)
	case err := <-activityDone:
		return false, discordGatewayRuntimeError("guild activity signals", err)
	case err := <-wakeDone:
		return false, discordGatewayRuntimeError("PostgreSQL wake listener", err)
	}
}

func awaitDiscordGatewayExit(ctx context.Context, writerDone, ownershipDone, reconcileDone, activityDone, wakeDone <-chan error) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-writerDone:
		return discordGatewayRuntimeError("cache writer", err)
	case err := <-ownershipDone:
		return discordGatewayRuntimeError("ownership", err)
	case err := <-reconcileDone:
		return discordGatewayRuntimeError("guild activity reconciliation", err)
	case err := <-activityDone:
		return discordGatewayRuntimeError("guild activity signals", err)
	case err := <-wakeDone:
		return discordGatewayRuntimeError("PostgreSQL wake listener", err)
	}
}

func discordGatewayRuntimeError(component string, err error) error {
	if err == nil {
		return fmt.Errorf("Discord gateway %s stopped unexpectedly", component)
	}
	return fmt.Errorf("Discord gateway %s: %w", component, err)
}

// discordLibraryLogHandler keeps third-party Gateway diagnostics useful without
// forwarding attributes. Disgo includes the complete raw payload inside some
// decoder errors, so even an ordinary error attribute can contain guild/member
// data. Tracking records its own bounded readiness and terminal errors.
type discordLibraryLogHandler struct {
	target   slog.Handler
	reporter platform.ErrorReporter
}

func (h discordLibraryLogHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.target != nil && h.target.Enabled(ctx, level)
}

func (h discordLibraryLogHandler) Handle(ctx context.Context, record slog.Record) error {
	if h.target == nil {
		return nil
	}
	// Treat both the message and attributes as untrusted. Third-party loggers
	// occasionally interpolate decoder input into either location.
	safe := slog.NewRecord(record.Time, record.Level, "Discord library diagnostic", record.PC)
	safe.AddAttrs(slog.Bool("discord_library", true))
	if record.Level >= slog.LevelError && strings.Contains(record.Message, "error while parsing gateway message") && h.reporter != nil {
		h.reporter.Capture(errors.New("Discord gateway payload could not be decoded"), map[string]string{
			"script": discordGatewayDomainName, "domain": discordGatewayDomainName, "category": "invalid_gateway_payload",
		})
	}
	return h.target.Handle(ctx, safe)
}

func (h discordLibraryLogHandler) WithAttrs(_ []slog.Attr) slog.Handler { return h }
func (h discordLibraryLogHandler) WithGroup(_ string) slog.Handler      { return h }

func validateDiscordGatewayConfig(cfg platform.Config) error {
	if cfg.DiscordBotToken == "" {
		return errors.New("DISCORD_BOT_TOKEN is required for discord-gateway")
	}
	if cfg.TimescaleURL == "" {
		return errors.New("TIMESCALE_* connection variables are required for discord-gateway")
	}
	if cfg.DiscordGatewayQueueSize < 1 {
		return errors.New("discord_gateway.queue_size must be greater than zero")
	}
	if cfg.ValkeyAddr == "" || cfg.EventStreamName == "" {
		return errors.New("Valkey and events.stream are required for discord-gateway activity signals")
	}
	return nil
}

func acquireDiscordGatewayOwnership(ctx context.Context, pool *pgxpool.Pool, applicationID string) (*pgxpool.Conn, error) {
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("acquire Discord gateway ownership connection: %w", err)
	}
	var acquired bool
	if err := conn.QueryRow(ctx, `SELECT pg_try_advisory_lock(hashtextextended('discord-gateway:' || $1, 0))`, applicationID).Scan(&acquired); err != nil {
		conn.Release()
		return nil, fmt.Errorf("acquire Discord gateway ownership lock: %w", err)
	}
	if !acquired {
		conn.Release()
		return nil, fmt.Errorf("Discord gateway application %s already has an active writer", applicationID)
	}
	if _, err := conn.Exec(ctx, `UPDATE discord_cache.gateway_shards SET healthy = false, heartbeat_at = now() WHERE application_id = $1`, applicationID); err != nil {
		conn.Release()
		return nil, fmt.Errorf("fence prior Discord gateway generation: %w", err)
	}
	return conn, nil
}

func monitorDiscordGatewayOwnership(ctx context.Context, conn *pgxpool.Conn) error {
	ticker := time.NewTicker(discordGatewayHeartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			var held bool
			if err := conn.QueryRow(ctx, `SELECT EXISTS (
				SELECT 1 FROM pg_locks
				WHERE locktype = 'advisory' AND pid = pg_backend_pid() AND granted
			)`).Scan(&held); err != nil {
				return err
			}
			if !held {
				return errors.New("application advisory lock was lost")
			}
		}
	}
}

func runDiscordGatewayHeartbeats(ctx context.Context, client *bot.Client, state *discordGatewayState, enqueue func(discordCacheMutation)) {
	ticker := time.NewTicker(discordGatewayHeartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			state.enqueueHeartbeats(client, enqueue)
		}
	}
}

func runDiscordGuildActivityReconciler(ctx context.Context, app *platform.App, pool *pgxpool.Pool, client *bot.Client, state *discordGatewayState, enqueue func(discordCacheMutation)) error {
	ticker := time.NewTicker(discordGuildActivityReconcileInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := reconcileDiscordGuildActivity(ctx, app, pool, client, state, enqueue); err != nil {
				return err
			}
		}
	}
}

func runDiscordGuildActivitySignals(ctx context.Context, app *platform.App, pool *pgxpool.Pool, client *bot.Client, state *discordGatewayState, enqueue func(discordCacheMutation)) error {
	const group = "discord-gateway-activity"
	err := app.Valkey.Do(ctx, app.Valkey.B().XgroupCreate().Key(app.Config.EventStreamName).Group(group).Id("0").Mkstream().Build()).Error()
	if err != nil && !strings.Contains(err.Error(), "BUSYGROUP") {
		return err
	}
	consumer := app.Config.EventStreamConsumer + ":discord-gateway-activity"
	for {
		result, err := app.Valkey.Do(ctx, app.Valkey.B().Xreadgroup().Group(group, consumer).Count(20).Block(5000).
			Streams().Key(app.Config.EventStreamName).Id(">").Build()).AsXRead()
		if err != nil {
			if valkey.IsValkeyNil(err) {
				continue
			}
			return err
		}
		for _, entry := range result[app.Config.EventStreamName] {
			event, valid := mobileEventFromEntry(entry)
			if valid && event.Topic == "discord_guild_activity" {
				guildID := stringMapValue(event.Value, "guild_id")
				if parsed, parseErr := snowflake.Parse(guildID); parseErr == nil && parsed.String() == guildID {
					if reconcileErr := reconcileOneDiscordGuildActivity(ctx, app, pool, client, state, enqueue, guildID); reconcileErr != nil {
						app.Logger.Error("Discord guild activity signal failed", "guild_id", guildID, "err", reconcileErr)
					}
				}
			}
			if err := app.Valkey.Do(ctx, app.Valkey.B().Xack().Key(app.Config.EventStreamName).Group(group).Id(entry.ID).Build()).Error(); err != nil {
				return err
			}
		}
	}
}

func runDiscordCacheWriter(
	ctx context.Context,
	pool *pgxpool.Pool,
	mutations <-chan discordCacheMutation,
) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case mutation, ok := <-mutations:
			if !ok {
				return nil
			}
			if mutation.Apply == nil {
				continue
			}
			if err := mutation.Apply(ctx, pool); err != nil {
				return err
			}
			if mutation.Meta.Generation == uuid.Nil {
				continue
			}
			if _, err := pool.Exec(ctx, `
				UPDATE discord_cache.gateway_shards
				SET heartbeat_at = now(),
					last_applied_sequence = GREATEST(COALESCE(last_applied_sequence, 0), $5)
				WHERE application_id = $1 AND shard_id = $2 AND shard_count = $3 AND generation = $4
			`, mutation.Meta.ApplicationID, mutation.Meta.ShardID, mutation.Meta.ShardCount, mutation.Meta.Generation, mutation.Meta.Sequence); err != nil {
				return err
			}
		}
	}
}

func (s *discordGatewayState) rotateShard(event *events.GenericEvent, shardCount int, readyGuilds []discord.UnavailableGuild, enqueue func(discordCacheMutation)) discordMutationMeta {
	s.mu.Lock()
	defer s.mu.Unlock()
	meta := discordMutationMeta{
		ApplicationID: s.appID,
		ShardID:       event.ShardID(),
		ShardCount:    shardCount,
		Generation:    uuid.New(),
		Sequence:      int64(max(0, event.SequenceNumber())),
	}
	s.shards[meta.ShardID] = discordShardState{Generation: meta.Generation, ShardCount: shardCount, Sequence: meta.Sequence}
	for guildID, memberSync := range s.syncs {
		if memberSync.Meta.ShardID == meta.ShardID {
			delete(s.syncs, guildID)
		}
	}
	enqueue(discordCacheMutation{Meta: meta, Apply: func(ctx context.Context, pool *pgxpool.Pool) error {
		_, err := pool.Exec(ctx, `
			INSERT INTO discord_cache.gateway_shards
				(application_id, shard_id, shard_count, generation, healthy, heartbeat_at, last_applied_sequence)
			VALUES ($1, $2, $3, $4, false, now(), $5)
			ON CONFLICT (application_id, shard_id) DO UPDATE SET
				shard_count = EXCLUDED.shard_count,
				generation = EXCLUDED.generation,
				healthy = false,
				heartbeat_at = now(),
				last_applied_sequence = EXCLUDED.last_applied_sequence
		`, meta.ApplicationID, meta.ShardID, meta.ShardCount, meta.Generation, meta.Sequence)
		return err
	}})
	guildIDs := make([]string, 0, len(readyGuilds))
	for _, guild := range readyGuilds {
		guildIDs = append(guildIDs, guild.ID.String())
	}
	enqueue(discordCacheMutation{Meta: meta, Apply: func(ctx context.Context, pool *pgxpool.Pool) error {
		return replaceDiscordReadyGuildInventory(ctx, pool, meta, guildIDs)
	}})
	return meta
}

func (s *discordGatewayState) enqueueEvent(event *events.GenericEvent, enqueue func(discordCacheMutation), apply func(discordMutationMeta, context.Context, *pgxpool.Pool) error) (discordMutationMeta, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	shard, ok := s.shards[event.ShardID()]
	if !ok {
		return discordMutationMeta{}, false
	}
	sequence := int64(max(0, event.SequenceNumber()))
	shard.Sequence = max(shard.Sequence, sequence)
	s.shards[event.ShardID()] = shard
	meta := discordMutationMeta{ApplicationID: s.appID, ShardID: event.ShardID(), ShardCount: shard.ShardCount, Generation: shard.Generation, Sequence: sequence}
	enqueue(discordCacheMutation{Meta: meta, Apply: func(ctx context.Context, pool *pgxpool.Pool) error { return apply(meta, ctx, pool) }})
	return meta, true
}

func (s *discordGatewayState) enqueueHeartbeats(client *bot.Client, enqueue func(discordCacheMutation)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for shardID, shard := range s.shards {
		meta := discordMutationMeta{ApplicationID: s.appID, ShardID: shardID, ShardCount: shard.ShardCount, Generation: shard.Generation, Sequence: shard.Sequence}
		gatewayShard := client.ShardManager.Shard(shardID)
		connected := gatewayShard != nil && gatewayShard.Status() == gateway.StatusReady
		enqueue(discordCacheMutation{Meta: meta, Apply: func(ctx context.Context, pool *pgxpool.Pool) error {
			if connected {
				return nil
			}
			return setDiscordShardHealthy(ctx, pool, meta, false)
		}})
	}
}

func (s *discordGatewayState) enqueueMemberSnapshot(event *events.GenericEvent, guildID string, token uuid.UUID, initial bool, enqueue func(discordCacheMutation), apply func(discordMutationMeta, context.Context, *pgxpool.Pool) error) (discordMutationMeta, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	shard, ok := s.shards[event.ShardID()]
	if !ok {
		return discordMutationMeta{}, false
	}
	sequence := int64(max(0, event.SequenceNumber()))
	shard.Sequence = max(shard.Sequence, sequence)
	s.shards[event.ShardID()] = shard
	meta := discordMutationMeta{ApplicationID: s.appID, ShardID: event.ShardID(), ShardCount: shard.ShardCount, Generation: shard.Generation, Sequence: sequence}
	s.syncs[guildID] = discordMemberSync{Meta: meta, Token: token}
	enqueue(discordCacheMutation{Meta: meta, Apply: func(ctx context.Context, pool *pgxpool.Pool) error { return apply(meta, ctx, pool) }})
	_ = initial
	return meta, true
}

func (s *discordGatewayState) cancelMemberSync(guildID string, token uuid.UUID) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if memberSync, ok := s.syncs[guildID]; ok && (token == uuid.Nil || memberSync.Token == token) {
		delete(s.syncs, guildID)
	}
}

func (s *discordGatewayState) currentMeta(shardID int) (discordMutationMeta, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	shard, ok := s.shards[shardID]
	if !ok {
		return discordMutationMeta{}, false
	}
	return discordMutationMeta{ApplicationID: s.appID, ShardID: shardID, ShardCount: shard.ShardCount, Generation: shard.Generation, Sequence: shard.Sequence}, true
}

func (s *discordGatewayState) beginReconciledMemberSync(guildID string, meta discordMutationMeta, token uuid.UUID) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	shard, ok := s.shards[meta.ShardID]
	if !ok || shard.Generation != meta.Generation {
		return false
	}
	if _, syncing := s.syncs[guildID]; syncing {
		return false
	}
	s.syncs[guildID] = discordMemberSync{Meta: meta, Token: token}
	return true
}

func (s *discordGatewayState) enqueueMemberDelta(event *events.GenericEvent, guildID string, delta discordMemberDelta, enqueue func(discordCacheMutation), apply func(discordMutationMeta, context.Context, *pgxpool.Pool) error) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	shard, ok := s.shards[event.ShardID()]
	if !ok {
		return false
	}
	sequence := int64(max(0, event.SequenceNumber()))
	shard.Sequence = max(shard.Sequence, sequence)
	s.shards[event.ShardID()] = shard
	meta := discordMutationMeta{ApplicationID: s.appID, ShardID: event.ShardID(), ShardCount: shard.ShardCount, Generation: shard.Generation, Sequence: sequence}
	memberSync, ok := s.syncs[guildID]
	if ok && memberSync.Meta.Generation == meta.Generation && memberSync.Meta.ShardID == meta.ShardID {
		memberSync.Deltas = append(memberSync.Deltas, delta)
		s.syncs[guildID] = memberSync
	}
	enqueue(discordCacheMutation{Meta: meta, Apply: func(ctx context.Context, pool *pgxpool.Pool) error { return apply(meta, ctx, pool) }})
	return true
}

func (s *discordGatewayState) completeMemberSync(guildID string, meta discordMutationMeta, token uuid.UUID, enqueue func(discordCacheMutation), members []discord.Member) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	memberSync, ok := s.syncs[guildID]
	if !ok || memberSync.Token != token || memberSync.Meta.Generation != meta.Generation || memberSync.Meta.ShardID != meta.ShardID {
		return false
	}
	delete(s.syncs, guildID)
	deltas := append([]discordMemberDelta(nil), memberSync.Deltas...)
	enqueue(discordCacheMutation{Meta: meta, Apply: func(ctx context.Context, pool *pgxpool.Pool) error {
		return replaceDiscordGuildMembers(ctx, pool, guildID, meta, token, members, deltas)
	}})
	return true
}

type discordMemberRequester interface {
	RequestAllMembers(context.Context, snowflake.ID) ([]discord.Member, error)
}

func requestDiscordGuildMembers(ctx context.Context, app *platform.App, requester discordMemberRequester, state *discordGatewayState, enqueue func(discordCacheMutation), guildID string, meta discordMutationMeta, token uuid.UUID, snapshotApplied <-chan bool) {
	requestDiscordGuildMembersWithTimeout(ctx, app, requester, state, enqueue, guildID, meta, token, snapshotApplied, discordMemberChunkTimeout)
}

func requestDiscordGuildMembersWithTimeout(ctx context.Context, app *platform.App, requester discordMemberRequester, state *discordGatewayState, enqueue func(discordCacheMutation), guildID string, meta discordMutationMeta, token uuid.UUID, snapshotApplied <-chan bool, timeout time.Duration) {
	go func() {
		select {
		case active := <-snapshotApplied:
			if !active {
				state.cancelMemberSync(guildID, token)
				return
			}
		case <-ctx.Done():
			state.cancelMemberSync(guildID, token)
			return
		}
		id, err := snowflake.Parse(guildID)
		var members []discord.Member
		if err == nil {
			requestCtx, cancel := context.WithTimeout(ctx, timeout)
			members, err = requester.RequestAllMembers(requestCtx, id)
			cancel()
		}
		if err == nil {
			state.completeMemberSync(guildID, meta, token, enqueue, members)
			return
		}
		state.cancelMemberSync(guildID, token)
		enqueue(discordCacheMutation{Meta: meta, Apply: func(ctx context.Context, pool *pgxpool.Pool) error {
			_, clearErr := pool.Exec(ctx, `UPDATE discord_cache.guilds SET members_sync_token = NULL, members_complete = false, updated_at = now() WHERE id = $1 AND application_id = $2 AND shard_id = $3 AND generation = $4 AND members_sync_token = $5`, guildID, meta.ApplicationID, meta.ShardID, meta.Generation, token)
			return clearErr
		}})
		if !errors.Is(err, context.Canceled) {
			app.Logger.Error("Discord member chunk failed", "guild_id", guildID, "err", err)
		}
	}()
}

func discordCacheListener(
	ctx context.Context,
	app *platform.App,
	pool *pgxpool.Pool,
	state *discordGatewayState,
	enqueue func(discordCacheMutation),
) *events.ListenerAdapter {
	handleGuildSnapshot := func(event *events.GenericEvent, client *bot.Client, guild discord.GatewayGuild, initial bool) {
		guildID := guild.ID.String()
		token := uuid.New()
		applied := make(chan bool, 1)
		meta, ok := state.enqueueMemberSnapshot(event, guildID, token, initial, enqueue, func(meta discordMutationMeta, ctx context.Context, pool *pgxpool.Pool) error {
			active, err := isActiveDiscordGuild(ctx, pool, guildID)
			if err == nil {
				err = replaceDiscordGuildSnapshot(ctx, pool, guild, meta, token, active)
			}
			applied <- err == nil && active
			return err
		})
		if !ok {
			applied <- false
			return
		}
		requestDiscordGuildMembers(ctx, app, client.MemberChunkingManager, state, enqueue, guildID, meta, token, applied)
	}

	return &events.ListenerAdapter{
		OnReady: func(event *events.Ready) {
			shard := event.Client().ShardManager.Shard(event.ShardID())
			if shard == nil {
				app.Logger.Error("Discord ready event has no shard", "shard_id", event.ShardID())
				return
			}
			state.rotateShard(event.GenericEvent, shard.ShardCount(), event.EventReady.Guilds, enqueue)
		},
		OnResumed: func(event *events.Resumed) {
			state.enqueueEvent(event.GenericEvent, enqueue, func(meta discordMutationMeta, ctx context.Context, pool *pgxpool.Pool) error {
				return setDiscordShardHealthy(ctx, pool, meta, true)
			})
		},
		OnGuildReady: func(event *events.GuildReady) {
			handleGuildSnapshot(event.GenericEvent, event.Client(), event.Guild, true)
		},
		OnGuildJoin: func(event *events.GuildJoin) {
			handleGuildSnapshot(event.GenericEvent, event.Client(), event.Guild, false)
		},
		OnGuildAvailable: func(event *events.GuildAvailable) {
			handleGuildSnapshot(event.GenericEvent, event.Client(), event.Guild, false)
		},
		OnGuildUnavailable: func(event *events.GuildUnavailable) {
			guildID := event.GuildID.String()
			state.cancelMemberSync(guildID, uuid.Nil)
			state.enqueueEvent(event.GenericEvent, enqueue, func(meta discordMutationMeta, ctx context.Context, pool *pgxpool.Pool) error {
				return markDiscordGuildUnavailable(ctx, pool, guildID, meta)
			})
		},
		OnGuildUpdate: func(event *events.GuildUpdate) {
			guild := event.Guild
			state.enqueueEvent(event.GenericEvent, enqueue, func(meta discordMutationMeta, ctx context.Context, pool *pgxpool.Pool) error {
				return upsertDiscordGuildScoped(ctx, pool, guild, meta)
			})
		},
		OnGuildLeave: func(event *events.GuildLeave) {
			guildID := event.GuildID.String()
			state.cancelMemberSync(guildID, uuid.Nil)
			state.enqueueEvent(event.GenericEvent, enqueue, func(meta discordMutationMeta, ctx context.Context, pool *pgxpool.Pool) error {
				return deleteDiscordGuild(ctx, pool, guildID, meta)
			})
		},
		OnGuildChannelCreate: func(event *events.GuildChannelCreate) {
			channel := event.Channel
			state.enqueueEvent(event.GenericEvent, enqueue, func(meta discordMutationMeta, ctx context.Context, pool *pgxpool.Pool) error {
				return upsertDiscordChannelScoped(ctx, pool, channel, meta)
			})
		},
		OnGuildChannelUpdate: func(event *events.GuildChannelUpdate) {
			channel := event.Channel
			state.enqueueEvent(event.GenericEvent, enqueue, func(meta discordMutationMeta, ctx context.Context, pool *pgxpool.Pool) error {
				return upsertDiscordChannelScoped(ctx, pool, channel, meta)
			})
		},
		OnGuildChannelDelete: func(event *events.GuildChannelDelete) {
			channelID := event.ChannelID.String()
			guildID := event.GuildID.String()
			state.enqueueEvent(event.GenericEvent, enqueue, func(meta discordMutationMeta, ctx context.Context, pool *pgxpool.Pool) error {
				_, err := pool.Exec(ctx, `DELETE FROM discord_cache.channels channel USING discord_cache.guilds guild WHERE channel.id = $1 AND channel.guild_id = $2 AND guild.id = channel.guild_id AND guild.application_id = $3 AND guild.shard_id = $4 AND guild.generation = $5`, channelID, guildID, meta.ApplicationID, meta.ShardID, meta.Generation)
				return err
			})
		},
		OnGuildMemberJoin: func(event *events.GuildMemberJoin) {
			guildID, member := event.GuildID.String(), event.Member
			copy := member
			state.enqueueMemberDelta(event.GenericEvent, guildID, discordMemberDelta{Member: &copy}, enqueue, func(meta discordMutationMeta, ctx context.Context, pool *pgxpool.Pool) error {
				return upsertDiscordMemberScoped(ctx, pool, guildID, member, meta)
			})
		},
		OnGuildMemberUpdate: func(event *events.GuildMemberUpdate) {
			guildID, member := event.GuildID.String(), event.Member
			copy := member
			state.enqueueMemberDelta(event.GenericEvent, guildID, discordMemberDelta{Member: &copy}, enqueue, func(meta discordMutationMeta, ctx context.Context, pool *pgxpool.Pool) error {
				return upsertDiscordMemberScoped(ctx, pool, guildID, member, meta)
			})
		},
		OnGuildMemberLeave: func(event *events.GuildMemberLeave) {
			guildID, userID := event.GuildID.String(), event.User.ID.String()
			state.enqueueMemberDelta(event.GenericEvent, guildID, discordMemberDelta{UserID: userID}, enqueue, func(meta discordMutationMeta, ctx context.Context, pool *pgxpool.Pool) error {
				_, err := pool.Exec(ctx, `DELETE FROM discord_cache.members member USING discord_cache.guilds guild WHERE member.guild_id = $1 AND member.user_id = $2 AND guild.id = member.guild_id AND guild.application_id = $3 AND guild.shard_id = $4 AND guild.generation = $5`, guildID, userID, meta.ApplicationID, meta.ShardID, meta.Generation)
				return err
			})
		},
		OnRoleCreate: func(event *events.RoleCreate) {
			guildID, role := event.GuildID.String(), event.Role
			state.enqueueEvent(event.GenericEvent, enqueue, func(meta discordMutationMeta, ctx context.Context, pool *pgxpool.Pool) error {
				return upsertDiscordRoleScoped(ctx, pool, guildID, role, meta)
			})
		},
		OnRoleUpdate: func(event *events.RoleUpdate) {
			guildID, role := event.GuildID.String(), event.Role
			state.enqueueEvent(event.GenericEvent, enqueue, func(meta discordMutationMeta, ctx context.Context, pool *pgxpool.Pool) error {
				return upsertDiscordRoleScoped(ctx, pool, guildID, role, meta)
			})
		},
		OnRoleDelete: func(event *events.RoleDelete) {
			guildID, roleID := event.GuildID.String(), event.RoleID.String()
			state.enqueueEvent(event.GenericEvent, enqueue, func(meta discordMutationMeta, ctx context.Context, pool *pgxpool.Pool) error {
				_, err := pool.Exec(ctx, `DELETE FROM discord_cache.roles role USING discord_cache.guilds guild WHERE role.guild_id = $1 AND role.id = $2 AND guild.id = role.guild_id AND guild.application_id = $3 AND guild.shard_id = $4 AND guild.generation = $5`, guildID, roleID, meta.ApplicationID, meta.ShardID, meta.Generation)
				return err
			})
		},
		OnGuildMessageCreate: func(event *events.GuildMessageCreate) {
			if !app.Config.DiscordMessageCreateEnabled || event.Message.Author.Bot || event.Message.WebhookID != nil {
				return
			}
			message := event.Message
			if firstClashLink(message.Content) == nil {
				return
			}
			go func() {
				err := app.PublishEvent(ctx, platform.Event{
					Topic: "discord_message_create",
					Value: map[string]any{
						"id":         message.ID.String(),
						"guild_id":   event.GuildID.String(),
						"channel_id": message.ChannelID.String(),
						"author_id":  message.Author.ID.String(),
						"content":    message.Content,
					},
				})
				if err != nil && !errors.Is(err, context.Canceled) {
					app.Logger.Error("publish Discord message-create event", "err", err)
				}
			}()
		},
	}
}

func isActiveDiscordGuild(ctx context.Context, pool *pgxpool.Pool, guildID string) (bool, error) {
	var active bool
	err := pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM servers WHERE id = $1 AND left_at IS NULL AND last_command_at >= now() - interval '90 days')`, guildID).Scan(&active)
	return active, err
}

type discordGuildActivity struct {
	guildID  string
	shardID  int
	complete bool
	syncing  bool
	active   bool
}

func reconcileDiscordGuildActivity(ctx context.Context, app *platform.App, pool *pgxpool.Pool, client *bot.Client, state *discordGatewayState, enqueue func(discordCacheMutation)) error {
	rows, err := pool.Query(ctx, `
		SELECT guild.id, guild.shard_id, guild.members_complete,
		       guild.members_sync_token IS NOT NULL,
		       active.id IS NOT NULL
		FROM discord_cache.guilds AS guild
		JOIN discord_cache.gateway_shards AS shard
		  ON (shard.application_id, shard.shard_id) = (guild.application_id, guild.shard_id)
		 AND shard.generation = guild.generation
		LEFT JOIN servers AS active
		  ON active.id = guild.id AND active.left_at IS NULL
		 AND active.last_command_at >= now() - interval '90 days'
		WHERE guild.application_id = $1 AND guild.available = true
	`, state.appID)
	if err != nil {
		return err
	}
	defer rows.Close()
	var changes []discordGuildActivity
	for rows.Next() {
		var item discordGuildActivity
		if err := rows.Scan(&item.guildID, &item.shardID, &item.complete, &item.syncing, &item.active); err != nil {
			return err
		}
		if discordGuildActivityAction(item.active, item.complete, item.syncing) != "" {
			changes = append(changes, item)
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	for _, item := range changes {
		applyDiscordGuildActivity(ctx, app, pool, client, state, enqueue, item)
	}
	return nil
}

func reconcileOneDiscordGuildActivity(ctx context.Context, app *platform.App, pool *pgxpool.Pool, client *bot.Client, state *discordGatewayState, enqueue func(discordCacheMutation), guildID string) error {
	var item discordGuildActivity
	err := pool.QueryRow(ctx, `
		SELECT guild.id, guild.shard_id, guild.members_complete,
		       guild.members_sync_token IS NOT NULL,
		       active.id IS NOT NULL
		FROM discord_cache.guilds AS guild
		JOIN discord_cache.gateway_shards AS shard
		  ON (shard.application_id, shard.shard_id) = (guild.application_id, guild.shard_id)
		 AND shard.generation = guild.generation
		LEFT JOIN servers AS active
		  ON active.id = guild.id AND active.left_at IS NULL
		 AND active.last_command_at >= now() - interval '90 days'
		WHERE guild.id = $1 AND guild.application_id = $2 AND guild.available = true
	`, guildID, state.appID).Scan(&item.guildID, &item.shardID, &item.complete, &item.syncing, &item.active)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	applyDiscordGuildActivity(ctx, app, pool, client, state, enqueue, item)
	return nil
}

func applyDiscordGuildActivity(ctx context.Context, app *platform.App, pool *pgxpool.Pool, client *bot.Client, state *discordGatewayState, enqueue func(discordCacheMutation), item discordGuildActivity) {
	if discordGuildActivityAction(item.active, item.complete, item.syncing) == "" {
		return
	}
	meta, ok := state.currentMeta(item.shardID)
	if !ok {
		return
	}
	if !item.active {
		state.cancelMemberSync(item.guildID, uuid.Nil)
		enqueue(discordCacheMutation{Meta: meta, Apply: func(ctx context.Context, pool *pgxpool.Pool) error {
			return deactivateDiscordGuildMembers(ctx, pool, item.guildID, meta)
		}})
		return
	}
	gatewayShard := client.ShardManager.Shard(item.shardID)
	if gatewayShard == nil || gatewayShard.Status() != gateway.StatusReady {
		return
	}
	token := uuid.New()
	if !state.beginReconciledMemberSync(item.guildID, meta, token) {
		return
	}
	applied := make(chan bool, 1)
	enqueue(discordCacheMutation{Meta: meta, Apply: func(ctx context.Context, pool *pgxpool.Pool) error {
		result, err := pool.Exec(ctx, `
			UPDATE discord_cache.guilds AS guild
			SET members_complete = false, members_sync_token = $5, updated_at = now()
			WHERE guild.id = $1 AND guild.application_id = $2 AND guild.shard_id = $3 AND guild.generation = $4
			  AND guild.available = true AND guild.members_sync_token IS NULL
			  AND EXISTS (SELECT 1 FROM servers WHERE id = guild.id AND left_at IS NULL AND last_command_at >= now() - interval '90 days')
		`, item.guildID, meta.ApplicationID, meta.ShardID, meta.Generation, token)
		applied <- err == nil && result.RowsAffected() == 1
		return err
	}})
	requestDiscordGuildMembers(ctx, app, client.MemberChunkingManager, state, enqueue, item.guildID, meta, token, applied)
}

func discordGuildActivityAction(active, complete, syncing bool) string {
	if active && !complete && !syncing {
		return "activate"
	}
	if !active && (complete || syncing) {
		return "deactivate"
	}
	return ""
}

func deactivateDiscordGuildMembers(ctx context.Context, pool *pgxpool.Pool, guildID string, meta discordMutationMeta) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	result, err := tx.Exec(ctx, `
		UPDATE discord_cache.guilds AS guild
		SET members_complete = false, members_sync_token = NULL, updated_at = now()
		WHERE guild.id = $1 AND guild.application_id = $2 AND guild.shard_id = $3 AND guild.generation = $4
		  AND NOT EXISTS (SELECT 1 FROM servers WHERE id = guild.id AND left_at IS NULL AND last_command_at >= now() - interval '90 days')
	`, guildID, meta.ApplicationID, meta.ShardID, meta.Generation)
	if err != nil || result.RowsAffected() == 0 {
		return err
	}
	if _, err := tx.Exec(ctx, `DELETE FROM discord_cache.members WHERE guild_id = $1`, guildID); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func setDiscordShardHealthy(ctx context.Context, pool *pgxpool.Pool, meta discordMutationMeta, healthy bool) error {
	_, err := pool.Exec(ctx, `UPDATE discord_cache.gateway_shards SET healthy = $5, heartbeat_at = now() WHERE application_id = $1 AND shard_id = $2 AND shard_count = $3 AND generation = $4`, meta.ApplicationID, meta.ShardID, meta.ShardCount, meta.Generation, healthy)
	return err
}

func replaceDiscordReadyGuildInventory(ctx context.Context, pool *pgxpool.Pool, meta discordMutationMeta, guildIDs []string) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if len(guildIDs) > 0 {
		if _, err := tx.Exec(ctx, `
			INSERT INTO discord_cache.guilds
				(id, data, updated_at, application_id, shard_id, generation, available, metadata_complete, members_complete, members_sync_token)
			SELECT guild_id, jsonb_build_object('id', guild_id), now(), $1, $2, $3, false, false, false, NULL
			FROM unnest($4::text[]) AS ready(guild_id)
			ON CONFLICT (id) DO UPDATE SET
				data = EXCLUDED.data, updated_at = now(), application_id = EXCLUDED.application_id,
				shard_id = EXCLUDED.shard_id, generation = EXCLUDED.generation, available = false,
				metadata_complete = false, members_complete = false, members_sync_token = NULL
		`, meta.ApplicationID, meta.ShardID, meta.Generation, guildIDs); err != nil {
			return err
		}
		for _, table := range []string{"channels", "roles", "members"} {
			if _, err := tx.Exec(ctx, `DELETE FROM discord_cache.`+table+` WHERE guild_id = ANY($1::text[])`, guildIDs); err != nil {
				return err
			}
		}
	}
	result, err := tx.Exec(ctx, `
		UPDATE discord_cache.gateway_shards
		SET healthy = true, heartbeat_at = now()
		WHERE application_id = $1 AND shard_id = $2 AND shard_count = $3 AND generation = $4
	`, meta.ApplicationID, meta.ShardID, meta.ShardCount, meta.Generation)
	if err != nil {
		return err
	}
	if result.RowsAffected() != 1 {
		return errors.New("Discord READY inventory generation was fenced before completion")
	}
	return tx.Commit(ctx)
}

func markDiscordGuildUnavailable(ctx context.Context, pool *pgxpool.Pool, guildID string, meta discordMutationMeta) error {
	_, err := pool.Exec(ctx, `UPDATE discord_cache.guilds SET available = false, metadata_complete = false, members_complete = false, members_sync_token = NULL, updated_at = now() WHERE id = $1 AND application_id = $2 AND shard_id = $3 AND generation = $4`, guildID, meta.ApplicationID, meta.ShardID, meta.Generation)
	return err
}

func replaceDiscordGuildSnapshot(ctx context.Context, pool *pgxpool.Pool, guild discord.GatewayGuild, meta discordMutationMeta, token uuid.UUID, active bool) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	data, err := json.Marshal(guild.Guild)
	if err != nil {
		return err
	}
	guildID := guild.ID.String()
	result, err := tx.Exec(ctx, `
		INSERT INTO discord_cache.guilds
			(id, data, updated_at, application_id, shard_id, generation, available, metadata_complete, members_complete, members_sync_token)
		SELECT $1, $2, now(), $3, $4, $5, true, true, false, CASE WHEN $7 THEN $6::uuid ELSE NULL::uuid END
		FROM discord_cache.gateway_shards
		WHERE application_id = $3 AND shard_id = $4 AND generation = $5
		ON CONFLICT (id) DO UPDATE SET
			data = EXCLUDED.data, updated_at = now(), application_id = EXCLUDED.application_id,
			shard_id = EXCLUDED.shard_id, generation = EXCLUDED.generation, available = true,
			metadata_complete = true, members_complete = false, members_sync_token = EXCLUDED.members_sync_token
	`, guildID, data, meta.ApplicationID, meta.ShardID, meta.Generation, token, active)
	if err != nil {
		return err
	}
	if result.RowsAffected() == 0 {
		return nil
	}
	for _, query := range []string{
		`DELETE FROM discord_cache.channels WHERE guild_id = $1`,
		`DELETE FROM discord_cache.roles WHERE guild_id = $1`,
		`DELETE FROM discord_cache.members WHERE guild_id = $1`,
	} {
		if _, err := tx.Exec(ctx, query, guildID); err != nil {
			return err
		}
	}

	batch := &pgx.Batch{}
	for _, channel := range guild.Channels {
		if err := queueDiscordChannel(batch, guildID, channel); err != nil {
			return err
		}
	}
	for _, thread := range guild.Threads {
		if err := queueDiscordChannel(batch, guildID, thread); err != nil {
			return err
		}
	}
	for _, role := range guild.Roles {
		if err := queueDiscordRole(batch, guildID, role); err != nil {
			return err
		}
	}
	if active {
		for _, member := range guild.Members {
			if err := queueDiscordMember(batch, guildID, member); err != nil {
				return err
			}
		}
	}
	if err := tx.SendBatch(ctx, batch).Close(); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func replaceDiscordGuildMembers(
	ctx context.Context,
	pool *pgxpool.Pool,
	guildID string,
	meta discordMutationMeta,
	token uuid.UUID,
	members []discord.Member,
	deltas []discordMemberDelta,
) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var currentID string
	if err := tx.QueryRow(ctx, `SELECT id FROM discord_cache.guilds WHERE id = $1 AND application_id = $2 AND shard_id = $3 AND generation = $4 AND members_sync_token = $5 FOR UPDATE`, guildID, meta.ApplicationID, meta.ShardID, meta.Generation, token).Scan(&currentID); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		return err
	}
	if _, err := tx.Exec(ctx, `DELETE FROM discord_cache.members WHERE guild_id = $1`, guildID); err != nil {
		return err
	}
	batch := &pgx.Batch{}
	for _, member := range members {
		if err := queueDiscordMember(batch, guildID, member); err != nil {
			return err
		}
	}
	if err := tx.SendBatch(ctx, batch).Close(); err != nil {
		return err
	}
	for _, delta := range deltas {
		if err := applyDiscordMemberDelta(ctx, tx, guildID, delta); err != nil {
			return err
		}
	}
	result, err := tx.Exec(ctx, `UPDATE discord_cache.guilds SET members_complete = true, members_sync_token = NULL, updated_at = now() WHERE id = $1 AND application_id = $2 AND shard_id = $3 AND generation = $4 AND members_sync_token = $5`, guildID, meta.ApplicationID, meta.ShardID, meta.Generation, token)
	if err != nil {
		return err
	}
	if result.RowsAffected() != 1 {
		return nil
	}
	return tx.Commit(ctx)
}

type discordDB interface {
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
}

func applyDiscordMemberDelta(ctx context.Context, db discordDB, guildID string, delta discordMemberDelta) error {
	if delta.Member != nil {
		return upsertDiscordMember(ctx, db, guildID, *delta.Member)
	}
	if delta.UserID != "" {
		_, err := db.Exec(ctx, `DELETE FROM discord_cache.members WHERE guild_id = $1 AND user_id = $2`, guildID, delta.UserID)
		return err
	}
	return nil
}

func upsertDiscordGuildScoped(ctx context.Context, pool *pgxpool.Pool, guild discord.Guild, meta discordMutationMeta) error {
	data, err := json.Marshal(guild)
	if err != nil {
		return err
	}
	_, err = pool.Exec(ctx, `UPDATE discord_cache.guilds SET data = $1, updated_at = now() WHERE id = $2 AND application_id = $3 AND shard_id = $4 AND generation = $5`, data, guild.ID.String(), meta.ApplicationID, meta.ShardID, meta.Generation)
	return err
}

func upsertDiscordChannelScoped(ctx context.Context, pool *pgxpool.Pool, channel discord.GuildChannel, meta discordMutationMeta) error {
	var current bool
	if err := pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM discord_cache.guilds WHERE id = $1 AND application_id = $2 AND shard_id = $3 AND generation = $4)`, channel.GuildID().String(), meta.ApplicationID, meta.ShardID, meta.Generation).Scan(&current); err != nil || !current {
		return err
	}
	return upsertDiscordChannel(ctx, pool, channel)
}

func upsertDiscordMemberScoped(ctx context.Context, pool *pgxpool.Pool, guildID string, member discord.Member, meta discordMutationMeta) error {
	var current bool
	if err := pool.QueryRow(ctx, `SELECT EXISTS (
		SELECT 1 FROM discord_cache.guilds guild
		JOIN servers server ON server.id = guild.id
		WHERE guild.id = $1 AND guild.application_id = $2 AND guild.shard_id = $3
		  AND guild.generation = $4 AND guild.available
		  AND server.left_at IS NULL AND server.last_command_at >= now() - interval '90 days'
	)`, guildID, meta.ApplicationID, meta.ShardID, meta.Generation).Scan(&current); err != nil || !current {
		return err
	}
	return upsertDiscordMember(ctx, pool, guildID, member)
}

func upsertDiscordRoleScoped(ctx context.Context, pool *pgxpool.Pool, guildID string, role discord.Role, meta discordMutationMeta) error {
	var current bool
	if err := pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM discord_cache.guilds WHERE id = $1 AND application_id = $2 AND shard_id = $3 AND generation = $4)`, guildID, meta.ApplicationID, meta.ShardID, meta.Generation).Scan(&current); err != nil || !current {
		return err
	}
	return upsertDiscordRole(ctx, pool, guildID, role)
}

func upsertDiscordChannel(ctx context.Context, db discordDB, channel discord.GuildChannel) error {
	data, err := json.Marshal(channel)
	if err != nil {
		return err
	}
	_, err = db.Exec(ctx, `
		INSERT INTO discord_cache.channels (id, guild_id, data, updated_at)
		VALUES ($1, $2, $3, now())
		ON CONFLICT (id) DO UPDATE SET guild_id = EXCLUDED.guild_id, data = EXCLUDED.data, updated_at = now()
	`, channel.ID().String(), channel.GuildID().String(), data)
	return err
}

func upsertDiscordMember(ctx context.Context, db discordDB, guildID string, member discord.Member) error {
	userData, err := json.Marshal(member.User)
	if err != nil {
		return err
	}
	memberData, err := json.Marshal(member)
	if err != nil {
		return err
	}
	if _, err := db.Exec(ctx, `
		INSERT INTO discord_cache.users (id, data, updated_at)
		VALUES ($1, $2, now())
		ON CONFLICT (id) DO UPDATE SET data = EXCLUDED.data, updated_at = now()
	`, member.User.ID.String(), userData); err != nil {
		return err
	}
	_, err = db.Exec(ctx, `
		INSERT INTO discord_cache.members (guild_id, user_id, data, updated_at)
		VALUES ($1, $2, $3, now())
		ON CONFLICT (guild_id, user_id) DO UPDATE SET data = EXCLUDED.data, updated_at = now()
	`, guildID, member.User.ID.String(), memberData)
	return err
}

func upsertDiscordRole(ctx context.Context, db discordDB, guildID string, role discord.Role) error {
	data, err := json.Marshal(role)
	if err != nil {
		return err
	}
	_, err = db.Exec(ctx, `
		INSERT INTO discord_cache.roles (guild_id, id, data, updated_at)
		VALUES ($1, $2, $3, now())
		ON CONFLICT (guild_id, id) DO UPDATE SET data = EXCLUDED.data, updated_at = now()
	`, guildID, role.ID.String(), data)
	return err
}

func deleteDiscordGuild(ctx context.Context, pool *pgxpool.Pool, guildID string, meta discordMutationMeta) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var currentID string
	if err := tx.QueryRow(ctx, `SELECT id FROM discord_cache.guilds WHERE id = $1 AND application_id = $2 AND shard_id = $3 AND generation = $4 FOR UPDATE`, guildID, meta.ApplicationID, meta.ShardID, meta.Generation).Scan(&currentID); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		return err
	}
	for _, query := range []string{
		`DELETE FROM discord_cache.channels WHERE guild_id = $1`,
		`DELETE FROM discord_cache.roles WHERE guild_id = $1`,
		`DELETE FROM discord_cache.members WHERE guild_id = $1`,
		`DELETE FROM discord_cache.guilds WHERE id = $1`,
	} {
		if _, err := tx.Exec(ctx, query, guildID); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

func queueDiscordChannel(batch *pgx.Batch, guildID string, channel discord.GuildChannel) error {
	// GUILD_CREATE embeds channels without guild_id. The enclosing snapshot
	// supplies their authoritative parent; GuildID() is zero in that payload.
	if guildID == "" || guildID == "0" {
		return errors.New("snapshot channel requires its parent guild")
	}
	data, err := json.Marshal(channel)
	if err != nil {
		return err
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		return err
	}
	fields["guild_id"], err = json.Marshal(guildID)
	if err != nil {
		return err
	}
	data, err = json.Marshal(fields)
	if err != nil {
		return err
	}
	batch.Queue(`
		INSERT INTO discord_cache.channels (id, guild_id, data, updated_at)
		VALUES ($1, $2, $3, now())
		ON CONFLICT (id) DO UPDATE SET guild_id = EXCLUDED.guild_id, data = EXCLUDED.data, updated_at = now()
	`, channel.ID().String(), guildID, data)
	return nil
}

func queueDiscordRole(batch *pgx.Batch, guildID string, role discord.Role) error {
	data, err := json.Marshal(role)
	if err != nil {
		return err
	}
	batch.Queue(`
		INSERT INTO discord_cache.roles (guild_id, id, data, updated_at)
		VALUES ($1, $2, $3, now())
		ON CONFLICT (guild_id, id) DO UPDATE SET data = EXCLUDED.data, updated_at = now()
	`, guildID, role.ID.String(), data)
	return nil
}

func queueDiscordMember(batch *pgx.Batch, guildID string, member discord.Member) error {
	userData, userErr := json.Marshal(member.User)
	memberData, memberErr := json.Marshal(member)
	if userErr != nil {
		return userErr
	}
	if memberErr != nil {
		return memberErr
	}
	batch.Queue(`
		INSERT INTO discord_cache.users (id, data, updated_at)
		VALUES ($1, $2, now())
		ON CONFLICT (id) DO UPDATE SET data = EXCLUDED.data, updated_at = now()
	`, member.User.ID.String(), userData)
	batch.Queue(`
		INSERT INTO discord_cache.members (guild_id, user_id, data, updated_at)
		VALUES ($1, $2, $3, now())
		ON CONFLICT (guild_id, user_id) DO UPDATE SET data = EXCLUDED.data, updated_at = now()
	`, guildID, member.User.ID.String(), memberData)
	return nil
}
