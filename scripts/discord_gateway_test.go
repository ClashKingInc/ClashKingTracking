package scripts

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strings"
	"testing"
	"time"

	"clashking_tracking/internal/platform"

	"github.com/disgoorg/disgo/discord"
	"github.com/disgoorg/disgo/events"
	"github.com/disgoorg/disgo/gateway"
	"github.com/disgoorg/snowflake/v2"
	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestSnapshotChannelUsesEnclosingGuildWhenDiscordOmitsGuildID(t *testing.T) {
	var channel discord.GuildTextChannel
	if err := json.Unmarshal([]byte(`{"id":"123456789012345678","name":"general","type":0}`), &channel); err != nil {
		t.Fatal(err)
	}
	if channel.GuildID() != 0 {
		t.Fatal("fixture must reproduce missing guild_id")
	}
	const guildID = "923764211845312533"
	batch := &pgx.Batch{}
	if err := queueDiscordChannel(batch, guildID, channel); err != nil {
		t.Fatal(err)
	}
	if len(batch.QueuedQueries) != 1 || batch.QueuedQueries[0].Arguments[1] != guildID {
		t.Fatal("snapshot channel did not retain its parent guild")
	}
	var data map[string]any
	if err := json.Unmarshal(batch.QueuedQueries[0].Arguments[2].([]byte), &data); err != nil {
		t.Fatal(err)
	}
	if data["guild_id"] != guildID || data["id"] != "123456789012345678" {
		t.Fatal("cached payload has an incorrect channel or guild identity")
	}
	if err := queueDiscordChannel(&pgx.Batch{}, "0", channel); err == nil {
		t.Fatal("zero guild ID must not be cached")
	}
}

func TestDiscordLibraryLoggerDropsRawPayloadFromMessageAttributesAndDerivedHandlers(t *testing.T) {
	var output bytes.Buffer
	reporter := &deliveryErrorRecorder{seen: map[string]bool{}}
	handler := discordLibraryLogHandler{target: slog.NewJSONHandler(&output, nil), reporter: reporter}
	handler = handler.WithAttrs([]slog.Attr{
		slog.String("bound", "bound-secret"),
		slog.Int("shard_id", 4),
		slog.Int("shard_count", 15),
	}).WithGroup("private").(discordLibraryLogHandler)
	record := slog.NewRecord(time.Now(), slog.LevelError, `error while parsing gateway message: {"message-secret":true}`, 0)
	record.AddAttrs(
		slog.Any("err", fmt.Errorf(`failed: {"guild":"private","members":["secret"]}: %w`, io.ErrUnexpectedEOF)),
		slog.String("url", "wss://secret.example/?session=private"),
	)
	if err := handler.Handle(t.Context(), record); err != nil {
		t.Fatal(err)
	}
	got := output.String()
	if strings.Contains(got, "private") || strings.Contains(got, "members") || strings.Contains(got, "secret") || strings.Contains(got, "failed") {
		t.Fatalf("Discord library logger exposed payload-bearing attributes: %s", got)
	}
	if !strings.Contains(got, `"discord_library":true`) || !strings.Contains(got, "Discord library diagnostic") {
		t.Fatalf("safe Discord library diagnostic missing: %s", got)
	}
	if !strings.Contains(got, `"shard_id":4`) || !strings.Contains(got, `"shard_count":15`) || !strings.Contains(got, `"category":"invalid_gateway_payload"`) {
		t.Fatalf("safe Discord library context missing: %s", got)
	}
	if !strings.Contains(got, `"error_kind":"unexpected_eof"`) {
		t.Fatalf("safe Discord library error classification missing: %s", got)
	}
	if len(reporter.captured) != 1 || strings.Contains(reporter.captured[0], "private") {
		t.Fatalf("safe Discord library Sentry capture = %#v", reporter.captured)
	}
	if reporter.details[0]["error_kind"] != "unexpected_eof" {
		t.Fatalf("safe Discord library Sentry tags = %#v", reporter.details[0])
	}
}

func TestDiscordLibraryLoggerKeepsOnlyAllowlistedLifecycleContext(t *testing.T) {
	var output bytes.Buffer
	handler := discordLibraryLogHandler{target: slog.NewJSONHandler(&output, nil)}
	handler = handler.WithGroup("private-session").WithAttrs([]slog.Attr{
		slog.String("name", "gateway"),
		slog.Int("shard_id", 7),
		slog.Int("shard_count", 15),
		slog.String("session_id", "secret-session"),
	}).(discordLibraryLogHandler)
	record := slog.NewRecord(time.Now(), slog.LevelError, "gateway close received", 0)
	record.AddAttrs(
		slog.Bool("reconnect", false),
		slog.Any("err", &websocket.CloseError{Code: 4014, Text: "secret close text"}),
		slog.String("url", "wss://secret.example/?session=secret-session"),
	)
	if err := handler.Handle(t.Context(), record); err != nil {
		t.Fatal(err)
	}
	got := output.String()
	for _, secret := range []string{"secret-session", "secret close text", "secret.example", "private-session"} {
		if strings.Contains(got, secret) {
			t.Fatalf("Discord lifecycle log exposed %q: %s", secret, got)
		}
	}
	for _, safe := range []string{`"category":"websocket_close"`, `"component":"gateway"`, `"shard_id":7`, `"shard_count":15`, `"close_code":4014`, `"error_kind":"websocket_close"`, `"reconnect":false`} {
		if !strings.Contains(got, safe) {
			t.Fatalf("Discord lifecycle log omitted %s: %s", safe, got)
		}
	}
}

func TestDiscordLibraryLoggerClassifiesNetworkErrorWithoutExposingDetails(t *testing.T) {
	var output bytes.Buffer
	handler := discordLibraryLogHandler{target: slog.NewJSONHandler(&output, nil)}
	record := slog.NewRecord(time.Now(), slog.LevelError, "failed to read next message", 0)
	record.AddAttrs(slog.Any("err", &net.OpError{
		Op:  "read",
		Net: "tcp",
		Err: errors.New("secret gateway endpoint and session"),
	}))
	if err := handler.Handle(t.Context(), record); err != nil {
		t.Fatal(err)
	}
	got := output.String()
	if strings.Contains(got, "secret") || strings.Contains(got, "endpoint") || strings.Contains(got, "session") {
		t.Fatalf("Discord network log exposed error details: %s", got)
	}
	for _, safe := range []string{`"category":"websocket_read_failed"`, `"error_kind":"network"`, `"network_op":"read"`} {
		if !strings.Contains(got, safe) {
			t.Fatalf("Discord network log omitted %s: %s", safe, got)
		}
	}
}

func TestDiscordGatewayDisconnectQueuesOneUnhealthyTransition(t *testing.T) {
	state := &discordGatewayState{appID: "123", shards: map[int]discordShardState{
		4: {Generation: uuid.New(), ShardCount: 15, Sequence: 91, Ready: true},
	}}
	var queued []discordCacheMutation
	enqueue := func(mutation discordCacheMutation) { queued = append(queued, mutation) }
	markDiscordShardDisconnected(state, 4, enqueue)
	markDiscordShardDisconnected(state, 4, enqueue)
	if state.shardReady(4) {
		t.Fatal("disconnected shard remained ready")
	}
	if len(queued) != 1 || queued[0].Meta.ShardID != 4 || queued[0].Meta.ShardCount != 15 || queued[0].Meta.Sequence != 91 {
		t.Fatalf("disconnect mutations = %#v, want one generation-fenced unhealthy write", queued)
	}
}

func TestDiscordGatewayReadinessTracksLiveShardStatusAndResumeEvidence(t *testing.T) {
	state := &discordGatewayState{appID: "123", shards: map[int]discordShardState{
		0: {Generation: uuid.New(), ShardCount: 2, Ready: true},
		1: {Generation: uuid.New(), ShardCount: 2, Ready: true},
	}}
	shards := []gateway.Gateway{
		&fakeDiscordGateway{id: 0, count: 2, status: gateway.StatusReady},
		&fakeDiscordGateway{id: 1, count: 2, status: gateway.StatusReady},
	}
	app := &platform.App{Stats: platform.NewTracker()}
	var queued []discordCacheMutation
	enqueue := func(mutation discordCacheMutation) { queued = append(queued, mutation) }
	if ready := reconcileDiscordShardReadiness(app, shards, state, enqueue); ready != 2 {
		t.Fatalf("ready shards = %d, want 2", ready)
	}
	shards[1].(*fakeDiscordGateway).status = gateway.StatusDisconnected
	if ready := reconcileDiscordShardReadiness(app, shards, state, enqueue); ready != 1 {
		t.Fatalf("ready shards after disconnect = %d, want 1", ready)
	}
	if len(queued) != 1 || state.shardReady(1) {
		t.Fatalf("disconnect did not enqueue one unhealthy transition: %#v", queued)
	}
	stats := app.Stats.Snapshot()
	if len(stats.Domains) != 1 || stats.Domains[0].Healthy || !strings.Contains(stats.Domains[0].LastError, "1 of 2") {
		t.Fatalf("overall readiness after disconnect = %#v", stats.Domains)
	}
	shards[1].(*fakeDiscordGateway).status = gateway.StatusReady
	state.setShardReady(1, true) // represents an observed RESUMED event
	if ready := reconcileDiscordShardReadiness(app, shards, state, enqueue); ready != 2 {
		t.Fatalf("ready shards after RESUMED = %d, want 2", ready)
	}
}

func TestDiscordGatewaySimultaneousShardFailuresAreContained(t *testing.T) {
	state := &discordGatewayState{appID: "123", shards: map[int]discordShardState{
		0: {Generation: uuid.New(), ShardCount: 2, Sequence: 50, Ready: true},
		1: {Generation: uuid.New(), ShardCount: 2, Sequence: 75, Ready: true},
	}}
	shards := []gateway.Gateway{
		&fakeDiscordGateway{id: 0, count: 2, status: gateway.StatusDisconnected},
		&fakeDiscordGateway{id: 1, count: 2, status: gateway.StatusDisconnected},
	}
	app := &platform.App{Stats: platform.NewTracker()}
	var queued []discordCacheMutation
	if ready := reconcileDiscordShardReadiness(app, shards, state, func(mutation discordCacheMutation) {
		queued = append(queued, mutation)
	}); ready != 0 {
		t.Fatalf("ready shards = %d, want 0", ready)
	}
	if len(queued) != 2 || queued[0].Meta.ShardID == queued[1].Meta.ShardID {
		t.Fatalf("simultaneous shard failures queued %#v, want one fenced transition per shard", queued)
	}
	if state.shardReady(0) || state.shardReady(1) {
		t.Fatal("simultaneously disconnected shards retained READY evidence")
	}
}

func TestDiscordGatewaySupervisorKeepsHealthyShardsUntilAllAreTerminal(t *testing.T) {
	state := &discordGatewayState{appID: "123", shards: map[int]discordShardState{
		0: {Generation: uuid.New(), ShardCount: 2, Ready: true},
		1: {Generation: uuid.New(), ShardCount: 2, Ready: true},
	}}
	shards := []gateway.Gateway{
		&fakeDiscordGateway{id: 0, count: 2, status: gateway.StatusReady},
		&fakeDiscordGateway{id: 1, count: 2, status: gateway.StatusReady},
	}
	app := &platform.App{Stats: platform.NewTracker(), Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	failures := make(chan discordShardFailure, 2)
	done := make(chan error, 1)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go func() {
		done <- runDiscordShardSupervisor(ctx, app, shards, state, func(discordCacheMutation) {}, failures)
	}()
	failures <- discordShardFailure{ShardID: 0, Err: &websocket.CloseError{Code: 4014}}
	select {
	case err := <-done:
		t.Fatalf("one terminal shard stopped the whole gateway: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	failures <- discordShardFailure{ShardID: 1, Err: &websocket.CloseError{Code: 4014}}
	select {
	case err := <-done:
		if err == nil || !strings.Contains(err.Error(), "all 2") {
			t.Fatalf("all-shard terminal result = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("all terminal shards did not stop supervision")
	}
}

func TestDiscordGuildCreateAcceptsUnquotedApplicationSnowflake(t *testing.T) {
	previous := snowflake.AllowUnquoted
	snowflake.AllowUnquoted = true
	t.Cleanup(func() { snowflake.AllowUnquoted = previous })
	const applicationID snowflake.ID = 123456789012345678
	payload := []byte(`{
		"id":"1","name":"fixture","owner_id":"1","application_id":123456789012345678,
		"roles":[],"emojis":[],"features":[],"channels":[],"threads":[],
		"members":[],"presences":[],"voice_states":[],"guild_scheduled_events":[],
		"stage_instances":[],"stickers":[]
	}`)
	decoded, err := gateway.UnmarshalEventData(payload, gateway.EventTypeGuildCreate)
	if err != nil {
		t.Fatalf("decode sanitized GUILD_CREATE with exact integer snowflake: %v", err)
	}
	guildCreate, ok := decoded.(gateway.EventGuildCreate)
	if !ok || guildCreate.ApplicationID == nil || *guildCreate.ApplicationID != applicationID {
		t.Fatalf("decoded application snowflake = %#v, want %d", decoded, applicationID)
	}
}

func TestDiscordGatewayOpenSurfacesWriterFailureBeforeShardStartupCompletes(t *testing.T) {
	openDone := make(chan error)
	writerDone := make(chan error, 1)
	other := make(chan error)
	writerDone <- errors.New("snapshot write failed")
	opened, err := awaitDiscordGatewayOpen(t.Context(), openDone, writerDone, other, other, other, other)
	if opened || err == nil || !strings.Contains(err.Error(), "cache writer") || !strings.Contains(err.Error(), "snapshot write failed") {
		t.Fatalf("startup supervision = opened %v, err %v", opened, err)
	}
}

func TestDiscordGatewayRequiresReadyEvidenceFromEveryExpectedShard(t *testing.T) {
	state := &discordGatewayState{shards: map[int]discordShardState{}}
	shards := make([]gateway.Gateway, 0, 15)
	for shardID := range 15 {
		state.shards[shardID] = discordShardState{ShardCount: 15, Ready: true}
		shards = append(shards, &fakeDiscordGateway{id: shardID, count: 15, status: gateway.StatusReady})
	}
	if err := validateDiscordShardReadiness(state, shards); err != nil {
		t.Fatalf("all expected shards should be ready: %v", err)
	}

	state.shards[14] = discordShardState{ShardCount: 15}
	if err := validateDiscordShardReadiness(state, shards); err == nil || !strings.Contains(err.Error(), "READY or RESUMED") {
		t.Fatalf("missing lifecycle evidence was accepted: %v", err)
	}
	state.shards[14] = discordShardState{ShardCount: 15, Ready: true}
	shards[14] = &fakeDiscordGateway{id: 14, count: 15, status: gateway.StatusDisconnected}
	if err := validateDiscordShardReadiness(state, shards); err == nil || !strings.Contains(err.Error(), "Disconnected") {
		t.Fatalf("disconnected shard was accepted: %v", err)
	}
}

func TestValidateDiscordGatewayConfig(t *testing.T) {
	valid := platform.Config{
		DiscordBotToken:                      "token",
		DiscordGatewayQueueSize:              1,
		DiscordGatewayMemberChunkConcurrency: 2,
		TimescaleURL:                         "postgres://localhost/test",
		ValkeyAddr:                           "localhost:6379",
		EventStreamName:                      "tracking:events",
	}
	if err := validateDiscordGatewayConfig(valid); err != nil {
		t.Fatalf("valid gateway config failed: %v", err)
	}
	dryRun := valid
	dryRun.DryRun = true
	dryRun.TimescaleURL = ""
	dryRun.ValkeyAddr = ""
	dryRun.EventStreamName = ""
	if err := validateDiscordGatewayConfig(dryRun); err != nil {
		t.Fatalf("dry-run gateway config without persistence failed: %v", err)
	}

	tests := []platform.Config{
		{DiscordGatewayQueueSize: 1, TimescaleURL: valid.TimescaleURL, ValkeyAddr: valid.ValkeyAddr, EventStreamName: valid.EventStreamName},
		{DiscordBotToken: "token", DiscordGatewayQueueSize: 1, ValkeyAddr: valid.ValkeyAddr, EventStreamName: valid.EventStreamName},
		{DiscordBotToken: "token", TimescaleURL: valid.TimescaleURL, ValkeyAddr: valid.ValkeyAddr, EventStreamName: valid.EventStreamName},
		{DiscordBotToken: "token", TimescaleURL: valid.TimescaleURL, DiscordGatewayQueueSize: 1},
	}
	for _, cfg := range tests {
		if err := validateDiscordGatewayConfig(cfg); err == nil {
			t.Fatalf("invalid gateway config passed: %+v", cfg)
		}
	}
}

func TestDiscordGatewayMutationCapturesSequenceAtEnqueue(t *testing.T) {
	state := &discordGatewayState{appID: "123", shards: map[int]discordShardState{}, syncs: map[string]discordMemberSync{}}
	queued := make([]discordCacheMutation, 0, 2)
	enqueue := func(mutation discordCacheMutation) { queued = append(queued, mutation) }
	ready := events.NewGenericEvent(nil, 3, 1)
	meta := state.rotateShard(ready, 2, []discord.UnavailableGuild{{ID: 456}}, enqueue)
	queued = queued[:0]
	event := events.NewGenericEvent(nil, 41, 1)
	state.enqueueEvent(event, enqueue, func(discordMutationMeta, context.Context, *pgxpool.Pool) error { return nil })

	if len(queued) != 1 || queued[0].Meta.Sequence != 41 {
		t.Fatalf("queued mutation did not capture event sequence: %#v", queued)
	}
	if queued[0].Meta.Generation != meta.Generation || queued[0].Meta.ApplicationID != "123" || queued[0].Meta.ShardCount != 2 {
		t.Fatalf("queued mutation lost shard generation scope: %#v", queued[0].Meta)
	}
}

func TestDiscordGatewayMemberSyncBuffersDeltasAndFencesOldGeneration(t *testing.T) {
	state := &discordGatewayState{appID: "123", shards: map[int]discordShardState{}, syncs: map[string]discordMemberSync{}}
	queued := make([]discordCacheMutation, 0, 4)
	enqueue := func(mutation discordCacheMutation) { queued = append(queued, mutation) }
	meta := state.rotateShard(events.NewGenericEvent(nil, 1, 0), 1, []discord.UnavailableGuild{{ID: 456}}, enqueue)
	queued = queued[:0]
	token := uuid.New()
	state.syncs["456"] = discordMemberSync{Meta: meta, Token: token}
	member := discord.Member{User: discord.User{ID: 789}}
	state.enqueueMemberDelta(events.NewGenericEvent(nil, 7, 0), "456", discordMemberDelta{Member: &member}, enqueue, func(discordMutationMeta, context.Context, *pgxpool.Pool) error { return nil })
	if got := len(state.syncs["456"].Deltas); got != 1 {
		t.Fatalf("buffered member deltas = %d, want 1", got)
	}
	if queued[0].Meta.Sequence != 7 {
		t.Fatalf("member delta sequence = %d, want 7", queued[0].Meta.Sequence)
	}

	state.rotateShard(events.NewGenericEvent(nil, 0, 0), 1, []discord.UnavailableGuild{{ID: 456}}, enqueue)
	before := len(queued)
	if state.completeMemberSync("456", meta, token, enqueue, []discord.Member{member}) {
		t.Fatal("old generation member chunk was accepted")
	}
	if len(queued) != before {
		t.Fatal("old generation member chunk queued a database replacement")
	}
}

func TestDiscordGatewayMemberSyncAcceptsOnlyCurrentToken(t *testing.T) {
	state := &discordGatewayState{appID: "123", shards: map[int]discordShardState{}, syncs: map[string]discordMemberSync{}}
	queued := make([]discordCacheMutation, 0, 2)
	enqueue := func(mutation discordCacheMutation) { queued = append(queued, mutation) }
	meta := state.rotateShard(events.NewGenericEvent(nil, 1, 0), 1, []discord.UnavailableGuild{{ID: 456}}, enqueue)
	queued = queued[:0]
	token := uuid.New()
	state.syncs["456"] = discordMemberSync{Meta: meta, Token: token}
	if state.completeMemberSync("456", meta, uuid.New(), enqueue, nil) {
		t.Fatal("wrong member sync token was accepted")
	}
	if !state.completeMemberSync("456", meta, token, enqueue, nil) {
		t.Fatal("current member sync token was rejected")
	}
	if len(queued) != 1 || queued[0].Meta.Generation != meta.Generation {
		t.Fatalf("current member replacement was not queued with generation: %#v", queued)
	}
}

func TestDiscordGatewayHeartbeatFitsAPIFreshnessWindow(t *testing.T) {
	if discordGatewayHeartbeatInterval != 15_000_000_000 {
		t.Fatalf("heartbeat interval = %s, want 15s", discordGatewayHeartbeatInterval)
	}
	if discordGuildActivityReconcileInterval != 5*time.Minute {
		t.Fatalf("guild activity reconciliation interval = %s, want 5m", discordGuildActivityReconcileInterval)
	}
}

func TestDiscordGatewayQueuesReadyInventoryBeforeGuildSnapshots(t *testing.T) {
	state := &discordGatewayState{appID: "123", shards: map[int]discordShardState{}, syncs: map[string]discordMemberSync{}}
	queued := make([]discordCacheMutation, 0, 4)
	enqueue := func(mutation discordCacheMutation) { queued = append(queued, mutation) }
	state.rotateShard(events.NewGenericEvent(nil, 0, 1), 2, []discord.UnavailableGuild{{ID: 1}, {ID: 2, Unavailable: true}}, enqueue)
	if len(queued) != 2 {
		t.Fatalf("READY queued %d mutations, want generation plus fenced guild inventory", len(queued))
	}
	noop := func(discordMutationMeta, context.Context, *pgxpool.Pool) error { return nil }
	state.enqueueMemberSnapshot(events.NewGenericEvent(nil, 1, 1), "guild-1", uuid.New(), true, enqueue, noop)
	if len(queued) != 3 {
		t.Fatalf("first guild snapshot queued %d total mutations, want 3", len(queued))
	}
	state.enqueueMemberSnapshot(events.NewGenericEvent(nil, 2, 1), "guild-2", uuid.New(), true, enqueue, noop)
	if len(queued) != 4 {
		t.Fatalf("second guild snapshot must queue independently after inventory, got %d total", len(queued))
	}
}

func TestDiscordGuildActivityReconciliationTransitions(t *testing.T) {
	tests := []struct {
		active, complete, syncing bool
		want                      string
	}{
		{true, false, false, "activate"},
		{true, true, false, ""},
		{true, false, true, ""},
		{false, true, false, "deactivate"},
		{false, false, true, "deactivate"},
		{false, false, false, ""},
	}
	for _, test := range tests {
		if got := discordGuildActivityAction(test.active, test.complete, test.syncing); got != test.want {
			t.Fatalf("activity action(%v,%v,%v) = %q, want %q", test.active, test.complete, test.syncing, got, test.want)
		}
	}
}

type fakeDiscordGateway struct {
	id, count int
	status    gateway.Status
}

func (g *fakeDiscordGateway) ShardID() int                             { return g.id }
func (g *fakeDiscordGateway) ShardCount() int                          { return g.count }
func (*fakeDiscordGateway) SessionID() *string                         { return nil }
func (*fakeDiscordGateway) LastSequenceReceived() *int                 { return nil }
func (*fakeDiscordGateway) ResumeURL() *string                         { return nil }
func (*fakeDiscordGateway) Intents() gateway.Intents                   { return gateway.IntentsNone }
func (*fakeDiscordGateway) Open(context.Context) error                 { return nil }
func (*fakeDiscordGateway) Close(context.Context)                      {}
func (*fakeDiscordGateway) CloseWithCode(context.Context, int, string) {}
func (g *fakeDiscordGateway) Status() gateway.Status                   { return g.status }
func (*fakeDiscordGateway) Send(context.Context, gateway.Opcode, gateway.MessageData) error {
	return nil
}
func (*fakeDiscordGateway) Latency() time.Duration                       { return 0 }
func (*fakeDiscordGateway) Presence() *gateway.MessageDataPresenceUpdate { return nil }
