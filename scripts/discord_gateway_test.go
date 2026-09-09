package scripts

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"clashking_tracking/internal/platform"

	"github.com/disgoorg/disgo/discord"
	"github.com/disgoorg/disgo/events"
	"github.com/disgoorg/disgo/gateway"
	"github.com/disgoorg/snowflake/v2"
	"github.com/google/uuid"
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
	handler = handler.WithAttrs([]slog.Attr{slog.String("bound", "bound-secret")}).WithGroup("private").(discordLibraryLogHandler)
	record := slog.NewRecord(time.Now(), slog.LevelError, `error while parsing gateway message: {"message-secret":true}`, 0)
	record.AddAttrs(slog.Any("err", errors.New(`failed: {"guild":"private","members":["secret"]}`)))
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
	if len(reporter.captured) != 1 || strings.Contains(reporter.captured[0], "private") {
		t.Fatalf("safe Discord library Sentry capture = %#v", reporter.captured)
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
