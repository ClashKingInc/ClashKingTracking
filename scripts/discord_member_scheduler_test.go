package scripts

import (
	"context"
	"io"
	"log/slog"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"clashking_tracking/internal/platform"

	"github.com/disgoorg/disgo/discord"
	"github.com/disgoorg/snowflake/v2"
	"github.com/google/uuid"
)

func TestDiscordMemberSchedulerWaitsForShardReadinessBeforeTimeout(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	state, meta, request := testDiscordMemberRequest("100", 0)
	var ready atomic.Bool
	started := make(chan time.Time, 1)
	cleanup := make(chan discordCacheMutation, 1)
	timeout := 40 * time.Millisecond
	scheduler := newDiscordMemberRequestScheduler(ctx, testGatewayApp(), discordMemberRequesterFunc(func(ctx context.Context, _ snowflake.ID) ([]discord.Member, error) {
		started <- time.Now()
		<-ctx.Done()
		return nil, ctx.Err()
	}), state, func(mutation discordCacheMutation) { cleanup <- mutation }, func(discordMutationMeta) bool { return ready.Load() }, 1, 10, timeout)
	if !scheduler.Enqueue(request) {
		t.Fatal("initial request was rejected")
	}
	select {
	case <-started:
		t.Fatal("member request started before its shard was ready")
	case <-time.After(150 * time.Millisecond):
	}
	readyAt := time.Now()
	ready.Store(true)
	select {
	case requestStarted := <-started:
		if requestStarted.Before(readyAt) {
			t.Fatal("member request started before readiness")
		}
	case <-time.After(time.Second):
		t.Fatal("member request did not start after readiness")
	}
	select {
	case mutation := <-cleanup:
		if time.Since(readyAt) < timeout-10*time.Millisecond || mutation.Meta.Generation != meta.Generation {
			t.Fatal("request timeout started before readiness or lost its generation")
		}
	case <-time.After(time.Second):
		t.Fatal("timed-out request did not enqueue cleanup")
	}
	cancel()
	scheduler.Wait()
}

func TestDiscordMemberSchedulerBoundsEachShardAndDeduplicatesBurst(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	state := &discordGatewayState{appID: "1", shards: map[int]discordShardState{}, syncs: map[string]discordMemberSync{}}
	generations := []uuid.UUID{uuid.New(), uuid.New()}
	for shardID := range 2 {
		state.shards[shardID] = discordShardState{Generation: generations[shardID], ShardCount: 2}
	}
	started := make(chan int, 100)
	release := make(chan struct{}, 100)
	var mu sync.Mutex
	current, peak := [2]int{}, [2]int{}
	scheduler := newDiscordMemberRequestScheduler(ctx, testGatewayApp(), discordMemberRequesterFunc(func(ctx context.Context, guildID snowflake.ID) ([]discord.Member, error) {
		shardID := 0
		if guildID >= 2000 {
			shardID = 1
		}
		mu.Lock()
		current[shardID]++
		peak[shardID] = max(peak[shardID], current[shardID])
		mu.Unlock()
		started <- shardID
		select {
		case <-release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		mu.Lock()
		current[shardID]--
		mu.Unlock()
		return nil, nil
	}), state, func(discordCacheMutation) {}, func(discordMutationMeta) bool { return true }, 2, 100, time.Second)

	requests := make([]discordMemberRequest, 0, 40)
	for shardID := range 2 {
		for index := range 20 {
			guildID := strconv.Itoa(1000 + shardID*1000 + index)
			token := uuid.New()
			meta := discordMutationMeta{ApplicationID: "1", ShardID: shardID, ShardCount: 2, Generation: generations[shardID]}
			state.syncs[guildID] = discordMemberSync{Meta: meta, Token: token}
			applied := make(chan bool, 1)
			applied <- true
			request := discordMemberRequest{guildID: guildID, meta: meta, token: token, snapshotApplied: applied}
			requests = append(requests, request)
		}
	}
	for _, request := range requests {
		if !scheduler.Enqueue(request) {
			t.Fatalf("burst request %s was rejected", request.guildID)
		}
	}
	if scheduler.Enqueue(requests[5]) {
		t.Fatal("duplicate queued/in-flight guild was accepted")
	}
	for range 4 {
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("shards did not start independently")
		}
	}
	select {
	case <-started:
		t.Fatal("a shard exceeded its configured concurrency")
	case <-time.After(75 * time.Millisecond):
	}
	for range 40 {
		release <- struct{}{}
	}
	for range 36 {
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("burst did not drain")
		}
	}
	mu.Lock()
	gotPeak := peak
	mu.Unlock()
	if gotPeak != [2]int{2, 2} {
		t.Fatalf("per-shard peak concurrency = %v, want [2 2]", gotPeak)
	}
	cancel()
	scheduler.Wait()
}

func TestDiscordMemberSchedulerPromotesReactivationAheadOfBackground(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	state := &discordGatewayState{appID: "1", shards: map[int]discordShardState{}, syncs: map[string]discordMemberSync{}}
	generation := uuid.New()
	state.shards[0] = discordShardState{Generation: generation, ShardCount: 1}
	started := make(chan snowflake.ID, 3)
	release := make(chan struct{}, 3)
	scheduler := newDiscordMemberRequestScheduler(ctx, testGatewayApp(), discordMemberRequesterFunc(func(ctx context.Context, guildID snowflake.ID) ([]discord.Member, error) {
		started <- guildID
		select {
		case <-release:
			return nil, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}), state, func(discordCacheMutation) {}, func(discordMutationMeta) bool { return true }, 1, 10, time.Second)
	requests := make(map[string]discordMemberRequest)
	for _, guildID := range []string{"100", "200", "300"} {
		token := uuid.New()
		meta := discordMutationMeta{ApplicationID: "1", ShardID: 0, ShardCount: 1, Generation: generation}
		state.syncs[guildID] = discordMemberSync{Meta: meta, Token: token}
		applied := make(chan bool, 1)
		applied <- true
		requests[guildID] = discordMemberRequest{guildID: guildID, meta: meta, token: token, snapshotApplied: applied, priority: discordMemberRequestBackground}
	}
	scheduler.Enqueue(requests["100"])
	if got := <-started; got != 100 {
		t.Fatalf("first request = %d", got)
	}
	scheduler.Enqueue(requests["200"])
	scheduler.Enqueue(requests["300"])
	promoted := requests["300"]
	promoted.priority = discordMemberRequestImmediate
	if scheduler.Enqueue(promoted) {
		t.Fatal("promotion should deduplicate the queued request")
	}
	release <- struct{}{}
	if got := <-started; got != 300 {
		t.Fatalf("request after reactivation promotion = %d, want 300", got)
	}
	release <- struct{}{}
	if got := <-started; got != 200 {
		t.Fatalf("remaining background request = %d, want 200", got)
	}
	release <- struct{}{}
	cancel()
	scheduler.Wait()
}

func TestDiscordMemberSchedulerDiscardsReconnectAndCancelsDeactivation(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	state, _, queued := testDiscordMemberRequest("100", 0)
	var ready atomic.Bool
	started := make(chan struct{}, 1)
	scheduler := newDiscordMemberRequestScheduler(ctx, testGatewayApp(), discordMemberRequesterFunc(func(ctx context.Context, _ snowflake.ID) ([]discord.Member, error) {
		started <- struct{}{}
		return nil, nil
	}), state, func(discordCacheMutation) {}, func(discordMutationMeta) bool { return ready.Load() }, 1, 10, time.Second)
	scheduler.Enqueue(queued)
	state.mu.Lock()
	newGeneration := uuid.New()
	newToken := uuid.New()
	newMeta := discordMutationMeta{ApplicationID: "1", ShardID: 0, ShardCount: 1, Generation: newGeneration}
	state.shards[0] = discordShardState{Generation: newGeneration, ShardCount: 1}
	state.syncs[queued.guildID] = discordMemberSync{Meta: newMeta, Token: newToken}
	state.mu.Unlock()
	newApplied := make(chan bool, 1)
	newApplied <- true
	if !scheduler.Enqueue(discordMemberRequest{guildID: queued.guildID, meta: newMeta, token: newToken, snapshotApplied: newApplied}) {
		t.Fatal("new reconnect generation was not allowed to replace obsolete work")
	}
	ready.Store(true)
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("new reconnect generation did not reach Discord")
	}
	cancel()
	scheduler.Wait()

	ctx, cancel = context.WithCancel(t.Context())
	defer cancel()
	state, _, active := testDiscordMemberRequest("200", 0)
	cancelled := make(chan struct{}, 1)
	scheduler = newDiscordMemberRequestScheduler(ctx, testGatewayApp(), discordMemberRequesterFunc(func(ctx context.Context, _ snowflake.ID) ([]discord.Member, error) {
		started <- struct{}{}
		<-ctx.Done()
		cancelled <- struct{}{}
		return nil, ctx.Err()
	}), state, func(discordCacheMutation) {}, func(discordMutationMeta) bool { return ready.Load() }, 1, 10, time.Second)
	ready.Store(true)
	scheduler.Enqueue(active)
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("active request did not start")
	}
	scheduler.CancelGuild(active.guildID)
	state.cancelMemberSync(active.guildID, uuid.Nil)
	select {
	case <-cancelled:
	case <-time.After(time.Second):
		t.Fatal("deactivation did not cancel in-flight request")
	}
	cancel()
	scheduler.Wait()
}

func testDiscordMemberRequest(guildID string, shardID int) (*discordGatewayState, discordMutationMeta, discordMemberRequest) {
	generation := uuid.New()
	meta := discordMutationMeta{ApplicationID: "1", ShardID: shardID, ShardCount: 1, Generation: generation}
	token := uuid.New()
	state := &discordGatewayState{
		appID: "1", shards: map[int]discordShardState{shardID: {Generation: generation, ShardCount: 1}},
		syncs: map[string]discordMemberSync{guildID: {Meta: meta, Token: token}},
	}
	applied := make(chan bool, 1)
	applied <- true
	return state, meta, discordMemberRequest{guildID: guildID, meta: meta, token: token, snapshotApplied: applied}
}

func testGatewayApp() *platform.App {
	return &platform.App{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
}
