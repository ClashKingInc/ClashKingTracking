package scripts

import (
	"context"
	"errors"
	"sync"
	"time"

	"clashking_tracking/internal/platform"

	"github.com/disgoorg/disgo/discord"
	"github.com/disgoorg/snowflake/v2"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

type discordMemberRequestPriority uint8

const (
	discordMemberRequestBackground discordMemberRequestPriority = iota
	discordMemberRequestImmediate
)

type discordMemberRequest struct {
	guildID         string
	meta            discordMutationMeta
	token           uuid.UUID
	snapshotApplied <-chan bool
	priority        discordMemberRequestPriority
}

type discordMemberRequesterFunc func(context.Context, snowflake.ID) ([]discord.Member, error)

func (f discordMemberRequesterFunc) RequestAllMembers(ctx context.Context, guildID snowflake.ID) ([]discord.Member, error) {
	return f(ctx, guildID)
}

type discordMemberRequestEntry struct {
	request discordMemberRequest
	ctx     context.Context
	cancel  context.CancelFunc
}

type discordMemberShardQueue struct {
	immediate  []string
	background []string
	pending    int
	wake       chan struct{}
}

// discordMemberRequestScheduler bounds Discord member chunks independently per
// shard. Guild IDs remain present in entries while queued or in flight, which
// deduplicates startup, reconciliation, and reactivation requests.
type discordMemberRequestScheduler struct {
	ctx         context.Context
	app         *platform.App
	requester   discordMemberRequester
	state       *discordGatewayState
	enqueue     func(discordCacheMutation)
	ready       func(discordMutationMeta) bool
	timeout     time.Duration
	concurrency int
	queueSize   int

	mu      sync.Mutex
	shards  map[int]*discordMemberShardQueue
	entries map[string]*discordMemberRequestEntry
	wg      sync.WaitGroup
}

func newDiscordMemberRequestScheduler(
	ctx context.Context,
	app *platform.App,
	requester discordMemberRequester,
	state *discordGatewayState,
	enqueue func(discordCacheMutation),
	ready func(discordMutationMeta) bool,
	concurrency int,
	queueSize int,
	timeout time.Duration,
) *discordMemberRequestScheduler {
	return &discordMemberRequestScheduler{
		ctx: ctx, app: app, requester: requester, state: state, enqueue: enqueue, ready: ready,
		concurrency: max(1, concurrency), queueSize: max(1, queueSize), timeout: timeout,
		shards: make(map[int]*discordMemberShardQueue), entries: make(map[string]*discordMemberRequestEntry),
	}
}

func (s *discordMemberRequestScheduler) Enqueue(request discordMemberRequest) bool {
	s.mu.Lock()
	if existing, ok := s.entries[request.guildID]; ok {
		if request.token != existing.request.token || request.meta.Generation != existing.request.meta.Generation {
			if existing.cancel != nil {
				existing.cancel()
			} else {
				s.removeQueuedLocked(existing.request.meta.ShardID, request.guildID)
			}
			delete(s.entries, request.guildID)
		} else {
			// A current queued/in-flight request already covers this guild. A direct
			// reactivation signal can promote queued reconciliation work.
			if request.priority > existing.request.priority && existing.cancel == nil {
				existing.request.priority = request.priority
				s.promoteLocked(request.meta.ShardID, request.guildID)
			}
			s.mu.Unlock()
			return false
		}
	}
	queue := s.shards[request.meta.ShardID]
	if queue == nil {
		queue = &discordMemberShardQueue{wake: make(chan struct{}, 1)}
		s.shards[request.meta.ShardID] = queue
		for range s.concurrency {
			s.wg.Add(1)
			go s.runShardWorker(request.meta.ShardID, queue)
		}
	}
	if queue.pending >= s.queueSize {
		s.mu.Unlock()
		s.state.cancelMemberSync(request.guildID, request.token)
		s.enqueueMemberSyncCleanup(request)
		return false
	}
	s.entries[request.guildID] = &discordMemberRequestEntry{request: request}
	if request.priority == discordMemberRequestImmediate {
		queue.immediate = append(queue.immediate, request.guildID)
	} else {
		queue.background = append(queue.background, request.guildID)
	}
	queue.pending++
	s.mu.Unlock()
	signalDiscordMemberQueue(queue)
	return true
}

func (s *discordMemberRequestScheduler) removeQueuedLocked(shardID int, guildID string) {
	queue := s.shards[shardID]
	if queue == nil {
		return
	}
	remove := func(items []string) ([]string, bool) {
		for index, candidate := range items {
			if candidate == guildID {
				return append(items[:index], items[index+1:]...), true
			}
		}
		return items, false
	}
	var removed bool
	queue.immediate, removed = remove(queue.immediate)
	if !removed {
		queue.background, removed = remove(queue.background)
	}
	if removed {
		queue.pending--
	}
}

func (s *discordMemberRequestScheduler) promoteLocked(shardID int, guildID string) {
	queue := s.shards[shardID]
	if queue == nil {
		return
	}
	for index, candidate := range queue.background {
		if candidate != guildID {
			continue
		}
		queue.background = append(queue.background[:index], queue.background[index+1:]...)
		queue.immediate = append(queue.immediate, guildID)
		signalDiscordMemberQueue(queue)
		return
	}
}

func signalDiscordMemberQueue(queue *discordMemberShardQueue) {
	select {
	case queue.wake <- struct{}{}:
	default:
	}
}

func (s *discordMemberRequestScheduler) CancelGuild(guildID string) {
	s.mu.Lock()
	entry := s.entries[guildID]
	if entry != nil {
		delete(s.entries, guildID)
		if entry.cancel != nil {
			entry.cancel()
		} else {
			s.removeQueuedLocked(entry.request.meta.ShardID, guildID)
		}
	}
	s.mu.Unlock()
}

func (s *discordMemberRequestScheduler) Wait() { s.wg.Wait() }

func (s *discordMemberRequestScheduler) runShardWorker(shardID int, queue *discordMemberShardQueue) {
	defer s.wg.Done()
	for {
		select {
		case <-s.ctx.Done():
			return
		default:
		}
		request, ok := s.pop(shardID, queue)
		if !ok {
			select {
			case <-s.ctx.Done():
				return
			case <-queue.wake:
				continue
			}
		}
		s.run(request)
	}
}

func (s *discordMemberRequestScheduler) pop(shardID int, queue *discordMemberShardQueue) (discordMemberRequest, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for {
		var guildID string
		if len(queue.immediate) > 0 {
			guildID, queue.immediate = queue.immediate[0], queue.immediate[1:]
		} else if len(queue.background) > 0 {
			guildID, queue.background = queue.background[0], queue.background[1:]
		} else {
			return discordMemberRequest{}, false
		}
		queue.pending--
		entry := s.entries[guildID]
		if entry == nil || entry.request.meta.ShardID != shardID {
			continue
		}
		requestCtx, cancel := context.WithCancel(s.ctx)
		entry.ctx = requestCtx
		entry.cancel = cancel
		return entry.request, true
	}
}

func (s *discordMemberRequestScheduler) run(request discordMemberRequest) {
	defer s.finish(request.guildID, request.token)
	ctx := s.entryContext(request.guildID, request.token)
	if ctx == nil {
		return
	}
	wait := time.NewTicker(100 * time.Millisecond)
	defer wait.Stop()
	snapshotReady := false
	for !snapshotReady {
		select {
		case active := <-request.snapshotApplied:
			if !active {
				s.state.cancelMemberSync(request.guildID, request.token)
				return
			}
			snapshotReady = true
		case <-wait.C:
			if !s.state.memberSyncCurrent(request.guildID, request.meta, request.token) {
				return
			}
		case <-ctx.Done():
			s.state.cancelMemberSync(request.guildID, request.token)
			return
		}
	}

	for !s.ready(request.meta) {
		if !s.state.memberSyncCurrent(request.guildID, request.meta, request.token) {
			return
		}
		select {
		case <-ctx.Done():
			s.state.cancelMemberSync(request.guildID, request.token)
			return
		case <-wait.C:
		}
	}
	if !s.state.memberSyncCurrent(request.guildID, request.meta, request.token) {
		return
	}

	id, err := snowflake.Parse(request.guildID)
	var members []discord.Member
	if err == nil {
		requestCtx, cancel := context.WithTimeout(ctx, s.timeout)
		members, err = s.requester.RequestAllMembers(requestCtx, id)
		cancel()
	}
	if err == nil {
		s.state.completeMemberSync(request.guildID, request.meta, request.token, s.enqueue, members)
		return
	}
	s.state.cancelMemberSync(request.guildID, request.token)
	s.enqueueMemberSyncCleanup(request)
	if !errors.Is(err, context.Canceled) && s.app.Logger != nil {
		s.app.Logger.Error("Discord member chunk failed", "guild_id", request.guildID, "err", err)
	}
}

func (s *discordMemberRequestScheduler) enqueueMemberSyncCleanup(request discordMemberRequest) {
	s.enqueue(discordCacheMutation{Meta: request.meta, Apply: func(ctx context.Context, pool *pgxpool.Pool) error {
		_, clearErr := pool.Exec(ctx, `UPDATE discord_cache.guilds SET members_sync_token = NULL, members_complete = false, updated_at = now() WHERE id = $1 AND application_id = $2 AND shard_id = $3 AND generation = $4 AND members_sync_token = $5`, request.guildID, request.meta.ApplicationID, request.meta.ShardID, request.meta.Generation, request.token)
		return clearErr
	}})
}

func (s *discordMemberRequestScheduler) entryContext(guildID string, token uuid.UUID) context.Context {
	s.mu.Lock()
	defer s.mu.Unlock()
	entry := s.entries[guildID]
	if entry == nil || entry.ctx == nil || entry.request.token != token {
		return nil
	}
	return entry.ctx
}

func (s *discordMemberRequestScheduler) finish(guildID string, token uuid.UUID) {
	s.mu.Lock()
	entry := s.entries[guildID]
	if entry != nil && entry.request.token == token {
		delete(s.entries, guildID)
		if entry.cancel != nil {
			entry.cancel()
		}
	}
	s.mu.Unlock()
}
