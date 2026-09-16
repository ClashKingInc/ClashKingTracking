package scripts

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"sync"
	"time"

	"clashking_tracking/internal/platform"
	clashy "github.com/clashkinginc/clashy.go"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	cwlWarQueueKey         = "tracking:cwl:war-tags"
	cwlHydrationDomainName = "wars.cwl-hydration"
)

func recordCWLHydrationFailure(app *platform.App, operation string, err error) {
	if err == nil {
		return
	}
	app.Stats.RecordRequest(cwlHydrationDomainName, 0, err)
	if app.Errors != nil {
		app.Errors.Capture(err, map[string]string{"domain": cwlHydrationDomainName, "operation": operation})
	}
}

type cwlWarJob struct {
	GroupID string `json:"groupId"`
	WarTag  string `json:"warTag"`
	// nil identifies jobs written before the setSize protocol was deployed.
	SetSize *bool `json:"setSize,omitempty"`
}

func newCWLWarTags(previous, fetched []string) []string {
	seen := make(map[string]bool, len(previous))
	for _, tag := range previous {
		seen[tag] = true
	}
	var out []string
	for _, tag := range fetched {
		if tag != "" && tag != "#0" && !seen[tag] {
			out = append(out, tag)
			seen[tag] = true
		}
	}
	return out
}

// Enqueue before committing the group snapshot. A repeated handoff is safe.
func enqueueCWLWarTags(ctx context.Context, app *platform.App, id string, tags []string, sizeTag ...string) error {
	if len(tags) == 0 {
		return nil
	}
	if app.Valkey == nil {
		if app.Config.MockDB || app.Config.DryRun {
			return nil
		}
		return errors.New("CWL war handoff requires Valkey")
	}
	chosen := tags[0]
	if len(sizeTag) > 0 {
		chosen = sizeTag[0]
	}
	args := []string{"NX"}
	for _, tag := range tags {
		setSize := tag == chosen
		raw, _ := json.Marshal(cwlWarJob{GroupID: id, WarTag: tag, SetSize: &setSize})
		args = append(args, "0", string(raw))
	}
	return app.Valkey.Do(ctx, app.Valkey.B().Arbitrary("ZADD").Keys(cwlWarQueueKey).Args(args...).Build()).Error()
}

type cwlHydrationWork struct {
	raw   string
	known map[string]int
}

// SQL membership checks are batched, but completion is not: workers immediately
// take another buffered job instead of waiting for the slowest job in a batch.
func (d *warsDomain) runCWLHydrationLoop(ctx context.Context, app *platform.App, pool *pgxpool.Pool) {
	capacity := min(256, max(1, platform.RequestConcurrency(app.Config.WarArchiveRequestsPerSecond)))
	done := make(chan string, capacity)
	inFlight := make(map[string]bool, capacity)
	var pending []cwlHydrationWork
	var workers sync.WaitGroup
	defer workers.Wait()
	for ctx.Err() == nil {
		for {
			select {
			case raw := <-done:
				delete(inFlight, raw)
			default:
				goto drained
			}
		}
	drained:
		if len(inFlight) == capacity {
			select {
			case raw := <-done:
				delete(inFlight, raw)
			case <-ctx.Done():
				return
			}
			continue
		}
		if len(pending) == 0 {
			raws, err := loadDueCWLJobs(ctx, app)
			if err != nil {
				recordCWLHydrationFailure(app, "queue-read", err)
				app.Logger.Error("CWL war-tag queue read failed", "err", err)
				if sleepOrDone(ctx, time.Second) != nil {
					return
				}
				continue
			}
			var available []string
			for _, raw := range raws {
				if !inFlight[raw] {
					available = append(available, raw)
				}
			}
			if len(available) > 0 {
				known, err := d.knownCWLJobs(ctx, available)
				if err != nil {
					recordCWLHydrationFailure(app, "known-war-lookup", err)
					app.Logger.Error("CWL known-war lookup failed", "err", err)
					if sleepOrDone(ctx, time.Second) != nil {
						return
					}
					continue
				}
				for _, raw := range available {
					pending = append(pending, cwlHydrationWork{raw: raw, known: known})
				}
			}
		}
		if len(pending) == 0 {
			select {
			case raw := <-done:
				delete(inFlight, raw)
			case <-ctx.Done():
				return
			case <-time.After(500 * time.Millisecond):
			}
			continue
		}
		for len(pending) > 0 && len(inFlight) < capacity {
			work := pending[0]
			pending = pending[1:]
			inFlight[work.raw] = true
			workers.Add(1)
			go func() {
				defer workers.Done()
				if err := d.processCWLJob(ctx, app, pool, work.raw, work.known); err != nil && ctx.Err() == nil {
					recordCWLHydrationFailure(app, "queue-acknowledgement", err)
					app.Logger.Error("CWL job acknowledgement failed", "err", err)
				} else if err == nil {
					app.Stats.SetReady(cwlHydrationDomainName, true, "")
				}
				done <- work.raw
			}()
		}
	}
}

func loadDueCWLJobs(ctx context.Context, app *platform.App) ([]string, error) {
	if app.Valkey == nil {
		return nil, errors.New("CWL war handoff requires Valkey")
	}
	return app.Valkey.Do(ctx, app.Valkey.B().Arbitrary("ZRANGEBYSCORE").Keys(cwlWarQueueKey).Args("-inf", strconv.FormatInt(time.Now().Unix(), 10), "LIMIT", "0", "1000").Build()).AsStrSlice()
}

func (d *warsDomain) knownCWLJobs(ctx context.Context, raws []string) (map[string]int, error) {
	tags := make([]string, 0, len(raws))
	for _, raw := range raws {
		var job cwlWarJob
		if json.Unmarshal([]byte(raw), &job) == nil {
			tags = append(tags, job.WarTag)
		}
	}
	return d.store.KnownCWLWarTags(ctx, tags)
}

// Bounded helper retained for local integration tests and one-pass callers.
func (d *warsDomain) hydrateCWLPass(ctx context.Context, app *platform.App, pool *pgxpool.Pool) error {
	jobs, err := loadDueCWLJobs(ctx, app)
	if err != nil {
		return err
	}
	known, err := d.knownCWLJobs(ctx, jobs)
	if err != nil {
		return err
	}
	return runBounded(ctx, max(1, min(256, platform.RequestConcurrency(app.Config.WarArchiveRequestsPerSecond))), jobs, func(ctx context.Context, raw string) error {
		return d.processCWLJob(ctx, app, pool, raw, known)
	})
}

func (d *warsDomain) processCWLJob(ctx context.Context, app *platform.App, pool *pgxpool.Pool, raw string, known map[string]int) error {
	var job cwlWarJob
	err := json.Unmarshal([]byte(raw), &job)
	legacyClaim := false
	setSize := job.SetSize != nil && *job.SetSize
	if err == nil && job.SetSize == nil {
		// Old queued jobs have no flag. At most one per group attempts the guarded
		// size update in this process, rather than rewriting it for every old tag.
		_, loaded := d.cwlSizeClaims.LoadOrStore(job.GroupID, struct{}{})
		legacyClaim = !loaded
		setSize = legacyClaim
	}
	if err == nil {
		var size int
		jobKnown := known
		if setSize && known[job.WarTag] == 0 {
			// A pending schedule with no participant timers cannot supply size.
			// Only its designated job needs a fresh payload in that case.
			jobKnown = make(map[string]int)
		}
		size, err = d.scheduleCWLWars(ctx, app, d.limiter, cwlGroupFromRounds([][]string{{job.WarTag}}), true, jobKnown)
		if err == nil && setSize && size <= 0 {
			err = errors.New("designated CWL war has no usable team size")
		}
		if err == nil && setSize && size > 0 {
			err = setCWLGroupSize(ctx, pool, job.GroupID, size)
		}
		if err != nil && setSize && isSkippableWarFetchError(err) {
			// A permanently unavailable designated tag must not strand the size lookup.
			tags, loadErr := d.store.LoadCWLWarTags(ctx, job.GroupID)
			if loadErr == nil {
				for i, tag := range tags {
					if tag == job.WarTag && i+1 < len(tags) {
						next := tags[i+1]
						loadErr = enqueueCWLWarTags(ctx, app, job.GroupID, []string{next}, next)
						if loadErr == nil {
							err = nil
						}
						break
					}
				}
			}
		}
	}
	if err != nil {
		if legacyClaim {
			d.cwlSizeClaims.Delete(job.GroupID)
		}
		recordCWLHydrationFailure(app, "process", err)
		app.Logger.Warn("CWL war-tag job deferred", "err", err)
		return app.Valkey.Do(ctx, app.Valkey.B().Arbitrary("ZADD").Keys(cwlWarQueueKey).Args("XX", strconv.FormatInt(time.Now().Add(5*time.Minute).Unix(), 10), raw).Build()).Error()
	}
	return app.Valkey.Do(ctx, app.Valkey.B().Arbitrary("ZREM").Keys(cwlWarQueueKey).Args(raw).Build()).Error()
}

func setCWLGroupSize(ctx context.Context, pool *pgxpool.Pool, id string, size int) error {
	// Existence is checked in the same statement: a job can precede the group's
	// first commit, but a previously filled size is success, not a retry.
	var exists bool
	err := pool.QueryRow(ctx, `WITH updated AS (
  UPDATE cwl_groups SET war_size=$2 WHERE cwl_id=$1 AND war_size IS NULL
 ) SELECT EXISTS(SELECT 1 FROM cwl_groups WHERE cwl_id=$1)`, id, size).Scan(&exists)
	if err == nil && !exists {
		return errors.New("CWL group snapshot not committed yet")
	}
	return err
}

func cwlGroupFromRounds(rounds [][]string) *clashy.ClanWarLeagueGroup {
	source := &clashy.ClanWarLeagueGroup{}
	for _, tags := range rounds {
		source.Rounds = append(source.Rounds, struct {
			WarTags []string `json:"warTags,omitempty"`
		}{tags})
	}
	return source
}
