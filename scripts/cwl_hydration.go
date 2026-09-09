package scripts

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"time"

	"clashking_tracking/internal/platform"
	clashy "github.com/clashkinginc/clashy.go"
	"github.com/jackc/pgx/v5/pgxpool"
)

const cwlWarQueueKey = "tracking:cwl:war-tags"

type cwlWarJob struct {
	GroupID string `json:"groupId"`
	WarTag  string `json:"warTag"`
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

// No TTL: jobs survive process restarts and are removed only after SQL storage.
// Enqueue precedes saving the snapshot so failed handoffs are retried next sweep.
func enqueueCWLWarTags(ctx context.Context, app *platform.App, id string, tags []string) error {
	if len(tags) == 0 {
		return nil
	}
	if app.Valkey == nil {
		if app.Config.MockDB || app.Config.DryRun {
			return nil
		}
		return errors.New("CWL war handoff requires Valkey")
	}
	args := []string{"NX"}
	for _, tag := range tags {
		raw, _ := json.Marshal(cwlWarJob{id, tag})
		args = append(args, "0", string(raw))
	}
	return app.Valkey.Do(ctx, app.Valkey.B().Arbitrary("ZADD").Keys(cwlWarQueueKey).Args(args...).Build()).Error()
}

func (d *warsDomain) runCWLHydrationLoop(ctx context.Context, app *platform.App, pool *pgxpool.Pool) {
	for ctx.Err() == nil {
		if err := d.hydrateCWLPass(ctx, app, pool); err != nil && ctx.Err() == nil {
			app.Logger.Error("CWL war-tag hydration failed", "err", err)
		}
		if sleepOrDone(ctx, 5*time.Second) != nil {
			return
		}
	}
}

func (d *warsDomain) hydrateCWLPass(ctx context.Context, app *platform.App, pool *pgxpool.Pool) error {
	if app.Valkey == nil {
		return errors.New("CWL war handoff requires Valkey")
	}
	jobs, err := app.Valkey.Do(ctx, app.Valkey.B().Arbitrary("ZRANGEBYSCORE").Keys(cwlWarQueueKey).Args("-inf", strconv.FormatInt(time.Now().Unix(), 10), "LIMIT", "0", "1000").Build()).AsStrSlice()
	if err != nil {
		return err
	}
	return runBounded(ctx, platform.RequestConcurrency(app.Config.WarArchiveRequestsPerSecond), jobs, func(ctx context.Context, raw string) error {
		var job cwlWarJob
		err := json.Unmarshal([]byte(raw), &job)
		if err == nil {
			var size int
			size, err = d.scheduleCWLWars(ctx, app, d.limiter, cwlGroupFromRounds([][]string{{job.WarTag}}), true)
			if err == nil && size > 0 {
				result, updateErr := pool.Exec(ctx, `UPDATE cwl_groups SET war_size=$2 WHERE cwl_id=$1`, job.GroupID, size)
				err = updateErr
				if err == nil && result.RowsAffected() == 0 {
					err = errors.New("CWL group snapshot not committed yet")
				}
			}
		}
		if err != nil {
			app.Logger.Warn("CWL war-tag job deferred", "err", err)
			return app.Valkey.Do(ctx, app.Valkey.B().Arbitrary("ZADD").Keys(cwlWarQueueKey).Args("XX", strconv.FormatInt(time.Now().Add(5*time.Minute).Unix(), 10), raw).Build()).Error()
		}
		return app.Valkey.Do(ctx, app.Valkey.B().Arbitrary("ZREM").Keys(cwlWarQueueKey).Args(raw).Build()).Error()
	})
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
