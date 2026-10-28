// Exercise the real clan/player tracking domains against disposable canonical
// Timescale/Valkey and loopback Clash responses. No Discord or public API calls.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"clashking_tracking/internal/platform"
	"clashking_tracking/internal/utils"
	"clashking_tracking/scripts"
	clashy "github.com/clashkinginc/clashy.go"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/valkey-io/valkey-go"
)

func must(err error) {
	if err != nil {
		panic(err)
	}
}
func check(ok bool, message string) {
	if !ok {
		panic(message)
	}
}
func main() {
	check(os.Getenv("CLASHKING_DISPOSABLE_TIMESCALE") == "1" && os.Getenv("CLASHKING_DISPOSABLE_VALKEY") == "1", "Disposable fixtures required")
	dsn := os.Getenv("TEST_DATABASE_URL")
	check(strings.Contains(dsn, "@127.0.0.1:"), "Loopback database required")
	ctx := context.Background()
	db, err := pgxpool.New(ctx, dsn)
	must(err)
	defer db.Close()
	cache, err := valkey.NewClient(valkey.ClientOption{InitAddress: []string{os.Getenv("TEST_VALKEY_ADDR")}})
	must(err)
	defer cache.Close()
	const guild = "999999999999995101"
	const disabled = "999999999999995102"
	const actor = "999999999999995103"
	const hidden = "999999999999995104"
	for _, sql := range []string{
		"INSERT INTO auth_users(user_id,provider) VALUES('" + actor + "','discord'),('" + hidden + "','discord')",
		"INSERT INTO servers(id,name,autoeval_enabled,role_leave_delay_minutes,last_command_at) VALUES('" + guild + "','Enabled',true,10,now()),('" + disabled + "','Disabled',false,10,now())",
		"INSERT INTO basic_clan(tag,name,public_war_log,war_wins,member_count,badge_token,troops_donated,troops_received) VALUES('#PYL','Fixture',true,0,0,'',0,0)",
		"INSERT INTO server_clans(server_id,tag) VALUES('" + guild + "','#PYL'),('" + disabled + "','#PYL')",
		"INSERT INTO player_links(tag,user_id,source,hidden,is_verified) VALUES('#PYLQ','" + actor + "','discord',false,true),('#PYLR','" + hidden + "','discord',true,true)",
	} {
		_, err = db.Exec(ctx, sql)
		must(err)
	}
	_, err = db.Exec(ctx, "INSERT INTO discord_cache.members(guild_id,user_id,data) VALUES($1,$2,'{}')", guild, actor)
	must(err)
	var currentClan atomic.Value
	currentClan.Store("#PYL")
	var stage atomic.Int32
	var hall atomic.Int32
	hall.Store(16)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if strings.HasPrefix(r.URL.Path, "/v1/players/") {
			tag := r.URL.Path[strings.LastIndex(r.URL.Path, "/")+1:]
			player := map[string]any{"tag": tag, "name": "Fixture", "townHallLevel": hall.Load(), "expLevel": 200, "trophies": 5000, "role": "member", "warPreference": "in", "troops": []any{}, "heroes": []any{}, "spells": []any{}, "heroEquipment": []any{}, "achievements": []any{}}
			if clan := currentClan.Load().(string); clan != "" {
				player["clan"] = map[string]any{"tag": clan, "name": "Fixture"}
			}
			must(json.NewEncoder(w).Encode(player))
			return
		}
		if r.URL.Path != "/v1/clans/#PYL" {
			http.Error(w, "Unexpected request", 500)
			return
		}
		members := []any{}
		if stage.Load() != 0 && stage.Load() != 3 {
			role := "member"
			if stage.Load() == 2 {
				role = "admin"
			}
			members = []any{map[string]any{"tag": "#PYLQ", "name": "Fixture", "townHallLevel": 16, "role": role}, map[string]any{"tag": "#PYLR", "name": "Hidden", "townHallLevel": 16, "role": "member"}}
		}
		must(json.NewEncoder(w).Encode(map[string]any{"tag": "#PYL", "name": "Fixture", "members": len(members), "memberList": members, "clanLevel": 10, "warWins": 0, "isWarLogPublic": true, "badgeUrls": map[string]string{}}))
	}))
	defer server.Close()
	cfg := clashy.DefaultClientConfig()
	cfg.BaseURL = server.URL + "/v1"
	cfg.LookupCache = false
	cfg.UpdateCache = false
	client, err := clashy.NewClient(cfg)
	must(err)
	defer client.Close()
	app := &platform.App{Config: platform.Config{TimescaleURL: dsn, ValkeyAddr: os.Getenv("TEST_VALKEY_ADDR"), TrackedClanRequestsPerSecond: 5, TrackedClanTargetRefreshSeconds: 1, TrackedClanSnapshotPrefix: "role-fixture:", TrackedClanCWLStateSnapshot: "role-fixture:cwl", TrackedPlayerRequestsPerSecond: 5, TrackedPlayerTargetRefreshSeconds: 1, TargetPageMultiplier: 1, EventStreamName: "role-fixture-events", EventStreamRetentionSeconds: 3600, CWLSyncSeconds: 3600}, Clash: client, Valkey: cache, Logger: slog.New(slog.NewTextHandler(os.Stderr, nil)), Stats: platform.NewTracker(), Availability: platform.NewAvailabilityGate(nil)}
	wait := func(label string, condition func() bool) {
		deadline := time.Now().Add(20 * time.Second)
		for time.Now().Before(deadline) {
			if condition() {
				return
			}
			time.Sleep(50 * time.Millisecond)
		}
		panic("Timeout: " + label)
	}
	run := func(domain platform.Domain) func() {
		runCtx, cancel := context.WithCancel(ctx)
		done := make(chan error, 1)
		go func() { done <- domain.Run(runCtx, app) }()
		var once sync.Once
		return func() {
			once.Do(func() {
				cancel()
				select {
				case err := <-done:
					check(err == nil || strings.Contains(err.Error(), "context canceled"), fmt.Sprint(err))
				case <-time.After(5 * time.Second):
					panic("Domain did not stop")
				}
			})
		}
	}
	stop := run(scripts.NewTrackedClansDomain())
	defer func() { stop() }()
	wait("initial clan snapshot", func() bool {
		count, err := cache.Do(ctx, cache.B().Exists().Key("role-fixture:clan:#PYL").Build()).ToInt64()
		return err == nil && count == 1
	})
	job := "auto:" + guild + ":" + actor
	revision := func() int64 {
		var value int64
		_ = db.QueryRow(ctx, "SELECT revision FROM role_refresh_jobs WHERE id=$1", job).Scan(&value)
		return value
	}
	stage.Store(1)
	wait("member join queues role refresh", func() bool { return revision() > 0 })
	var others int
	must(db.QueryRow(ctx, "SELECT count(*) FROM role_refresh_jobs WHERE server_id=$1 OR user_id=$2", disabled, hidden).Scan(&others))
	check(others == 0, "Disabled server or hidden link queued")
	old := revision()
	stage.Store(2)
	wait("rank change queues role refresh", func() bool { return revision() > old })
	old = revision()
	stage.Store(3)
	wait("member leave queues role refresh", func() bool { return revision() > old })
	wait("leave snapshot committed", func() bool {
		raw, err := cache.Do(ctx, cache.B().Get().Key("role-fixture:clan:#PYL").Build()).ToString()
		decoded, decodeErr := utils.Decompress([]byte(raw))
		var clan clashy.Clan
		return err == nil && decodeErr == nil && json.Unmarshal(decoded, &clan) == nil && len(clan.Members) == 0
	})
	var grace, due time.Time
	must(db.QueryRow(ctx, "SELECT grace_until,due_at FROM role_refresh_jobs WHERE id=$1", job).Scan(&grace, &due))
	check(grace.Equal(due) && grace.After(time.Now().Add(9*time.Minute)), "Leave grace missing")
	stage.Store(4)
	wait("rejoin cancels leave grace", func() bool {
		var cleared bool
		_ = db.QueryRow(ctx, "SELECT grace_until IS NULL AND due_at<=now() FROM role_refresh_jobs WHERE id=$1", job).Scan(&cleared)
		return cleared
	})

	wait("rejoin snapshot committed", func() bool {
		raw, err := cache.Do(ctx, cache.B().Get().Key("role-fixture:clan:#PYL").Build()).ToString()
		decoded, decodeErr := utils.Decompress([]byte(raw))
		var clan clashy.Clan
		return err == nil && decodeErr == nil && json.Unmarshal(decoded, &clan) == nil && len(clan.Members) == 2
	})
	stop()
	stop = run(scripts.NewTrackedPlayersDomain())
	wait("player snapshot", func() bool {
		count, err := cache.Do(ctx, cache.B().Exists().Key("ps:#PYLQ").Build()).ToInt64()
		return err == nil && count == 1
	})
	old = revision()
	hall.Store(17)
	wait("player change queues refresh", func() bool { return revision() > old })
	_, err = db.Exec(ctx, "UPDATE role_refresh_jobs SET grace_until=now()+interval '10 minutes',due_at=now()+interval '10 minutes' WHERE id=$1", job)
	must(err)
	old = revision()
	currentClan.Store("#PYC")
	hall.Store(18)
	wait("non-family player change queues refresh", func() bool { return revision() > old })
	old = revision()
	currentClan.Store("")
	hall.Store(19)
	wait("clanless player change queues refresh", func() bool { return revision() > old })
	must(db.QueryRow(ctx, "SELECT grace_until,due_at FROM role_refresh_jobs WHERE id=$1", job).Scan(&grace, &due))
	check(grace.Equal(due) && grace.After(time.Now().Add(9*time.Minute)), "Account update bypassed leave grace")
	must(db.QueryRow(ctx, "SELECT count(*) FROM role_refresh_jobs WHERE server_id=$1 OR user_id=$2", disabled, hidden).Scan(&others))
	check(others == 0, "Account update queued disabled server or hidden link")
	stop()
	fmt.Println("PASS actual tracking: join, rank, leave grace, rejoin, player/non-family/clanless changes, disabled-server and hidden-link filters; no Discord writes")
}
