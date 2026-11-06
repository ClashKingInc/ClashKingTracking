// Exercise the actual gateway domain through a local WebSocket provider and SQL.
package main

import (
	"bytes"
	"clashking_tracking/internal/platform"
	"clashking_tracking/scripts"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/disgoorg/disgo/rest"
	"github.com/gorilla/websocket"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/klauspost/compress/zstd"
	valkey "github.com/valkey-io/valkey-go"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"time"
)

func must(e error) {
	if e != nil {
		panic(e)
	}
}
func check(v bool, s string) {
	if !v {
		panic(s)
	}
}
func main() {
	check(os.Getenv("CLASHKING_DISPOSABLE_TIMESCALE") == "1" && os.Getenv("CLASHKING_DISPOSABLE_VALKEY") == "1", "disposable services required")
	dsn, addr := os.Getenv("TEST_DATABASE_URL"), os.Getenv("TEST_VALKEY_ADDR")
	check(strings.Contains(dsn, "@127.0.0.1:") && strings.HasPrefix(addr, "127.0.0.1:"), "loopback services required")
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Second)
	defer cancel()
	db, e := pgxpool.New(ctx, dsn)
	must(e)
	defer db.Close()
	cache, e := valkey.NewClient(valkey.ClientOption{InitAddress: []string{addr}})
	must(e)
	defer cache.Close()
	const guild = "999999999999993201"
	const appID = "999999999999993202"
	const unknown = "999999999999993203"
	const known = "999999999999993204"
	_, e = db.Exec(ctx, `CREATE TABLE gateway_checkpoint_audit (sequence bigint);
 CREATE FUNCTION audit_gateway_checkpoint() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
 INSERT INTO gateway_checkpoint_audit VALUES (NEW.last_applied_sequence); RETURN NEW; END $$;
 CREATE TRIGGER audit_gateway_checkpoint AFTER UPDATE ON discord_cache.gateway_shards FOR EACH ROW EXECUTE FUNCTION audit_gateway_checkpoint();`)
	must(e)
	_, e = db.Exec(ctx, `CREATE FUNCTION block_fixture_revocation() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
 IF NEW.data->>'permissions'='0' THEN PERFORM pg_advisory_xact_lock(93202); END IF; RETURN NEW; END $$;
 CREATE TRIGGER block_fixture_revocation BEFORE UPDATE ON discord_cache.roles FOR EACH ROW EXECUTE FUNCTION block_fixture_revocation();`)
	must(e)
	blocker, e := db.Acquire(ctx)
	must(e)
	defer blocker.Release()
	_, e = blocker.Exec(ctx, "SELECT pg_advisory_lock(93202)")
	must(e)
	var connections atomic.Int32
	var origin string
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v10/gateway/bot" {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]any{"url": "ws" + strings.TrimPrefix(origin, "http") + "/gateway", "shards": 1, "session_start_limit": map[string]any{"total": 100, "remaining": 100, "reset_after": 60000, "max_concurrency": 1}})
			return
		}
		if r.URL.Path != "/gateway" {
			http.Error(w, "unexpected local route", 404)
			return
		}
		conn, e := (&websocket.Upgrader{}).Upgrade(w, r, nil)
		if e != nil {
			return
		}
		defer conn.Close()
		_ = conn.SetReadDeadline(time.Now().Add(95 * time.Second))
		connections.Add(1)
		var compressed bytes.Buffer
		encoder, e := zstd.NewWriter(&compressed, zstd.WithEncoderConcurrency(1))
		if e != nil {
			return
		}
		defer encoder.Close()
		send := func(value any) error {
			b, e := json.Marshal(value)
			if e != nil {
				return e
			}
			if _, e = encoder.Write(b); e != nil {
				return e
			}
			if e = encoder.Flush(); e != nil {
				return e
			}
			e = conn.WriteMessage(websocket.BinaryMessage, append([]byte(nil), compressed.Bytes()...))
			compressed.Reset()
			return e
		}
		if send(map[string]any{"op": 10, "d": map[string]any{"heartbeat_interval": 1000}}) != nil {
			return
		}
		sequence := 0
		dispatch := func(kind string, value any) error {
			sequence++
			return send(map[string]any{"op": 0, "s": sequence, "t": kind, "d": value})
		}
		for {
			var incoming struct {
				Op int `json:"op"`
			}
			if conn.ReadJSON(&incoming) != nil {
				return
			}
			switch incoming.Op {
			case 1:
				if send(map[string]any{"op": 11, "d": nil}) != nil {
					return
				}
			case 2:
				if dispatch("READY", map[string]any{"v": 10, "user": map[string]any{"id": appID, "username": "fixture", "discriminator": "0", "bot": true}, "guilds": []any{map[string]any{"id": guild, "unavailable": true}}, "session_id": "local-session", "resume_gateway_url": "ws" + strings.TrimPrefix(origin, "http") + "/gateway", "shard": []int{0, 1}, "application": map[string]any{"id": appID, "flags": 0}}) != nil {
					return
				}
				channels := []any{map[string]any{"id": unknown, "type": 20, "name": "unknown-before", "position": 1, "permission_overwrites": []any{}, "future_field": map[string]any{"preserved": true}}, map[string]any{"id": known, "type": 0, "name": "known-before", "position": 2, "permission_overwrites": []any{}}}
				if dispatch("GUILD_CREATE", map[string]any{"id": guild, "name": "Fixture", "owner_id": appID, "unavailable": false, "large": false, "member_count": 0, "roles": []any{map[string]any{"id": guild, "name": "@everyone", "permissions": "32", "color": 0, "position": 0}}, "channels": channels, "threads": []any{}, "members": []any{}, "emojis": []any{}, "features": []any{}, "voice_states": []any{}, "presences": []any{}}) != nil {
					return
				}
				if dispatch("CHANNEL_UPDATE", map[string]any{"id": unknown, "guild_id": guild, "type": 20, "name": "unknown-after", "permission_overwrites": []any{}, "future_field": map[string]any{"preserved": true}}) != nil {
					return
				}
				if dispatch("CHANNEL_UPDATE", map[string]any{"id": known, "guild_id": guild, "type": 0, "name": "known-after", "permission_overwrites": []any{}}) != nil {
					return
				}

				for i := 0; i < 200; i++ {
					if dispatch("CHANNEL_UPDATE", map[string]any{"id": known, "guild_id": guild, "type": 0, "name": fmt.Sprintf("burst-%d", i), "permission_overwrites": []any{}}) != nil {
						return
					}
				}
				if dispatch("GUILD_ROLE_UPDATE", map[string]any{"guild_id": guild, "role": map[string]any{"id": guild, "name": "@everyone", "permissions": "0", "color": 0, "position": 0}}) != nil {
					return
				}
			}
		}
	}))
	defer provider.Close()
	origin = provider.URL
	oldAPI := rest.API
	rest.API = origin + "/"
	defer func() { rest.API = oldAPI }()
	app := &platform.App{Config: platform.Config{TimescaleURL: dsn, ValkeyAddr: addr, DiscordBotToken: base64.RawStdEncoding.EncodeToString([]byte(appID)) + ".local.local", DiscordGatewayQueueSize: 4096, DiscordGatewayMemberChunkConcurrency: 1, EventStreamName: "gateway-channel-fixture"}, Valkey: cache, Logger: slog.New(slog.NewTextHandler(os.Stderr, nil)), Stats: platform.NewTracker(), Availability: platform.NewAvailabilityGate(nil)}
	done := make(chan error, 1)
	go func() { done <- scripts.NewDiscordGatewayDomain().Run(ctx, app) }()
	ready := false
	for !ready && ctx.Err() == nil {
		select {
		case err := <-done:
			panic(fmt.Sprintf("gateway stopped before validation: %v", err))
		default:
		}
		var n int
		e = db.QueryRow(ctx, `SELECT count(*) FROM discord_cache.channels c JOIN discord_cache.guilds g ON g.id=c.guild_id WHERE g.id=$1 AND g.metadata_complete AND g.available AND ((c.id=$2 AND c.data->>'type'='20' AND c.data->>'name'='unknown-after' AND c.data->'future_field'->>'preserved'='true') OR (c.id=$3 AND c.data->>'type'='0' AND c.data->>'name' LIKE 'burst-%'))`, guild, unknown, known).Scan(&n)
		must(e)
		ready = n == 2
		if !ready {
			time.Sleep(30 * time.Millisecond)
		}
	}
	check(ready, "unknown channel or subsequent known event not persisted")
	check(connections.Load() == 1, "gateway reconnected after unknown channel")
	// Hold a real permission write beyond the API freshness window. Heartbeats
	// queued behind that write must not falsely renew access to the old role.
	time.Sleep(46 * time.Second)
	var stale bool
	must(db.QueryRow(ctx, `SELECT heartbeat_at < clock_timestamp()-interval '45 seconds' AND last_applied_sequence < 205 FROM discord_cache.gateway_shards WHERE application_id=$1`, appID).Scan(&stale))
	check(stale, "blocked permission write falsely renewed authorization freshness")
	_, e = blocker.Exec(ctx, "SELECT pg_advisory_unlock(93202)")
	must(e)
	// Observe the actual service checkpoint after a burst and permission revocation.
	// A checkpoint must never expose the new sequence with the old permission.
	var sequence int64
	for sequence < 205 && ctx.Err() == nil {
		var permissions string
		must(db.QueryRow(ctx, `SELECT s.last_applied_sequence, r.data->>'permissions' FROM discord_cache.gateway_shards s JOIN discord_cache.roles r ON r.guild_id=$1 AND r.id=$1 WHERE s.application_id=$2`, guild, appID).Scan(&sequence, &permissions))
		check(sequence < 205 || permissions == "0", "checkpoint passed unapplied permission revocation")
		time.Sleep(20 * time.Millisecond)
	}
	check(sequence >= 205, "burst checkpoint never completed")
	var writes int
	must(db.QueryRow(ctx, `SELECT count(*) FROM gateway_checkpoint_audit`).Scan(&writes))
	check(writes < 25, fmt.Sprintf("checkpoint writes were not batched: %d", writes))
	fmt.Printf("checkpoint burst: 205 events, %d checkpoint row updates\n", writes)
	// With no new dispatches, the ordinary heartbeat must keep the cache fresh.
	time.Sleep(16 * time.Second)
	var fresh bool
	must(db.QueryRow(ctx, `SELECT heartbeat_at > clock_timestamp()-interval '10 seconds' FROM discord_cache.gateway_shards WHERE application_id=$1`, appID).Scan(&fresh))
	check(fresh, "idle shard heartbeat stopped")
	cancel()
	select {
	case <-done:
	case <-time.After(12 * time.Second):
		panic("gateway did not stop")
	}
	fmt.Println("PASS actual gateway: compressed WebSocket READY/GUILD_CREATE; unknown channel 20 and raw fields preserved in SQL; following known/unknown updates; no reconnect; graceful shutdown")
}
