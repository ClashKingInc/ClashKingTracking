// Exercises the actual delivery service through its event stream and SQL settings.
// All Discord traffic goes to a loopback provider; no real guild is contacted.
package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"clashking_tracking/internal/platform"
	"clashking_tracking/scripts"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/valkey-io/valkey-go"
)

const guild = "999999999999997510"
const channel = "999999999999997511"
const user = "999999999999997512"
const application = "999999999999997501"

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func run() error {
	if os.Getenv("CLASHKING_WELCOME_E2E") != "1" {
		return fmt.Errorf("local welcome integration flag required")
	}
	dsn := os.Getenv("TEST_DATABASE_URL")
	u, err := url.Parse(dsn)
	if err != nil || u.Hostname() != "127.0.0.1" {
		return fmt.Errorf("loopback database required")
	}
	addr := os.Getenv("TEST_VALKEY_ADDR")
	if !strings.HasPrefix(addr, "127.0.0.1:") {
		return fmt.Errorf("loopback Valkey required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	db, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return err
	}
	defer db.Close()
	var exists bool
	if err = db.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM servers WHERE id=$1)", guild).Scan(&exists); err != nil {
		return err
	}
	if exists {
		return fmt.Errorf("synthetic welcome server already exists")
	}
	if _, err = db.Exec(ctx, "INSERT INTO servers(id,name) VALUES($1,'Welcome integration')", guild); err != nil {
		return err
	}
	defer db.Exec(context.Background(), "DELETE FROM servers WHERE id=$1", guild)
	if _, err = db.Exec(ctx, "INSERT INTO server_custom_embeds(server_id,name,data) VALUES($1,'Welcome',$2)", guild, `{"content":"Saved @everyone text","embeds":[{"title":"Local saved welcome"}]}`); err != nil {
		return err
	}
	if _, err = db.Exec(ctx, "INSERT INTO server_welcome_panels(server_id,embed_name,button_color,welcome_channel_id) VALUES($1,'Welcome','Blue',$2)", guild, channel); err != nil {
		return err
	}
	for i, name := range []string{"Link Button", "Link Help Button", "Refresh Button", "To-Do Button", "Roster Button"} {
		if _, err = db.Exec(ctx, "INSERT INTO server_welcome_panel_buttons(server_id,button_name,position) VALUES($1,$2,$3)", guild, name, i); err != nil {
			return err
		}
	}
	var mu sync.Mutex
	var posts []map[string]any
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method == "GET" && strings.Contains(r.URL.Path, "/channels/") {
			owner := guild
			if strings.HasSuffix(r.URL.Path, "999999999999997599") {
				owner = "999999999999997600"
			}
			json.NewEncoder(w).Encode(map[string]any{"id": channel, "guild_id": owner, "type": 0, "name": "welcome"})
			return
		}
		if r.Method == "POST" && strings.HasSuffix(r.URL.Path, "/messages") {
			var body map[string]any
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				w.WriteHeader(400)
				return
			}
			mu.Lock()
			posts = append(posts, body)
			mu.Unlock()
			body["id"] = "999999999999997520"
			body["channel_id"] = channel
			json.NewEncoder(w).Encode(body)
			return
		}
		w.WriteHeader(404)
		fmt.Fprint(w, `{"code":10003,"message":"Unknown channel"}`)
	}))
	defer provider.Close()
	cache, err := valkey.NewClient(valkey.ClientOption{InitAddress: []string{addr}})
	if err != nil {
		return err
	}
	defer cache.Close()
	stream := fmt.Sprintf("welcome-e2e:%d", os.Getpid())
	defer cache.Do(context.Background(), cache.B().Del().Key(stream).Build())
	app := &platform.App{Config: platform.Config{TimescaleURL: dsn, DiscordBotToken: base64.RawStdEncoding.EncodeToString([]byte(application)) + ".local.local", DiscordAPIURL: provider.URL, DiscordGuildAllowlist: map[string]struct{}{guild: {}}, DiscordDeliveryBatchSize: 10, EventStreamName: stream, EventStreamReclaimIdleSeconds: 1}, Valkey: cache, Logger: slog.New(slog.NewTextHandler(os.Stderr, nil)), Stats: platform.NewTracker(), Availability: platform.NewAvailabilityGate(nil)}
	done := make(chan error, 1)
	go func() { done <- scripts.NewDiscordDeliveryDomain().Run(ctx, app) }()
	defer func() { cancel(); <-done }()
	// Wait until each published event has been delivered and acknowledged. This
	// proves negative cases without relying on a fixed sleep or just no HTTP call.
	total := int64(0)
	publish := func(appID string, at time.Time) error {
		if err := app.PublishEvent(ctx, platform.Event{Topic: "discord_member_join", Timestamp: at, Value: map[string]any{"guild_id": guild, "user_id": user, "application_id": appID}}); err != nil {
			return err
		}
		total++
		ticker := time.NewTicker(20 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return fmt.Errorf("delivery acknowledgement timed out")
			case <-ticker.C:
				groups, err := cache.Do(ctx, cache.B().Arbitrary("XINFO", "GROUPS", stream).Build()).ToArray()
				if err != nil {
					continue
				}
				for _, g := range groups {
					values, err := g.AsMap()
					if err != nil {
						continue
					}
					readValue, pendingValue := values["entries-read"], values["pending"]
					read, _ := readValue.AsInt64()
					pending, _ := pendingValue.AsInt64()
					if read >= total && pending == 0 {
						return nil
					}
				}
			}
		}
	}
	count := func() int { mu.Lock(); defer mu.Unlock(); return len(posts) }
	if err = publish(application, time.Now()); err != nil {
		return err
	}
	if count() != 1 {
		return fmt.Errorf("expected one saved welcome, got %d", count())
	}
	mu.Lock()
	raw, _ := json.Marshal(posts[0])
	mu.Unlock()
	var payload struct {
		Content string `json:"content"`
		Embeds  []struct {
			Title string `json:"title"`
		} `json:"embeds"`
		Components []struct {
			Components []struct {
				CustomID string `json:"custom_id"`
				Style    int    `json:"style"`
			} `json:"components"`
		} `json:"components"`
		Allowed struct {
			Parse []string `json:"parse"`
			Users []string `json:"users"`
		} `json:"allowed_mentions"`
	}
	json.Unmarshal(raw, &payload)
	if !strings.Contains(payload.Content, "<@"+user+">") || len(payload.Embeds) != 1 || payload.Embeds[0].Title != "Local saved welcome" || len(payload.Allowed.Parse) != 0 || len(payload.Allowed.Users) != 1 || payload.Allowed.Users[0] != user {
		return fmt.Errorf("saved content or allowed mention contract failed: %s", raw)
	}
	if len(payload.Components) != 1 || len(payload.Components[0].Components) != 5 {
		return fmt.Errorf("welcome controls missing")
	}
	for _, b := range payload.Components[0].Components {
		if !strings.HasPrefix(b.CustomID, "ck:quick:"+guild+":") || b.Style != 1 {
			return fmt.Errorf("welcome control identity/color mismatch")
		}
	}
	if err = publish("999999999999997599", time.Now()); err != nil {
		return err
	}
	if count() != 1 {
		return fmt.Errorf("foreign application delivered")
	}
	if err = publish(application, time.Now().Add(-6*time.Minute)); err != nil {
		return err
	}
	if count() != 1 {
		return fmt.Errorf("expired join delivered")
	}
	if _, err = db.Exec(ctx, "UPDATE server_welcome_panels SET welcome_channel_id=NULL WHERE server_id=$1", guild); err != nil {
		return err
	}
	if err = publish(application, time.Now()); err != nil {
		return err
	}
	if count() != 1 {
		return fmt.Errorf("disabled panel delivered")
	}
	if _, err = db.Exec(ctx, "UPDATE server_welcome_panels SET welcome_channel_id='999999999999997599' WHERE server_id=$1", guild); err != nil {
		return err
	}
	if err = publish(application, time.Now()); err != nil {
		return err
	}
	if count() != 1 {
		return fmt.Errorf("foreign channel delivered")
	}
	if _, err = db.Exec(ctx, "UPDATE server_welcome_panels SET welcome_channel_id=$2,embed_name=NULL,button_color='Grey' WHERE server_id=$1", guild, channel); err != nil {
		return err
	}
	if err = publish(application, time.Now()); err != nil {
		return err
	}
	if count() != 2 {
		return fmt.Errorf("default welcome did not deliver")
	}
	fmt.Println("PASS welcome event stream -> actual delivery domain -> SQL settings -> loopback Discord: saved/default panels, five action IDs, color, constrained mention, application isolation, disabled setting, stale event and foreign channel; no external Discord writes")
	return nil
}
