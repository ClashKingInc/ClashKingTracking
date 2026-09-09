//go:build local_integration

package scripts

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"clashking_tracking/internal/platform"

	"github.com/disgoorg/disgo/discord"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	valkey "github.com/valkey-io/valkey-go"
)

const localIntegrationToken = "MTIz.NA.local-integration"

type synchronizedBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (b *synchronizedBuffer) Write(value []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.Write(value)
}

func (b *synchronizedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.String()
}

type integrationProcess struct {
	name string
	cmd  *exec.Cmd
	logs *synchronizedBuffer
	done chan struct{}
	mu   sync.Mutex
	err  error
}

func (p *integrationProcess) exitError() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.err
}

func TestLocalCrossProcessBoundaries(t *testing.T) {
	if os.Getenv("CLASHKING_DISPOSABLE_TIMESCALE") != "1" || os.Getenv("CLASHKING_DISPOSABLE_VALKEY") != "1" {
		t.Fatal("local integration requires disposable canonical Timescale and Valkey fixtures")
	}
	databaseURL := os.Getenv("TEST_DATABASE_URL")
	valkeyAddress := os.Getenv("TEST_VALKEY_ADDR")
	if databaseURL == "" || valkeyAddress == "" {
		t.Fatal("fixture did not provide TEST_DATABASE_URL and TEST_VALKEY_ADDR")
	}

	ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	cache, err := valkey.NewClient(valkey.ClientOption{InitAddress: []string{valkeyAddress}, DisableCache: true})
	if err != nil {
		t.Fatal(err)
	}
	defer cache.Close()

	provider := newFakeDiscordProvider(t)
	defer provider.Close()
	pushProvider := newFakePushProvider(t)
	defer pushProvider.Close()
	clash := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		http.Error(response, "bounded fixture has no Clash reads", http.StatusNotFound)
	}))
	defer clash.Close()

	root := repositoryRoot(t)
	binary := filepath.Join(t.TempDir(), "clashking-tracking")
	build := exec.CommandContext(ctx, "go", "build", "-o", binary, ".")
	build.Dir = root
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build Tracking fixture binary: %v\n%s", err, output)
	}
	runtimeDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(runtimeDir, "config.json"), []byte(localIntegrationConfig), 0o600); err != nil {
		t.Fatal(err)
	}
	mobileRuntimeDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(mobileRuntimeDir, "config.json"), []byte(strings.Replace(localIntegrationConfig, "tracking:events", "tracking:mobile-events", 1)), 0o600); err != nil {
		t.Fatal(err)
	}
	baseEnv := integrationEnvironment(t, databaseURL, valkeyAddress, clash.URL, provider.URL+"/v10", pushProvider)

	reminders := startIntegrationProcess(t, ctx, binary, runtimeDir, baseEnv, "reminders")
	delivery := startIntegrationProcess(t, ctx, binary, runtimeDir, baseEnv, "discord-delivery")
	notifications := startIntegrationProcess(t, ctx, binary, mobileRuntimeDir, baseEnv, "notifications")
	processes := []*integrationProcess{reminders, delivery, notifications}
	defer func() { stopIntegrationProcesses(t, processes) }()

	waitForConsumerGroup(t, ctx, cache, reminders, "reminders")
	waitForConsumerGroup(t, ctx, cache, delivery, "discord-delivery")
	waitForConsumerGroupOnStream(t, ctx, cache, notifications, "tracking:mobile-events", "mobilepush")

	seedReminderSchedule(t, ctx, pool)
	proveDiscordGuildSnapshotUUIDFence(t, ctx, pool)
	proveCWLAttributionShape(t, ctx, pool)
	proveReminderRescheduling(t, ctx, pool)
	proveDiscordDeliveryAndReadiness(t, ctx, pool, cache, provider)
	retry := proveMobilePushRetry(t, ctx, pool, cache, notifications, pushProvider, binary, mobileRuntimeDir, baseEnv)
	processes = append(processes, retry)
}

func proveDiscordGuildSnapshotUUIDFence(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	meta := discordMutationMeta{ApplicationID: "123456789012345678", ShardID: 0, ShardCount: 1, Generation: uuid.New(), Sequence: 1}
	token := uuid.New()
	if _, err := pool.Exec(ctx, `
		INSERT INTO discord_cache.gateway_shards
			(application_id, shard_id, shard_count, generation, healthy, heartbeat_at, last_applied_sequence)
		VALUES ($1, $2, $3, $4, false, now(), $5)
	`, meta.ApplicationID, meta.ShardID, meta.ShardCount, meta.Generation, meta.Sequence); err != nil {
		t.Fatal(err)
	}
	if err := replaceDiscordReadyGuildInventory(ctx, pool, meta, []string{"456", "457"}); err != nil {
		t.Fatalf("store fenced Discord READY inventory: %v", err)
	}
	var healthy, metadataComplete, available bool
	var inventoryGeneration uuid.UUID
	if err := pool.QueryRow(ctx, `
		SELECT shard.healthy, guild.metadata_complete, guild.available, guild.generation
		FROM discord_cache.gateway_shards AS shard
		JOIN discord_cache.guilds AS guild ON guild.application_id = shard.application_id AND guild.shard_id = shard.shard_id
		WHERE shard.application_id = $1 AND shard.shard_id = $2 AND guild.id = '457'
	`, meta.ApplicationID, meta.ShardID).Scan(&healthy, &metadataComplete, &available, &inventoryGeneration); err != nil {
		t.Fatal(err)
	}
	if !healthy || metadataComplete || available || inventoryGeneration != meta.Generation {
		t.Fatalf("READY inventory fence = healthy %v metadata %v available %v generation %s", healthy, metadataComplete, available, inventoryGeneration)
	}
	guild := discord.GatewayGuild{RestGuild: discord.RestGuild{Guild: discord.Guild{ID: 456, Name: "fixture", OwnerID: 789}}}
	if err := replaceDiscordGuildSnapshot(ctx, pool, guild, meta, token, true); err != nil {
		t.Fatalf("store active Discord guild snapshot with UUID sync token: %v", err)
	}
	var storedToken uuid.UUID
	if err := pool.QueryRow(ctx, `SELECT members_sync_token FROM discord_cache.guilds WHERE id = '456'`).Scan(&storedToken); err != nil {
		t.Fatal(err)
	}
	if storedToken != token {
		t.Fatalf("stored member sync token = %s, want %s", storedToken, token)
	}
}

func proveCWLAttributionShape(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	if _, err := pool.Exec(ctx, `
		INSERT INTO cwl_groups (cwl_id, season, cwl_league_id, rounds)
		VALUES ('abcdefghijkl', '2025-01', 48000001, '[{"warTags":["#WAR1","#WAR2"]}]'::jsonb)
	`); err != nil {
		t.Fatal(err)
	}
	var tags []string
	rows, err := pool.Query(ctx, `
		SELECT tag.value
		FROM cwl_groups AS groups
		CROSS JOIN LATERAL jsonb_array_elements(groups.rounds) AS round(value)
		CROSS JOIN LATERAL jsonb_array_elements_text(round.value -> 'warTags') AS tag(value)
		WHERE groups.cwl_id = 'abcdefghijkl'
		ORDER BY tag.value
	`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var tag string
		if err := rows.Scan(&tag); err != nil {
			t.Fatal(err)
		}
		tags = append(tags, tag)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if strings.Join(tags, ",") != "#WAR1,#WAR2" {
		t.Fatalf("CWL war tags from persisted round objects = %v", tags)
	}
}

const localIntegrationConfig = `{
  "stats": {"timescale_flush_seconds": 0},
  "events": {
    "stream": "tracking:events",
    "consumer": "local-integration",
    "retention_seconds": 300,
    "reclaim_idle_seconds": 1
  },
  "reminders": {"requests_per_second": 2},
  "mobile_push": {"scan_seconds": 60},
  "discord_gateway": {"queue_size": 64, "message_create_enabled": false},
  "discord_delivery": {"batch_size": 50}
}`

func repositoryRoot(t *testing.T) string {
	t.Helper()
	directory, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	return filepath.Dir(directory)
}

func integrationEnvironment(t *testing.T, databaseURL, valkeyAddress, clashOrigin, discordURL string, pushProvider *fakePushProvider) []string {
	t.Helper()
	database, err := url.Parse(databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	password, _ := database.User.Password()
	valkeyHost, valkeyPort, err := net.SplitHostPort(valkeyAddress)
	if err != nil {
		t.Fatal(err)
	}
	environment := []string{
		"PATH=" + os.Getenv("PATH"),
		"HOME=" + os.Getenv("HOME"),
		"TMPDIR=" + os.TempDir(),
		"TIMESCALE_HOST=" + database.Hostname(),
		"TIMESCALE_PORT=" + database.Port(),
		"TIMESCALE_USERNAME=" + database.User.Username(),
		"TIMESCALE_PASSWORD=" + password,
		"TIMESCALE_DATABASE=" + strings.TrimPrefix(database.Path, "/"),
		"TIMESCALE_SSLMODE=disable",
		"VALKEY_HOST=" + valkeyHost,
		"VALKEY_PORT=" + valkeyPort,
		"CLASHKING_PROXY_INTERNAL_ORIGIN=" + clashOrigin,
		"DISCORD_BOT_TOKEN=" + localIntegrationToken,
		"CLASHKING_LOCAL_DISCORD_API_URL=" + discordURL,
		"MOBILE_PUSH_FCM_PROJECT_ID=local-project",
		"MOBILE_PUSH_FCM_SERVICE_ACCOUNT_JSON=" + pushProvider.serviceAccountJSON(t),
		"CLASHKING_LOCAL_FCM_API_ORIGIN=" + pushProvider.URL,
		"DATA_ENCRYPTION_KEY=local-integration-encryption",
		"DISCORD_MESSAGE_CREATE_ENABLED=false",
	}
	return environment
}

type fakePushProvider struct {
	*httptest.Server
	mu           sync.Mutex
	tokenCalls   int
	messageCalls int
}

func newFakePushProvider(t *testing.T) *fakePushProvider {
	t.Helper()
	provider := &fakePushProvider{}
	provider.Server = httptest.NewServer(http.HandlerFunc(provider.handle))
	return provider
}

func (p *fakePushProvider) handle(response http.ResponseWriter, request *http.Request) {
	p.mu.Lock()
	defer p.mu.Unlock()
	response.Header().Set("Content-Type", "application/json")
	switch {
	case request.Method == http.MethodPost && request.URL.Path == "/token":
		p.tokenCalls++
		_, _ = response.Write([]byte(`{"access_token":"local-access","token_type":"Bearer","expires_in":3600}`))
	case request.Method == http.MethodPost && request.URL.Path == "/v1/projects/local-project/messages:send":
		p.messageCalls++
		if request.Header.Get("Authorization") != "Bearer local-access" {
			http.Error(response, `{"error":"unauthorized"}`, http.StatusUnauthorized)
			return
		}
		if p.messageCalls == 1 {
			http.Error(response, `{"error":"temporary"}`, http.StatusInternalServerError)
			return
		}
		_, _ = response.Write([]byte(`{"name":"projects/local-project/messages/1"}`))
	default:
		http.NotFound(response, request)
	}
}

func (p *fakePushProvider) serviceAccountJSON(t *testing.T) string {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	privateKey, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(map[string]string{
		"type": "service_account", "project_id": "local-project", "private_key_id": "local-key",
		"private_key":  string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: privateKey})),
		"client_email": "local@local-project.iam.gserviceaccount.com", "client_id": "1",
		"auth_uri": "http://127.0.0.1/unused", "token_uri": p.URL + "/token",
		"auth_provider_x509_cert_url": "http://127.0.0.1/unused", "client_x509_cert_url": "http://127.0.0.1/unused",
	})
	if err != nil {
		t.Fatal(err)
	}
	return string(encoded)
}

func (p *fakePushProvider) calls() (int, int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.tokenCalls, p.messageCalls
}

func startIntegrationProcess(t *testing.T, ctx context.Context, binary, directory string, environment []string, script string) *integrationProcess {
	t.Helper()
	logs := &synchronizedBuffer{}
	command := exec.CommandContext(ctx, binary, "--script", script)
	command.Dir = directory
	command.Env = environment
	command.Stdout = logs
	command.Stderr = logs
	if err := command.Start(); err != nil {
		t.Fatalf("start %s: %v", script, err)
	}
	process := &integrationProcess{name: script, cmd: command, logs: logs, done: make(chan struct{})}
	go func() {
		err := command.Wait()
		process.mu.Lock()
		process.err = err
		process.mu.Unlock()
		close(process.done)
	}()
	return process
}

func stopIntegrationProcesses(t *testing.T, processes []*integrationProcess) {
	t.Helper()
	for _, process := range processes {
		if process.cmd.Process != nil {
			_ = process.cmd.Process.Signal(syscall.SIGTERM)
		}
	}
	for _, process := range processes {
		select {
		case <-process.done:
			err := process.exitError()
			if err != nil {
				t.Logf("%s exited with %v\n%s", process.name, err, process.logs.String())
			}
		case <-time.After(5 * time.Second):
			_ = process.cmd.Process.Kill()
			<-process.done
			t.Errorf("%s did not stop within five seconds\n%s", process.name, process.logs.String())
		}
	}
}

func waitForConsumerGroup(t *testing.T, ctx context.Context, cache valkey.Client, process *integrationProcess, group string) {
	waitForConsumerGroupOnStream(t, ctx, cache, process, "tracking:events", group)
}

func waitForConsumerGroupOnStream(t *testing.T, ctx context.Context, cache valkey.Client, process *integrationProcess, stream, group string) {
	t.Helper()
	waitFor(t, ctx, "Valkey consumer group "+group, func() bool {
		select {
		case <-process.done:
			t.Fatalf("%s exited before creating its consumer group: %v\n%s", process.name, process.exitError(), process.logs.String())
		default:
		}
		groups, err := cache.Do(ctx, cache.B().Arbitrary("XINFO", "GROUPS").Keys(stream).Build()).ToArray()
		if err != nil {
			return false
		}
		for index := range groups {
			fields, fieldsErr := groups[index].ToMap()
			if fieldsErr != nil {
				continue
			}
			name, ok := fields["name"]
			if value, valueErr := name.ToString(); ok && valueErr == nil && value == group {
				return true
			}
		}
		return false
	})
}

func seedReminderSchedule(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	_, err := pool.Exec(ctx, `
		INSERT INTO servers (id, name, last_command_at) VALUES
		('1001', 'one', now()), ('1002', 'two', now()), ('1003', 'three', now()), ('1004', 'four', now())
		ON CONFLICT (id) DO NOTHING
	`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = pool.Exec(ctx, `
		INSERT INTO war_schedule (schedule_key, source_clan_tag, opponent_tag, prep_time, end_time, next_run_at, war_type)
		VALUES ('local-integration-war', '#P0Y', '#2PP', now(), now() + interval '4 hours', now() + interval '1 minute', 'regular')
	`)
	if err != nil {
		t.Fatal(err)
	}
}

func proveReminderRescheduling(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	const reminderID = "00000000-0000-4000-8000-000000000010"
	_, err := pool.Exec(ctx, `
		INSERT INTO reminders (id, server_id, type, clan_tag, webhook_token, minutes_remaining, type_name, channel_id, war_type_names)
		VALUES ($1, '1001', 1, '#P0Y', 'token', 60, 'War', '2001', ARRAY['regular'])
	`, reminderID)
	if err != nil {
		t.Fatal(err)
	}
	publishReminderConfig(t, ctx, pool, "created")
	waitForReminderJobs(t, ctx, pool, map[int]bool{60: true})

	if _, err := pool.Exec(ctx, `UPDATE reminders SET minutes_remaining = 90, updated_at = clock_timestamp() WHERE id = $1`, reminderID); err != nil {
		t.Fatal(err)
	}
	publishReminderConfig(t, ctx, pool, "updated")
	waitForReminderJobs(t, ctx, pool, map[int]bool{90: true})

	if _, err := pool.Exec(ctx, `DELETE FROM reminders WHERE id = $1`, reminderID); err != nil {
		t.Fatal(err)
	}
	publishReminderConfig(t, ctx, pool, "deleted")
	waitForReminderJobs(t, ctx, pool, map[int]bool{})
}

func publishReminderConfig(t *testing.T, ctx context.Context, pool *pgxpool.Pool, action string) {
	t.Helper()
	payload, err := json.Marshal(trackingWakeEvent{Version: 1, Kind: "reminder_config", ClanTag: "#P0Y", ReminderType: "War"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `SELECT pg_notify($1, $2)`, trackingWakeChannel, string(payload)); err != nil {
		t.Fatalf("publish %s reminder wake: %v", action, err)
	}
}

func waitForReminderJobs(t *testing.T, ctx context.Context, pool *pgxpool.Pool, want map[int]bool) {
	t.Helper()
	waitFor(t, ctx, "war reminder job reconciliation", func() bool {
		rows, err := pool.Query(ctx, `SELECT minutes_remaining FROM war_reminder_jobs WHERE schedule_key = 'local-integration-war' ORDER BY minutes_remaining`)
		if err != nil {
			return false
		}
		defer rows.Close()
		got := map[int]bool{}
		for rows.Next() {
			var minutes int
			if rows.Scan(&minutes) != nil {
				return false
			}
			got[minutes] = true
		}
		return mapsEqual(got, want)
	})
}

func mapsEqual(left, right map[int]bool) bool {
	if len(left) != len(right) {
		return false
	}
	for key := range left {
		if !right[key] {
			return false
		}
	}
	return true
}

type fakeDiscordProvider struct {
	*httptest.Server
	mu       sync.Mutex
	calls    map[string][]map[string]any
	raceSeen chan struct{}
	raceOnce sync.Once
	release  chan struct{}
}

func newFakeDiscordProvider(t *testing.T) *fakeDiscordProvider {
	t.Helper()
	provider := &fakeDiscordProvider{calls: map[string][]map[string]any{}, raceSeen: make(chan struct{}), release: make(chan struct{})}
	provider.Server = httptest.NewServer(http.HandlerFunc(provider.handle))
	return provider
}

func (p *fakeDiscordProvider) handle(response http.ResponseWriter, request *http.Request) {
	parts := strings.Split(strings.Trim(request.URL.Path, "/"), "/")
	if request.Method != http.MethodPost || len(parts) != 4 || parts[0] != "v10" || parts[1] != "channels" || parts[3] != "messages" {
		http.NotFound(response, request)
		return
	}
	channelID := parts[2]
	var body map[string]any
	_ = json.NewDecoder(request.Body).Decode(&body)
	p.mu.Lock()
	p.calls[channelID] = append(p.calls[channelID], body)
	call := len(p.calls[channelID])
	p.mu.Unlock()
	switch channelID {
	case "2002":
		response.Header().Set("Content-Type", "application/json")
		response.WriteHeader(http.StatusInternalServerError)
		_, _ = response.Write([]byte(`{"message":"temporary","code":0}`))
	case "2003":
		response.Header().Set("Content-Type", "application/json")
		response.WriteHeader(http.StatusNotFound)
		_, _ = response.Write([]byte(`{"message":"unknown channel","code":10003}`))
	case "2004":
		if call == 1 {
			p.raceOnce.Do(func() { close(p.raceSeen) })
			<-p.release
		}
		response.Header().Set("Content-Type", "application/json")
		response.WriteHeader(http.StatusNotFound)
		_, _ = response.Write([]byte(`{"message":"unknown channel","code":10003}`))
	default:
		response.Header().Set("Content-Type", "application/json")
		_, _ = response.Write([]byte(`{"id":"9001","channel_id":"2001","content":"ok"}`))
	}
}

func (p *fakeDiscordProvider) callCount(channelID string) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.calls[channelID])
}

func (p *fakeDiscordProvider) call(channelID string, index int) map[string]any {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.calls[channelID][index]
}

func proveDiscordDeliveryAndReadiness(t *testing.T, ctx context.Context, pool *pgxpool.Pool, cache valkey.Client, provider *fakeDiscordProvider) {
	t.Helper()
	ids := []string{
		"00000000-0000-4000-8000-000000000101",
		"00000000-0000-4000-8000-000000000102",
		"00000000-0000-4000-8000-000000000103",
		"00000000-0000-4000-8000-000000000104",
	}
	for index, id := range ids {
		_, err := pool.Exec(ctx, `
			INSERT INTO reminders (id, server_id, type, clan_tag, webhook_token, minutes_remaining, type_name, channel_id)
			VALUES ($1, $2, 1, '#P0Y', 'token', 55, 'War', $3)
		`, id, strconv.Itoa(1001+index), strconv.Itoa(2001+index))
		if err != nil {
			t.Fatal(err)
		}
	}
	generation := uuid.New()
	_, err := pool.Exec(ctx, `
		INSERT INTO discord_cache.gateway_shards (application_id, shard_id, shard_count, generation, healthy, heartbeat_at, last_applied_sequence)
		VALUES ('123', 0, 1, $1, true, now(), 1)
	`, generation)
	if err != nil {
		t.Fatal(err)
	}
	_, err = pool.Exec(ctx, `
		INSERT INTO discord_cache.guilds (id, data, application_id, shard_id, generation, available, metadata_complete, members_complete)
		VALUES ('1001', '{}'::jsonb, '123', 0, $1, true, true, true)
	`, generation)
	if err != nil {
		t.Fatal(err)
	}
	for _, statement := range []string{
		`INSERT INTO discord_cache.users (id, data) VALUES ('3001', '{}'::jsonb)`,
		`INSERT INTO discord_cache.members (guild_id, user_id, data) VALUES ('1001', '3001', '{}'::jsonb)`,
		`INSERT INTO player_links (tag, source, user_id) VALUES ('#P0Y', 'local-integration', '3001')`,
	} {
		if _, err := pool.Exec(ctx, statement); err != nil {
			t.Fatal(err)
		}
	}

	publishDeliveryEvent(t, ctx, cache)
	select {
	case <-provider.raceSeen:
	case <-ctx.Done():
		t.Fatal("race destination was not attempted")
	}
	if _, err := pool.Exec(ctx, `UPDATE reminders SET custom_text = 'repaired', updated_at = clock_timestamp() WHERE id = $1`, ids[3]); err != nil {
		t.Fatal(err)
	}
	close(provider.release)
	waitFor(t, ctx, "all Discord destinations", func() bool {
		return provider.callCount("2001") >= 1 && provider.callCount("2002") >= 1 && provider.callCount("2003") >= 1 && provider.callCount("2004") >= 1
	})
	waitForDeliveryPending(t, ctx, cache, 0)
	assertReminderDisabled(t, ctx, pool, ids[0], false, "")
	assertReminderDisabled(t, ctx, pool, ids[1], false, "")
	assertReminderDisabled(t, ctx, pool, ids[2], true, "discord_unknown_channel")
	assertReminderDisabled(t, ctx, pool, ids[3], false, "")
	assertAllowedUsers(t, provider.call("2001", 0), []string{"3001"})

	if _, err := pool.Exec(ctx, `UPDATE discord_cache.gateway_shards SET healthy = false WHERE application_id = '123' AND shard_id = 0`); err != nil {
		t.Fatal(err)
	}
	publishDeliveryEvent(t, ctx, cache)
	waitFor(t, ctx, "second Discord delivery", func() bool { return provider.callCount("2001") >= 2 && provider.callCount("2004") >= 2 })
	waitForDeliveryPending(t, ctx, cache, 0)
	assertAllowedUsers(t, provider.call("2001", 1), nil)
	assertReminderDisabled(t, ctx, pool, ids[3], true, "discord_unknown_channel")
}

func publishDeliveryEvent(t *testing.T, ctx context.Context, cache valkey.Client) {
	t.Helper()
	err := platform.AppendEvent(ctx, cache, platform.Config{EventStreamName: "tracking:events", EventStreamRetentionSeconds: 300}, platform.Event{
		Topic: "reminder", ClanTag: "#P0Y", Timestamp: time.Now().UTC(),
		Value: map[string]any{"type": "war", "minutes_remaining": 55, "members": []any{map[string]any{"tag": "#P0Y"}}},
	})
	if err != nil {
		t.Fatal(err)
	}
}

func proveMobilePushRetry(t *testing.T, ctx context.Context, pool *pgxpool.Pool, cache valkey.Client, first *integrationProcess, provider *fakePushProvider, binary, runtimeDir string, environment []string) *integrationProcess {
	t.Helper()
	const userID = "mobile-user"
	const deviceID = "mobile-device"
	for _, statement := range []string{
		`INSERT INTO auth_users (user_id, provider) VALUES ('mobile-user', 'discord')`,
		`INSERT INTO mobile_notification_accounts (user_id, player_tag, source, active) VALUES ('mobile-user', '#P0Y', 'verified', true)`,
		`INSERT INTO player_timers (player_tag, event_type, event_key, expires_at) VALUES ('#P0Y', 'war', 'local-integration-war', now() + interval '1 hour')`,
	} {
		if _, err := pool.Exec(ctx, statement); err != nil {
			t.Fatal(err)
		}
	}
	ciphertext := encryptIntegrationSecret(t, "local-device-token", "local-integration-encryption")
	if _, err := pool.Exec(ctx, `
		INSERT INTO mobile_push_devices (
			user_id, device_id, platform, provider, environment, token_ciphertext, token_hash,
			enabled, authorization_status, war_state_enabled
		) VALUES ($1, $2, 'ios', 'fcm', 'sandbox', $3, 'local-device-token-hash', true, 'authorized', true)
	`, userID, deviceID, ciphertext); err != nil {
		t.Fatal(err)
	}
	if err := platform.AppendEvent(ctx, cache, platform.Config{EventStreamName: "tracking:mobile-events", EventStreamRetentionSeconds: 300}, platform.Event{
		Topic: "war", ClanTag: "#P0Y", Timestamp: time.Now().UTC(), Value: map[string]any{"type": "new_war"},
	}); err != nil {
		t.Fatal(err)
	}

	select {
	case <-first.done:
		if first.exitError() == nil {
			t.Fatalf("notifications exited successfully after transient FCM failure\n%s", first.logs.String())
		}
	case <-ctx.Done():
		t.Fatalf("notifications did not expose transient FCM failure for supervisor retry: %v\n%s", ctx.Err(), first.logs.String())
	}
	_, messageCalls := provider.calls()
	if messageCalls != 1 {
		t.Fatalf("FCM message calls after transient failure = %d, want 1", messageCalls)
	}
	assertMobileDeliveryCount(t, ctx, pool, 0)
	waitForGroupPendingOnStream(t, ctx, cache, "tracking:mobile-events", "mobilepush", 1)

	retry := startIntegrationProcess(t, ctx, binary, runtimeDir, environment, "notifications")
	retry.name = "notifications-retry"
	waitFor(t, ctx, "mobile FCM retry", func() bool {
		_, calls := provider.calls()
		return calls >= 2
	})
	assertMobileDeliveryCount(t, ctx, pool, 1)
	waitForGroupPendingOnStream(t, ctx, cache, "tracking:mobile-events", "mobilepush", 0)
	return retry
}

func encryptIntegrationSecret(t *testing.T, value, key string) string {
	t.Helper()
	keyHash := sha256.Sum256([]byte(key))
	block, err := aes.NewCipher(keyHash[:])
	if err != nil {
		t.Fatal(err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		t.Fatal(err)
	}
	nonce := make([]byte, gcm.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		t.Fatal(err)
	}
	sealed := gcm.Seal(nonce, nonce, []byte(value), nil)
	return "v1." + base64.RawURLEncoding.EncodeToString(sealed)
}

func assertMobileDeliveryCount(t *testing.T, ctx context.Context, pool *pgxpool.Pool, want int) {
	t.Helper()
	var got int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM mobile_notification_deliveries WHERE user_id = 'mobile-user'`).Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("mobile delivery receipt count = %d, want %d", got, want)
	}
}

func waitForDeliveryPending(t *testing.T, ctx context.Context, cache valkey.Client, want int64) {
	t.Helper()
	waitForGroupPending(t, ctx, cache, "discord-delivery", want)
}

func waitForGroupPending(t *testing.T, ctx context.Context, cache valkey.Client, group string, want int64) {
	waitForGroupPendingOnStream(t, ctx, cache, "tracking:events", group, want)
}

func waitForGroupPendingOnStream(t *testing.T, ctx context.Context, cache valkey.Client, stream, group string, want int64) {
	t.Helper()
	waitFor(t, ctx, group+" acknowledgement", func() bool {
		value, err := cache.Do(ctx, cache.B().Arbitrary("XPENDING").Keys(stream).Args(group).Build()).ToArray()
		if err != nil || len(value) == 0 {
			return false
		}
		count, err := value[0].AsInt64()
		return err == nil && count == want
	})
}

func assertReminderDisabled(t *testing.T, ctx context.Context, pool *pgxpool.Pool, id string, want bool, reason string) {
	t.Helper()
	var disabled bool
	var gotReason *string
	if err := pool.QueryRow(ctx, `SELECT disabled, disabled_reason FROM reminders WHERE id = $1`, id).Scan(&disabled, &gotReason); err != nil {
		t.Fatal(err)
	}
	if disabled != want || reason == "" && gotReason != nil || reason != "" && (gotReason == nil || *gotReason != reason) {
		t.Fatalf("reminder %s disabled state = (%v, %v), want (%v, %q)", id, disabled, gotReason, want, reason)
	}
}

func assertAllowedUsers(t *testing.T, body map[string]any, want []string) {
	t.Helper()
	mentions, _ := body["allowed_mentions"].(map[string]any)
	raw, _ := mentions["users"].([]any)
	got := make([]string, 0, len(raw))
	for _, item := range raw {
		got = append(got, fmt.Sprint(item))
	}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("allowed mention users = %v, want %v; body=%v", got, want, body)
	}
}

func waitFor(t *testing.T, ctx context.Context, description string, condition func() bool) {
	t.Helper()
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		if condition() {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("timed out waiting for %s: %v", description, ctx.Err())
		case <-ticker.C:
		}
	}
}
