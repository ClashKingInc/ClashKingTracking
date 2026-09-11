package scripts

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/disgoorg/disgo/gateway"
	"github.com/gorilla/websocket"
)

func TestDiscordGatewayOrdinaryTCPLossResumesWithoutIdentify(t *testing.T) {
	harness := newDiscordGatewayHarness(t, discordGatewayTriggerTCPLoss)
	defer harness.Close()

	client := gateway.New("test-token", func(gateway.Gateway, gateway.EventType, int, gateway.EventData) {},
		gateway.WithURL(harness.URL()),
		gateway.WithCompression(gateway.CompressionNone),
		gateway.WithReconnectDelay(func(int) time.Duration { return time.Millisecond }),
		gateway.WithReadyTimeout(500*time.Millisecond),
	)
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	if err := client.Open(ctx); err != nil {
		t.Fatalf("open gateway: %v", err)
	}
	select {
	case <-harness.resumed:
	case <-ctx.Done():
		t.Fatal("gateway did not resume after ordinary TCP loss")
	}
	closeGateway(client)
	identify, resume, connections := harness.Counts()
	if identify != 1 || resume != 1 || connections != 2 {
		t.Fatalf("ordinary TCP loss used identify=%d resume=%d connections=%d, want 1/1/2", identify, resume, connections)
	}
}

func TestDiscordGatewayInvalidatedSessionIdentifiesExactlyOnce(t *testing.T) {
	harness := newDiscordGatewayHarness(t, discordGatewayTriggerInvalidSession)
	defer harness.Close()

	identifyLimiter := &recordingIdentifyLimiter{}
	client := newHarnessGateway(harness, gateway.WithIdentifyRateLimiter(identifyLimiter))
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	if err := client.Open(ctx); err != nil {
		t.Fatalf("open gateway: %v", err)
	}
	select {
	case <-harness.recovered:
	case <-ctx.Done():
		t.Fatal("gateway did not identify after Discord invalidated its session")
	}
	closeGateway(client)
	identify, resume, connections := harness.Counts()
	if identify != 2 || resume != 0 || connections != 2 {
		t.Fatalf("invalidated session used identify=%d resume=%d connections=%d, want 2/0/2", identify, resume, connections)
	}
	if waits, unlocks, maxConcurrent := identifyLimiter.Counts(); waits != 2 || unlocks != 2 || maxConcurrent != 1 {
		t.Fatalf("identify limiter waits=%d unlocks=%d max_concurrent=%d, want 2/2/1", waits, unlocks, maxConcurrent)
	}
}

func TestDiscordGatewayResumableInvalidSessionDoesNotIdentifyAgain(t *testing.T) {
	harness := newDiscordGatewayHarness(t, discordGatewayTriggerResumableInvalidSession)
	defer harness.Close()
	client := newHarnessGateway(harness)
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	if err := client.Open(ctx); err != nil {
		t.Fatalf("open gateway: %v", err)
	}
	select {
	case <-harness.recovered:
	case <-ctx.Done():
		t.Fatal("gateway did not resume the recoverable invalid session")
	}
	closeGateway(client)
	identify, resume, connections := harness.Counts()
	if identify != 1 || resume != 1 || connections != 2 {
		t.Fatalf("resumable invalid session used identify=%d resume=%d connections=%d, want 1/1/2", identify, resume, connections)
	}
}

func TestDiscordGatewayReconnectTriggersHaveOneResumeOwner(t *testing.T) {
	for _, test := range []struct {
		name    string
		trigger discordGatewayTrigger
	}{
		{name: "opcode 7", trigger: discordGatewayTriggerOpcodeReconnect},
		{name: "missed heartbeat", trigger: discordGatewayTriggerMissedHeartbeat},
		{name: "concurrent opcode and TCP close", trigger: discordGatewayTriggerConcurrent},
	} {
		t.Run(test.name, func(t *testing.T) {
			harness := newDiscordGatewayHarness(t, test.trigger)
			defer harness.Close()
			client := newHarnessGateway(harness)
			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
			defer cancel()
			if err := client.Open(ctx); err != nil {
				t.Fatalf("open gateway: %v", err)
			}
			select {
			case <-harness.recovered:
			case <-ctx.Done():
				t.Fatal("gateway did not recover")
			}
			closeGateway(client)
			identify, resume, connections := harness.Counts()
			if identify != 1 || resume != 1 || connections != 2 {
				t.Fatalf("trigger used identify=%d resume=%d connections=%d, want 1/1/2", identify, resume, connections)
			}
		})
	}
}

func TestDiscordGatewayInvalidSequenceAndExpiredSessionIdentifyOnce(t *testing.T) {
	for _, test := range []struct {
		name    string
		trigger discordGatewayTrigger
	}{
		{name: "invalid sequence", trigger: discordGatewayTriggerInvalidSequence},
		{name: "session timeout", trigger: discordGatewayTriggerSessionTimeout},
	} {
		t.Run(test.name, func(t *testing.T) {
			harness := newDiscordGatewayHarness(t, test.trigger)
			defer harness.Close()
			client := newHarnessGateway(harness)
			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
			defer cancel()
			if err := client.Open(ctx); err != nil {
				t.Fatalf("open gateway: %v", err)
			}
			select {
			case <-harness.recovered:
			case <-ctx.Done():
				t.Fatal("gateway did not establish a replacement session")
			}
			closeGateway(client)
			identify, resume, connections := harness.Counts()
			if identify != 2 || resume != 0 || connections != 2 {
				t.Fatalf("invalid resume state used identify=%d resume=%d connections=%d, want 2/0/2", identify, resume, connections)
			}
		})
	}
}

func TestDiscordGatewayRepeatedResumeFailuresRetainTheSession(t *testing.T) {
	harness := newDiscordGatewayHarness(t, discordGatewayTriggerRepeatedResumeFailure)
	defer harness.Close()
	client := newHarnessGateway(harness)
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	if err := client.Open(ctx); err != nil {
		t.Fatalf("open gateway: %v", err)
	}
	select {
	case <-harness.recovered:
	case <-ctx.Done():
		t.Fatal("gateway did not recover after repeated resume failures")
	}
	closeGateway(client)
	identify, resume, connections := harness.Counts()
	if identify != 1 || resume != 3 || connections != 4 {
		t.Fatalf("resume retries used identify=%d resume=%d connections=%d, want 1/3/4", identify, resume, connections)
	}
}

func TestDiscordGatewayTerminalCloseSurfacesWithoutReconnect(t *testing.T) {
	for _, test := range []struct {
		name    string
		trigger discordGatewayTrigger
		code    int
	}{
		{name: "authentication", trigger: discordGatewayTriggerTerminalAuth, code: 4004},
		{name: "invalid shard", trigger: discordGatewayTriggerTerminalShard, code: 4010},
		{name: "invalid API version", trigger: discordGatewayTriggerTerminalVersion, code: 4012},
		{name: "invalid intents", trigger: discordGatewayTriggerTerminalInvalidIntent, code: 4013},
		{name: "disallowed intents", trigger: discordGatewayTriggerTerminalIntent, code: 4014},
	} {
		t.Run(test.name, func(t *testing.T) {
			harness := newDiscordGatewayHarness(t, test.trigger)
			defer harness.Close()
			terminal := make(chan error, 1)
			client := newHarnessGateway(harness, gateway.WithCloseHandler(func(_ gateway.Gateway, err error, reconnect bool) {
				if reconnect {
					terminal <- errors.New("terminal close was incorrectly classified as reconnectable")
					return
				}
				terminal <- err
			}))
			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
			defer cancel()
			if err := client.Open(ctx); err != nil {
				t.Fatalf("open gateway: %v", err)
			}
			select {
			case err := <-terminal:
				var closeErr *websocket.CloseError
				if !errors.As(err, &closeErr) || closeErr.Code != test.code {
					t.Fatalf("terminal error = %v, want close code %d", err, test.code)
				}
			case <-ctx.Done():
				t.Fatal("terminal close did not reach supervision")
			}
			closeGateway(client)
			time.Sleep(10 * time.Millisecond)
			identify, resume, connections := harness.Counts()
			if identify != 1 || resume != 0 || connections != 1 {
				t.Fatalf("terminal close retried identify=%d resume=%d connections=%d", identify, resume, connections)
			}
		})
	}
}

func TestDiscordGatewayStalledResumeIsRecoveredPerShard(t *testing.T) {
	harness := newDiscordGatewayHarness(t, discordGatewayTriggerStalledResume)
	defer harness.Close()
	client := newHarnessGateway(harness, gateway.WithReadyTimeout(30*time.Millisecond))
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	if err := client.Open(ctx); err != nil {
		t.Fatalf("open gateway: %v", err)
	}
	select {
	case <-harness.recovered:
	case <-ctx.Done():
		t.Fatal("gateway did not recover a stalled resume")
	}
	closeGateway(client)
	identify, resume, connections := harness.Counts()
	if identify != 1 || resume != 2 || connections != 3 {
		t.Fatalf("stalled resume used identify=%d resume=%d connections=%d, want 1/2/3", identify, resume, connections)
	}
}

func TestDiscordGatewayRepeatedDialFailuresResumeTheExistingSession(t *testing.T) {
	harness := newDiscordGatewayHarness(t, discordGatewayTriggerDialFailure)
	defer harness.Close()
	client := newHarnessGateway(harness)
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	if err := client.Open(ctx); err != nil {
		t.Fatalf("open gateway: %v", err)
	}
	select {
	case <-harness.recovered:
	case <-ctx.Done():
		t.Fatal("gateway did not recover after repeated dial failures")
	}
	closeGateway(client)
	identify, resume, connections := harness.Counts()
	if identify != 1 || resume != 1 || connections != 2 || harness.Requests() != 4 {
		t.Fatalf("dial retries used identify=%d resume=%d connections=%d requests=%d, want 1/1/2/4", identify, resume, connections, harness.Requests())
	}
}

func newHarnessGateway(harness *discordGatewayHarness, opts ...gateway.ConfigOpt) gateway.Gateway {
	options := []gateway.ConfigOpt{
		gateway.WithURL(harness.URL()),
		gateway.WithCompression(gateway.CompressionNone),
		gateway.WithReconnectDelay(func(int) time.Duration { return time.Millisecond }),
		gateway.WithReadyTimeout(500 * time.Millisecond),
	}
	options = append(options, opts...)
	return gateway.New("test-token", func(gateway.Gateway, gateway.EventType, int, gateway.EventData) {}, options...)
}

func closeGateway(client gateway.Gateway) {
	deadline := time.Now().Add(100 * time.Millisecond)
	for client.Status() != gateway.StatusReady && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	client.Close(ctx)
	cancel()
}

type discordGatewayTrigger int

const (
	discordGatewayTriggerTCPLoss discordGatewayTrigger = iota
	discordGatewayTriggerInvalidSession
	discordGatewayTriggerResumableInvalidSession
	discordGatewayTriggerOpcodeReconnect
	discordGatewayTriggerMissedHeartbeat
	discordGatewayTriggerInvalidSequence
	discordGatewayTriggerSessionTimeout
	discordGatewayTriggerTerminalAuth
	discordGatewayTriggerTerminalShard
	discordGatewayTriggerTerminalVersion
	discordGatewayTriggerTerminalInvalidIntent
	discordGatewayTriggerTerminalIntent
	discordGatewayTriggerConcurrent
	discordGatewayTriggerRepeatedResumeFailure
	discordGatewayTriggerStalledResume
	discordGatewayTriggerDialFailure
)

type discordGatewayHarness struct {
	t           *testing.T
	server      *httptest.Server
	upgrader    websocket.Upgrader
	mu          sync.Mutex
	identify    int
	resume      int
	connections int
	requests    int
	trigger     discordGatewayTrigger
	resumed     chan struct{}
	recovered   chan struct{}
	resumeOnce  sync.Once
	recoverOnce sync.Once
}

func newDiscordGatewayHarness(t *testing.T, trigger discordGatewayTrigger) *discordGatewayHarness {
	h := &discordGatewayHarness{t: t, trigger: trigger, resumed: make(chan struct{}), recovered: make(chan struct{})}
	h.server = httptest.NewServer(http.HandlerFunc(h.handle))
	return h
}

func (h *discordGatewayHarness) URL() string {
	return "ws" + strings.TrimPrefix(h.server.URL, "http")
}

func (h *discordGatewayHarness) Close() { h.server.Close() }

func (h *discordGatewayHarness) Counts() (int, int, int) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.identify, h.resume, h.connections
}

func (h *discordGatewayHarness) Requests() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.requests
}

func (h *discordGatewayHarness) handle(writer http.ResponseWriter, request *http.Request) {
	h.mu.Lock()
	h.requests++
	requestAttempt := h.requests
	h.mu.Unlock()
	if h.trigger == discordGatewayTriggerDialFailure && (requestAttempt == 2 || requestAttempt == 3) {
		http.Error(writer, "unavailable", http.StatusServiceUnavailable)
		return
	}
	conn, err := h.upgrader.Upgrade(writer, request, nil)
	if err != nil {
		return
	}
	defer conn.Close()
	h.mu.Lock()
	h.connections++
	connection := h.connections
	h.mu.Unlock()
	heartbeatInterval := 1_000
	if h.trigger == discordGatewayTriggerMissedHeartbeat && connection == 1 {
		heartbeatInterval = 10
	}
	if err := conn.WriteJSON(map[string]any{"op": 10, "d": map[string]any{"heartbeat_interval": heartbeatInterval}}); err != nil {
		return
	}
	var message struct {
		Op int `json:"op"`
	}
	if err := conn.ReadJSON(&message); err != nil {
		return
	}
	switch message.Op {
	case int(gateway.OpcodeIdentify):
		h.mu.Lock()
		h.identify++
		h.mu.Unlock()
		if err := conn.WriteJSON(map[string]any{
			"op": 0, "t": "READY", "s": 1,
			"d": map[string]any{
				"session_id": "test-session", "resume_gateway_url": h.URL(),
				"user":   map[string]any{"id": "123", "username": "test"},
				"guilds": []any{}, "application": map[string]any{"id": "456"},
			},
		}); err != nil {
			return
		}
		if connection == 1 {
			time.Sleep(10 * time.Millisecond)
			switch h.trigger {
			case discordGatewayTriggerInvalidSession:
				_ = conn.WriteJSON(map[string]any{"op": 9, "d": false})
			case discordGatewayTriggerResumableInvalidSession:
				_ = conn.WriteJSON(map[string]any{"op": 9, "d": true})
			case discordGatewayTriggerOpcodeReconnect:
				_ = conn.WriteJSON(map[string]any{"op": 7, "d": nil})
			case discordGatewayTriggerInvalidSequence:
				_ = conn.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(4007, "invalid sequence"), time.Now().Add(time.Second))
			case discordGatewayTriggerSessionTimeout:
				_ = conn.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(4009, "session timed out"), time.Now().Add(time.Second))
			case discordGatewayTriggerTerminalIntent:
				_ = conn.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(4014, "disallowed intents"), time.Now().Add(time.Second))
			case discordGatewayTriggerTerminalAuth:
				_ = conn.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(4004, "authentication failed"), time.Now().Add(time.Second))
			case discordGatewayTriggerTerminalShard:
				_ = conn.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(4010, "invalid shard"), time.Now().Add(time.Second))
			case discordGatewayTriggerTerminalVersion:
				_ = conn.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(4012, "invalid API version"), time.Now().Add(time.Second))
			case discordGatewayTriggerTerminalInvalidIntent:
				_ = conn.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(4013, "invalid intents"), time.Now().Add(time.Second))
			case discordGatewayTriggerConcurrent:
				_ = conn.WriteJSON(map[string]any{"op": 7, "d": nil})
				_ = conn.UnderlyingConn().Close()
			case discordGatewayTriggerMissedHeartbeat:
				// Keep the socket open but intentionally withhold heartbeat ACKs.
			default:
				_ = conn.UnderlyingConn().Close()
			}
			if h.trigger != discordGatewayTriggerMissedHeartbeat {
				return
			}
		}
		if connection > 1 {
			h.recoverOnce.Do(func() { close(h.recovered) })
		}
	case int(gateway.OpcodeResume):
		h.mu.Lock()
		h.resume++
		resumeAttempt := h.resume
		h.mu.Unlock()
		if h.trigger == discordGatewayTriggerRepeatedResumeFailure && resumeAttempt < 3 {
			_ = conn.UnderlyingConn().Close()
			return
		}
		if h.trigger == discordGatewayTriggerStalledResume && resumeAttempt == 1 {
			for {
				if err := conn.ReadJSON(&message); err != nil {
					return
				}
			}
		}
		if err := conn.WriteJSON(map[string]any{"op": 0, "t": "RESUMED", "s": 2, "d": map[string]any{}}); err != nil {
			return
		}
		h.resumeOnce.Do(func() { close(h.resumed) })
		h.recoverOnce.Do(func() { close(h.recovered) })
	}
	for {
		if err := conn.ReadJSON(&message); err != nil {
			return
		}
		if message.Op == int(gateway.OpcodeHeartbeat) && !(h.trigger == discordGatewayTriggerMissedHeartbeat && connection == 1) {
			if err := conn.WriteJSON(map[string]any{"op": 11}); err != nil {
				return
			}
		}
	}
}

type recordingIdentifyLimiter struct {
	mu            sync.Mutex
	waits         int
	unlocks       int
	concurrent    int
	maxConcurrent int
}

func (*recordingIdentifyLimiter) Close(context.Context) {}

func (l *recordingIdentifyLimiter) Wait(context.Context, int) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.waits++
	l.concurrent++
	if l.concurrent > l.maxConcurrent {
		l.maxConcurrent = l.concurrent
	}
	return nil
}

func (l *recordingIdentifyLimiter) Unlock(int) {
	l.mu.Lock()
	l.unlocks++
	l.concurrent--
	l.mu.Unlock()
}

func (l *recordingIdentifyLimiter) Counts() (int, int, int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.waits, l.unlocks, l.maxConcurrent
}
