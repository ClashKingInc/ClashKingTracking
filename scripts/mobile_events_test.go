package scripts

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"

	"clashking_tracking/internal/platform"
)

type mobileRoundTripFunc func(*http.Request) (*http.Response, error)

func (fn mobileRoundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return fn(request)
}

func TestMobileSubscriptionsUseOnlyOrdinaryNotificationColumns(t *testing.T) {
	for _, column := range []string{
		"war_start_enabled",
		"score_change_enabled",
		"war_end_enabled",
		"cwl_rank_enabled",
	} {
		if !strings.Contains(mobileSubscriptionsSQL, column) {
			t.Fatalf("subscription query missing %q", column)
		}
	}
	if strings.Contains(mobileSubscriptionsSQL, "live_activity_enabled") {
		t.Fatal("subscription query still reads removed live_activity_enabled column")
	}
}

func TestSubscriptionWantsWarAndCWLEvents(t *testing.T) {
	tests := []struct {
		name  string
		sub   mobileSubscription
		event mobileWarEvent
		want  bool
	}{
		{
			name:  "war start",
			sub:   mobileSubscription{WarStartEnabled: true},
			event: mobileWarEvent{Topic: "war", Value: map[string]any{"type": "new_war"}},
			want:  true,
		},
		{
			name:  "score change",
			sub:   mobileSubscription{ScoreChangeEnabled: true},
			event: mobileWarEvent{Topic: "war", Value: map[string]any{"type": "new_attacks"}},
			want:  true,
		},
		{
			name:  "war end",
			sub:   mobileSubscription{WarEndEnabled: true},
			event: mobileWarEvent{Topic: "war", Value: map[string]any{"type": "war_state"}},
			want:  true,
		},
		{
			name:  "cwl update",
			sub:   mobileSubscription{CWLRankEnabled: true},
			event: mobileWarEvent{Topic: "cwl", Value: map[string]any{"type": "cwl_war_update"}},
			want:  true,
		},
		{
			name:  "disabled preference",
			sub:   mobileSubscription{},
			event: mobileWarEvent{Topic: "war", Value: map[string]any{"type": "new_war"}},
			want:  false,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := subscriptionWantsEvent(test.sub, test.event); got != test.want {
				t.Fatalf("subscriptionWantsEvent() = %v, want %v", got, test.want)
			}
		})
	}
}

func TestAPNSNotificationRemainsAnAlertPush(t *testing.T) {
	var request *http.Request
	var payload map[string]any
	client := &http.Client{Transport: mobileRoundTripFunc(func(got *http.Request) (*http.Response, error) {
		request = got
		raw, err := io.ReadAll(got.Body)
		if err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(raw, &payload); err != nil {
			t.Fatal(err)
		}
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(strings.NewReader("")),
			Header:     make(http.Header),
		}, nil
	})}
	worker := &mobileEventsWorker{
		cfg: platform.Config{
			MobilePushAPNSBearerToken: "bearer-token",
			MobilePushAPNSBundleID:    "com.clashking.app",
		},
		http: client,
	}

	if err := worker.sendAPNSNotification(context.Background(), "sandbox", "device-token", "War started", "A new war started."); err != nil {
		t.Fatal(err)
	}
	if request == nil {
		t.Fatal("ordinary APNS request was not sent")
	}
	if got := request.URL.String(); got != "https://api.sandbox.push.apple.com/3/device/device-token" {
		t.Fatalf("APNS URL = %q", got)
	}
	if got := request.Header.Get("apns-topic"); got != "com.clashking.app" {
		t.Fatalf("apns-topic = %q", got)
	}
	if got := request.Header.Get("apns-push-type"); got != "alert" {
		t.Fatalf("apns-push-type = %q", got)
	}
	aps, ok := payload["aps"].(map[string]any)
	if !ok {
		t.Fatalf("aps payload = %#v", payload["aps"])
	}
	alert, ok := aps["alert"].(map[string]any)
	if !ok || alert["title"] != "War started" || alert["body"] != "A new war started." {
		t.Fatalf("alert payload = %#v", aps["alert"])
	}
	if _, exists := aps["content-state"]; exists {
		t.Fatalf("ordinary APNS payload contains Live Activity content state: %#v", aps)
	}
}
