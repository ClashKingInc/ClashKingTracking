package scripts

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"clashking_tracking/internal/platform"
	"clashking_tracking/models"

	"golang.org/x/oauth2"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) { return f(request) }

func TestCampaignNotificationPreference(t *testing.T) {
	if got := campaignNotificationPreference(models.NotificationCampaign{Key: "monthly-support"}); got != "monthly_support" {
		t.Fatalf("monthly support preference = %q", got)
	}
	if got := campaignNotificationPreference(models.NotificationCampaign{Key: "game-event-clan-games-2026-08"}); got != "events" {
		t.Fatalf("game event campaign preference = %q, want events", got)
	}
	if got := campaignNotificationPreference(models.NotificationCampaign{Key: "product-news"}); got != "announcements" {
		t.Fatalf("ordinary campaign preference = %q", got)
	}
}

func TestGameEventCampaignsUseClashCalendarStartTimes(t *testing.T) {
	now := time.Date(2026, time.August, 4, 12, 0, 0, 0, time.UTC)
	campaigns := gameEventCampaigns(now)
	if len(campaigns) != 4 {
		t.Fatalf("campaign count = %d, want 4", len(campaigns))
	}
	starts := map[string]time.Time{}
	for _, campaign := range campaigns {
		if campaign.SendAt == nil || !campaign.SendAt.After(now) {
			t.Fatalf("campaign %q has invalid start: %#v", campaign.Key, campaign.SendAt)
		}
		starts[campaign.Title] = campaign.SendAt.UTC()
	}
	if got := starts["Clan Games have started"]; !got.Equal(time.Date(2026, time.August, 22, 8, 0, 0, 0, time.UTC)) {
		t.Fatalf("Clan Games start = %s", got)
	}
	if got := starts["Clan War League has started"]; !got.Equal(time.Date(2026, time.September, 1, 8, 0, 0, 0, time.UTC)) {
		t.Fatalf("CWL start = %s", got)
	}
	if got := starts["Raid Weekend has started"]; !got.Equal(time.Date(2026, time.August, 7, 7, 0, 0, 0, time.UTC)) {
		t.Fatalf("Raid Weekend start = %s", got)
	}
}

func TestStoryReplacementCreatesRestorableRevision(t *testing.T) {
	store := newMemoryMobilePushStore()
	first := "https://example.com/story-v1.html"
	post, err := store.CreatePost(context.Background(), models.AdminPostInput{
		Title: ptr("Story"), Summary: ptr("Summary"), PresentationType: ptr("story"), StoryURL: &first,
	})
	if err != nil {
		t.Fatal(err)
	}
	second := "https://example.com/story-v2.html"
	updated, found, err := store.UpdatePost(context.Background(), post.ID, models.AdminPostInput{StoryURL: &second})
	if err != nil || !found {
		t.Fatalf("replacement failed: found=%v err=%v", found, err)
	}
	if updated.StoryVersion != 2 || len(updated.StoryHistory) != 1 || updated.StoryHistory[0] != first {
		t.Fatalf("unexpected revision state: %#v", updated)
	}
}

func TestPostRevisionUsesEditingActor(t *testing.T) {
	store := newMemoryMobilePushStore()
	post, err := store.CreatePost(context.Background(), models.AdminPostInput{
		Title: ptr("Post"), Summary: ptr("Summary"), CreatedBy: ptr("Alice"),
	})
	if err != nil {
		t.Fatal(err)
	}
	newTitle := "Edited"
	if _, _, err := store.UpdatePost(context.Background(), post.ID, models.AdminPostInput{Title: &newTitle, CreatedBy: ptr("Bob")}); err != nil {
		t.Fatal(err)
	}
	revisions, err := store.ListPostRevisions(context.Background(), post.ID)
	if err != nil || len(revisions) != 1 {
		t.Fatalf("revisions = %d, err=%v", len(revisions), err)
	}
	if revisions[0].CreatedBy != "Bob" {
		t.Fatalf("revision actor = %q, want Bob", revisions[0].CreatedBy)
	}
}

func TestMovingLivePostIntoFutureReschedulesIt(t *testing.T) {
	store := newMemoryMobilePushStore()
	post, err := store.CreatePost(context.Background(), models.AdminPostInput{
		Title: ptr("Post"), Summary: ptr("Summary"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.MarkPublished(context.Background(), post.ID); err != nil {
		t.Fatal(err)
	}
	future := time.Now().UTC().Add(time.Hour)
	updated, found, err := store.UpdatePost(context.Background(), post.ID, models.AdminPostInput{StartsAt: &future})
	if err != nil || !found {
		t.Fatalf("reschedule failed: found=%v err=%v", found, err)
	}
	if updated.Status != "scheduled" {
		t.Fatalf("live post status = %q, want scheduled", updated.Status)
	}
}

func TestEditingDueScheduledPostKeepsItScheduled(t *testing.T) {
	store := newMemoryMobilePushStore()
	past := time.Now().UTC().Add(-time.Minute)
	post, err := store.CreatePost(context.Background(), models.AdminPostInput{
		Title: ptr("Post"), Summary: ptr("Summary"), StartsAt: &past,
	})
	if err != nil {
		t.Fatal(err)
	}
	post.Status = "scheduled"
	store.posts[post.ID] = post

	newTitle := "Updated post"
	updated, found, err := store.UpdatePost(context.Background(), post.ID, models.AdminPostInput{Title: &newTitle})
	if err != nil || !found {
		t.Fatalf("update failed: found=%v err=%v", found, err)
	}
	if updated.Status != "scheduled" {
		t.Fatalf("due post status = %q, want scheduled", updated.Status)
	}
}

func TestClearingScheduledStartReturnsPostToDraft(t *testing.T) {
	store := newMemoryMobilePushStore()
	future := time.Now().UTC().Add(time.Hour)
	post, err := store.CreatePost(context.Background(), models.AdminPostInput{
		Title: ptr("Post"), Summary: ptr("Summary"), StartsAt: &future,
	})
	if err != nil {
		t.Fatal(err)
	}
	updated, found, err := store.UpdatePost(context.Background(), post.ID, models.AdminPostInput{ClearStartsAt: true})
	if err != nil || !found {
		t.Fatalf("clear start failed: found=%v err=%v", found, err)
	}
	if updated.Status != "draft" || updated.StartsAt != nil {
		t.Fatalf("cleared post = %#v, want draft without starts_at", updated)
	}
}

func TestDuePostsSkipExpiredPublicationWindows(t *testing.T) {
	store := newMemoryMobilePushStore()
	past := time.Now().UTC().Add(-time.Hour)
	ended := time.Now().UTC().Add(-time.Minute)
	post, err := store.CreatePost(context.Background(), models.AdminPostInput{
		Title: ptr("Expired"), Summary: ptr("Summary"), StartsAt: &past, EndsAt: &ended,
	})
	if err != nil {
		t.Fatal(err)
	}
	post.Status = "scheduled"
	store.posts[post.ID] = post
	due, err := store.DuePosts(context.Background(), time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	if len(due) != 0 {
		t.Fatalf("expired post was returned as due: %#v", due)
	}
}

func TestCampaignClaimRunsOncePerScheduledOccurrence(t *testing.T) {
	store := newMemoryMobilePushStore()
	now := time.Now().UTC()
	sendAt := now.Add(-time.Minute)
	campaign := models.NotificationCampaign{ID: "campaign", Status: "scheduled", TriggerType: "manual", SendAt: &sendAt}
	store.campaigns[campaign.ID] = campaign

	claimed, err := store.ClaimDueCampaigns(context.Background(), now)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("first claim = %d, err=%v", len(claimed), err)
	}
	claimed, err = store.ClaimDueCampaigns(context.Background(), now.Add(time.Minute))
	if err != nil || len(claimed) != 0 {
		t.Fatalf("duplicate claim = %d, err=%v", len(claimed), err)
	}
	if err := store.RecordCampaignDelivery(context.Background(), campaign, now, 1, 0, 1, "failed"); err != nil {
		t.Fatal(err)
	}
	claimed, err = store.ClaimDueCampaigns(context.Background(), now.Add(6*time.Minute))
	if err != nil || len(claimed) != 0 {
		t.Fatalf("failed campaign was reclaimed = %d, err=%v", len(claimed), err)
	}
}

func TestPartialCampaignDoesNotResendSuccessfulAudience(t *testing.T) {
	store := newMemoryMobilePushStore()
	now := time.Now().UTC()
	sendAt := now.Add(-time.Minute)
	campaign := models.NotificationCampaign{ID: "campaign", Status: "scheduled", TriggerType: "manual", SendAt: &sendAt}
	store.campaigns[campaign.ID] = campaign

	claimed, err := store.ClaimDueCampaigns(context.Background(), now)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("first claim = %d, err=%v", len(claimed), err)
	}
	if err := store.RecordCampaignDelivery(context.Background(), campaign, now, 2, 1, 1, "partial"); err != nil {
		t.Fatal(err)
	}
	claimed, err = store.ClaimDueCampaigns(context.Background(), now.Add(6*time.Minute))
	if err != nil || len(claimed) != 0 {
		t.Fatalf("partial campaign was reclaimed = %d, err=%v", len(claimed), err)
	}
}

func TestPartialPostDeliveryRemainsRetryable(t *testing.T) {
	store := newMemoryMobilePushStore()
	now := time.Now().UTC()
	post := models.AdminPost{ID: "post", Status: "live", AlsoPushOnPublish: true}
	store.posts[post.ID] = post
	store.attempts[post.ID] = []models.AdminPostDeliveryAttempt{{
		PostID: post.ID, AttemptNumber: 1, Status: "partial", AttemptedAt: now.Add(-3 * time.Minute),
	}}
	retries, err := store.ClaimDuePushRetries(context.Background(), now)
	if err != nil || len(retries) != 1 {
		t.Fatalf("partial retries = %d, err=%v", len(retries), err)
	}
	retries, err = store.ClaimDuePushRetries(context.Background(), now.Add(time.Minute))
	if err != nil || len(retries) != 0 {
		t.Fatalf("duplicate partial retry claim = %d, err=%v", len(retries), err)
	}
}

func TestQueuedManualPostDeliveryCompletesClaimedAttempt(t *testing.T) {
	store := newMemoryMobilePushStore()
	now := time.Now().UTC()
	post := models.AdminPost{ID: "post", Status: "live", AlsoPushOnPublish: false}
	store.posts[post.ID] = post
	store.attempts[post.ID] = []models.AdminPostDeliveryAttempt{{
		ID: "attempt", PostID: post.ID, AttemptNumber: 1, Trigger: "manual", Status: "queued", AttemptedAt: now,
	}}

	claimed, err := store.ClaimDuePushRetries(context.Background(), now)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("queued claim = %d, err=%v", len(claimed), err)
	}
	if got := store.attempts[post.ID]; len(got) != 1 || got[0].Status != "processing" {
		t.Fatalf("processing attempts = %#v", got)
	}

	recorded, err := store.RecordDeliveryAttempt(context.Background(), models.AdminPostDeliveryAttempt{
		PostID: post.ID, Trigger: "retry", EligibleCount: 1, SentCount: 1, Status: "sent",
	})
	if err != nil {
		t.Fatal(err)
	}
	if recorded.AttemptNumber != 1 || recorded.Trigger != "manual" {
		t.Fatalf("recorded attempt = %#v", recorded)
	}
	if got := store.attempts[post.ID]; len(got) != 1 || got[0].Status != "sent" {
		t.Fatalf("completed attempts = %#v", got)
	}
}

func ptr[T any](value T) *T { return &value }

func TestMergeAdminPostClearsNullableFields(t *testing.T) {
	value := "old"
	now := time.Now()
	merged := mergeAdminPost(models.AdminPost{
		HeroImageURL: &value,
		TargetRoute:  &value,
		StartsAt:     &now,
		PushTitle:    &value,
	}, models.AdminPostInput{
		ClearHeroImageURL: true,
		ClearTargetRoute:  true,
		ClearStartsAt:     true,
		ClearPushTitle:    true,
	})
	if merged.HeroImageURL != nil || merged.TargetRoute != nil || merged.StartsAt != nil || merged.PushTitle != nil {
		t.Fatalf("nullable fields were not cleared: %#v", merged)
	}
}

func TestUnconfiguredPushProvidersReturnErrors(t *testing.T) {
	app := &platform.App{Config: platform.Config{}}
	message := pushMessage{Title: "Title", Body: "Body"}
	if err := sendFCM(t.Context(), app, "token", message); err == nil {
		t.Fatal("unconfigured FCM must not be reported as sent")
	}
}

func TestFCMServiceAccountJSONIsValidated(t *testing.T) {
	fcmADC.Lock()
	fcmADC.source = nil
	fcmADC.Unlock()

	app := &platform.App{Config: platform.Config{MobilePushFCMServiceAccountJSON: "{"}}
	if _, err := fcmAccessToken(app); err == nil {
		t.Fatal("invalid FCM service-account JSON must be rejected")
	}
}

func TestSendFCMBuildsV2RequestAtTransportBoundary(t *testing.T) {
	fcmADC.Lock()
	previousSource := fcmADC.source
	fcmADC.source = oauth2.StaticTokenSource(&oauth2.Token{AccessToken: "access-token"})
	fcmADC.Unlock()
	previousClient := pushHTTPClient
	t.Cleanup(func() {
		fcmADC.Lock()
		fcmADC.source = previousSource
		fcmADC.Unlock()
		pushHTTPClient = previousClient
	})

	var captured map[string]any
	pushHTTPClient = &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		if request.Method != http.MethodPost {
			t.Fatalf("method = %s, want POST", request.Method)
		}
		if request.URL.String() != "https://fcm.googleapis.com/v1/projects/test-project/messages:send" {
			t.Fatalf("url = %s", request.URL)
		}
		if request.Header.Get("Authorization") != "Bearer access-token" {
			t.Fatalf("authorization = %q", request.Header.Get("Authorization"))
		}
		if request.Header.Get("Content-Type") != "application/json" {
			t.Fatalf("content type = %q", request.Header.Get("Content-Type"))
		}
		if err := json.NewDecoder(request.Body).Decode(&captured); err != nil {
			t.Fatal(err)
		}
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(`{"name":"projects/test/messages/1"}`)), Header: make(http.Header)}, nil
	})}

	app := &platform.App{Config: platform.Config{MobilePushFCMProjectID: "test-project"}}
	err := sendFCM(t.Context(), app, "device-token", pushMessage{
		Title: "War attacks remaining",
		Body:  "45 minutes & 7 attacks left in war!",
		Data:  map[string]string{"type": "war_reminder", "target_tag": "#AAA"},
	})
	if err != nil {
		t.Fatal(err)
	}
	message, ok := captured["message"].(map[string]any)
	if !ok || message["token"] != "device-token" {
		t.Fatalf("message = %#v", captured["message"])
	}
	notification, ok := message["notification"].(map[string]any)
	if !ok || notification["title"] != "War attacks remaining" || notification["body"] != "45 minutes & 7 attacks left in war!" {
		t.Fatalf("notification = %#v", message["notification"])
	}
	data, ok := message["data"].(map[string]any)
	if !ok || data["type"] != "war_reminder" || data["target_tag"] != "#AAA" {
		t.Fatalf("data = %#v", message["data"])
	}
}
