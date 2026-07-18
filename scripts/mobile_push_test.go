package scripts

import (
	"context"
	"testing"
	"time"

	"clashking_tracking/internal/platform"
	"clashking_tracking/models"
)

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

func TestCampaignClaimPreventsDuplicateAndRetriesFailures(t *testing.T) {
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
	if err != nil || len(claimed) != 1 {
		t.Fatalf("retry claim = %d, err=%v", len(claimed), err)
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
	retries, err := store.DuePushRetries(context.Background(), now)
	if err != nil || len(retries) != 1 {
		t.Fatalf("partial retries = %d, err=%v", len(retries), err)
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
	if err := sendAPNS(t.Context(), app, "token", "production", message); err == nil {
		t.Fatal("unconfigured APNs must not be reported as sent")
	}
}

func TestFCMAccessTokenAllowsExplicitOverride(t *testing.T) {
	app := &platform.App{Config: platform.Config{MobilePushFCMBearerToken: "test-token"}}
	token, err := fcmAccessToken(app)
	if err != nil {
		t.Fatalf("explicit FCM token rejected: %v", err)
	}
	if token != "test-token" {
		t.Fatalf("FCM token = %q, want explicit override", token)
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
