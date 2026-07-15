package scripts

import (
	"context"
	"strings"
	"testing"
	"time"

	"clashking_tracking/internal/platform"
	"clashking_tracking/models"
)

func TestDecodeAdminPostInputTracksExplicitNulls(t *testing.T) {
	input, err := decodeAdminPostInput(strings.NewReader(`{
		"title":"Post",
		"starts_at":null,
		"target_route":null,
		"push_body":null
	}`))
	if err != nil {
		t.Fatal(err)
	}
	if !input.ClearStartsAt || !input.ClearTargetRoute || !input.ClearPushBody {
		t.Fatalf("explicit null fields were not retained: %#v", input)
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

func TestStoryPresentationRequiresHTTPSURL(t *testing.T) {
	httpURL := "http://example.com/story.html"
	if err := validatePostPresentation("story", &httpURL, true, false); err == nil {
		t.Fatal("story URLs must use HTTPS")
	}
	httpsURL := "https://example.com/story.html"
	if err := validatePostPresentation("story", &httpsURL, true, true); err != nil {
		t.Fatalf("valid pinned story rejected: %v", err)
	}
	if err := validatePostPresentation("article", nil, false, true); err == nil {
		t.Fatal("a hidden post cannot remain pinned")
	}
}
