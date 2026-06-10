//go:build script_internal_tests

package scripts

import (
	"context"
	"encoding/json"
	"os"
	"reflect"
	"testing"
	"time"

	"clashking_tracking/internal/platform"

	"github.com/turnage/graw/reddit"
	valkey "github.com/valkey-io/valkey-go"
)

func TestRedditPostEventFromGRAWPostFiltersAndNormalizes(t *testing.T) {
	post, ok := redditPostEventFromGRAWPost(&reddit.Post{
		ID:            "abc",
		Title:         "Need clan #PYLQGR",
		Score:         7,
		URL:           "https://example.test/post",
		LinkFlairText: "Searching",
	})

	if !ok {
		t.Fatal("post should be accepted")
	}
	if post.ID != "abc" || post.CommentsURL != "https://www.reddit.com/r/ClashOfClansRecruit/comments/abc" {
		t.Fatalf("post was not normalized: %+v", post)
	}
	if !reflect.DeepEqual(post.Tags, []string{"#PYLQGR"}) {
		t.Fatalf("tags = %v", post.Tags)
	}

	if _, ok := redditPostEventFromGRAWPost(&reddit.Post{ID: "abc", LinkFlairText: "Recruiting"}); ok {
		t.Fatal("non-searching flair should be skipped")
	}
}

func TestRedditPostHandlerDedupesPostsSeenAfterStartup(t *testing.T) {
	app := &platform.App{
		Config: platform.Config{},
		Stats:  platform.NewTracker(),
	}
	handler := newRedditPostHandler(t.Context(), app)
	var events int
	handler.publish = func(_ context.Context, _ platform.Event) error {
		events++
		return nil
	}

	post := &reddit.Post{
		ID:            "abc",
		Title:         "Need clan #PYLQGR",
		URL:           "https://example.test/post",
		LinkFlairText: "Searching",
	}
	if err := handler.Post(post); err != nil {
		t.Fatal(err)
	}
	if err := handler.Post(post); err != nil {
		t.Fatal(err)
	}
	if events != 1 {
		t.Fatalf("events = %d, want 1", events)
	}
}

func TestRedditPostHandlerPublishesInsertedSearchPost(t *testing.T) {
	app := &platform.App{
		Config: platform.Config{},
		Stats:  platform.NewTracker(),
	}
	handler := newRedditPostHandler(t.Context(), app)
	var event platform.Event
	handler.publish = func(_ context.Context, got platform.Event) error {
		event = got
		return nil
	}

	err := handler.Post(&reddit.Post{
		ID:            "abc",
		Title:         "Need clan #PYLQGR",
		SelfText:      "th15",
		Score:         7,
		URL:           "https://example.test/post",
		LinkFlairText: "Searching",
	})
	if err != nil {
		t.Fatal(err)
	}

	if event.Topic != "reddit" {
		t.Fatalf("event topic = %q", event.Topic)
	}
	raw, err := json.Marshal(event.Value)
	if err != nil {
		t.Fatal(err)
	}
	if !json.Valid(raw) || event.Value["type"] != "reddit" {
		t.Fatalf("event value = %#v", event.Value)
	}
}

func TestRedditPostHandlerPublishesToValkeyWhenConfigured(t *testing.T) {
	addr := os.Getenv("VALKEY_ADDR")
	if addr == "" {
		t.Skip("VALKEY_ADDR is not set")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	client, err := valkey.NewClient(valkey.ClientOption{
		InitAddress:  []string{addr},
		DisableCache: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	stream := "reddit:test:" + time.Now().UTC().Format("20060102150405.000000000")
	app := &platform.App{
		Config: platform.Config{EventStreamName: stream},
		Valkey: client,
		Stats:  platform.NewTracker(),
	}
	handler := newRedditPostHandler(ctx, app)
	if err := handler.Post(&reddit.Post{
		ID:            "valkeyabc",
		Title:         "Need clan #PYLQGR",
		Score:         7,
		URL:           "https://example.test/post",
		LinkFlairText: "Searching",
	}); err != nil {
		t.Fatal(err)
	}

	entries, err := client.Do(ctx, client.B().Xrange().Key(stream).Start("-").End("+").Build()).AsXRange()
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Fatalf("stream entries = %d, want 1", len(entries))
	}
	if entries[0].FieldValues["topic"] != "reddit" {
		t.Fatalf("stream topic = %q", entries[0].FieldValues["topic"])
	}
	if entries[0].FieldValues["value"] == "" {
		t.Fatal("stream value was empty")
	}
	_ = client.Do(ctx, client.B().Del().Key(stream).Build()).Error()
}
