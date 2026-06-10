package scripts

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"time"

	"clashking_tracking/internal/platform"

	"github.com/turnage/graw"
	grawreddit "github.com/turnage/graw/reddit"
)

const (
	redditDomainName = "reddit"
	redditSubreddit  = "ClashOfClansRecruit"
	redditUserAgent  = "Reply Recruit"
)

// Match plausible clan tags in recruit posts without trying to fully validate them.
var tagPattern = regexp.MustCompile(`[#PYLQGRJCUVOpylqgrjcuvo0289]{5,11}`)

type redditDomain struct {
}

func NewRedditDomain() platform.Domain { return &redditDomain{} }

func (d *redditDomain) Name() string { return redditDomainName }

func (d *redditDomain) Run(ctx context.Context, app *platform.App) error {
	if err := validateRedditConfig(app.Config); err != nil {
		return err
	}

	start := time.Now()
	err := d.runStream(ctx, app)
	app.Stats.RecordProcess(redditDomainName, time.Since(start))
	if err != nil {
		app.Stats.SetReady(redditDomainName, false, err.Error())
	}
	return err
}

func validateRedditConfig(cfg platform.Config) error {
	if cfg.RedditPollSeconds <= 0 {
		return errors.New("reddit.poll_seconds must be greater than zero")
	}
	if cfg.RedditClientID == "" || cfg.RedditSecret == "" || cfg.RedditUsername == "" || cfg.RedditPassword == "" {
		return errors.New("REDDIT_CLIENT_ID, REDDIT_CLIENT_SECRET, REDDIT_USERNAME, and REDDIT_PASSWORD are required for reddit")
	}
	return nil
}

func (d *redditDomain) runStream(ctx context.Context, app *platform.App) error {
	bot, err := grawreddit.NewBot(grawreddit.BotConfig{
		Agent: redditUserAgent,
		App: grawreddit.App{
			ID:       app.Config.RedditClientID,
			Secret:   app.Config.RedditSecret,
			Username: app.Config.RedditUsername,
			Password: app.Config.RedditPassword,
		},
		Rate: time.Duration(app.Config.RedditPollSeconds) * time.Second,
	})
	if err != nil {
		return err
	}
	handler := newRedditPostHandler(ctx, app)
	stop, wait, err := graw.Run(handler, bot, graw.Config{Subreddits: []string{redditSubreddit}})
	if err != nil {
		return err
	}
	app.Stats.SetReady(redditDomainName, true, "")

	done := make(chan error, 1)
	go func() {
		done <- wait()
	}()
	select {
	case <-ctx.Done():
		stop()
		if err := <-done; err != nil {
			if app.Logger != nil {
				app.Logger.Error("reddit stream stopped", "err", err)
			}
		}
		return ctx.Err()
	case err := <-done:
		return err
	}
}

type redditPostHandler struct {
	ctx     context.Context
	app     *platform.App
	mu      sync.Mutex
	seen    map[string]struct{}
	publish func(context.Context, platform.Event) error
}

func newRedditPostHandler(ctx context.Context, app *platform.App) *redditPostHandler {
	return &redditPostHandler{
		ctx:     ctx,
		app:     app,
		seen:    make(map[string]struct{}),
		publish: app.PublishEvent,
	}
}

func (h *redditPostHandler) Post(post *grawreddit.Post) error {
	event, ok := redditPostEventFromGRAWPost(post)
	if !ok {
		return nil
	}
	if !h.markSeen(event.ID) {
		return nil
	}
	h.app.Stats.RecordWrite(redditDomainName, 1)
	return h.publish(h.ctx, platform.Event{
		Topic: "reddit",
		Value: map[string]any{"type": "reddit", "data": event.CompactData},
	})
}

func (h *redditPostHandler) markSeen(id string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	if _, ok := h.seen[id]; ok {
		return false
	}
	h.seen[id] = struct{}{}
	return true
}

type redditPostEvent struct {
	ID          string
	Title       string
	SelfText    string
	Score       int
	URL         string
	CommentsURL string
	Tags        []string
	CompactData map[string]any
}

func redditPostEventFromGRAWPost(post *grawreddit.Post) (redditPostEvent, bool) {
	if post == nil || post.ID == "" || post.LinkFlairText != "Searching" {
		return redditPostEvent{}, false
	}
	text := strings.TrimSpace(post.SelfText + " " + post.Title)
	tags := tagPattern.FindAllString(text, -1)
	commentsURL := fmt.Sprintf("https://www.reddit.com/r/%s/comments/%s", redditSubreddit, post.ID)
	data := map[string]any{
		"title":         post.Title,
		"selftext":      post.SelfText,
		"score":         int(post.Score),
		"url":           post.URL,
		"id":            post.ID,
		"comments_link": commentsURL,
		"tags":          tags,
	}
	return redditPostEvent{
		ID:          post.ID,
		Title:       post.Title,
		SelfText:    post.SelfText,
		Score:       int(post.Score),
		URL:         post.URL,
		CommentsURL: commentsURL,
		Tags:        tags,
		CompactData: data,
	}, true
}
