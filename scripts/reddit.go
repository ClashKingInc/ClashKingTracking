package scripts

import (
	"context"
	"encoding/json"
	"net/http"
	"regexp"
	"strings"
	"time"

	"clashking_tracking/internal/platform"
)

const redditDomainName = "reddit"

// Match plausible clan tags in recruit posts without trying to fully validate them.
var tagPattern = regexp.MustCompile(`[#PYLQGRJCUVOpylqgrjcuvo0289]{5,11}`)

type redditDomain struct {
	seen map[string]struct{}
}

func NewRedditDomain() platform.Domain { return &redditDomain{seen: make(map[string]struct{})} }

func (d *redditDomain) Name() string { return redditDomainName }

func (d *redditDomain) Run(ctx context.Context, app *platform.App) error {
	for {
		start := time.Now()
		err := d.poll(ctx, app)
		app.Stats.RecordProcess(redditDomainName, time.Since(start))
		if err != nil && app.Config.RunOnce {
			return err
		}
		if app.Config.RunOnce {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(2 * time.Minute):
		}
	}
}

func (d *redditDomain) poll(ctx context.Context, app *platform.App) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://www.reddit.com/r/ClashOfClansRecruit/new.json?limit=10", nil)
	if err != nil {
		return err
	}
	req.Header.Set("User-Agent", "clashking-tracking/1.0")
	start := time.Now()
	resp, err := http.DefaultClient.Do(req)
	app.Stats.RecordRequest(redditDomainName, time.Since(start), err)
	if err != nil {
		return nil
	}
	defer resp.Body.Close()

	var payload struct {
		Data struct {
			Children []struct {
				Data struct {
					ID            string `json:"id"`
					Title         string `json:"title"`
					SelfText      string `json:"selftext"`
					Score         int    `json:"score"`
					URL           string `json:"url"`
					Permalink     string `json:"permalink"`
					LinkFlairText string `json:"link_flair_text"`
				} `json:"data"`
			} `json:"children"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return nil
	}
	for _, child := range payload.Data.Children {
		post := child.Data
		if post.LinkFlairText != "Searching" {
			continue
		}
		if _, ok := d.seen[post.ID]; ok {
			continue
		}
		d.seen[post.ID] = struct{}{}
		text := post.Title + " " + post.SelfText
		app.Bus.Publish(platform.Event{
			Topic: "reddit",
			Value: map[string]any{
				"type": "reddit",
				"data": map[string]any{
					"title":         post.Title,
					"selftext":      post.SelfText,
					"score":         post.Score,
					"url":           post.URL,
					"id":            post.ID,
					"comments_link": "https://www.reddit.com" + post.Permalink,
					"tags":          tagPattern.FindAllString(strings.TrimSpace(text), -1),
				},
			},
		})
	}
	return nil
}
