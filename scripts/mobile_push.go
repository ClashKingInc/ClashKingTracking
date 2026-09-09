package scripts

import (
	"context"
	"errors"
	"strings"
	"time"

	"clashking_tracking/internal/platform"
	"clashking_tracking/models"

	clashy "github.com/clashkinginc/clashy.go"
)

const mobilePushDomainName = "mobile_push"

// mobilePushDomain publishes scheduled posts, retries push deliveries, runs
// recurring campaigns, and expires live posts without exposing an HTTP API.
type mobilePushDomain struct {
	store mobilePushStore
}

func NewMobilePushDomain() platform.Domain { return &mobilePushDomain{} }

func (d *mobilePushDomain) Name() string { return mobilePushDomainName }

func (d *mobilePushDomain) Run(ctx context.Context, app *platform.App) error {
	if err := validateMobilePushConfig(app.Config); err != nil {
		return err
	}
	store, err := newMobilePushStore(ctx, app)
	if err != nil {
		return err
	}
	defer store.Close()
	d.store = store

	interval := time.Duration(app.Config.MobilePushScanSeconds) * time.Second
	for {
		start := time.Now()
		err := d.runCycle(ctx, app)
		app.Stats.RecordProcess(mobilePushDomainName, time.Since(start))
		if err != nil {
			if app.Config.RunOnce {
				return err
			}
			app.Stats.SetReady(mobilePushDomainName, false, err.Error())
		}
		if app.Config.RunOnce {
			return nil
		}
		if err := sleepOrDone(ctx, interval); err != nil {
			return err
		}
	}
}

func validateMobilePushConfig(cfg platform.Config) error {
	if cfg.MobilePushScanSeconds <= 0 {
		return errors.New("mobile_push.scan_seconds must be greater than zero")
	}
	if !cfg.DryRun && !cfg.MockDB && cfg.TimescaleURL == "" {
		return errors.New("TIMESCALE_* connection variables are required for mobile_push")
	}
	return nil
}

func (d *mobilePushDomain) runCycle(ctx context.Context, app *platform.App) error {
	now := time.Now().UTC()

	due, err := d.store.DuePosts(ctx, now)
	if err != nil {
		return err
	}
	for _, post := range due {
		if _, err := publishAndNotify(ctx, app, d.store, post); err != nil {
			app.Logger.Error("mobile_push: failed to publish due post", "post_id", post.ID, "err", err)
			continue
		}
		app.Stats.RecordWrite(mobilePushDomainName, 1)
	}

	retries, err := d.store.ClaimDuePushRetries(ctx, now)
	if err != nil {
		return err
	}
	for _, post := range retries {
		if _, _, err := deliverPostPush(ctx, app, d.store, post, "retry"); err != nil {
			app.Logger.Warn("mobile_push: automatic retry failed", "post_id", post.ID, "err", err)
		}
	}
	if err := d.store.EnsureGameEventCampaigns(ctx, now); err != nil {
		return err
	}

	campaigns, err := d.store.ClaimDueCampaigns(ctx, now)
	if err != nil {
		return err
	}
	for _, campaign := range campaigns {
		devices, loadErr := d.store.DevicesForPlatforms(ctx, campaign.Platforms, campaign.TargetLocales, campaignNotificationPreference(campaign))
		if loadErr != nil {
			app.Logger.Warn("mobile_push: campaign audience failed", "campaign_id", campaign.ID, "err", loadErr)
			continue
		}
		route := valueOr(campaign.TargetRoute, "")
		sent, skipped := sendLocalizedPush(ctx, app, devices, func(locale string) pushMessage {
			title, body := campaign.Title, campaign.Body
			if translation, ok := campaign.Translations[locale]; ok {
				if translation.Title != "" {
					title = translation.Title
				}
				if translation.Body != "" {
					body = translation.Body
				}
			}
			return pushMessage{Title: title, Body: body, Data: map[string]string{"campaign_id": campaign.ID, "type": "admin_campaign", "route": route}}
		})
		status := "failed"
		if len(devices) == 0 {
			status = "no_audience"
		} else if sent == len(devices) {
			status = "sent"
		} else if sent > 0 {
			status = "partial"
		}
		if err := d.store.RecordCampaignDelivery(ctx, campaign, now, len(devices), sent, skipped, status); err != nil {
			app.Logger.Warn("mobile_push: campaign delivery record failed", "campaign_id", campaign.ID, "err", err)
		}
	}

	expired, err := d.store.DueExpirations(ctx, now)
	if err != nil {
		return err
	}
	for _, post := range expired {
		if err := d.store.MarkExpired(ctx, post.ID); err != nil {
			app.Logger.Error("mobile_push: failed to expire post", "post_id", post.ID, "err", err)
			continue
		}
		app.Stats.RecordWrite(mobilePushDomainName, 1)
	}

	app.Stats.SetReady(mobilePushDomainName, true, "")
	return nil
}

type publishResult struct {
	models.AdminPost
	PushSent    int `json:"push_sent"`
	PushSkipped int `json:"push_skipped"`
}

// publishAndNotify is the worker's single publish-and-maybe-push path.
func publishAndNotify(ctx context.Context, app *platform.App, store mobilePushStore, post models.AdminPost) (publishResult, error) {
	updated, err := store.MarkPublished(ctx, post.ID)
	if err != nil {
		return publishResult{}, err
	}
	result := publishResult{AdminPost: updated}
	if !updated.AlsoPushOnPublish {
		return result, nil
	}

	sent, skipped, err := deliverPostPush(ctx, app, store, updated, "publish")
	if err != nil {
		app.Logger.Warn("mobile_push: push attempt failed", "post_id", updated.ID, "err", err)
	}
	result.PushSent = sent
	result.PushSkipped = skipped
	return result, nil
}

func deliverPostPush(ctx context.Context, app *platform.App, store mobilePushStore, post models.AdminPost, trigger string) (int, int, error) {
	devices, err := store.DevicesForPlatforms(ctx, post.Platforms, nil, "announcements")
	if err != nil {
		_, _ = store.RecordDeliveryAttempt(ctx, models.AdminPostDeliveryAttempt{PostID: post.ID, Trigger: trigger, Status: "failed", ErrorSummary: err.Error()})
		return 0, 0, err
	}
	sent, skipped := sendLocalizedPush(ctx, app, devices, func(locale string) pushMessage {
		title, body := valueOr(post.PushTitle, post.Title), valueOr(post.PushBody, post.Summary)
		if translation, ok := post.Translations[locale]; ok {
			if translation.PushTitle != "" {
				title = translation.PushTitle
			} else if translation.Title != "" {
				title = translation.Title
			}
			if translation.PushBody != "" {
				body = translation.PushBody
			} else if translation.Summary != "" {
				body = translation.Summary
			}
		}
		return pushMessage{Title: title, Body: body, Data: map[string]string{"post_id": post.ID, "type": "admin_post", "route": "/posts/" + post.ID}}
	})
	status := "failed"
	if len(devices) == 0 {
		status = "no_audience"
	} else if sent == len(devices) {
		status = "sent"
	} else if sent > 0 {
		status = "partial"
	}
	_, recordErr := store.RecordDeliveryAttempt(ctx, models.AdminPostDeliveryAttempt{
		PostID: post.ID, Trigger: trigger, EligibleCount: len(devices), SentCount: sent, SkippedCount: skipped, Status: status,
	})
	if status == "sent" || status == "no_audience" {
		_ = store.MarkPushSent(ctx, post.ID)
	}
	return sent, skipped, recordErr
}

func campaignNotificationPreference(campaign models.NotificationCampaign) string {
	if campaign.Key == "monthly-support" {
		return "monthly_support"
	}
	if strings.HasPrefix(campaign.Key, "game-event-") ||
		strings.HasPrefix(campaign.Key, "clan-games-") ||
		strings.HasPrefix(campaign.Key, "cwl-") ||
		strings.HasPrefix(campaign.Key, "raid-weekend-") ||
		strings.HasPrefix(campaign.Key, "season-start-") {
		return "events"
	}
	return "announcements"
}

func gameEventCampaigns(now time.Time) []models.NotificationCampaign {
	now = now.UTC()
	events := []struct {
		slug  string
		title string
		body  string
		start time.Time
	}{
		{slug: "cwl", title: "Clan War League has started", body: "Clan War League is now live in game.", start: nextMonthlyEventStart(now, 1, 8)},
		{slug: "clan-games", title: "Clan Games have started", body: "Clan Games are now live in game.", start: nextMonthlyEventStart(now, 22, 8)},
		{slug: "raid-weekend", title: "Raid Weekend has started", body: "Raid Weekend is now live in game.", start: nextRaidWeekendStart(now)},
		{slug: "season-start", title: "A new season has started", body: "The new Clash of Clans season is now live.", start: clashy.GetSeasonEnd(now).UTC()},
	}
	campaigns := make([]models.NotificationCampaign, 0, len(events))
	for _, event := range events {
		start := event.start
		campaigns = append(campaigns, models.NotificationCampaign{
			Key:       "game-event-" + event.slug + "-" + start.Format("20060102T1504"),
			Title:     event.title,
			Body:      event.body,
			Platforms: []string{"ios", "android"},
			SendAt:    &start,
		})
	}
	return campaigns
}

func nextMonthlyEventStart(now time.Time, day, hour int) time.Time {
	start := time.Date(now.Year(), now.Month(), day, hour, 0, 0, 0, time.UTC)
	if !start.After(now) {
		start = time.Date(now.Year(), now.Month()+1, day, hour, 0, 0, 0, time.UTC)
	}
	return start
}

func nextRaidWeekendStart(now time.Time) time.Time {
	daysUntilFriday := (int(time.Friday) - int(now.Weekday()) + 7) % 7
	start := time.Date(now.Year(), now.Month(), now.Day(), 7, 0, 0, 0, time.UTC).AddDate(0, 0, daysUntilFriday)
	if !start.After(now) {
		start = start.AddDate(0, 0, 7)
	}
	return start
}

func sendLocalizedPush(ctx context.Context, app *platform.App, devices []models.PushDevice, messageForLocale func(string) pushMessage) (int, int) {
	groups := map[string][]models.PushDevice{}
	for _, device := range devices {
		locale := normalizeCampaignLocale(device.Locale)
		if locale == "" {
			locale = "en"
		}
		groups[locale] = append(groups[locale], device)
	}
	sent, skipped := 0, 0
	for locale, localizedDevices := range groups {
		groupSent, groupSkipped := sendPushToDevices(ctx, app, localizedDevices, messageForLocale(locale))
		sent += groupSent
		skipped += groupSkipped
	}
	return sent, skipped
}
