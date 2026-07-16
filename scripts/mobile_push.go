package scripts

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"clashking_tracking/internal/platform"
	"clashking_tracking/models"
)

const mobilePushDomainName = "mobile_push"

// mobilePushDomain serves the admin Posts API and, on the same interval
// loop shape as giveaways.go/scheduled.go, publishes scheduled posts whose
// starts_at has passed and expires live posts whose ends_at has passed —
// with no admin action required for either.
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

	server := newMobilePushHTTPServer(app, store)
	go func() {
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			app.Logger.Error("mobile_push: http server exited", "err", err)
		}
	}()
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
	}()
	app.Logger.Info("mobile_push: http server starting", "addr", app.Config.MobilePushHTTPAddr)

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
	if cfg.MobilePushHTTPAddr == "" {
		return errors.New("mobile_push.http_addr is required")
	}
	if !cfg.DryRun && !cfg.MockDB && cfg.TimescaleURL == "" {
		return errors.New("TIMESCALE_URL is required for mobile_push")
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

	retries, err := d.store.DuePushRetries(ctx, now)
	if err != nil {
		return err
	}
	for _, post := range retries {
		if _, _, err := deliverPostPush(ctx, app, d.store, post, "retry"); err != nil {
			app.Logger.Warn("mobile_push: automatic retry failed", "post_id", post.ID, "err", err)
		}
	}

	campaigns, err := d.store.DueCampaigns(ctx, now)
	if err != nil {
		return err
	}
	for _, campaign := range campaigns {
		devices, loadErr := d.store.DevicesForPlatforms(ctx, campaign.Platforms, campaign.TargetLocales)
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

// publishAndNotify is the single publish-and-maybe-push path shared by the
// scheduler's due-post scan and the "Publish now" HTTP handler, so both
// behave identically.
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
	devices, err := store.DevicesForPlatforms(ctx, post.Platforms, nil)
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
	if sent > 0 {
		_ = store.MarkPushSent(ctx, post.ID)
	}
	return sent, skipped, recordErr
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

// bunnyUpload mirrors clashking-api/internal/routes/cdn.go's BunnyCDN PUT
// technique exactly, targeting the same storage zone and public CDN host
// under an admin-posts/ prefix, so it needs no new hosting infrastructure —
// just its own access key.
func bunnyUpload(accessKey, path, ext string, data []byte) (string, error) {
	fullPath := fmt.Sprintf("%s.%s", path, ext)
	uploadURL := fmt.Sprintf("https://storage.bunnycdn.com/clashking-files/%s", fullPath)

	req, err := http.NewRequest(http.MethodPut, uploadURL, bytes.NewReader(data))
	if err != nil {
		return "", err
	}
	req.Header.Set("AccessKey", accessKey)
	req.Header.Set("Content-Type", "application/octet-stream")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return "", fmt.Errorf("bunnycdn upload failed: status %d", resp.StatusCode)
	}
	return fmt.Sprintf("https://cdn.clashk.ing/%s", fullPath), nil
}
