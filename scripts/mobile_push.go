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

	devices, err := store.DevicesForPlatforms(ctx, updated.Platforms)
	if err != nil {
		// The post is already live; a device-lookup failure shouldn't undo that.
		app.Logger.Warn("mobile_push: failed to load devices for push", "post_id", updated.ID, "err", err)
		return result, nil
	}

	title := valueOr(updated.PushTitle, updated.Title)
	body := valueOr(updated.PushBody, updated.Summary)
	sent, skipped := sendPushToDevices(ctx, app, devices, pushMessage{
		Title: title,
		Body:  body,
		Data:  map[string]string{"post_id": updated.ID, "type": "admin_post"},
	})
	result.PushSent = sent
	result.PushSkipped = skipped
	if sent > 0 || skipped > 0 {
		_ = store.MarkPushSent(ctx, updated.ID)
	}
	return result, nil
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
