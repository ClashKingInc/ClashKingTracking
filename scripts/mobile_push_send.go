package scripts

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"clashking_tracking/internal/platform"
	"clashking_tracking/internal/utils"
	"clashking_tracking/models"
)

type pushMessage struct {
	Title string
	Body  string
	Data  map[string]string
}

// sendPushToDevices decrypts each device's token (AES-GCM, same scheme
// clashking-api used to write it) and sends via the matching provider.
// Missing bearer-token config is logged and skipped rather than treated as
// an error, so the scheduler and status transitions stay testable without
// real push credentials.
func sendPushToDevices(ctx context.Context, app *platform.App, devices []models.PushDevice, msg pushMessage) (sent int, skipped int) {
	for _, device := range devices {
		token, err := utils.DecryptSecret(device.TokenCiphertext, app.Config.MobilePushTokenKey)
		if err != nil {
			app.Logger.Warn("mobile_push: failed to decrypt device token", "device_id", device.DeviceID, "err", err)
			skipped++
			continue
		}

		var sendErr error
		switch device.Provider {
		case "fcm":
			sendErr = sendFCM(ctx, app, token, msg)
		case "apns":
			sendErr = sendAPNS(ctx, app, token, msg)
		default:
			app.Logger.Warn("mobile_push: unknown provider", "device_id", device.DeviceID, "provider", device.Provider)
			skipped++
			continue
		}
		if sendErr != nil {
			app.Logger.Warn("mobile_push: send failed", "device_id", device.DeviceID, "provider", device.Provider, "err", sendErr)
			skipped++
			continue
		}
		sent++
	}
	return sent, skipped
}

func sendFCM(ctx context.Context, app *platform.App, token string, msg pushMessage) error {
	if app.Config.MobilePushFCMBearerToken == "" || app.Config.MobilePushFCMProjectID == "" {
		app.Logger.Info("mobile_push: FCM not configured, would send", "provider", "fcm")
		return nil
	}
	payload := map[string]any{
		"message": map[string]any{
			"token": token,
			"notification": map[string]string{
				"title": msg.Title,
				"body":  msg.Body,
			},
			"data": msg.Data,
		},
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	url := fmt.Sprintf("https://fcm.googleapis.com/v1/projects/%s/messages:send", app.Config.MobilePushFCMProjectID)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+app.Config.MobilePushFCMBearerToken)
	req.Header.Set("Content-Type", "application/json")
	return doSend(req)
}

// sendAPNS relies on net/http's automatic HTTP/2-over-TLS negotiation
// (built into the standard library since Go 1.6) — APNs requires HTTP/2 and
// no extra transport setup is needed for that to happen here.
func sendAPNS(ctx context.Context, app *platform.App, token string, msg pushMessage) error {
	if app.Config.MobilePushAPNSBearerToken == "" || app.Config.MobilePushAPNSBundleID == "" {
		app.Logger.Info("mobile_push: APNs not configured, would send", "provider", "apns")
		return nil
	}
	payload := map[string]any{
		"aps": map[string]any{
			"alert": map[string]string{
				"title": msg.Title,
				"body":  msg.Body,
			},
		},
	}
	for key, value := range msg.Data {
		payload[key] = value
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	url := fmt.Sprintf("https://api.push.apple.com/3/device/%s", token)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("authorization", "bearer "+app.Config.MobilePushAPNSBearerToken)
	req.Header.Set("apns-topic", app.Config.MobilePushAPNSBundleID)
	req.Header.Set("apns-push-type", "alert")
	return doSend(req)
}

func doSend(req *http.Request) error {
	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return fmt.Errorf("push provider returned status %d", resp.StatusCode)
	}
	return nil
}
