package scripts

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"clashking_tracking/internal/platform"
	"clashking_tracking/internal/utils"
	"clashking_tracking/models"

	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
)

const firebaseMessagingScope = "https://www.googleapis.com/auth/firebase.messaging"

var fcmADC = struct {
	sync.Mutex
	source oauth2.TokenSource
}{}

var pushHTTPClient = &http.Client{Timeout: 10 * time.Second}

type pushMessage struct {
	Title string
	Body  string
	Data  map[string]string
}

// sendPushToDevices decrypts each device's token (AES-GCM, same scheme
// clashking-api used to write it) and sends via the matching provider.
// Provider/configuration failures are counted as skipped and never reported
// as successful deliveries. A bounded worker pool avoids serial network
// latency without creating an unbounded goroutine per registered device.
func sendPushToDevices(ctx context.Context, app *platform.App, devices []models.PushDevice, msg pushMessage) (sent int, skipped int) {
	if len(devices) == 0 {
		return 0, 0
	}
	workerCount := min(20, len(devices))
	jobs := make(chan models.PushDevice)
	var wg sync.WaitGroup
	var mu sync.Mutex
	for range workerCount {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for device := range jobs {
				token, err := utils.DecryptSecret(device.TokenCiphertext, app.Config.MobilePushTokenKey)
				if err != nil {
					app.Logger.Warn("mobile_push: failed to decrypt device token", "device_id", device.DeviceID, "err", err)
					mu.Lock()
					skipped++
					mu.Unlock()
					continue
				}

				var sendErr error
				switch device.Provider {
				case "fcm":
					sendErr = sendFCM(ctx, app, token, msg)
				default:
					app.Logger.Warn("mobile_push: unknown provider", "device_id", device.DeviceID, "provider", device.Provider)
					mu.Lock()
					skipped++
					mu.Unlock()
					continue
				}
				if sendErr != nil {
					app.Logger.Warn("mobile_push: send failed", "device_id", device.DeviceID, "provider", device.Provider, "err", sendErr)
					mu.Lock()
					skipped++
					mu.Unlock()
					continue
				}
				mu.Lock()
				sent++
				mu.Unlock()
			}
		}()
	}
	for _, device := range devices {
		select {
		case jobs <- device:
		case <-ctx.Done():
			close(jobs)
			wg.Wait()
			return sent, skipped + len(devices) - sent - skipped
		}
	}
	close(jobs)
	wg.Wait()
	return sent, skipped
}

func sendFCM(ctx context.Context, app *platform.App, token string, msg pushMessage) error {
	if app.Config.MobilePushFCMProjectID == "" {
		return fmt.Errorf("FCM is not configured")
	}
	accessToken, err := fcmAccessToken(app)
	if err != nil {
		return err
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
	req.Header.Set("Authorization", "Bearer "+accessToken)
	req.Header.Set("Content-Type", "application/json")
	return doSend(req)
}

// fcmAccessToken uses service-account JSON or Application Default Credentials.
// ReuseTokenSource caches valid OAuth tokens and refreshes them
// automatically before expiry, so long-running schedulers need no restart.
func fcmAccessToken(app *platform.App) (string, error) {
	fcmADC.Lock()
	if fcmADC.source == nil {
		var source oauth2.TokenSource
		var err error
		if app.Config.MobilePushFCMServiceAccountJSON != "" {
			credentials, credentialsErr := google.CredentialsFromJSON(
				context.Background(),
				[]byte(app.Config.MobilePushFCMServiceAccountJSON),
				firebaseMessagingScope,
			)
			if credentialsErr != nil {
				fcmADC.Unlock()
				return "", fmt.Errorf("parse FCM service account JSON: %w", credentialsErr)
			}
			source = credentials.TokenSource
		} else {
			source, err = google.DefaultTokenSource(context.Background(), firebaseMessagingScope)
			if err != nil {
				fcmADC.Unlock()
				return "", fmt.Errorf("FCM Application Default Credentials: %w", err)
			}
		}
		fcmADC.source = oauth2.ReuseTokenSource(nil, source)
	}
	source := fcmADC.source
	fcmADC.Unlock()

	token, err := source.Token()
	if err != nil {
		return "", fmt.Errorf("refresh FCM access token: %w", err)
	}
	if token.AccessToken == "" {
		return "", fmt.Errorf("FCM Application Default Credentials returned an empty access token")
	}
	return token.AccessToken, nil
}

func doSend(req *http.Request) error {
	resp, err := pushHTTPClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return fmt.Errorf("push provider returned status %d", resp.StatusCode)
	}
	return nil
}
