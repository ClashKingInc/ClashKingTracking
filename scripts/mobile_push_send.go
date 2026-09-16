package scripts

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
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

type pushProviderHTTPError struct{ Status int }

func (e *pushProviderHTTPError) Error() string {
	switch e.Status {
	case http.StatusUnauthorized, http.StatusForbidden:
		return fmt.Sprintf("push provider authentication or configuration failed with status %d", e.Status)
	case http.StatusBadRequest, http.StatusNotFound, http.StatusGone:
		return fmt.Sprintf("push provider permanently rejected the request or device token with status %d", e.Status)
	default:
		return fmt.Sprintf("push provider returned status %d", e.Status)
	}
}

func isRetryablePushError(err error) bool {
	if err == nil || errors.Is(err, context.Canceled) {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var providerErr *pushProviderHTTPError
	if !errors.As(err, &providerErr) {
		return true
	}
	return providerErr.Status == http.StatusUnauthorized ||
		providerErr.Status == http.StatusForbidden ||
		providerErr.Status == http.StatusRequestTimeout ||
		providerErr.Status == http.StatusTooEarly ||
		providerErr.Status == http.StatusTooManyRequests ||
		providerErr.Status >= http.StatusInternalServerError
}

// sendPushToDevices decrypts each device's token (AES-GCM, same scheme
// clashking-api used to write it) and sends via the matching provider.
// Provider/configuration failures are counted as skipped and never reported
// as successful deliveries. A bounded worker pool avoids serial network
// latency without creating an unbounded goroutine per registered device.
func sendPushToDevices(ctx context.Context, app *platform.App, devices []models.PushDevice, msg pushMessage) (sent int, skipped int, deliveryErr error) {
	if len(devices) == 0 {
		return 0, 0, nil
	}
	workerCount := min(20, len(devices))
	jobs := make(chan models.PushDevice)
	var wg sync.WaitGroup
	var mu sync.Mutex
	var deliveryErrors []error
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
					if isRetryablePushError(sendErr) {
						deliveryErrors = append(deliveryErrors, fmt.Errorf("deliver to device %s: %w", device.DeviceID, sendErr))
					}
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
			return sent, skipped + len(devices) - sent - skipped, errors.Join(append(deliveryErrors, ctx.Err())...)
		}
	}
	close(jobs)
	wg.Wait()
	return sent, skipped, errors.Join(deliveryErrors...)
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
	origin := "https://fcm.googleapis.com"
	if app.Config.MobilePushFCMAPIOrigin != "" {
		if err := platform.ValidateLoopbackProviderURL(app.Config.MobilePushFCMAPIOrigin); err != nil {
			return fmt.Errorf("invalid CLASHKING_LOCAL_FCM_API_ORIGIN: %w", err)
		}
		origin = app.Config.MobilePushFCMAPIOrigin
	}
	url := fmt.Sprintf("%s/v1/projects/%s/messages:send", origin, app.Config.MobilePushFCMProjectID)
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
		return &pushProviderHTTPError{Status: resp.StatusCode}
	}
	return nil
}
