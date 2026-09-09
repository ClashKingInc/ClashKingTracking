package scripts

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"clashking_tracking/internal/platform"

	clashy "github.com/clashkinginc/clashy.go"
	"github.com/jackc/pgx/v5"
)

const trackingWakeChannel = "clashking_tracking_wake_v1"
const trackingWakePayloadLimit = 4096

type trackingWakeEvent struct {
	Version      int    `json:"v"`
	Kind         string `json:"kind"`
	ClanTag      string `json:"clanTag,omitempty"`
	ReminderType string `json:"reminderType,omitempty"`
	UserID       string `json:"userId,omitempty"`
	ServerID     string `json:"serverId,omitempty"`
}

func parseTrackingWakeEvent(payload string) (trackingWakeEvent, error) {
	if len(payload) == 0 || len(payload) > trackingWakePayloadLimit {
		return trackingWakeEvent{}, errors.New("tracking wake payload has invalid size")
	}
	decoder := json.NewDecoder(bytes.NewBufferString(payload))
	decoder.DisallowUnknownFields()
	var event trackingWakeEvent
	if err := decoder.Decode(&event); err != nil {
		return trackingWakeEvent{}, err
	}
	if err := ensureJSONEnd(decoder); err != nil {
		return trackingWakeEvent{}, err
	}
	if event.Version != 1 {
		return trackingWakeEvent{}, fmt.Errorf("unsupported tracking wake version %d", event.Version)
	}
	switch event.Kind {
	case "reminder_config":
		if event.ClanTag == "" || clashy.CorrectTag(event.ClanTag) != event.ClanTag || event.ReminderType == "" || len(event.ReminderType) > 64 {
			return trackingWakeEvent{}, errors.New("invalid reminder_config wake")
		}
	case "mobile_reminder_config":
		if !decimalIdentifier(event.UserID) {
			return trackingWakeEvent{}, errors.New("invalid mobile_reminder_config wake")
		}
	case "guild_reactivated":
		if !decimalIdentifier(event.ServerID) {
			return trackingWakeEvent{}, errors.New("invalid guild_reactivated wake")
		}
	default:
		return trackingWakeEvent{}, fmt.Errorf("unsupported tracking wake kind %q", event.Kind)
	}
	return event, nil
}

func ensureJSONEnd(decoder *json.Decoder) error {
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("tracking wake payload contains multiple values")
		}
		return err
	}
	return nil
}

func decimalIdentifier(value string) bool {
	if value == "" || len(value) > 20 {
		return false
	}
	for _, character := range value {
		if character < '0' || character > '9' {
			return false
		}
	}
	return true
}

func runTrackingWakeListener(ctx context.Context, app *platform.App, handle func(context.Context, trackingWakeEvent) error) error {
	for {
		err := listenForTrackingWakes(ctx, app.Config.TimescaleURL, func(event trackingWakeEvent) {
			if handleErr := handle(ctx, event); handleErr != nil && ctx.Err() == nil {
				app.Logger.Error("tracking wake reconciliation failed", "kind", event.Kind, "err", handleErr)
			}
		})
		if ctx.Err() != nil {
			return ctx.Err()
		}
		app.Logger.Error("tracking wake listener disconnected; reconnecting", "err", err)
		if err := sleepOrDone(ctx, time.Second); err != nil {
			return err
		}
	}
}

func listenForTrackingWakes(ctx context.Context, dsn string, handle func(trackingWakeEvent)) error {
	connection, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return err
	}
	defer connection.Close(context.WithoutCancel(ctx))
	if _, err := connection.Exec(ctx, "LISTEN "+trackingWakeChannel); err != nil {
		return err
	}
	for {
		notification, err := connection.WaitForNotification(ctx)
		if err != nil {
			return err
		}
		if notification.Channel != trackingWakeChannel {
			continue
		}
		event, err := parseTrackingWakeEvent(strings.TrimSpace(notification.Payload))
		if err != nil {
			continue
		}
		handle(event)
	}
}
