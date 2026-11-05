package scripts

import (
	"context"
	"time"
)

// Channel renames can be heavily rate-limited. They must never prevent the
// independent stream consumer from processing current reminders and logs.
func (w *discordDeliveryWorker) runPeriodicChannels(ctx context.Context) {
	ticker := time.NewTicker(15 * time.Minute)
	defer ticker.Stop()
	for {
		if ctx.Err() != nil {
			return
		}
		now := time.Now()
		wars := map[string]countdownWar{}
		if err := w.refreshWarEvents(ctx, now, wars, ""); err != nil && ctx.Err() == nil {
			w.app.Logger.Error("war event scan failed", "err", err)
		}
		if ctx.Err() != nil {
			return
		}
		if err := w.refreshCountdownChannels(ctx, now, wars); err != nil && ctx.Err() == nil {
			w.app.Logger.Error("countdown scan failed", "err", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
