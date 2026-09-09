package scripts

import (
	"context"
	"errors"
	"sync"

	"clashking_tracking/internal/platform"
)

type compositeDomain struct {
	name     string
	children []platform.Domain
}

func (d *compositeDomain) Name() string { return d.name }

func (d *compositeDomain) Run(ctx context.Context, app *platform.App) error {
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	errCh := make(chan error, len(d.children))
	var wg sync.WaitGroup
	for _, child := range d.children {
		child := child
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := child.Run(runCtx, app); err != nil && !errors.Is(err, context.Canceled) {
				errCh <- err
			}
		}()
	}
	go func() {
		wg.Wait()
		close(errCh)
	}()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err, ok := <-errCh:
		if !ok {
			return nil
		}
		cancel()
		return err
	}
}

func NewNotificationsDomain() platform.Domain {
	return &compositeDomain{name: "notifications", children: []platform.Domain{
		NewMobileEventsDomain(), NewMobilePushDomain(),
	}}
}

func NewBotAutomationsDomain() platform.Domain {
	return &compositeDomain{name: "bot-automations", children: []platform.Domain{
		NewRedditDomain(), NewGiveawaysDomain(), NewRosterAutomationsDomain(),
	}}
}
