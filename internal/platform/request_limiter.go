package platform

import (
	"context"
	"errors"
	"sync"
	"time"
)

var ErrInvalidRequestLimit = errors.New("request limiter: limit must be greater than zero")

type RequestLimiter struct {
	limit int

	mu       sync.Mutex
	starts   []time.Time
	inFlight int
	notify   chan struct{}
}

func NewRequestLimiter(limit int) *RequestLimiter {
	return &RequestLimiter{
		limit:  limit,
		notify: make(chan struct{}),
	}
}

func (l *RequestLimiter) Acquire(ctx context.Context) (func(), error) {
	if l == nil || l.limit <= 0 {
		return nil, ErrInvalidRequestLimit
	}
	for {
		l.mu.Lock()
		now := time.Now()
		l.trimStarts(now)

		if l.inFlight < l.limit && len(l.starts) < l.limit {
			l.inFlight++
			l.starts = append(l.starts, now)
			l.mu.Unlock()
			var once sync.Once
			return func() {
				once.Do(l.release)
			}, nil
		}

		notify := l.notify
		wait := time.Hour
		if len(l.starts) >= l.limit {
			wait = l.starts[0].Add(time.Second).Sub(now)
			if wait < 0 {
				wait = 0
			}
		}
		l.mu.Unlock()

		if wait == 0 {
			continue
		}
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-notify:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
		case <-timer.C:
		}
	}
}

func (l *RequestLimiter) release() {
	l.mu.Lock()
	if l.inFlight > 0 {
		l.inFlight--
	}
	close(l.notify)
	l.notify = make(chan struct{})
	l.mu.Unlock()
}

func (l *RequestLimiter) trimStarts(now time.Time) {
	cutoff := now.Add(-time.Second)
	keep := 0
	for keep < len(l.starts) && !l.starts[keep].After(cutoff) {
		keep++
	}
	if keep > 0 {
		copy(l.starts, l.starts[keep:])
		l.starts = l.starts[:len(l.starts)-keep]
	}
}
