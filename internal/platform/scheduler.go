package platform

import (
	"container/heap"
	"context"
	"sync"
	"time"
)

type Job struct {
	ID   string
	When time.Time
	Run  func(context.Context)
}

type Scheduler struct {
	mu     sync.Mutex
	queue  jobHeap
	wakeup chan struct{}
}

func NewScheduler() *Scheduler {
	return &Scheduler{wakeup: make(chan struct{}, 1)}
}

func (s *Scheduler) Schedule(job Job) {
	s.mu.Lock()
	heap.Push(&s.queue, job)
	s.mu.Unlock()
	// A buffered wakeup collapses bursts of schedules into a single loop interrupt.
	select {
	case s.wakeup <- struct{}{}:
	default:
	}
}

func (s *Scheduler) Pending() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.queue)
}

func (s *Scheduler) Run(ctx context.Context) error {
	for {
		delay := s.nextDelay()
		if delay < 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-s.wakeup:
			}
			continue
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-s.wakeup:
			timer.Stop()
			continue
		case <-timer.C:
			s.runDue(ctx)
		}
	}
}

func (s *Scheduler) nextDelay() time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.queue) == 0 {
		return -1
	}
	delay := time.Until(s.queue[0].When)
	if delay < 0 {
		return 0
	}
	return delay
}

func (s *Scheduler) runDue(ctx context.Context) {
	for {
		s.mu.Lock()
		if len(s.queue) == 0 || time.Until(s.queue[0].When) > 0 {
			s.mu.Unlock()
			return
		}
		job := heap.Pop(&s.queue).(Job)
		s.mu.Unlock()
		// Jobs run asynchronously so one slow callback does not stall later deadlines.
		go job.Run(ctx)
	}
}

type jobHeap []Job

func (h jobHeap) Len() int           { return len(h) }
func (h jobHeap) Less(i, j int) bool { return h[i].When.Before(h[j].When) }
func (h jobHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *jobHeap) Push(x any)        { *h = append(*h, x.(Job)) }
func (h *jobHeap) Pop() any {
	old := *h
	item := old[len(old)-1]
	*h = old[:len(old)-1]
	return item
}
