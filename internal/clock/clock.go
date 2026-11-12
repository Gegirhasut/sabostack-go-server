// Package clock abstracts time so the game loop can be driven deterministically in tests.
//
// Match code never calls time.Now or time.NewTicker directly; it asks a Clock. Production
// uses Real, tests use Fake and advance it by hand.
package clock

import (
	"sync"
	"time"
)

// Clock is the minimal time source the server needs.
type Clock interface {
	Now() time.Time
	NewTicker(d time.Duration) Ticker
}

// Ticker mirrors *time.Ticker behind an interface.
type Ticker interface {
	C() <-chan time.Time
	Stop()
}

// Real is the wall clock.
type Real struct{}

func (Real) Now() time.Time { return time.Now() }

func (Real) NewTicker(d time.Duration) Ticker { return realTicker{time.NewTicker(d)} }

type realTicker struct{ t *time.Ticker }

func (r realTicker) C() <-chan time.Time { return r.t.C }
func (r realTicker) Stop()               { r.t.Stop() }

// Fake is a manually advanced clock. Tickers created from it fire only inside Advance,
// and Advance blocks until every due tick has been received. Combined with a
// single-goroutine consumer this makes timer behaviour fully deterministic.
type Fake struct {
	mu      sync.Mutex
	now     time.Time
	tickers []*fakeTicker
}

// NewFake returns a fake clock starting at t.
func NewFake(t time.Time) *Fake { return &Fake{now: t} }

func (f *Fake) Now() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.now
}

func (f *Fake) NewTicker(d time.Duration) Ticker {
	if d <= 0 {
		panic("clock: non-positive ticker interval")
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	t := &fakeTicker{clock: f, every: d, next: f.now.Add(d), c: make(chan time.Time), stop: make(chan struct{})}
	f.tickers = append(f.tickers, t)
	return t
}

// Advance moves time forward by d, delivering each due tick in chronological order.
// Each delivery blocks until the ticker's consumer receives it (or the ticker is stopped).
func (f *Fake) Advance(d time.Duration) {
	f.mu.Lock()
	target := f.now.Add(d)
	f.mu.Unlock()
	for {
		f.mu.Lock()
		var due *fakeTicker
		for _, t := range f.tickers {
			if !t.stopped && !t.next.After(target) && (due == nil || t.next.Before(due.next)) {
				due = t
			}
		}
		if due == nil {
			f.now = target
			f.mu.Unlock()
			return
		}
		f.now = due.next
		at := due.next
		due.next = due.next.Add(due.every)
		f.mu.Unlock()
		select {
		case due.c <- at:
		case <-due.stop:
		}
	}
}

type fakeTicker struct {
	clock   *Fake
	every   time.Duration
	next    time.Time
	c       chan time.Time
	stop    chan struct{}
	stopped bool
}

func (t *fakeTicker) C() <-chan time.Time { return t.c }

func (t *fakeTicker) Stop() {
	t.clock.mu.Lock()
	defer t.clock.mu.Unlock()
	if !t.stopped {
		t.stopped = true
		close(t.stop)
	}
}
