package clock

import (
	"testing"
	"time"
)

func TestFakeTickerFiresInOrderAndBlocks(t *testing.T) {
	start := time.Unix(0, 0)
	f := NewFake(start)
	tk := f.NewTicker(10 * time.Millisecond)
	got := make(chan time.Time, 10)
	go func() {
		for i := 0; i < 3; i++ {
			got <- <-tk.C()
		}
	}()
	f.Advance(35 * time.Millisecond) // returns only after 3 ticks were received
	// Received by the goroutine, not necessarily forwarded to got yet: read with a timeout
	// instead of checking len(got) at once (that raced and failed under load).
	for i := 1; i <= 3; i++ {
		select {
		case at := <-got:
			if !at.Equal(start.Add(time.Duration(i) * 10 * time.Millisecond)) {
				t.Fatalf("tick %d at %v", i, at)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("tick %d never received", i)
		}
	}
	if !f.Now().Equal(start.Add(35 * time.Millisecond)) {
		t.Fatalf("now = %v", f.Now())
	}
	tk.Stop()
	f.Advance(time.Second) // a stopped ticker must not block Advance
}
