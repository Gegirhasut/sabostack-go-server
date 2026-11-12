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
	if len(got) != 3 {
		t.Fatalf("received %d ticks, want 3", len(got))
	}
	for i := 1; i <= 3; i++ {
		if at := <-got; !at.Equal(start.Add(time.Duration(i) * 10 * time.Millisecond)) {
			t.Fatalf("tick %d at %v", i, at)
		}
	}
	if !f.Now().Equal(start.Add(35 * time.Millisecond)) {
		t.Fatalf("now = %v", f.Now())
	}
	tk.Stop()
	f.Advance(time.Second) // a stopped ticker must not block Advance
}
