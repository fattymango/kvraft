package clock

import (
	"sync"
	"testing"
	"time"
)

// TestOrdering checks that timers fire in deadline order regardless of
// goroutine scheduling, and that Step advances no further than necessary.
func TestOrdering(t *testing.T) {
	c := New(1)
	var mu sync.Mutex
	mu2 := &Mutex{C: c}
	var order []string

	record := func(name string) {
		mu.Lock()
		order = append(order, name)
		mu.Unlock()
	}

	go func() {
		c.Sleep(30 * time.Millisecond)
		mu2.Lock()
		record("c")
		mu2.Unlock()
	}()
	go func() {
		c.Sleep(10 * time.Millisecond)
		mu2.Lock()
		record("a")
		mu2.Unlock()
	}()
	go func() {
		c.Sleep(20 * time.Millisecond)
		mu2.Lock()
		record("b")
		mu2.Unlock()
	}()

	c.Drain() // let the three goroutines register their timers
	for c.HasPending() {
		if !c.Step() {
			break
		}
	}
	if c.Stuck() {
		t.Fatal("clock reported stuck")
	}

	mu.Lock()
	got := append([]string(nil), order...)
	mu.Unlock()
	want := []string{"a", "b", "c"}
	if len(got) != len(want) {
		t.Fatalf("order = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("order = %v, want %v", got, want)
		}
	}
	if c.Now() != 30*time.Millisecond {
		t.Fatalf("Now() = %v, want 30ms", c.Now())
	}
}

// TestDeterministicReplay checks that running the same scenario twice with
// the same seed produces the same trace and the same RNG stream.
func TestDeterministicReplay(t *testing.T) {
	run := func(seed int64) []int64 {
		c := New(seed)
		var mu sync.Mutex
		m := &Mutex{C: c}
		var nums []int64
		var wg sync.WaitGroup
		for i := 0; i < 5; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				c.Sleep(time.Duration(i+1) * time.Millisecond)
				n := c.Int63n(1000)
				m.Lock()
				mu.Lock()
				nums = append(nums, n)
				mu.Unlock()
				m.Unlock()
			}(i)
		}
		c.Drain()
		for c.HasPending() {
			if !c.Step() {
				break
			}
		}
		wg.Wait()
		return nums
	}

	a := run(42)
	b := run(42)
	if len(a) != len(b) {
		t.Fatalf("len mismatch: %d vs %d", len(a), len(b))
	}
	for i := range a {
		if a[i] != b[i] {
			t.Fatalf("replay mismatch at %d: %d vs %d", i, a[i], b[i])
		}
	}
}

func TestTicker(t *testing.T) {
	c := New(1)
	ticker := c.NewTicker(10 * time.Millisecond)
	count := 0
	done := make(chan struct{})
	go func() {
		for i := 0; i < 3; i++ {
			<-ticker.C
			count++
		}
		ticker.Stop()
		close(done)
	}()

	for i := 0; i < 10; i++ {
		if !c.Step() {
			break
		}
		select {
		case <-done:
			i = 10
		default:
		}
	}
	<-done
	if count != 3 {
		t.Fatalf("count = %d, want 3", count)
	}
}
