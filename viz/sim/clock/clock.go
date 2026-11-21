// Package clock provides a deterministic virtual clock and seeded RNG used
// by the copies under viz/sim in place of the real time and math/rand
// packages, plus the quiescence-detection primitives (Mutex, Drain) that let
// a driver step a goroutine-based simulation one virtual event at a time.
//
// Determinism model: every meaningful state change in the copied
// raft/rsm/kv code happens while holding a clock.Mutex (a drop-in
// replacement for sync.Mutex). Each Lock() bumps a shared activity counter.
// After firing a timer or delivering a message, the driver calls Drain,
// which yields the scheduler until the counter stops moving for a run of
// consecutive checks -- i.e. every goroutine has either finished its work or
// durably parked on a clock wait. This is a heuristic, not a formally
// verified barrier; see viz/README.md for the tradeoff.
package clock

import (
	"container/heap"
	"math/rand"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

// Duration mirrors time.Duration so copied code can keep using e.g.
// 100*time.Millisecond unchanged.
type Duration = time.Duration

// stableChecks is how many consecutive Gosched passes with no activity
// bump must occur before Drain considers the simulation quiesced.
const stableChecks = 200

// maxDrainChecks bounds Drain so a genuine bug (an infinite busy loop that
// never touches a clock.Mutex) surfaces as an error instead of hanging the
// browser tab forever.
const maxDrainChecks = 2_000_000

type timer struct {
	deadline Duration
	seq      uint64
	fire     func() // invoked with the clock's internal lock NOT held
	index    int    // heap index, maintained by container/heap
}

type timerHeap []*timer

func (h timerHeap) Len() int { return len(h) }
func (h timerHeap) Less(i, j int) bool {
	if h[i].deadline != h[j].deadline {
		return h[i].deadline < h[j].deadline
	}
	return h[i].seq < h[j].seq
}
func (h timerHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index, h[j].index = i, j
}
func (h *timerHeap) Push(x any) {
	t := x.(*timer)
	t.index = len(*h)
	*h = append(*h, t)
}
func (h *timerHeap) Pop() any {
	old := *h
	n := len(old)
	t := old[n-1]
	old[n-1] = nil
	t.index = -1
	*h = old[:n-1]
	return t
}

// Clock is a single simulation's virtual time source, event queue and
// seeded RNG.
type Clock struct {
	mu     sync.Mutex
	now    Duration
	seq    uint64
	timers timerHeap

	rngMu sync.Mutex
	rng   *rand.Rand

	activity atomic.Int64
	stuck    atomic.Bool
}

// New creates a Clock whose RNG is seeded deterministically from seed, so
// two Clocks created with the same seed and driven by the same sequence of
// external actions (Step calls, injected events) produce identical traces.
func New(seed int64) *Clock {
	return &Clock{rng: rand.New(rand.NewSource(seed))}
}

// Bump records a unit of work. Call it from any wrapper around a
// synchronization primitive that proves a goroutine made progress; Mutex
// already does this for every Lock().
func (c *Clock) Bump() { c.activity.Add(1) }

// Now returns the current virtual time.
func (c *Clock) Now() Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

// Since is a convenience wrapper matching time.Since(t) semantics against
// the virtual clock.
func (c *Clock) Since(t Duration) Duration { return c.Now() - t }

func (c *Clock) schedule(d Duration, fire func()) *timer {
	c.mu.Lock()
	c.seq++
	t := &timer{deadline: c.now + d, seq: c.seq, fire: fire}
	heap.Push(&c.timers, t)
	c.mu.Unlock()
	// Registering a timer is itself progress: Drain must not return before
	// every goroutine that is about to sleep has actually reached its
	// Sleep/After call, otherwise a driver stepping immediately after
	// starting goroutines could fire timers before some of them have
	// registered, corrupting event order.
	c.Bump()
	return t
}

func (c *Clock) cancel(t *timer) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if t.index >= 0 && t.index < len(c.timers) && c.timers[t.index] == t {
		heap.Remove(&c.timers, t.index)
	}
}

// Sleep blocks the calling goroutine until the virtual clock advances by d.
func (c *Clock) Sleep(d Duration) {
	done := make(chan struct{})
	c.schedule(d, func() { c.Bump(); close(done) })
	<-done
}

// After returns a channel that receives the virtual fire time once the
// clock advances by d, mirroring time.After.
func (c *Clock) After(d Duration) <-chan time.Time {
	ch := make(chan time.Time, 1)
	c.schedule(d, func() {
		c.Bump()
		ch <- epoch.Add(c.Now())
	})
	return ch
}

// epoch is an arbitrary fixed instant used only so that After/Ticker can
// hand back a time.Time (some call sites compare or log it); the absolute
// value carries no meaning, only Now()/Since() deltas do.
var epoch = time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)

// Ticker mirrors time.Ticker, backed by virtual time.
type Ticker struct {
	C <-chan time.Time
	c *Clock
	d Duration

	mu     sync.Mutex
	active bool
}

func (c *Clock) NewTicker(d Duration) *Ticker {
	ch := make(chan time.Time, 1)
	t := &Ticker{C: ch, c: c, d: d, active: true}
	t.scheduleNext(ch)
	return t
}

func (t *Ticker) scheduleNext(ch chan time.Time) {
	t.c.schedule(t.d, func() {
		t.mu.Lock()
		active := t.active
		t.mu.Unlock()
		if !active {
			return
		}
		t.c.Bump()
		select {
		case ch <- epoch.Add(t.c.Now()):
		default:
		}
		t.scheduleNext(ch)
	})
}

func (t *Ticker) Stop() {
	t.mu.Lock()
	t.active = false
	t.mu.Unlock()
}

// AfterFunc schedules f to run (in a new goroutine) once the virtual clock
// advances by d, mirroring time.AfterFunc. Used by the network simulator to
// model message latency without an extra goroutine-per-wait.
func (c *Clock) AfterFunc(d Duration, f func()) {
	c.schedule(d, func() {
		c.Bump()
		go f()
	})
}

// --- seeded RNG, safe for concurrent use ---

func (c *Clock) Int63() int64 {
	c.rngMu.Lock()
	defer c.rngMu.Unlock()
	return c.rng.Int63()
}

func (c *Clock) Int63n(n int64) int64 {
	c.rngMu.Lock()
	defer c.rngMu.Unlock()
	return c.rng.Int63n(n)
}

func (c *Clock) Intn(n int) int {
	c.rngMu.Lock()
	defer c.rngMu.Unlock()
	return c.rng.Intn(n)
}

func (c *Clock) Float64() float64 {
	c.rngMu.Lock()
	defer c.rngMu.Unlock()
	return c.rng.Float64()
}

// --- driver side: stepping virtual time forward ---

// HasPending reports whether any timer is scheduled.
func (c *Clock) HasPending() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.timers) > 0
}

// NextDeadline returns the virtual time of the earliest pending timer.
func (c *Clock) NextDeadline() (Duration, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.timers) == 0 {
		return 0, false
	}
	return c.timers[0].deadline, true
}

// fireDueAt pops and fires every timer due at or before `at`, advancing now
// to `at`. Firing happens outside the lock so a timer callback can itself
// schedule new timers.
func (c *Clock) fireDueAt(at Duration) int {
	fired := 0
	for {
		c.mu.Lock()
		if len(c.timers) == 0 || c.timers[0].deadline > at {
			c.mu.Unlock()
			break
		}
		t := heap.Pop(&c.timers).(*timer)
		if c.now < at {
			c.now = at
		}
		c.mu.Unlock()
		t.fire()
		fired++
	}
	return fired
}

// Step advances the clock to the next pending timer, fires every timer due
// at that instant, drains the resulting goroutine activity, and reports
// whether it did anything.
func (c *Clock) Step() bool {
	at, ok := c.NextDeadline()
	if !ok {
		return false
	}
	c.fireDueAt(at)
	c.Drain()
	return true
}

// Drain yields the scheduler until no goroutine has bumped the activity
// counter for stableChecks consecutive passes, i.e. the simulation has
// settled following an injected event. It panics via Stuck() reporting if
// the system never settles within maxDrainChecks passes.
func (c *Clock) Drain() {
	last := c.activity.Load()
	stable := 0
	for checks := 0; checks < maxDrainChecks; checks++ {
		runtime.Gosched()
		cur := c.activity.Load()
		if cur == last {
			stable++
			if stable >= stableChecks {
				return
			}
		} else {
			stable = 0
			last = cur
		}
	}
	c.stuck.Store(true)
}

// Stuck reports whether the last Drain gave up without settling, which
// means some goroutine is spinning without ever touching a clock.Mutex --
// a bug in the copied code or its instrumentation, not a normal simulation
// state.
func (c *Clock) Stuck() bool { return c.stuck.Load() }

// Mutex is a drop-in replacement for sync.Mutex that bumps the owning
// Clock's activity counter on every successful Lock, which is how Drain
// detects that a goroutine made progress. Zero value is unusable; call
// Init before first use (done once by the constructor of whatever struct
// embeds it).
type Mutex struct {
	sync.Mutex
	C *Clock
}

func (m *Mutex) Lock() {
	m.Mutex.Lock()
	if m.C != nil {
		m.C.Bump()
	}
}
