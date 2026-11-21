// Package engine wires the copied raft/rsm/kv-store/client packages
// together into a running 5-node (configurable) cluster over the
// simulated network, drives it one virtual event at a time, and checks
// safety invariants against the trace stream it collects. This is the Go
// surface the WASM JS API (viz/sim/wasm) wraps.
package engine

import (
	"fmt"
	"sync"
	"time"

	"github.com/anishathalye/porcupine"
	"kvapp/internal/cluster"
	"kvapp/internal/kv/proto"
	"kvapp/viz/sim/client"
	"kvapp/viz/sim/clock"
	"kvapp/viz/sim/kv/store"
	"kvapp/viz/sim/network"
	"kvapp/viz/sim/raft"
	"kvapp/viz/sim/trace"
)

// trafficKeys is the small, fixed keyspace the background traffic
// generator reads and writes; the game's "cluster is serving" story is
// about availability, not data variety.
var trafficKeys = []string{"alpha", "bravo", "charlie", "delta"}

// Config are the parameters passed to New / init(seed, config) in the JS
// API.
type Config struct {
	Seed         int64
	NumNodes     int
	MaxRaftState int // -1 disables snapshotting
	Network      network.Config
}

// Violation is a single detected safety-invariant breach. Once any
// violation is recorded, Engine.Safe() returns false and the UI freezes
// with a description of exactly what broke, per the spec.
type Violation struct {
	Kind   string // "election_safety" | "state_machine_safety"
	Detail string
	At     clock.Duration
}

type node struct {
	persister *cluster.Persister
	kv        *store.KVServer
	rf        *raft.Raft
	alive     bool
}

// Engine is not safe for concurrent use from multiple goroutines beyond
// what its own methods do internally; the WASM wrapper serializes calls
// from JS onto a single goroutine, which is the intended usage.
type Engine struct {
	cfg Config
	clk *clock.Clock
	net *network.Network

	mu    sync.Mutex
	nodes []*node

	events       []trace.Event
	listeners    []func(trace.Event)
	leaderOfTerm map[int]int
	commandAt    map[int]string
	pendingOps   map[int]pendingCall
	history      []porcupine.Operation
	violations   []Violation

	trafficEnds     []*network.ClientEnd
	uiClient        *client.Client // reused by Submit*, so it remembers the last leader like a real client
	trafficClient   *client.Client // same for background traffic
	trafficGen      uint64
	steps           uint64
	trafficOn       bool
	trafficSeq      int
	trafficVersions map[string]proto.Tversion

	results map[uint64]Result
	nextReq uint64
}

// Result is the outcome of a manually submitted request (JS API's
// submit(op)), polled via Result(id) since the virtual clock only
// advances via explicit Step/Play calls from JS.
type Result struct {
	Done    bool
	Value   string
	Version proto.Tversion
	Err     string
}

func New(cfg Config) *Engine {
	if cfg.NumNodes <= 0 {
		cfg.NumNodes = 5
	}
	if cfg.MaxRaftState == 0 {
		cfg.MaxRaftState = -1
	}
	e := &Engine{
		cfg:             cfg,
		clk:             clock.New(cfg.Seed),
		leaderOfTerm:    map[int]int{},
		commandAt:       map[int]string{},
		results:         map[uint64]Result{},
		trafficVersions: map[string]proto.Tversion{},
		pendingOps:      map[int]pendingCall{},
	}
	e.net = network.New(e.clk, netSink{e}, cfg.Network)
	e.buildCluster()
	return e
}

// netSink adapts Engine to network.EventSink, turning message send/deliver
// records into msg_dropped / generic message trace events for the UI.
type netSink struct{ e *Engine }

func (s netSink) OnMessage(m network.Message) {
	if m.Dropped {
		s.e.record(trace.Event{T: s.e.clk.Now(), Kind: "msg_dropped", Data: map[string]any{
			"from": m.From, "to": m.To, "svc_method": m.Kind, "id": m.ID,
		}})
	}
}

func (e *Engine) buildCluster() {
	n := e.cfg.NumNodes
	e.nodes = make([]*node, n)
	for i := 0; i < n; i++ {
		e.startNode(i, cluster.MakePersister())
	}
}

// startNode (re)creates node i's Raft+RSM+KVServer on top of persister,
// registers its services with the network, and marks it alive. Used both
// for the initial build and for Restart (same persister, fresh volatile
// state).
func (e *Engine) startNode(i int, persister *cluster.Persister) {
	n := e.cfg.NumNodes
	ends := make([]*network.ClientEnd, n)
	for j := 0; j < n; j++ {
		end := e.net.MakeEnd(fmt.Sprintf("n%d->n%d", i, j))
		e.net.Connect(fmt.Sprintf("n%d->n%d", i, j), network.NodeName(j))
		e.net.SetOwner(fmt.Sprintf("n%d->n%d", i, j), network.NodeName(i))
		ends[j] = end
	}

	objs := store.StartKVServer(ends, i, persister, e.cfg.MaxRaftState, e.clk, e, i)
	kv := objs[0].(*store.KVServer)
	rf := objs[1].(*raft.Raft)

	srv := network.MakeServer()
	srv.AddService(network.MakeService(kv))
	srv.AddService(network.MakeService(rf))
	e.net.AddServer(network.NodeName(i), srv)
	e.net.SetAlive(network.NodeName(i), true)

	e.nodes[i] = &node{persister: persister, kv: kv, rf: rf, alive: true}
}

// --- trace.Sink: Engine itself collects every event and checks invariants ---

func (e *Engine) Emit(ev trace.Event) { e.record(ev) }

func (e *Engine) record(ev trace.Event) {
	e.mu.Lock()
	e.events = append(e.events, ev)
	switch ev.Kind {
	case "become_leader":
		if existing, ok := e.leaderOfTerm[ev.Term]; ok && existing != ev.Node {
			e.violations = append(e.violations, Violation{
				Kind:   "election_safety",
				Detail: fmt.Sprintf("two leaders in term %d: node%d and node%d", ev.Term, existing, ev.Node),
				At:     ev.T,
			})
		} else {
			e.leaderOfTerm[ev.Term] = ev.Node
		}
	case "apply":
		if ev.Data != nil {
			if idx, ok := ev.Data["index"].(int); ok {
				cmd, _ := ev.Data["command"].(string)
				if existing, ok := e.commandAt[idx]; ok && existing != cmd {
					e.violations = append(e.violations, Violation{
						Kind:   "state_machine_safety",
						Detail: fmt.Sprintf("index %d applied as %q on node%d but %q elsewhere", idx, cmd, ev.Node, existing),
						At:     ev.T,
					})
				} else {
					e.commandAt[idx] = cmd
				}
			}
		}
	case "op_invoke", "op_return":
		e.recordOpEvent(ev.Kind, int64(ev.T), ev.Data)
	}
	listeners := append([]func(trace.Event){}, e.listeners...)
	e.mu.Unlock()

	for _, l := range listeners {
		l(ev)
	}
}

// OnEvent registers a callback invoked synchronously (from whatever
// goroutine emitted the event) for every trace event from this point on.
func (e *Engine) OnEvent(f func(trace.Event)) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.listeners = append(e.listeners, f)
}

// EventsSince returns every trace event recorded from index since onward
// (clamping an out-of-range since to 0), without copying events the
// caller already has. The frontend polls this every animation frame, so
// unlike a plain Events()-then-slice, this must not cost O(total history)
// per call -- that was a real performance bug caught while profiling why
// the UI felt sluggish at high speed (see viz/README.md).
func (e *Engine) EventsSince(since int) []trace.Event {
	e.mu.Lock()
	defer e.mu.Unlock()
	if since < 0 || since > len(e.events) {
		since = 0
	}
	return append([]trace.Event{}, e.events[since:]...)
}

// Safe reports whether no safety invariant has been violated yet.
func (e *Engine) Safe() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return len(e.violations) == 0
}

// Violations returns every detected safety violation so far.
func (e *Engine) Violations() []Violation {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]Violation{}, e.violations...)
}

// --- driving virtual time ---

// Step advances to the next pending virtual-time event and reports
// whether there was one.
func (e *Engine) Step() bool {
	advanced := e.clk.Step()
	// the full log-matching scan copies and compares every node's whole log;
	// doing it every step made each step slower as logs grew. Every 100 steps
	// still catches a violation within a fraction of a simulated second.
	e.steps++
	if e.steps%100 == 0 {
		e.checkLogMatching()
	}
	return advanced
}

// HasPending reports whether any timer remains scheduled.
func (e *Engine) HasPending() bool { return e.clk.HasPending() }

// Now returns the current virtual time.
func (e *Engine) Now() clock.Duration { return e.clk.Now() }

// Stuck reports whether the clock's quiescence detector gave up (see
// clock.Clock.Drain); a stuck simulation is a bug, not a normal state.
func (e *Engine) Stuck() bool { return e.clk.Stuck() }

// --- chaos / game controls ---

func (e *Engine) Crash(i int) {
	if i < 0 || i >= len(e.nodes) {
		return
	}
	e.net.SetAlive(network.NodeName(i), false)
	e.nodes[i].rf.Kill()
	e.nodes[i].alive = false
	e.record(trace.Event{T: e.clk.Now(), Node: i, Kind: "crash"})
}

func (e *Engine) Restart(i int) {
	if i < 0 || i >= len(e.nodes) || e.nodes[i].alive {
		return
	}
	persister := e.nodes[i].persister
	e.startNode(i, persister)
	e.record(trace.Event{T: e.clk.Now(), Node: i, Kind: "restart"})
}

// Wipe crashes node i and throws away its persisted Raft state and
// snapshot, so its next Restart comes back blank (term 0, empty log). Raft
// assumes stable storage survives crashes, so this can expose real safety
// violations -- that is the point of the "amnesia" tool.
func (e *Engine) Wipe(i int) {
	if i < 0 || i >= len(e.nodes) {
		return
	}
	if e.nodes[i].alive {
		e.Crash(i)
	}
	e.nodes[i].persister = cluster.MakePersister()
	e.record(trace.Event{T: e.clk.Now(), Node: i, Kind: "amnesia"})
}

// Partition splits nodes into unreachable groups, e.g. [][]int{{0,1},{2,3,4}}.
func (e *Engine) Partition(groups [][]int) {
	named := make([][]string, len(groups))
	for gi, g := range groups {
		for _, i := range g {
			named[gi] = append(named[gi], network.NodeName(i))
		}
	}
	e.net.SetPartition(named)
	e.record(trace.Event{T: e.clk.Now(), Kind: "partition", Data: map[string]any{"groups": groups}})
}

func (e *Engine) Heal() {
	e.net.Heal()
	e.record(trace.Event{T: e.clk.Now(), Kind: "heal"})
}

func (e *Engine) SetNetwork(cfg network.Config) { e.net.SetConfig(cfg) }

// --- client requests (manual or background traffic) ---

// newClient returns a fresh *client.Client for a single call. A Client's
// leader/seq fields aren't safe to share across concurrently-running
// goroutines, and the background traffic generator and manual
// submitGet/submitPut calls can genuinely overlap in time, so each call
// gets its own Client wrapping the same (lazily built, then cached)
// ClientEnds.
func (e *Engine) newClient() *client.Client {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.trafficEnds == nil {
		e.trafficEnds = make([]*network.ClientEnd, e.cfg.NumNodes)
		for j := 0; j < e.cfg.NumNodes; j++ {
			name := fmt.Sprintf("client->n%d", j)
			e.trafficEnds[j] = e.net.MakeEnd(name)
			e.net.Connect(name, network.NodeName(j))
		}
	}
	return client.New(e.trafficEnds, e.clk, e)
}

// trafficInterval is the virtual-time gap between background requests.
const trafficInterval = 300 * time.Millisecond

// SetTraffic turns the background client-traffic generator on or off: a
// small, fixed keyspace of read-modify-write Puts and Gets, issued
// continuously while on. This is what gives the game's score ("successful
// requests while safe") and the serving/unavailable indicator something to
// measure under chaos.
func (e *Engine) SetTraffic(on bool) {
	e.mu.Lock()
	wasOn := e.trafficOn
	e.trafficOn = on
	if on && !wasOn {
		// new generation: any loop still scheduled from before stops itself,
		// so toggling traffic off/on never leaves two loops running
		e.trafficGen++
	}
	gen := e.trafficGen
	e.mu.Unlock()
	if on && !wasOn {
		e.scheduleTraffic(gen)
	}
}

func (e *Engine) scheduleTraffic(gen uint64) {
	e.clk.AfterFunc(trafficInterval, func() {
		e.mu.Lock()
		live := e.trafficOn && e.trafficGen == gen
		e.mu.Unlock()
		if !live {
			return
		}
		e.fireTrafficRequest()
		e.scheduleTraffic(gen)
	})
}

func (e *Engine) fireTrafficRequest() {
	if e.trafficClient == nil {
		e.trafficClient = e.newClient()
	}
	c := e.trafficClient
	e.mu.Lock()
	e.trafficSeq++
	seq := e.trafficSeq
	key := trafficKeys[seq%len(trafficKeys)]
	version := e.trafficVersions[key]
	e.mu.Unlock()

	go func() {
		if seq%3 == 0 {
			if err := c.Put(key, fmt.Sprintf("v%d", seq), version); err == proto.OK {
				e.mu.Lock()
				e.trafficVersions[key] = version + 1
				e.mu.Unlock()
			}
		} else {
			c.Get(key)
		}
	}()
}

func (e *Engine) nextResultID() uint64 {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.nextReq++
	return e.nextReq
}

func (e *Engine) setResult(id uint64, r Result) {
	e.mu.Lock()
	e.results[id] = r
	e.mu.Unlock()
}

// Result polls a request submitted via SubmitGet/SubmitPut.
func (e *Engine) Result(id uint64) (Result, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	r, ok := e.results[id]
	return r, ok
}

// getUIClient returns the one long-lived client behind the UI's Get/Put
// buttons. A fresh client per request would forget the leader every time and
// always start by asking a non-leader (ErrWrongLeader).
func (e *Engine) getUIClient() *client.Client {
	if e.uiClient == nil {
		e.uiClient = e.newClient()
	}
	return e.uiClient
}

// SubmitGet issues a Get asynchronously (it can only complete as virtual
// time advances via Step/Play) and returns a request id to poll via
// Result.
func (e *Engine) SubmitGet(key string) uint64 {
	id := e.nextResultID()
	e.setResult(id, Result{})
	c := e.getUIClient()
	go func() {
		v, ver, err := c.Get(key)
		e.setResult(id, Result{Done: true, Value: v, Version: ver, Err: string(err)})
	}()
	return id
}

// SubmitPut issues a read-then-conditional-write asynchronously, like a real
// client would: Get the key's current version, then Put at that version
// (0 for a new key). Puts are conditional, so a fixed version would fail
// with ErrVersion/ErrMaybe on every write after the first. See SubmitGet.
func (e *Engine) SubmitPut(key, value string) uint64 {
	id := e.nextResultID()
	e.setResult(id, Result{})
	c := e.getUIClient()
	go func() {
		_, version, gerr := c.Get(key)
		if gerr == proto.ErrNoKey {
			version = 0
		}
		err := c.Put(key, value, version)
		e.setResult(id, Result{Done: true, Version: version, Err: string(err)})
	}()
	return id
}

// NodeView is one node's state as rendered by the UI.
type NodeView struct {
	ID                int
	Alive             bool
	Term              int
	Role              string
	VotedFor          int
	LogLen            int
	CommitIndex       int
	LastApplied       int
	LastIncludedIndex int
}

// ClusterSnapshot is the full state the UI needs to redraw every frame.
type ClusterSnapshot struct {
	Now        clock.Duration
	Safe       bool
	Stuck      bool
	Nodes      []NodeView
	Violations []Violation
	EventCount int
	AliveCount int
	Serving    bool // majority alive, i.e. a leader can be elected/can commit
}

func (e *Engine) Snapshot() ClusterSnapshot {
	e.mu.Lock()
	nodes := make([]NodeView, len(e.nodes))
	alive := 0
	for i, n := range e.nodes {
		if n.alive {
			alive++
			d := n.rf.Debug()
			nodes[i] = NodeView{
				ID: i, Alive: true, Term: d.Term, Role: d.State, VotedFor: d.VotedFor,
				LogLen: d.LogLen, CommitIndex: d.CommitIndex, LastApplied: d.LastApplied,
				LastIncludedIndex: d.LastIncludedIndex,
			}
		} else {
			nodes[i] = NodeView{ID: i, Alive: false}
		}
	}
	violations := append([]Violation{}, e.violations...)
	eventCount := len(e.events)
	e.mu.Unlock()

	return ClusterSnapshot{
		Now: e.clk.Now(), Safe: len(violations) == 0, Stuck: e.clk.Stuck(),
		Nodes: nodes, Violations: violations, EventCount: eventCount,
		AliveCount: alive, Serving: alive > len(e.nodes)/2,
	}
}
