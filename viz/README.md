# Raft Under Fire — simulator (work in progress)

An in-browser, deterministic simulator of this repo's Raft-replicated KV
store, compiled to WebAssembly. This README documents the architecture and,
per the project's hard constraint, the exact list of changes made in the
copies under `viz/sim/` relative to the real code under `internal/`.

**Status:** the Go simulation core (deterministic clock, simulated network,
copied Raft/RSM/KV-store/client, 5-node engine with safety checking, and a
WASM JS API) is built and compiles both natively (`go build ./...`) and as
WebAssembly (`GOOS=js GOARCH=wasm go build ./viz/sim/wasm/`). The frontend
(`viz/web`, Vite + TypeScript, plain Canvas 2D — no PixiJS/original art
beyond a simple procedural bomber shape yet) now also has a game layer: a
wandering bomber that plants bombs and crashes nodes, four chaos modes
(Calm/Gremlin/Storm/Nightmare), a "kill the leader" button, a background
client-traffic generator, a serving/unavailable indicator with the "Raft
prefers being unavailable to being wrong" explanation, and a score counter.
`make viz` builds both into a single static site at `viz/dist/` (open
`viz/dist/index.html` or `npx serve viz/dist`).

Verified working end to end in a browser: a leader gets elected, heartbeats
and log replication animate between nodes, full Put/Get requests commit
end-to-end (client_request → log_append → replication →
commit_index_advance → apply → kv_state_change → client_reply, score
incrementing), the safety badge stays green throughout, Gremlin chaos
crashes nodes down to 1/5 alive and the UI correctly reports "unavailable
✗ (1/5 alive)" with no false safety violation, restarting crashed nodes
lets the cluster recover and re-elect, and the event log streams real
trace events with correct virtual timestamps throughout (a bug where
KVServer-level trace events — client_request/client_reply/kv_state_change —
always showed `T=0ms` because `store.KVServer.trace()` never read the
virtual clock was caught during this verification and fixed: it now reads
`kv.mu.C.Now()`, the clock already reachable through the `clock.Mutex` it
embeds).

## Why the paths differ from a typical MIT 6.5840 lab checkout

This repo is not laid out as the standard 6.5840 lab tree (`raft1/`,
`kvraft1/`, `kvsrv1/`, `labrpc/`, `tester1/`); it's a small
deployment-oriented rewrite. The equivalents used here are:

| Lab concept | This repo |
|---|---|
| `raft1/raft.go` | `internal/raft/raft.go` |
| `kvraft1/rsm` | `internal/kv/store/rsm` |
| `kvraft1` server/client | `internal/kv/store` (server.go, txn.go) + `internal/client` |
| `kvsrv1/rpc` (types) | `internal/kv/proto` |
| `labrpc` | `internal/rpc` (channel-based simulated network with its own `Reliable`/`Enable`/`Connect`/drop/delay support) |

`internal/raft`, `internal/kv/store`, `internal/client`, `internal/kv/proto`,
`internal/rpc`, `internal/cluster`, `internal/codec`, `internal/raftapi` and
everything under `cmd/` are **untouched** by this work — verified by `git
diff --stat internal/ cmd/` being empty. `internal/rpc` ("labrpc" here) is
treated the same as the lab's `labrpc`: left alone, not copied. The
simulator's network (see below) is new code built to the same `Call(...)`
contract, not a copy of `internal/rpc`.

## Architecture

```
viz/sim/clock    virtual time + seeded RNG + quiescence detection (new code)
viz/sim/trace    trace event schema + call-site capture (new code)
viz/sim/network  simulated network: latency/jitter/drop/dup/partition,
                 per-message trace events (new code, not a copy of
                 internal/rpc — see "Simulated network" below)
viz/sim/raft     copy of internal/raft, adapted (see "Changes" below)
viz/sim/kv/store/rsm   copy of internal/kv/store/rsm, adapted
viz/sim/kv/store       copy of internal/kv/store, adapted
viz/sim/client         copy of internal/client, adapted
viz/sim/engine   wires 5 nodes together over viz/sim/network, drives
                 virtual time, checks safety invariants from the trace
                 stream (new code)
viz/sim/wasm     GOOS=js GOARCH=wasm main exporting window.raftUnderFire
                 (new code)
viz/web          Vite + TypeScript frontend: Canvas 2D node ring, message
                 animation, event log, safety badge, play/step/speed and
                 crash/restart/partition/heal/drop-rate controls, plus a
                 game layer (src/bomber.ts): a wandering bomber, chaos
                 modes, kill-the-leader, serving indicator, score, and a
                 code-view panel (src/codeview.ts) that highlights the
                 live raft.go line behind whatever trace event just fired,
                 following a chosen node or "whoever is leader". Owns no
                 timer loop of its own in Go; the frontend calls step()
                 repeatedly from requestAnimationFrame, which is what
                 keeps pause/step/speed/replay simple (new code)
```

### Determinism model

Every goroutine in the copied code blocks only on channels, `sync.Cond`, or
`viz/sim/clock`'s virtual-time primitives (`Sleep`/`After`/`NewTicker`) —
never on real wall-clock time or unseeded `math/rand`. `clock.Clock` holds a
min-heap of pending timers and a seeded RNG; `Step()` advances to the next
timer, fires everything due at that instant, and calls `Drain()`.

`Drain()` is a documented heuristic, not a formally verified barrier: every
mutex in the copied code is a `clock.Mutex` (a drop-in `sync.Mutex`
replacement) that bumps a shared atomic counter on every successful
`Lock()`. `Drain()` yields the scheduler (`runtime.Gosched`) until that
counter has been stable for 200 consecutive checks, which in practice means
every goroutine has either finished its work or durably parked on a clock
wait or a channel/cond wait that only an already-counted goroutine can
satisfy. If the counter never stabilizes (a real bug — a goroutine spinning
without ever touching a `clock.Mutex`), `Drain` gives up after a bound
number of checks and `Clock.Stuck()` reports it, surfaced through
`engine.Engine.Stuck()` / the WASM `stuck()` export, instead of hanging the
browser tab forever.

`testing/synctest` (stable in the Go toolchain used here) was considered
and rejected: it requires a real `*testing.T` and only supports one-shot
run-to-completion bubbles, not a simulation stepped interactively from
outside across many separate calls spanning a UI's lifetime.

### Simulated network (`viz/sim/network`)

Built fresh rather than copied, matching the spec's "simulated network,
modeled on labrpc" — `internal/rpc` already plays the role of labrpc for
the real system and stays untouched. `network.ClientEnd.Call(svcMeth,
args, reply) bool` matches `internal/rpc.ClientEnd.Call`'s signature
exactly, so the copied Raft/RSM/KV code only needed its import path (and
the `*rpc.ClientEnd` → `*network.ClientEnd` type name) changed, never its
logic. It reuses only `internal/rpc`'s pure, time/rand-free
`Marshall`/`Unmarshall` helpers. Adds over the original: a partition
matrix (`SetPartition`/`Heal`, groups of unreachable node names),
per-call crash/alive toggling (`SetAlive`), and a visible
`Message{ID,From,To,Kind,SendTime,Arrive,Dropped}` emitted to an
`EventSink` for every send and delivery, driven entirely by
`viz/sim/clock` (`AfterFunc` for delivery delay) instead of real
`time.Sleep`/`math/rand`.

### Exact changes in each copy

**`viz/sim/raft` vs `internal/raft`:**
- `math/rand`/`time.Now`/`time.Since`/`time.Sleep`/`time.After` replaced by
  `*clock.Clock` methods (`Int63`, `Now`, `Since`, `Sleep`, `After`).
  `lastHeard`/`electionTimeout` changed from `time.Time`/`time.Duration` to
  `clock.Duration` (a virtual-time reading); arithmetic is unchanged.
- `mu sync.Mutex` → `mu clock.Mutex` (adds activity-counter bumping on
  `Lock()`, used by `Drain()`; `Unlock()` and `sync.Cond` built on it are
  unaffected).
- `peers []*rpc.ClientEnd` → `peers []*network.ClientEnd`.
- Added `dead atomic.Bool` + `Kill()`/`killed()`, with early-return checks
  in `ticker()`, `applier()` and `StartHeartBeat()`. This is standard MIT
  6.5840 lab scaffolding (every lab's starter `raft.go` ships a
  `Kill()`/`killed()` pair so the tester can crash peers in-process) that
  this deployment-oriented repo omitted because it kills real OS processes
  instead. No election/replication/commit/snapshot logic was changed to
  add it — only early returns at loop tops, plus a `Broadcast()` in `Kill()`
  so a parked `applyCond.Wait()` notices promptly.
- Added one-line `trace()` calls at exactly the points listed in the task
  spec: `election_timeout`, `become_candidate`, `request_vote_sent`,
  `vote_granted`, `vote_denied` (with reason), `become_leader`,
  `step_down`, `heartbeat_sent`/`append_entries_sent`,
  `append_entries_ok`, `append_entries_reject` (with xterm/xindex/xlen),
  `next_index_update`, `log_append`, `commit_index_advance`, `apply` (with
  a `fmt.Sprintf("%v", ...)` summary of the applied command, used by the
  engine's state-machine-safety check), `snapshot_taken`,
  `install_snapshot_sent`, `install_snapshot_applied`.
- `Make()` takes three added parameters: `*clock.Clock`, a `trace.Sink`,
  and a node id, threaded through from `viz/sim/engine`.
- One dead `return` after an unreachable point was not an issue here (the
  original has none); no other statements were removed or reordered.

**`viz/sim/kv/store/rsm` vs `internal/kv/store/rsm`:**
- `math/rand/v2`'s `rand.Int64()` → `clk.Int63()`; `time.NewTicker` →
  `clk.NewTicker` (same `.C`/`.Stop()` shape).
- `mu sync.Mutex` → `mu clock.Mutex`, for the same Drain-visibility reason
  as Raft's.
- `*rpc.ClientEnd` → `*network.ClientEnd`; `raft.Make` call site updated
  for its three added parameters.
- `MakeRSM` takes three added parameters (clock, sink, node), passed
  straight through to `raft.Make`.
- Dropped one line of dead code after the original's infinite `for{}` loop
  in `Submit` (an unreachable `return proto.ErrWrongLeader, nil` the Go
  compiler already treats as unreachable); no behavior change.

**`viz/sim/kv/store` vs `internal/kv/store`:**
- `mu sync.Mutex` → `mu clock.Mutex`.
- `StartKVServer` takes three added parameters (clock, sink, node),
  passed through to `rsm.MakeRSM`; dropped the `gid cluster.Tgid`
  parameter the original takes for sharding, since the simulator is a
  single replica group (not used by `doGet`/`doPut`/`doTxn`/`Snapshot`/
  `Restore`, which are unchanged).
- Added one-line `trace()` calls: `client_request`/`client_reply` around
  each RPC handler (Get/Put/Txn), `kv_state_change` wherever `doPut`/
  `doTxn` write `kv.store`. `txn.go` is an unmodified copy.

**`viz/sim/client` vs `internal/client`:**
- The original dials a `*transport.Peer` over real TCP to reach a
  `cmd/kvgateway` process, which doesn't exist in-process in the
  simulator. This copy calls `KVServer.Get/Put/Txn` directly over
  `*network.ClientEnd` instead (same RPC names/shapes), and
  `time.Sleep(100ms)` backoff → `clk.Sleep(100ms)`. The leader-sticky
  retry loop itself (try server, advance on `ErrWrongLeader`/failure,
  remember the last leader) is unchanged.
- Added a `sink trace.Sink` (threaded through `New`) and `op_invoke`/
  `op_return` trace calls in `Get`/`Put`/`Txn`, bracketing the call's
  true client-visible invoke/return boundary with a unique `call_id` —
  used by the engine's linearizability checker (see below). Purely
  observational, like every other trace() addition.
- `New` no longer takes `addrs []string`; it takes the already-constructed
  `[]*network.ClientEnd` and `*clock.Clock` instead, since there's no DNS/
  TCP dialing step in the simulator. The engine hands out a **fresh**
  `*Client` per call rather than sharing one across concurrent goroutines
  — the original's `leader`/`seq` fields were written assuming a single
  caller using the Client sequentially, which doesn't hold once a
  background traffic generator and manual requests can overlap.

**Reused unchanged (not copied):** `internal/cluster` (Persister has no
time/rand dependency), `internal/codec`, `internal/raftapi`,
`internal/kv/proto`, and `internal/rpc`'s pure `Marshall`/`Unmarshall`
helpers.

## Invariant checking (current scope)

`engine.Engine` implements `trace.Sink` and checks invariants directly
from the event stream as it's recorded:

- **Election Safety**: at most one `become_leader` per term; a second
  node claiming leadership in an already-claimed term is flagged.
- **State Machine Safety (simplified)**: for a given log index, every
  node's `apply` event must carry the same command summary; a mismatch is
  flagged.
- **Linearizability** (`viz/sim/engine/linearizability.go`): a
  [porcupine](https://github.com/anishathalye/porcupine) nondeterministic
  model of the Get/Put subset of `store.KVServer` (Txn isn't modeled).
  `viz/sim/client.Client.Get/Put/Txn` emit `op_invoke`/`op_return` trace
  events (added relative to the original, distinct from
  `store.KVServer`'s own per-RPC-attempt `client_request`/`client_reply`
  events) carrying a `call_id` so the engine can pair each call's true
  invoke/return boundary regardless of how many RPC attempts it took.
  `ErrMaybe` (the client's "a previous attempt might have silently
  succeeded" case, which only arises on a Put's *second* distinct
  attempt) is modeled nondeterministically: both "the write never landed"
  and "the write took effect" are valid continuations, since both are
  real possibilities. Unlike the other two checks, this one runs
  **on demand** (`Engine.CheckLinearizable(timeoutMs)` / WASM
  `checkLinearizable(timeoutMs)`, wired to a "Check linearizability"
  button), not on every event: linearizability checking is NP-hard in
  general, and re-running it from scratch on every trace event would make
  the simulation unusable at speed.

- **Log Matching** (`viz/sim/engine/log_matching.go`): after every
  `Step()`, compares every pair of currently-alive nodes' log terms (via
  `raft.Raft.LogTerms()`, another read-only accessor added purely for this)
  over their overlapping index range. If two logs ever disagree at some
  index and then agree again at a later index, that's a Log Matching
  violation (impossible under correct Raft) and gets flagged. `LogTerms()`
  must only ever be called from outside a peer's own trace()/RPC-handler
  call stack (e.g. the engine's driver code, after a `Step()` has
  quiesced) since it takes the same `rf.mu` that may already be held by
  whatever triggered the check -- it is not reentrant.

`Engine.Safe()` / the WASM `safe()` export report false once any
violation is recorded (the linearizability check adds to the same
`Violations()` list on a confirmed `Illegal` result), with
`Engine.Violations()` / `violations()` describing exactly what broke.

**Bug caught and fixed while building this:** the linearizability checker
initially reported `Illegal` on a perfectly healthy, uncontested run
(Calm chaos, 5/5 alive). The cause was a model bug, not a real violation:
`engine.asInt` (used to pull the `version` field back out of a trace
event's `map[string]any` for the in-process, non-JSON-roundtripped path)
didn't handle `proto.Tversion` (a named `uint64`), silently returning 0
for every Put's requested version — so the model thought every Put
against an existing key was requesting version 0 and rejected it as
invalid. Fixed by adding a `case proto.Tversion` to `asInt`; re-verified
`Ok` afterward on both a healthy Calm run and, more meaningfully, a Storm
run with real crashes/drops down to 2/5 nodes alive (which exercises the
retry and `ErrMaybe` paths the Calm case never touches).

## JS API (`viz/sim/wasm`, `window.raftUnderFire`)

Implemented so far: `init(seed, config)`, `step()`, `hasPending()`,
`now()`, `safe()`, `stuck()`, `violations()`, `eventsSince(n)`,
`getState()` (now includes per-node term/role/log/commit/alive via
`raft.Raft.Debug()`, a read-only accessor added purely for the UI, plus
`AliveCount`/`Serving`), `crash(node)`, `restart(node)`,
`partition(groups)`, `heal()`,
`setNetwork({latencyMs,jitterMs,dropRate,dupRate})`,
`submitGet(key)`/`submitPut(key,value,version)` (return a request id),
`result(id)`, `setTraffic(on)` (background read-modify-write Put/Get
generator over a small fixed keyspace, driven by the virtual clock), and
`checkLinearizable(timeoutMs)` (on-demand porcupine check; see "Invariant
checking" above). There is no internal timer loop in Go — the
frontend is expected to call `step()` repeatedly (e.g. from
`requestAnimationFrame`) to advance the simulation, which is what keeps
pause/step/speed-control and replay determinism straightforward. `play()`/
`pause()`/`setSpeed()` as named JS-API entry points, and `onEvent()`
callback-style streaming (vs. the current `eventsSince` polling), are not
implemented yet.

## Code-view panel (`viz/web/src/codeview.ts`)

`make viz` copies `viz/sim/raft/raft.go` verbatim to
`viz/web/public/raft-source.txt` (gitignored, regenerated on every
build), which the frontend fetches once at startup and renders as a
line-numbered `<pre>`-like view with light keyword coloring (no syntax
highlighter dependency). Every trace event already carries `fn`/`file`/
`line` (captured via `runtime.Caller` in `trace.Caller`, see "Architecture"
above); `CodeView.onEvent` matches `file` against `"raft.go"`, and if the
event's node matches the current follow target (a specific node, or
"leader" -- whoever `getState()` currently reports as leader), it
highlights that line, scrolls it into view, and keeps the last few
highlighted lines faintly lit (a fading trail) so the panel reads like
watching the leader (or whichever node you pick) execute, per the spec's
"like watching a CPU execute" framing. Only `raft.go` is wired up today;
the rsm/kv-store/client/network source aren't shown in the panel.

## Building

```bash
make viz
npx serve viz/dist   # or open viz/dist/index.html directly
```

`make viz` builds the WASM binary into `viz/web/public/raftviz.wasm`, then
runs `npm install && npm run build` in `viz/web`, which emits the full
static site (HTML/JS/CSS/wasm) into `viz/dist/`.

## Tests

`viz/sim/clock`, `viz/sim/network` have unit tests exercising ordering,
deterministic replay, ticking, drop/dup, crash and partition behavior.
`viz/sim/engine/fuzz_test.go`'s `TestStormChaosInvariants` is the spec's
headless multi-seed fuzz test: 10 seeds, Storm-level chaos (random
crash/restart/partition/heal every 500ms of virtual time, 20% packet
loss) for 60 virtual seconds each, asserting every safety invariant holds
throughout and linearizability holds at the end.

Per the user's instruction during this work, **none of these have been
executed in this session** — only `go build`/`go vet` were used to check
the code (the fuzz test specifically was verified with `go vet`, which
compiles test files without running them). Run them yourself before
relying on this code:

```bash
go test ./viz/sim/...                                              # unit tests
go test ./viz/sim/engine/ -run TestStormChaosInvariants -v          # fuzz test
```

Treat this code as compiled-but-unverified at the invariant/behavior
level until you do -- "builds clean" and "actually correct" are not the
same claim, and this session already found two real bugs (see above)
that `go build`/`go vet` couldn't have caught, only running the thing
could.

## The election cinematic (`viz/web/src/election.ts`)

This is the centerpiece interaction, added after the user pointed at
TigerBeetle's simulator (sim.tigerbeetle.com) and asked for the UI to
*feel* like a game built around "the leader dies -> election -> it keeps
serving" story, not a traffic dashboard. Their simulator's hand-illustrated
character art is a real art/illustration pipeline this session can't
responsibly reproduce (and the hard constraint forbids copying their
assets/branding anyway); what's copied in spirit, not asset, is the
structure -- a radial replica layout, glowing role-colored nodes, and a
status bubble floating above whichever node is doing something notable.

`ElectionDirector` is a small frontend-only state machine (`idle` ->
`inProgress` -> `crowning` -> `idle`) that watches `getState()` snapshots:
it has no effect on the simulation's correctness, only on how the
frontend paces itself and what it draws.

- **Trigger**: the moment a previously-known leader stops being reported
  (crashed by chaos, the bomber, or the "Kill the leader" button -- all
  go through the same `crash()` call, so detection doesn't care which).
- **`inProgress`**: the canvas dims, a pulsing red
  "⚡ LEADER DOWN — ELECTION IN PROGRESS ⚡" banner takes over the top of
  the stage, candidates get a bright pulsing ring and a "voting..."
  status bubble, and -- this is the "everything stops" part of the ask --
  `App.frame()` stops honoring the play/speed controls and instead
  advances exactly one `step()` every ~110ms of real time, so the
  election is watchable instead of blurring past at whatever speed was
  previously selected.
- **`crowning`**: once a new leader is reported, a gold
  "👑 nX ELECTED — TERM N" banner replaces it, a crown animates onto the
  winner with a bob and a bright glow, held for 1.6s.
- **resume**: play/speed are restored to exactly what they were before
  the leader went down ("...and after that it continues to serve").

Routine heartbeat/append-entries traffic is deliberately **not** drawn as
flying messages anymore (only `request_vote_sent`/`install_snapshot_sent`
are) -- per the user's "it's not about heavy writes and reads," so the
vote beams during an election actually stand out instead of competing
with constant replication dots. Nodes also got a real visual pass: radial
gradient fill, glow halo (brighter for the leader, brightest and pulsing
for candidates mid-election), and a small drawn crown above the leader --
replacing the original flat colored circles.

## Not yet done

- Polished original art (the bomber is still a procedural triangle, not a
  designed character/sprite), a guided tour.
- `onEvent` push-style streaming API; `play()`/`pause()`/`setSpeed()` JS
  entry points (today the frontend must drive `step()` itself, which is
  what `src/main.ts`'s requestAnimationFrame loop does).
- Reply-direction message animation (only request-direction sends are
  animated today: vote/append/heartbeat/snapshot "sent" events carry a
  `to` field; reply events don't yet).

**Added but only partly verified (session ended on a usage-limit warning
before full browser testing):**
- Manual client request UI: key/value fields + Get/Put buttons in
  `src/main.ts`, polling `result(id)` each frame. Verified live: a manual
  Put and Get both round-tripped correctly (score incremented, event log
  showed the matching `client_request`/`client_reply`).
- Drag-to-partition: dragging across the node ring in `src/main.ts` splits
  nodes into two groups by which side of the drag line they fall on and
  calls `partition()`, drawing a persistent red "crack" until `Heal()`; a
  short drag (under 15px) instead tries to defuse a bomb, same as a plain
  click did before. Compiles clean (`tsc --noEmit`) but **not yet
  exercised in the browser** -- the geometry (cross-product side test) is
  straightforward but unverified; check it actually splits the right
  nodes before relying on it.
- Sound effects (`src/sound.ts`): WebAudio-only oscillator blips/booms/
  chimes, off by default via a "Sound" checkbox -- no audio files. Blip on
  `request_vote_sent`/`install_snapshot_sent`, chime on
  `commit_index_advance`, boom on `crash` (heartbeats/append-entries are
  deliberately not wired to sound; at their natural frequency it would be
  an unlistenable buzz). **Not yet exercised in the browser** -- built but
  unheard; the browser's console showed no errors on load, which is as far
  as this session got.
- ~~`Drain()`'s Gosched-based quiescence check appears to cost real
  wall-clock time per `step()` call~~ -- profiled and fixed. A native Go
  run (`engine.New` + a tight `Step()` loop, no WASM/UI involved) showed
  `Step()` itself is fast (~85µs/step; 295 steps advanced 5s of virtual
  time in 25ms of real time), so `Drain()` was never the bottleneck. The
  actual cause: `jsEventsSince` called `Engine.Events()`, which copied the
  **entire** event history on every single poll (every animation frame)
  just to slice off the new tail -- an O(total history) cost per frame
  that gets worse the longer a session runs. Replaced `Events()` with
  `EventsSince(since)`, which copies only the new events directly under
  the lock. Reverified in-browser: at max speed with Storm chaos and
  traffic on, 5 real seconds now advances >10 virtual seconds (previously
  a few hundred ms), with no loss of correctness.
