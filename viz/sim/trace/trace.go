// Package trace defines the event schema emitted by the one-line trace()
// calls added to the copied Raft/RSM/KV code, and a small helper for
// capturing the call site via runtime.Caller so the frontend's code-view
// panel can highlight the exact source line that produced each event.
package trace

import (
	"runtime"
	"strings"
	"time"
)

// Event is one entry in the simulation's trace stream. Kind is one of the
// fixed set listed in viz/README.md (election_timeout, become_leader,
// append_entries_sent, apply, msg_dropped, ...). Data holds kind-specific
// fields (e.g. the conflict term/index/length used for AppendEntries fast
// backup) and must only ever contain plain, JSON-marshalable values.
type Event struct {
	T    time.Duration  `json:"t"`
	Node int            `json:"node"`
	Term int            `json:"term"`
	Kind string         `json:"kind"`
	Fn   string         `json:"fn"`
	File string         `json:"file"`
	Line int            `json:"line"`
	Data map[string]any `json:"data,omitempty"`
}

// Sink receives trace events as they're emitted. Implementations must be
// safe for concurrent use; the simulation runs many goroutines.
type Sink interface {
	Emit(Event)
}

// NullSink discards every event; used where no UI is attached (e.g. unit
// tests of the copied logic).
type NullSink struct{}

func (NullSink) Emit(Event) {}

// Caller reports the function, file and line of the trace() call site,
// skip frames up from Caller itself (skip=1 means "my caller's caller",
// i.e. the code that called trace(), when trace() calls Caller(1)).
func Caller(skip int) (fn, file string, line int) {
	pc, f, l, ok := runtime.Caller(skip + 1)
	if !ok {
		return "", "", 0
	}
	file, line = f, l
	if details := runtime.FuncForPC(pc); details != nil {
		name := details.Name()
		if i := strings.LastIndex(name, "."); i >= 0 {
			name = name[i+1:]
		}
		fn = name
	}
	return fn, file, line
}
