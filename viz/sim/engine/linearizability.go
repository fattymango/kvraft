package engine

import (
	"fmt"
	"time"

	"github.com/anishathalye/porcupine"
	"kvapp/internal/kv/proto"
)

// kvEntry is one key's value in the linearizability model's state.
type kvEntry struct {
	Value   string
	Version proto.Tversion
}

// kvState is the model's state: the whole key/value store as the client
// sees it through Get/Put (Txn isn't modeled -- see viz/README.md).
type kvState map[string]kvEntry

func cloneKVState(s kvState) kvState {
	out := make(kvState, len(s))
	for k, v := range s {
		out[k] = v
	}
	return out
}

func kvStateEqual(a, b kvState) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}

type kvInput struct {
	Op      string
	Key     string
	Value   string
	Version proto.Tversion
}

type kvOutput struct {
	Value   string
	Version proto.Tversion
	Err     string
}

// kvModel is a nondeterministic sequential specification of the Get/Put
// subset of store.KVServer, used to check linearizability of the
// op_invoke/op_return history viz/sim/client records.
//
// ErrMaybe is the interesting case: the client only returns it when a
// retried Put got ErrVersion on its *second* distinct attempt, meaning an
// earlier attempt (using the exact same key/value/version, since a client
// never changes them between retries) may have silently landed before the
// network dropped its reply. Both possibilities -- the write took effect,
// or it never did -- are legal continuations, so this is modeled
// nondeterministically rather than picking one.
func kvModel() porcupine.Model {
	nm := porcupine.NondeterministicModel{
		Init: func() []interface{} {
			return []interface{}{kvState{}}
		},
		Step: func(st, in, out interface{}) []interface{} {
			state := st.(kvState)
			input := in.(kvInput)
			output := out.(kvOutput)

			switch input.Op {
			case "Get":
				entry, ok := state[input.Key]
				if ok {
					if output.Err == "OK" && output.Value == entry.Value && output.Version == entry.Version {
						return []interface{}{state}
					}
					return nil
				}
				if output.Err == "ErrNoKey" {
					return []interface{}{state}
				}
				return nil

			case "Put":
				entry, exists := state[input.Key]
				wouldSucceed := (!exists && input.Version == 0) || (exists && input.Version == entry.Version)
				afterSuccess := func() kvState {
					ns := cloneKVState(state)
					newVer := proto.Tversion(1)
					if exists {
						newVer = entry.Version + 1
					}
					ns[input.Key] = kvEntry{Value: input.Value, Version: newVer}
					return ns
				}

				switch output.Err {
				case "OK":
					if !wouldSucceed {
						return nil
					}
					return []interface{}{afterSuccess()}
				case "ErrNoKey":
					if exists || input.Version == 0 {
						return nil
					}
					return []interface{}{state}
				case "ErrVersion":
					if wouldSucceed {
						return nil
					}
					return []interface{}{state}
				case "ErrMaybe":
					results := []interface{}{state} // the write never landed
					if wouldSucceed {
						results = append(results, afterSuccess()) // or it did
					}
					return results
				}
				return nil
			}
			return nil
		},
		Equal: func(a, b interface{}) bool {
			return kvStateEqual(a.(kvState), b.(kvState))
		},
		DescribeOperation: func(in, out interface{}) string {
			input := in.(kvInput)
			output := out.(kvOutput)
			if input.Op == "Get" {
				return fmt.Sprintf("Get(%q) -> (%q, v%d, %s)", input.Key, output.Value, output.Version, output.Err)
			}
			return fmt.Sprintf("Put(%q, %q, v%d) -> %s", input.Key, input.Value, input.Version, output.Err)
		},
	}
	return nm.ToModel()
}

// pendingCall tracks an op_invoke not yet matched with its op_return.
type pendingCall struct {
	op    string
	input kvInput
	call  int64
}

// recordOpEvent folds one op_invoke/op_return trace event (emitted by
// viz/sim/client) into the engine's linearizability history; call under
// e.mu.
func (e *Engine) recordOpEvent(kind string, t int64, data map[string]any) {
	if data == nil {
		return
	}
	callID := asInt(data["call_id"])
	op, _ := data["op"].(string)
	if op != "Get" && op != "Put" {
		return // Txn isn't modeled; see viz/README.md
	}

	switch kind {
	case "op_invoke":
		in := kvInput{Op: op, Key: asString(data["key"])}
		if op == "Put" {
			in.Value = asString(data["value"])
			in.Version = proto.Tversion(asInt(data["version"]))
		}
		e.pendingOps[callID] = pendingCall{op: op, input: in, call: t}
	case "op_return":
		p, ok := e.pendingOps[callID]
		if !ok {
			return
		}
		delete(e.pendingOps, callID)
		out := kvOutput{Err: asString(data["err"])}
		if op == "Get" {
			out.Value = asString(data["value"])
			out.Version = proto.Tversion(asInt(data["version"]))
		}
		e.history = append(e.history, porcupine.Operation{
			Input: p.input, Call: p.call, Output: out, Return: t,
		})
	}
}

func asString(v any) string {
	s, _ := v.(string)
	return s
}

func asInt(v any) int {
	switch n := v.(type) {
	case int:
		return n
	case int64:
		return int(n)
	case uint64:
		return int(n)
	case proto.Tversion: // the trace data recorded in-process, not round-tripped through JSON
		return int(n)
	case float64: // json.Unmarshal into map[string]any decodes numbers as float64
		return int(n)
	}
	return 0
}

// LinearizabilityResult is the outcome of an on-demand CheckLinearizable
// call.
type LinearizabilityResult struct {
	Result       string // "Ok", "Illegal", or "Unknown" (timed out)
	OpCount      int
	PendingCount int
}

// CheckLinearizable runs porcupine against every completed Get/Put
// recorded so far (Txn isn't modeled) and, on a confirmed violation,
// records it as a Violation like the other invariants (freezing the UI).
// This is deliberately on-demand rather than run continuously: checking
// linearizability is NP-hard in general, and re-running it from scratch on
// every trace event would make the simulation unusable at speed. Call it
// from the UI (or periodically from a slow timer) instead.
func (e *Engine) CheckLinearizable(timeoutMs int) LinearizabilityResult {
	e.mu.Lock()
	history := append([]porcupine.Operation{}, e.history...)
	pending := len(e.pendingOps)
	e.mu.Unlock()

	timeout := time.Duration(timeoutMs) * time.Millisecond
	res := porcupine.CheckOperationsTimeout(kvModel(), history, timeout)

	if res == porcupine.Illegal {
		e.mu.Lock()
		e.violations = append(e.violations, Violation{
			Kind:   "linearizability",
			Detail: fmt.Sprintf("client-observed Get/Put history (%d completed ops) is not linearizable", len(history)),
			At:     e.clk.Now(),
		})
		e.mu.Unlock()
	}

	return LinearizabilityResult{Result: string(res), OpCount: len(history), PendingCount: pending}
}
