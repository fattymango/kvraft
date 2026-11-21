//go:build js && wasm

// Command wasm exports the engine's JS API (window.raftUnderFire.*) per
// viz/README.md. It owns no simulation logic itself: every exported
// function is a thin JSON-marshaling wrapper around viz/sim/engine.Engine.
// The frontend drives virtual time itself by calling step() repeatedly
// (e.g. from requestAnimationFrame while "playing"); this file does not
// run its own timer loop, which is what keeps the whole simulation
// pausable, steppable and replayable from JS.
package main

import (
	"encoding/json"
	"syscall/js"
	"time"

	"kvapp/viz/sim/engine"
	"kvapp/viz/sim/network"
)

var sim *engine.Engine

func jsonValue(v any) js.Value {
	b, err := json.Marshal(v)
	if err != nil {
		return js.ValueOf(map[string]any{"error": err.Error()})
	}
	var out any
	if err := json.Unmarshal(b, &out); err != nil {
		return js.ValueOf(map[string]any{"error": err.Error()})
	}
	return js.ValueOf(out)
}

func wrap(f func(this js.Value, args []js.Value) any) js.Func {
	return js.FuncOf(func(this js.Value, args []js.Value) any {
		return f(this, args)
	})
}

// init(seed, config) -- config: {numNodes, maxRaftState, latencyMs, jitterMs, dropRate, dupRate}
func jsInit(this js.Value, args []js.Value) any {
	cfg := engine.Config{NumNodes: 5, MaxRaftState: -1}
	if len(args) > 0 {
		cfg.Seed = int64(args[0].Int())
	}
	if len(args) > 1 && args[1].Truthy() {
		c := args[1]
		if v := c.Get("numNodes"); v.Truthy() {
			cfg.NumNodes = v.Int()
		}
		if v := c.Get("maxRaftState"); v.Truthy() {
			cfg.MaxRaftState = v.Int()
		}
		cfg.Network = networkConfigFromJS(c)
	}
	sim = engine.New(cfg)
	return nil
}

func networkConfigFromJS(c js.Value) network.Config {
	var cfg network.Config
	if v := c.Get("latencyMs"); v.Truthy() {
		cfg.Latency = time.Duration(v.Int()) * time.Millisecond
	}
	if v := c.Get("jitterMs"); v.Truthy() {
		cfg.Jitter = time.Duration(v.Int()) * time.Millisecond
	}
	if v := c.Get("dropRate"); v.Truthy() {
		cfg.DropRate = v.Float()
	}
	if v := c.Get("dupRate"); v.Truthy() {
		cfg.DupRate = v.Float()
	}
	return cfg
}

func jsStep(this js.Value, args []js.Value) any {
	return sim.Step()
}

func jsHasPending(this js.Value, args []js.Value) any {
	return sim.HasPending()
}

func jsNow(this js.Value, args []js.Value) any {
	return float64(sim.Now().Milliseconds())
}

func jsSafe(this js.Value, args []js.Value) any {
	return sim.Safe()
}

func jsStuck(this js.Value, args []js.Value) any {
	return sim.Stuck()
}

func jsViolations(this js.Value, args []js.Value) any {
	return jsonValue(sim.Violations())
}

func jsEventsSince(this js.Value, args []js.Value) any {
	since := 0
	if len(args) > 0 {
		since = args[0].Int()
	}
	return jsonValue(sim.EventsSince(since))
}

func jsGetState(this js.Value, args []js.Value) any {
	return jsonValue(sim.Snapshot())
}

func jsCrash(this js.Value, args []js.Value) any {
	sim.Crash(args[0].Int())
	return nil
}

func jsWipe(this js.Value, args []js.Value) any {
	sim.Wipe(args[0].Int())
	return nil
}

func jsRestart(this js.Value, args []js.Value) any {
	sim.Restart(args[0].Int())
	return nil
}

func jsPartition(this js.Value, args []js.Value) any {
	groups := make([][]int, args[0].Length())
	for i := 0; i < args[0].Length(); i++ {
		g := args[0].Index(i)
		group := make([]int, g.Length())
		for j := 0; j < g.Length(); j++ {
			group[j] = g.Index(j).Int()
		}
		groups[i] = group
	}
	sim.Partition(groups)
	return nil
}

func jsHeal(this js.Value, args []js.Value) any {
	sim.Heal()
	return nil
}

func jsSetNetwork(this js.Value, args []js.Value) any {
	sim.SetNetwork(networkConfigFromJS(args[0]))
	return nil
}

func jsSubmitGet(this js.Value, args []js.Value) any {
	return float64(sim.SubmitGet(args[0].String()))
}

func jsSubmitPut(this js.Value, args []js.Value) any {
	return float64(sim.SubmitPut(args[0].String(), args[1].String()))
}

func jsSetTraffic(this js.Value, args []js.Value) any {
	sim.SetTraffic(args[0].Bool())
	return nil
}

func jsCheckLinearizable(this js.Value, args []js.Value) any {
	timeoutMs := 500
	if len(args) > 0 {
		timeoutMs = args[0].Int()
	}
	return jsonValue(sim.CheckLinearizable(timeoutMs))
}

func jsResult(this js.Value, args []js.Value) any {
	r, ok := sim.Result(uint64(args[0].Int()))
	if !ok {
		return js.Null()
	}
	return jsonValue(r)
}

func main() {
	api := map[string]any{
		"init":              wrap(jsInit),
		"step":              wrap(jsStep),
		"hasPending":        wrap(jsHasPending),
		"now":               wrap(jsNow),
		"safe":              wrap(jsSafe),
		"stuck":             wrap(jsStuck),
		"violations":        wrap(jsViolations),
		"eventsSince":       wrap(jsEventsSince),
		"getState":          wrap(jsGetState),
		"crash":             wrap(jsCrash),
		"restart":           wrap(jsRestart),
		"wipe":              wrap(jsWipe),
		"partition":         wrap(jsPartition),
		"heal":              wrap(jsHeal),
		"setNetwork":        wrap(jsSetNetwork),
		"submitGet":         wrap(jsSubmitGet),
		"submitPut":         wrap(jsSubmitPut),
		"result":            wrap(jsResult),
		"setTraffic":        wrap(jsSetTraffic),
		"checkLinearizable": wrap(jsCheckLinearizable),
	}
	js.Global().Set("raftUnderFire", js.ValueOf(api))
	select {} // keep the wasm instance alive; it's driven entirely by JS calls above
}
