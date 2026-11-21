package engine

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"kvapp/viz/sim/network"
)

// TestStormChaosInvariants is the headless multi-seed fuzz test the spec
// asks for: run Storm-level chaos (crashes, partitions, 20% packet loss)
// against a live cluster for 60 virtual seconds under 10 different seeds,
// and assert every safety invariant holds throughout, plus linearizability
// at the end. Per this session's instruction not to run `go test` myself,
// this file is written but has not been executed -- run it yourself with:
//
//	go test ./viz/sim/engine/ -run TestStormChaosInvariants -v
func TestStormChaosInvariants(t *testing.T) {
	const virtualEnd = 60 * time.Second
	const chaosInterval = 500 * time.Millisecond

	for seed := int64(1); seed <= 10; seed++ {
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			e := New(Config{
				Seed:     seed,
				NumNodes: 5,
				Network: network.Config{
					Latency: 20 * time.Millisecond, Jitter: 15 * time.Millisecond,
					DropRate: 0.2,
				},
			})
			e.SetTraffic(true)
			chaos := rand.New(rand.NewSource(seed))
			nextChaos := chaosInterval

			for e.Now() < virtualEnd {
				if !e.Step() {
					t.Fatalf("clock ran out of pending events at t=%v (expected traffic/tickers to keep it busy until %v)", e.Now(), virtualEnd)
				}
				if e.Stuck() {
					t.Fatalf("clock reported stuck (Drain never settled) at t=%v", e.Now())
				}
				if !e.Safe() {
					for _, v := range e.Violations() {
						t.Errorf("safety violation: %s: %s (at t=%v)", v.Kind, v.Detail, v.At)
					}
					t.FailNow()
				}
				if e.Now() >= nextChaos {
					nextChaos += chaosInterval
					applyStormAction(e, chaos)
				}
			}

			if res := e.CheckLinearizable(5000); res.Result == "Illegal" {
				t.Fatalf("linearizability violated after %v of Storm chaos: %+v", virtualEnd, res)
			}
		})
	}
}

func applyStormAction(e *Engine, r *rand.Rand) {
	switch r.Intn(4) {
	case 0:
		e.Crash(r.Intn(e.cfg.NumNodes))
	case 1:
		e.Restart(r.Intn(e.cfg.NumNodes))
	case 2:
		e.Partition([][]int{{0, 1}, {2, 3, 4}})
	case 3:
		e.Heal()
	}
}
