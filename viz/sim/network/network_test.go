package network

import (
	"sync"
	"testing"
	"time"

	"kvapp/viz/sim/clock"
)

type EchoArgs struct{ N int }
type EchoReply struct{ N int }

type Echo struct{ mu sync.Mutex }

func (e *Echo) Double(args *EchoArgs, reply *EchoReply) {
	e.mu.Lock()
	defer e.mu.Unlock()
	reply.N = args.N * 2
}

func run(t *testing.T, seed int64, cfg Config, calls int) (ok, total int) {
	t.Helper()
	c := clock.New(seed)
	net := New(c, nil, cfg)

	srv := MakeServer()
	srv.AddService(MakeService(&Echo{}))
	net.AddServer("node0", srv)

	end := net.MakeEnd("client")
	net.Connect("client", "node0")

	var wg sync.WaitGroup
	results := make([]bool, calls)
	for i := 0; i < calls; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			reply := EchoReply{}
			good := end.Call("Echo.Double", &EchoArgs{N: i}, &reply)
			if good && reply.N != i*2 {
				t.Errorf("call %d: got %d, want %d", i, reply.N, i*2)
			}
			results[i] = good
		}(i)
	}

	c.Drain()
	for c.HasPending() {
		if !c.Step() {
			break
		}
	}
	wg.Wait()
	if c.Stuck() {
		t.Fatal("clock reported stuck")
	}

	for _, r := range results {
		if r {
			ok++
		}
	}
	return ok, calls
}

func TestReliableDelivery(t *testing.T) {
	ok, total := run(t, 1, Config{Latency: 10 * time.Millisecond}, 20)
	if ok != total {
		t.Fatalf("ok=%d, want %d (reliable network should never drop)", ok, total)
	}
}

func TestDropIsDeterministic(t *testing.T) {
	cfg := Config{Latency: 10 * time.Millisecond, Jitter: 5 * time.Millisecond, DropRate: 0.3}
	a, _ := run(t, 7, cfg, 50)
	b, _ := run(t, 7, cfg, 50)
	if a != b {
		t.Fatalf("same seed produced different delivery counts: %d vs %d", a, b)
	}
}

func TestCrashDropsAllMessages(t *testing.T) {
	c := clock.New(3)
	net := New(c, nil, Config{Latency: time.Millisecond})
	srv := MakeServer()
	srv.AddService(MakeService(&Echo{}))
	net.AddServer("node0", srv)
	net.SetAlive("node0", false)

	end := net.MakeEnd("client")
	net.Connect("client", "node0")

	var ok bool
	done := make(chan struct{})
	go func() {
		reply := EchoReply{}
		ok = end.Call("Echo.Double", &EchoArgs{N: 5}, &reply)
		close(done)
	}()
	c.Drain()
	for c.HasPending() {
		if !c.Step() {
			break
		}
	}
	<-done
	if ok {
		t.Fatal("crashed server should never reply")
	}
}

func TestPartitionBlocksAcrossGroups(t *testing.T) {
	c := clock.New(4)
	net := New(c, nil, Config{Latency: time.Millisecond})
	srv := MakeServer()
	srv.AddService(MakeService(&Echo{}))
	net.AddServer("node0", srv)
	net.SetPartition([][]string{{"client"}, {"node0"}})

	end := net.MakeEnd("client")
	net.Connect("client", "node0")

	var ok bool
	done := make(chan struct{})
	go func() {
		reply := EchoReply{}
		ok = end.Call("Echo.Double", &EchoArgs{N: 5}, &reply)
		close(done)
	}()
	c.Drain()
	for c.HasPending() {
		if !c.Step() {
			break
		}
	}
	<-done
	if ok {
		t.Fatal("partitioned call should not be delivered")
	}

	net.Heal()
	done2 := make(chan struct{})
	go func() {
		reply := EchoReply{}
		ok = end.Call("Echo.Double", &EchoArgs{N: 5}, &reply)
		close(done2)
	}()
	c.Drain()
	for c.HasPending() {
		if !c.Step() {
			break
		}
	}
	<-done2
	if !ok {
		t.Fatal("call after Heal should be delivered")
	}
}
