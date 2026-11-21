// Package network is a deterministic, virtual-time replacement for
// internal/rpc (labrpc-style channel RPC), built fresh for the simulator
// rather than copied: it adds a partition matrix, per-message trace
// events and live-configurable latency/drop/duplication on top of the
// same Call(svcMeth, args, reply) shape internal/rpc.ClientEnd exposes, so
// the copied Raft/RSM/KV code only needs its import path (and the
// *rpc.ClientEnd type name) changed, never its logic. internal/rpc itself
// is untouched; this package reuses only its pure, time/rand-free
// Marshall/Unmarshall helpers.
package network

import (
	"fmt"
	"log"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"kvapp/internal/rpc"
	"kvapp/viz/sim/clock"
)

// Config holds the live-tunable network conditions, mirroring the JS API's
// setNetwork({latency, jitter, drop, dup}).
type Config struct {
	Latency  time.Duration
	Jitter   time.Duration // delivery delay is Latency plus Uniform[0,Jitter)
	DropRate float64       // probability a message is dropped in flight, 0..1
	DupRate  float64       // probability a message is delivered twice, 0..1
}

// Message is the visible, UI-facing record of one RPC request or reply in
// flight, emitted to the Network's EventSink.
type Message struct {
	ID       uint64
	From     string
	To       string
	Kind     string // e.g. "Raft.AppendEntries", or "reply:Raft.AppendEntries"
	SendTime time.Duration
	Arrive   time.Duration // valid only once delivered
	Dropped  bool
}

// EventSink receives network-level trace events for the UI. Both methods
// must be safe to call from arbitrary goroutines.
type EventSink interface {
	OnMessage(Message)
}

type nullSink struct{}

func (nullSink) OnMessage(Message) {}

// Network is the shared switchboard every node's ClientEnds and Servers
// register with, analogous to rpc.Network / labrpc.Network.
type Network struct {
	clock *clock.Clock
	sink  EventSink

	mu          sync.Mutex
	cfg         Config
	ends        map[string]*ClientEnd
	servers     map[string]*Server
	connections map[string]string // endname -> servername
	alive       map[string]bool   // servername -> alive (crash/restart)
	partitioned bool
	groupOf     map[string]int    // servername -> partition group id
	owner       map[string]string // endname -> servername of the node that sends on it

	msgSeq atomic.Uint64
}

func New(c *clock.Clock, sink EventSink, cfg Config) *Network {
	if sink == nil {
		sink = nullSink{}
	}
	return &Network{
		clock:       c,
		sink:        sink,
		cfg:         cfg,
		ends:        map[string]*ClientEnd{},
		servers:     map[string]*Server{},
		connections: map[string]string{},
		alive:       map[string]bool{},
		owner:       map[string]string{},
	}
}

func (n *Network) SetConfig(cfg Config) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.cfg = cfg
}

// SetPartition splits the network into unreachable groups; nodes in
// different groups cannot deliver messages in either direction until Heal.
// Names are whatever was passed to MakeEnd/AddServer (this simulator uses
// "nodeN" for both ends of node N).
func (n *Network) SetPartition(groups [][]string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.groupOf = map[string]int{}
	for gi, g := range groups {
		for _, name := range g {
			n.groupOf[name] = gi
		}
	}
	n.partitioned = true
}

func (n *Network) Heal() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.partitioned = false
	n.groupOf = nil
}

// SetOwner records which server sends on an end, so partitions (which are
// keyed by server name) apply to that end's outgoing messages.
func (n *Network) SetOwner(endname, servername string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.owner[endname] = servername
}

func (n *Network) reachable(a, b string) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	if o, ok := n.owner[a]; ok {
		a = o
	}
	if !n.partitioned || a == b {
		return true
	}
	ga, oka := n.groupOf[a]
	gb, okb := n.groupOf[b]
	if !oka || !okb {
		return true
	}
	return ga == gb
}

// SetAlive marks a server as crashed (false) or restarted (true). A crashed
// server drops every incoming RPC, as if the process were down.
func (n *Network) SetAlive(servername string, alive bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.alive[servername] = alive
}

func (n *Network) isAlive(servername string) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	a, ok := n.alive[servername]
	return !ok || a
}

func (n *Network) MakeEnd(name string) *ClientEnd {
	e := &ClientEnd{name: name, net: n}
	n.mu.Lock()
	n.ends[name] = e
	n.mu.Unlock()
	return e
}

func (n *Network) AddServer(name string, s *Server) {
	n.mu.Lock()
	n.servers[name] = s
	if _, ok := n.alive[name]; !ok {
		n.alive[name] = true
	}
	n.mu.Unlock()
}

func (n *Network) Connect(endname, servername string) {
	n.mu.Lock()
	n.connections[endname] = servername
	n.mu.Unlock()
}

func (n *Network) delay() time.Duration {
	n.mu.Lock()
	cfg := n.cfg
	n.mu.Unlock()
	d := cfg.Latency
	if cfg.Jitter > 0 {
		d += time.Duration(n.clock.Int63n(int64(cfg.Jitter)))
	}
	return d
}

func (n *Network) shouldDrop() bool {
	n.mu.Lock()
	p := n.cfg.DropRate
	n.mu.Unlock()
	if p <= 0 {
		return false
	}
	return n.clock.Float64() < p
}

func (n *Network) shouldDup() bool {
	n.mu.Lock()
	p := n.cfg.DupRate
	n.mu.Unlock()
	if p <= 0 {
		return false
	}
	return n.clock.Float64() < p
}

// ClientEnd is a node's handle for calling one specific peer, matching
// internal/rpc.ClientEnd's Call signature.
type ClientEnd struct {
	name string
	net  *Network
}

// Call sends svcMeth(args) to the connected server and blocks (via the
// virtual clock, not a real goroutine park on wall time) until the reply
// arrives, is dropped, or the destination is unreachable/crashed. The
// return value matches labrpc semantics: false means no reply was
// received.
func (e *ClientEnd) Call(svcMeth string, args interface{}, reply interface{}) bool {
	n := e.net
	n.mu.Lock()
	servername, connected := n.connections[e.name]
	n.mu.Unlock()
	if !connected {
		return false
	}
	server, ok := n.servers[servername]
	if !ok {
		return false
	}

	id := n.msgSeq.Add(1)
	sendTime := n.clock.Now()
	reachable := n.reachable(e.name, servername)
	alive := n.isAlive(servername)
	dropped := !reachable || !alive || n.shouldDrop()

	n.sink.OnMessage(Message{ID: id, From: e.name, To: servername, Kind: svcMeth, SendTime: sendTime, Dropped: dropped})

	if dropped {
		return false
	}

	argBytes := rpc.Marshall(args)
	replyCh := make(chan []byte, 1)
	deliver := func() {
		rep, ok := server.dispatch(svcMeth, argBytes)
		n.sink.OnMessage(Message{ID: id, From: servername, To: e.name, Kind: "reply:" + svcMeth, SendTime: n.clock.Now(), Arrive: n.clock.Now()})
		if ok {
			replyCh <- rep
		} else {
			replyCh <- nil
		}
	}

	n.clock.AfterFunc(n.delay(), deliver)
	if n.shouldDup() {
		n.clock.AfterFunc(n.delay(), func() { deliver() })
	}

	rep := <-replyCh
	if rep == nil {
		return false
	}
	rpc.Unmarshall(rep, reply)
	return true
}

// Server hosts one or more Services (e.g. "Raft", "KVServer") behind a
// single name, matching internal/rpc.Server/Service's reflection dispatch.
type Server struct {
	mu       sync.Mutex
	services map[string]*Service
}

func MakeServer() *Server {
	return &Server{services: map[string]*Service{}}
}

func (s *Server) AddService(svc *Service) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.services[svc.name] = svc
}

func (s *Server) dispatch(svcMeth string, args []byte) ([]byte, bool) {
	dot := strings.LastIndex(svcMeth, ".")
	if dot < 0 {
		log.Printf("network: malformed method name %q", svcMeth)
		return nil, false
	}
	serviceName, methodName := svcMeth[:dot], svcMeth[dot+1:]

	s.mu.Lock()
	svc, ok := s.services[serviceName]
	s.mu.Unlock()
	if !ok {
		log.Printf("network: unknown service %q", serviceName)
		return nil, false
	}
	return svc.dispatch(methodName, args)
}

// Service wraps a receiver's RPC-shaped methods (func(*Args, *Reply)) for
// dispatch by name, matching internal/rpc.Service's behavior.
type Service struct {
	name    string
	rcvr    reflect.Value
	methods map[string]reflect.Method
}

func MakeService(rcvr interface{}) *Service {
	svc := &Service{rcvr: reflect.ValueOf(rcvr), methods: map[string]reflect.Method{}}
	typ := reflect.TypeOf(rcvr)
	svc.name = reflect.Indirect(svc.rcvr).Type().Name()
	for m := 0; m < typ.NumMethod(); m++ {
		method := typ.Method(m)
		mtype := method.Type
		if method.PkgPath != "" || mtype.NumIn() != 3 || mtype.In(2).Kind() != reflect.Ptr || mtype.NumOut() != 0 {
			continue
		}
		svc.methods[method.Name] = method
	}
	return svc
}

func (svc *Service) dispatch(methodName string, args []byte) ([]byte, bool) {
	method, ok := svc.methods[methodName]
	if !ok {
		log.Printf("network: unknown method %q on %q", methodName, svc.name)
		return nil, false
	}
	argv := reflect.New(method.Type.In(1))
	rpc.Unmarshall(args, argv.Interface())

	replyType := method.Type.In(2).Elem()
	replyv := reflect.New(replyType)

	method.Func.Call([]reflect.Value{svc.rcvr, argv.Elem(), replyv})

	return rpc.Marshall(replyv.Interface()), true
}

// NodeName is the shared end/server name for node index i, e.g. "node0".
func NodeName(i int) string { return fmt.Sprintf("node%d", i) }
