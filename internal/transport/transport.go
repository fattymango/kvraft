// Package transport carries RPC calls over real TCP connections between
// kvnode processes, using internal/demux for framing/multiplexing and
// internal/rpc's Server/MakeService for dispatch, so internal/raft and
// internal/kv/store run unmodified outside the test harness.
package transport

import (
	"net"
	"sync"
	"time"

	"kvapp/internal/demux"
	"kvapp/internal/rpc"
)

// RPCArgs is the wire envelope for one RPC call: the "Service.Method"
// name plus the already-gob-encoded arguments.
type RPCArgs struct {
	Method string
	Args   []byte
}

// Server accepts TCP connections and dispatches incoming calls to a
// rpc.Server (which holds the KVServer and Raft services).
type Server struct {
	name string
	ln   net.Listener
	srv  *rpc.Server
}

func Listen(name, addr string, srv *rpc.Server) (*Server, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	s := &Server{name: name, ln: ln, srv: srv}
	go s.acceptLoop()
	return s, nil
}

func (s *Server) Close() error { return s.ln.Close() }

func (s *Server) acceptLoop() {
	for {
		c, err := s.ln.Accept()
		if err != nil {
			return
		}
		demux.NewDemuxSrv(s.name, s, demux.NewTransport(c))
	}
}

// ServeRequest implements demux.ServerI.
func (s *Server) ServeRequest(clntEnd string, b []byte) ([]byte, bool) {
	var req RPCArgs
	rpc.Unmarshall(b, &req)
	return s.srv.Dispatch(s.name, req.Method, clntEnd, req.Args)
}

// callTimeout bounds how long a Peer.Call waits for a reply, so a dead or
// unreachable peer makes rpc.ClientEnd.Call return false instead of
// hanging, the way the simulated network does.
const callTimeout = 2 * time.Second

// Peer is a rpc.ClientEnd's real-network backend: it lazily dials addr
// over TCP and reconnects after any error.
type Peer struct {
	self string
	addr string

	mu  sync.Mutex
	dmx *demux.DemuxClnt
}

func NewPeer(self, addr string) *Peer {
	return &Peer{self: self, addr: addr}
}

func (p *Peer) connectedLocked() (*demux.DemuxClnt, error) {
	if p.dmx != nil && !p.dmx.IsClosed() {
		return p.dmx, nil
	}
	c, err := net.DialTimeout("tcp", p.addr, callTimeout)
	if err != nil {
		return nil, err
	}
	dmx, err := demux.NewDemuxClnt(p.self, p.addr, demux.NewTransport(c))
	if err != nil {
		c.Close()
		return nil, err
	}
	p.dmx = dmx
	return dmx, nil
}

// Call is a rpc.Fcall: it can be installed with ClientEnd.SetCall.
func (p *Peer) Call(_, method string, args []byte) ([]byte, bool) {
	p.mu.Lock()
	dmx, err := p.connectedLocked()
	p.mu.Unlock()
	if err != nil {
		return nil, false
	}

	b := rpc.Marshall(&RPCArgs{Method: method, Args: args})

	type result struct {
		rep []byte
		ok  bool
	}
	ch := make(chan result, 1)
	go func() {
		rep, ok, err := dmx.SendReceive(b)
		ch <- result{rep, ok && err == nil}
	}()

	select {
	case r := <-ch:
		if !r.ok {
			p.drop(dmx)
		}
		return r.rep, r.ok
	case <-time.After(callTimeout):
		p.drop(dmx)
		return nil, false
	}
}

func (p *Peer) drop(dmx *demux.DemuxClnt) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.dmx == dmx {
		dmx.Close()
		p.dmx = nil
	}
}
