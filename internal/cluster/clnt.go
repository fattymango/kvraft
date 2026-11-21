package cluster

import (
	"sync"

	"kvapp/internal/rpc"
	"kvapp/internal/transport"
)

// Clnt is the real-network stand-in for the lab tester's Clnt: Call sends an
// RPC to a server named by its host:port, over one cached TCP peer per address.
type Clnt struct {
	name  string
	mu    sync.Mutex
	peers map[string]*transport.Peer
}

func MakeClnt(name string) *Clnt {
	return &Clnt{name: name, peers: make(map[string]*transport.Peer)}
}

func (c *Clnt) Call(server string, method string, args any, reply any) bool {
	c.mu.Lock()
	p, ok := c.peers[server]
	if !ok {
		p = transport.NewPeer(c.name, server)
		c.peers[server] = p
	}
	c.mu.Unlock()
	rep, ok := p.Call(c.name, method, rpc.Marshall(args))
	if !ok {
		return false
	}
	rpc.Unmarshall(rep, reply)
	return true
}
