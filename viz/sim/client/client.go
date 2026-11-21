// Package client is a copy of internal/client adapted for the simulator:
// the real client dials a *transport.Peer over TCP to reach a
// cmd/kvgateway process, which doesn't exist in-process here. This copy
// instead calls the KVServer's "KVServer.Get/Put/Txn" methods directly
// over the simulated *network.ClientEnd each node already exposes, and
// backs off with the virtual clock instead of time.Sleep. The
// leader-sticky retry loop itself (try server, advance on
// ErrWrongLeader/failure, remember the last leader) is otherwise
// unchanged from internal/client/client.go.
package client

import (
	"sync/atomic"
	"time"

	"kvapp/internal/kv/proto"
	"kvapp/viz/sim/clock"
	"kvapp/viz/sim/network"
	"kvapp/viz/sim/trace"
)

// callSeq assigns each Get/Put/Txn call a globally unique id, used to pair
// its op_invoke/op_return trace events (added relative to the original;
// see New/trace below) for the engine's linearizability checker. A
// package-level counter is simplest here since the engine hands out a
// fresh *Client per call rather than sharing one across concurrent
// goroutines (the original's leader/seq fields aren't safe to share).
var callSeq atomic.Int64

type Client struct {
	peers    []*network.ClientEnd
	leader   int
	clientID int64
	seq      int64
	clk      *clock.Clock
	sink     trace.Sink
}

// sink is added relative to the original: it's how Get/Put report the
// client-visible invoke/return boundary of each call (op_invoke/op_return,
// distinct from the per-RPC-attempt client_request/client_reply events
// store.KVServer emits) for porcupine-based linearizability checking.
func New(peers []*network.ClientEnd, clk *clock.Clock, sink trace.Sink) *Client {
	return &Client{peers: peers, clientID: clk.Int63(), clk: clk, sink: sink}
}

func (c *Client) trace(kind string, data map[string]any) {
	if c.sink == nil {
		return
	}
	fn, file, line := trace.Caller(1)
	c.sink.Emit(trace.Event{T: c.clk.Now(), Node: -1, Kind: kind, Fn: fn, File: file, Line: line, Data: data})
}

func (c *Client) call(server int, method string, args, reply any) bool {
	return c.peers[server].Call(method, args, reply)
}

func (c *Client) Get(key string) (string, proto.Tversion, proto.Err) {
	callID := callSeq.Add(1)
	c.trace("op_invoke", map[string]any{"call_id": callID, "op": "Get", "key": key})
	args := proto.GetArgs{Key: key}
	server := c.leader
	for {
		reply := proto.GetReply{}
		ok := c.call(server, "KVServer.Get", &args, &reply)
		if !ok || reply.Err == proto.ErrWrongLeader {
			server = (server + 1) % len(c.peers)
			c.clk.Sleep(100 * time.Millisecond)
			continue
		}
		c.leader = server
		c.trace("op_return", map[string]any{
			"call_id": callID, "op": "Get", "key": key,
			"value": reply.Value, "version": reply.Version, "err": string(reply.Err),
		})
		return reply.Value, reply.Version, reply.Err
	}
}

func (c *Client) Put(key, value string, version proto.Tversion) proto.Err {
	callID := callSeq.Add(1)
	c.trace("op_invoke", map[string]any{"call_id": callID, "op": "Put", "key": key, "value": value, "version": version})
	args := proto.PutArgs{Key: key, Value: value, Version: version}
	server := c.leader
	sentBefore := false
	for {
		reply := proto.PutReply{}
		ok := c.call(server, "KVServer.Put", &args, &reply)
		if ok {
			switch reply.Err {
			case proto.OK, proto.ErrNoKey:
				c.leader = server
				c.trace("op_return", map[string]any{"call_id": callID, "op": "Put", "key": key, "value": value, "version": version, "err": string(reply.Err)})
				return reply.Err
			case proto.ErrVersion:
				c.leader = server
				var err proto.Err = proto.ErrVersion
				if sentBefore {
					err = proto.ErrMaybe
				}
				c.trace("op_return", map[string]any{"call_id": callID, "op": "Put", "key": key, "value": value, "version": version, "err": string(err)})
				return err
			}
		}
		sentBefore = true
		server = (server + 1) % len(c.peers)
		c.clk.Sleep(100 * time.Millisecond)
	}
}

func (c *Client) Txn(compares []proto.TxnCompare, then, els []proto.TxnOp) proto.TxnReply {
	callID := callSeq.Add(1)
	c.trace("op_invoke", map[string]any{"call_id": callID, "op": "Txn"})
	c.seq++
	args := proto.TxnArgs{ClientId: c.clientID, Seq: c.seq, Compares: compares, Then: then, Else: els}
	server := c.leader
	for {
		reply := proto.TxnReply{}
		ok := c.call(server, "KVServer.Txn", &args, &reply)
		if !ok || reply.Err == proto.ErrWrongLeader {
			server = (server + 1) % len(c.peers)
			c.clk.Sleep(100 * time.Millisecond)
			continue
		}
		c.leader = server
		c.trace("op_return", map[string]any{"call_id": callID, "op": "Txn", "err": string(reply.Err)})
		return reply
	}
}
