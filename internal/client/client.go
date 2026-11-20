// Package client is a thin stand-in for store.Clerk: the real Clerk
// requires a *cluster.Clnt, which only the test harness can construct, so
// this reimplements its small leader-sticky retry loop over our own TCP
// transport instead.
package client

import (
	"math/rand/v2"
	"time"

	"kvapp/internal/kv/proto"
	"kvapp/internal/rpc"
	"kvapp/internal/transport"
)

type Client struct {
	peers    []*transport.Peer
	leader   int
	clientID int64
	seq      int64
}

func New(addrs []string) *Client {
	peers := make([]*transport.Peer, len(addrs))
	for i, a := range addrs {
		peers[i] = transport.NewPeer("kvgateway", a)
	}
	return &Client{peers: peers, clientID: rand.Int64()}
}

func (c *Client) call(server int, method string, args, reply any) bool {
	rep, ok := c.peers[server].Call("kvgateway", method, rpc.Marshall(args))
	if !ok {
		return false
	}
	rpc.Unmarshall(rep, reply)
	return true
}

func (c *Client) Get(key string) (string, proto.Tversion, proto.Err) {
	args := proto.GetArgs{Key: key}
	server := c.leader
	for {
		reply := proto.GetReply{}
		ok := c.call(server, "KVServer.Get", &args, &reply)
		if !ok || reply.Err == proto.ErrWrongLeader {
			server = (server + 1) % len(c.peers)
			time.Sleep(100 * time.Millisecond)
			continue
		}
		c.leader = server
		return reply.Value, reply.Version, reply.Err
	}
}

func (c *Client) Put(key, value string, version proto.Tversion) proto.Err {
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
				return reply.Err
			case proto.ErrVersion:
				c.leader = server
				if sentBefore {
					return proto.ErrMaybe
				}
				return proto.ErrVersion
			}
		}
		sentBefore = true
		server = (server + 1) % len(c.peers)
		time.Sleep(100 * time.Millisecond)
	}
}

func (c *Client) Txn(compares []proto.TxnCompare, then, els []proto.TxnOp) proto.TxnReply {
	c.seq++
	args := proto.TxnArgs{ClientId: c.clientID, Seq: c.seq, Compares: compares, Then: then, Else: els}
	server := c.leader
	for {
		reply := proto.TxnReply{}
		ok := c.call(server, "KVServer.Txn", &args, &reply)
		if !ok || reply.Err == proto.ErrWrongLeader {
			server = (server + 1) % len(c.peers)
			time.Sleep(100 * time.Millisecond)
			continue
		}
		c.leader = server
		return reply
	}
}
