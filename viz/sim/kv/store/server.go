// Package store is a copy of internal/kv/store adapted for the simulator.
// Changes relative to the original (full list in viz/README.md): the mu
// mutex is a clock.Mutex, StartKVServer takes the extra clock/trace/node
// arguments to thread through to rsm.MakeRSM, and trace() calls were added
// around each RPC handler and state mutation. DoOp/doGet/doPut/doTxn and
// Snapshot/Restore are otherwise byte-for-byte the same as
// internal/kv/store/server.go.
package store

import (
	"bytes"

	"kvapp/internal/cluster"
	"kvapp/internal/codec"
	"kvapp/internal/kv/proto"
	"kvapp/viz/sim/clock"
	"kvapp/viz/sim/kv/store/rsm"
	"kvapp/viz/sim/network"
	"kvapp/viz/sim/trace"
)

type Value struct {
	Value   string
	Version proto.Tversion
}
type KVServer struct {
	me  int
	rsm *rsm.RSM

	store map[string]Value
	mu    clock.Mutex
	dedup map[int64]DedupEntry // ClientID

	sink trace.Sink
	node int
}

func (kv *KVServer) trace(kind string, data map[string]any) {
	if kv.sink == nil {
		return
	}
	fn, file, line := trace.Caller(1)
	kv.sink.Emit(trace.Event{T: kv.mu.C.Now(), Node: kv.node, Kind: kind, Fn: fn, File: file, Line: line, Data: data})
}

// To type-cast req to the right type, take a look at Go's type switches or type
// assertions below:
//
// https://go.dev/tour/methods/16
// https://go.dev/tour/methods/15
func (kv *KVServer) DoOp(req any) any {
	switch r := req.(type) {
	case proto.GetArgs:
		return kv.doGet(&r)
	case proto.PutArgs:
		return kv.doPut(&r)
	case proto.TxnArgs:
		return kv.doTxn(&r)
	}
	return nil
}

func (kv *KVServer) doTxn(args *proto.TxnArgs) *proto.TxnReply {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	entry, ok := kv.dedup[args.ClientId]

	if ok && args.Seq <= entry.Seq {
		return &entry.Reply
	}
	reply := &proto.TxnReply{}

	reply.Succeeded = true
	for _, c := range args.Compares {
		v := proto.Tversion(0)
		if key, ok := kv.store[c.Key]; ok {
			v = key.Version
		}
		if c.Version != v {
			reply.Succeeded = false
			break
		}
	}

	var ops []proto.TxnOp
	if reply.Succeeded {
		ops = args.Then
	} else {
		ops = args.Else
	}

	for _, op := range ops {
		result := proto.TxnOpResult{}
		switch op.Type {
		case proto.TxnGet:
			if val, ok := kv.store[op.Key]; ok {
				result = proto.TxnOpResult{Value: val.Value, Version: val.Version, Err: proto.OK}
			} else {
				result = proto.TxnOpResult{Err: proto.ErrNoKey}
			}
		case proto.TxnPut:
			version := proto.Tversion(0)
			if val, ok := kv.store[op.Key]; ok {
				version = val.Version
			}

			kv.store[op.Key] = Value{Value: op.Value, Version: version + 1}
			result = proto.TxnOpResult{Version: version + 1, Err: proto.OK, Value: op.Value}
			kv.trace("kv_state_change", map[string]any{"key": op.Key, "version": version + 1})
		}
		reply.Results = append(reply.Results, result)
	}

	reply.Err = proto.OK
	kv.dedup[args.ClientId] = DedupEntry{Seq: args.Seq, Reply: *reply}
	return reply

}

func (kv *KVServer) doGet(args *proto.GetArgs) *proto.GetReply {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply := &proto.GetReply{}
	if v, ok := kv.store[args.Key]; ok {
		reply.Value = v.Value
		reply.Version = v.Version
		reply.Err = proto.OK
		return reply
	}

	reply.Err = proto.ErrNoKey
	return reply

}
func (kv *KVServer) doPut(args *proto.PutArgs) *proto.PutReply {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply := &proto.PutReply{}
	if v, ok := kv.store[args.Key]; ok {
		if args.Version == v.Version {
			kv.store[args.Key] = Value{Value: args.Value, Version: args.Version + 1}
			reply.Err = proto.OK
			kv.trace("kv_state_change", map[string]any{"key": args.Key, "version": args.Version + 1})
			return reply
		}
		reply.Err = proto.ErrVersion
		return reply
	}

	if args.Version == 0 {
		kv.store[args.Key] = Value{Value: args.Value, Version: 1}
		reply.Err = proto.OK
		kv.trace("kv_state_change", map[string]any{"key": args.Key, "version": 1})
		return reply
	}

	reply.Err = proto.ErrNoKey
	return reply
}
func (kv *KVServer) Snapshot() []byte {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	w := new(bytes.Buffer)
	encoder := codec.NewEncoder(w)
	encoder.Encode(kv.store)
	encoder.Encode(kv.dedup)
	return w.Bytes()
}

func (kv *KVServer) Restore(data []byte) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	r := bytes.NewBuffer(data)
	decoder := codec.NewDecoder(r)

	var newStore map[string]Value
	if err := decoder.Decode(&newStore); err != nil {
		panic(err)
	}
	if newStore == nil {
		newStore = make(map[string]Value)
	}
	kv.store = newStore

	var newDedup map[int64]DedupEntry
	if err := decoder.Decode(&newDedup); err != nil || newDedup == nil {
		newDedup = make(map[int64]DedupEntry)
	}
	kv.dedup = newDedup
}

func (kv *KVServer) Get(args *proto.GetArgs, reply *proto.GetReply) {
	kv.trace("client_request", map[string]any{"op": "Get", "key": args.Key})
	err, res := kv.rsm.Submit(*args)
	if err != proto.OK {
		reply.Err = err
		kv.trace("client_reply", map[string]any{"op": "Get", "err": string(err)})
		return
	}
	rep, ok := res.(*proto.GetReply)
	if !ok {
		reply.Err = proto.ErrWrongLeader
		kv.trace("client_reply", map[string]any{"op": "Get", "err": string(proto.ErrWrongLeader)})
		return
	}

	reply.Err = rep.Err
	reply.Value = rep.Value
	reply.Version = rep.Version
	kv.trace("client_reply", map[string]any{"op": "Get", "err": string(rep.Err)})
}

func (kv *KVServer) Put(args *proto.PutArgs, reply *proto.PutReply) {
	kv.trace("client_request", map[string]any{"op": "Put", "key": args.Key})
	err, res := kv.rsm.Submit(*args)
	if err != proto.OK {
		reply.Err = err
		kv.trace("client_reply", map[string]any{"op": "Put", "err": string(err)})
		return
	}
	rep, ok := res.(*proto.PutReply)
	if !ok {
		reply.Err = proto.ErrWrongLeader
		kv.trace("client_reply", map[string]any{"op": "Put", "err": string(proto.ErrWrongLeader)})
		return
	}

	reply.Err = rep.Err
	kv.trace("client_reply", map[string]any{"op": "Put", "err": string(rep.Err)})
}
func (kv *KVServer) Txn(args *proto.TxnArgs, reply *proto.TxnReply) {
	kv.trace("client_request", map[string]any{"op": "Txn"})
	err, res := kv.rsm.Submit(*args)
	if err != proto.OK {
		reply.Err = err
		kv.trace("client_reply", map[string]any{"op": "Txn", "err": string(err)})
		return
	}
	rep, ok := res.(*proto.TxnReply)
	if !ok {
		reply.Err = proto.ErrWrongLeader
		kv.trace("client_reply", map[string]any{"op": "Txn", "err": string(proto.ErrWrongLeader)})
		return
	}

	reply.Err = rep.Err
	reply.Succeeded = rep.Succeeded
	reply.Results = rep.Results
	kv.trace("client_reply", map[string]any{"op": "Txn", "err": string(rep.Err)})
}

// StartKVServer() and MakeRSM() must return quickly, so they should
// start goroutines for any long-running work.
//
// clk, sink and node are added relative to the original, threaded through
// to rsm.MakeRSM/raft.Make.
func StartKVServer(servers []*network.ClientEnd, me int, persister *cluster.Persister, maxraftstate int,
	clk *clock.Clock, sink trace.Sink, node int) []any {
	// call codec.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	codec.Register(rsm.Op{})
	codec.Register(proto.PutArgs{})
	codec.Register(proto.GetArgs{})
	codec.Register(proto.TxnArgs{})
	kv := &KVServer{me: me, sink: sink, node: node}
	kv.mu.C = clk

	kv.store = make(map[string]Value)
	kv.dedup = make(map[int64]DedupEntry)

	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv, clk, sink, node)
	// You may need initialization code here.
	return []any{kv, kv.rsm.Raft()}
}
