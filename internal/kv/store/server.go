package store

import (
	"bytes"
	"sync"

	"kvapp/internal/cluster"
	"kvapp/internal/codec"
	"kvapp/internal/kv/proto"
	"kvapp/internal/kv/store/rsm"
	"kvapp/internal/rpc"
)

type Value struct {
	Value   string
	Version proto.Tversion
}
type KVServer struct {
	me  int
	rsm *rsm.RSM

	store map[string]Value
	mu    sync.Mutex
	dedup map[int64]DedupEntry // ClientID
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
			return reply
		}
		reply.Err = proto.ErrVersion
		return reply
	}

	if args.Version == 0 {
		kv.store[args.Key] = Value{Value: args.Value, Version: 1}
		reply.Err = proto.OK
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
	err, res := kv.rsm.Submit(*args)
	if err != proto.OK {
		reply.Err = err
		return
	}
	rep, ok := res.(*proto.GetReply)
	if !ok {
		reply.Err = proto.ErrWrongLeader
		return
	}

	reply.Err = rep.Err
	reply.Value = rep.Value
	reply.Version = rep.Version
}

func (kv *KVServer) Put(args *proto.PutArgs, reply *proto.PutReply) {
	err, res := kv.rsm.Submit(*args)
	if err != proto.OK {
		reply.Err = err
		return
	}
	rep, ok := res.(*proto.PutReply)
	if !ok {
		reply.Err = proto.ErrWrongLeader
		return
	}

	reply.Err = rep.Err
}
func (kv *KVServer) Txn(args *proto.TxnArgs, reply *proto.TxnReply) {
	err, res := kv.rsm.Submit(*args)
	if err != proto.OK {
		reply.Err = err
		return
	}
	rep, ok := res.(*proto.TxnReply)
	if !ok {
		reply.Err = proto.ErrWrongLeader
		return
	}

	reply.Err = rep.Err
	reply.Succeeded = rep.Succeeded
	reply.Results = rep.Results
}

// StartKVServer() and MakeRSM() must return quickly, so they should
// start goroutines for any long-running work.
func StartKVServer(servers []*rpc.ClientEnd, gid cluster.Tgid, me int, persister *cluster.Persister, maxraftstate int) []any {
	// call codec.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	codec.Register(rsm.Op{})
	codec.Register(proto.PutArgs{})
	codec.Register(proto.GetArgs{})
	codec.Register(proto.TxnArgs{})
	kv := &KVServer{me: me}

	kv.store = make(map[string]Value)
	kv.dedup = make(map[int64]DedupEntry)

	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)
	// You may need initialization code here.
	return []any{kv, kv.rsm.Raft()}
}
