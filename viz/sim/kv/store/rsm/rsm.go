// Package rsm is a copy of internal/kv/store/rsm adapted for the
// simulator. Changes relative to the original (full list in
// viz/README.md): math/rand/v2 and time.NewTicker are replaced by
// viz/sim/clock, *rpc.ClientEnd becomes *network.ClientEnd, raft.Make
// takes the extra clock/trace/node arguments, and the rsm.mu mutex is a
// clock.Mutex so the simulator's quiescence detector sees its activity.
// Submit's retry/commit logic is otherwise unchanged.
package rsm

import (
	"time"

	"kvapp/internal/cluster"
	"kvapp/internal/codec"
	"kvapp/internal/kv/proto"
	"kvapp/internal/raftapi"
	"kvapp/viz/sim/clock"
	"kvapp/viz/sim/network"
	"kvapp/viz/sim/raft"
	"kvapp/viz/sim/trace"
)

type Op struct {
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ServerID  int
	RequestID int64
	Command   interface{}
}

// A server (i.e., ../server.go) that wants to replicate itself calls
// MakeRSM and must implement the StateMachine interface.  This
// interface allows the rsm package to interact with the server for
// server-specific operations: the server must implement DoOp to
// execute an operation (e.g., a Get or Put request), and
// Snapshot/Restore to snapshot and restore the server's state.
type StateMachine interface {
	DoOp(any) any
	Snapshot() []byte
	Restore([]byte)
}

type OpResult struct {
	RequestID int64
	Result    interface{}
}
type Waiter struct {
	RequestID int64
	Term      int
	ch        chan OpResult
}
type RSM struct {
	mu           clock.Mutex
	me           int
	rf           raftapi.Raft
	applyCh      chan raftapi.ApplyMsg
	maxraftstate int // snapshot if log grows this big
	sm           StateMachine
	waiterMap    map[int]Waiter
	clk          *clock.Clock
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// The RSM should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
//
// MakeRSM() must return quickly, so it should start goroutines for
// any long-running work.
//
// clk, sink and node are added relative to the original, threaded through
// to raft.Make.
func MakeRSM(servers []*network.ClientEnd, me int, persister *cluster.Persister, maxraftstate int, sm StateMachine,
	clk *clock.Clock, sink trace.Sink, node int) *RSM {
	rsm := &RSM{
		me:           me,
		maxraftstate: maxraftstate,
		applyCh:      make(chan raftapi.ApplyMsg),
		sm:           sm,
		waiterMap:    make(map[int]Waiter),
		clk:          clk,
	}
	rsm.mu.C = clk
	if !cluster.UseRaftStateMachine {
		rsm.rf = raft.Make(servers, me, persister, rsm.applyCh, clk, sink, node)
	}
	codec.Register(Op{})
	s := persister.ReadSnapshot()
	if len(s) > 0 {
		rsm.sm.Restore(s)
	}
	go rsm.reader()
	return rsm
}

func (rsm *RSM) reader() {
	for msg := range rsm.applyCh {
		if msg.SnapshotValid {
			rsm.sm.Restore(msg.Snapshot)
			continue
		}

		op, ok := msg.Command.(Op)
		if !ok {
			continue
		}

		res := rsm.sm.DoOp(op.Command)

		if rsm.maxraftstate != -1 && rsm.rf.PersistBytes() >= rsm.maxraftstate {
			rsm.rf.Snapshot(msg.CommandIndex, rsm.sm.Snapshot())
		}

		rsm.mu.Lock()
		if w, ok := rsm.waiterMap[msg.CommandIndex]; ok {
			w.ch <- OpResult{
				RequestID: op.RequestID,
				Result:    res,
			}
			delete(rsm.waiterMap, msg.CommandIndex)
		}
		rsm.mu.Unlock()
	}
	rsm.mu.Lock()
	for _, w := range rsm.waiterMap {
		w.ch <- OpResult{
			RequestID: -1,
			Result:    nil,
		}
	}
	rsm.waiterMap = make(map[int]Waiter)
	rsm.mu.Unlock()
}

func (rsm *RSM) Raft() raftapi.Raft {
	return rsm.rf
}

// Submit a command to Raft, and wait for it to be committed.  It
// should return ErrWrongLeader if client should find new leader and
// try again.
func (rsm *RSM) Submit(req any) (proto.Err, any) {

	// Submit creates an Op structure to run a command through Raft;
	// for example: op := Op{Me: rsm.me, Id: id, Req: req}, where req
	// is the argument to Submit and id is a unique id for the op.

	// your code here

	op := Op{
		ServerID:  rsm.me,
		RequestID: rsm.clk.Int63(),
		Command:   req,
	}
	rsm.mu.Lock()
	index, term, ok := rsm.rf.Start(op)
	if !ok {
		rsm.mu.Unlock()
		return proto.ErrWrongLeader, nil
	}
	w := Waiter{
		RequestID: op.RequestID,
		Term:      term,
		ch:        make(chan OpResult, 1),
	}
	rsm.waiterMap[index] = w
	rsm.mu.Unlock()
	ticker := rsm.clk.NewTicker(time.Millisecond * 50)
	defer ticker.Stop()
	for {
		select {
		case res := <-w.ch:
			if res.RequestID == op.RequestID {
				return proto.OK, res.Result
			}
			return proto.ErrWrongLeader, nil
		case <-ticker.C:
			currentTerm, ok := rsm.rf.GetState()

			if !ok || currentTerm != term {
				rsm.mu.Lock()
				if entry, ok := rsm.waiterMap[index]; ok {
					if entry.RequestID == op.RequestID {
						delete(rsm.waiterMap, index)
					}
				}
				rsm.mu.Unlock()
				return proto.ErrWrongLeader, nil
			}
		}
	}
}
