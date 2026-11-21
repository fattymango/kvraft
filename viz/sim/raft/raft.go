// Package raft is a copy of internal/raft adapted for the deterministic
// simulator. Every change made relative to the original is listed in
// viz/README.md; in summary: real time and math/rand are replaced by
// viz/sim/clock, *rpc.ClientEnd is replaced by *network.ClientEnd, a
// dead/Kill mechanism was added so the simulator can crash/restart a node
// in-process (this deployment-oriented repo kills real OS processes
// instead), and one-line trace() calls were added at the points listed in
// viz/README.md. The election/replication/commit/snapshot algorithm itself
// is byte-for-byte the same as internal/raft/raft.go.
package raft

import (
	"bytes"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"kvapp/internal/cluster"
	"kvapp/internal/codec"
	"kvapp/internal/raftapi"
	"kvapp/viz/sim/clock"
	"kvapp/viz/sim/network"
	"kvapp/viz/sim/trace"
)

const (
	RaftState_Leader    = "leader"
	RaftState_Follower  = "follower"
	RaftState_Candidate = "candidate"
)

type LogEntry struct {
	Term    int
	Command interface{}
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        clock.Mutex          // sync.Mutex replaced by a clock-aware mutex (see viz/README.md)
	peers     []*network.ClientEnd // *rpc.ClientEnd replaced by the simulated network's end type
	persister *cluster.Persister
	me        int

	clk  *clock.Clock // added: virtual clock + seeded RNG, replaces time/math-rand
	sink trace.Sink   // added: trace() event destination
	node int          // added: node id reported on every trace event
	dead atomic.Bool  // added: lets the simulator crash/restart a node in-process

	// Persistent and volatile state a Raft server must maintain
	// (see the Raft paper's Figure 2).
	// election params

	currentTerm     int
	votedFor        int
	log             []LogEntry
	state           string
	lastHeard       clock.Duration // time.Time replaced by a virtual-clock reading
	electionTimeout clock.Duration

	// log params
	applyCh     chan raftapi.ApplyMsg
	commitIndex int
	lastApplied int
	nextIndex   []int
	matchIndex  []int
	applyCond   *sync.Cond

	lastIncludedIndex int
	lastIncludedTerm  int
	snapshot          []byte
	pendingSnapshot   bool
	replicateCh       chan struct{}
}

func (rf *Raft) lastIndex() int {
	return rf.lastIncludedIndex + len(rf.log) - 1
}

func (rf *Raft) termAt(index int) int {
	return rf.log[index-rf.lastIncludedIndex].Term
}

func (rf *Raft) entryAt(index int) LogEntry {
	return rf.log[index-rf.lastIncludedIndex]
}

func (rf *Raft) entriesFrom(index int) []LogEntry {
	src := rf.log[index-rf.lastIncludedIndex:]
	out := make([]LogEntry, len(src))
	copy(out, src)
	return out
}

// trace emits one event (added relative to the original: not part of the
// Raft algorithm, purely observational).
func (rf *Raft) trace(kind string, data map[string]any) {
	if rf.sink == nil {
		return
	}
	fn, file, line := trace.Caller(1)
	rf.sink.Emit(trace.Event{
		T: rf.clk.Now(), Node: rf.node, Term: rf.currentTerm,
		Kind: kind, Fn: fn, File: file, Line: line, Data: data,
	})
}

// Kill marks this peer crashed: its background loops exit and it stops
// answering RPCs with fresh state (volatile state is lost; persisted state
// and the Persister survive, matching the spec's crash semantics). Added
// for the simulator; the deployment version of this code has no in-process
// crash concept.
func (rf *Raft) Kill() {
	rf.dead.Store(true)
	rf.mu.Lock()
	rf.applyCond.Broadcast() // wake applier() so it can observe killed() and exit
	rf.mu.Unlock()
}

func (rf *Raft) killed() bool {
	return rf.dead.Load()
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == RaftState_Leader
}

// DebugSnapshot is a read-only view of a peer's state for the UI. Added
// for the simulator, purely observational like trace() -- it changes no
// behavior.
type DebugSnapshot struct {
	Term              int
	VotedFor          int
	State             string
	LogLen            int
	CommitIndex       int
	LastApplied       int
	LastIncludedIndex int
}

func (rf *Raft) Debug() DebugSnapshot {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return DebugSnapshot{
		Term: rf.currentTerm, VotedFor: rf.votedFor, State: rf.state,
		LogLen: len(rf.log), CommitIndex: rf.commitIndex, LastApplied: rf.lastApplied,
		LastIncludedIndex: rf.lastIncludedIndex,
	}
}

// LogTerms is a read-only view of this peer's log for the Log Matching
// invariant check: terms[i] is the term of the entry at absolute index
// start+i (terms[0], at index start==lastIncludedIndex, is the snapshot
// boundary placeholder entry). Added for the simulator, purely
// observational like trace()/Debug() -- it changes no behavior. Must only
// be called from outside this peer's own trace()/RPC-handler call stack
// (e.g. from the engine's driver after a Step() has quiesced), since it
// takes rf.mu and that lock is not reentrant.
func (rf *Raft) LogTerms() (start int, terms []int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	terms = make([]int, len(rf.log))
	for i, entry := range rf.log {
		terms[i] = entry.Term
	}
	return rf.lastIncludedIndex, terms
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Example:
	w := new(bytes.Buffer)
	e := codec.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	rf.persister.Save(w.Bytes(), rf.snapshot)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Example:
	r := bytes.NewBuffer(data)
	d := codec.NewDecoder(r)
	var currentTerm, votedFor, lastIncludedIndex, lastIncludedTerm int
	var logEntries []LogEntry
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&logEntries) != nil ||
		d.Decode(&lastIncludedIndex) != nil ||
		d.Decode(&lastIncludedTerm) != nil {
		panic("decode error")
	}
	rf.currentTerm = currentTerm
	rf.votedFor = votedFor
	rf.log = logEntries
	rf.lastIncludedIndex = lastIncludedIndex
	rf.lastIncludedTerm = lastIncludedTerm
}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if index <= rf.lastIncludedIndex || index > rf.lastIndex() {
		return
	}

	term := rf.termAt(index)
	newLog := []LogEntry{{Term: term}}
	newLog = append(newLog, rf.entriesFrom(index+1)...)

	rf.log = newLog
	rf.lastIncludedIndex = index
	rf.lastIncludedTerm = term
	rf.snapshot = snapshot
	rf.persist()
	rf.trace("snapshot_taken", map[string]any{"index": index, "term": term})
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		rf.trace("vote_denied", map[string]any{"to": args.CandidateId, "candidate": args.CandidateId, "reason": "stale term"})
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.state = RaftState_Follower
	}

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	lastIndex := rf.lastIndex()
	lastTerm := rf.termAt(lastIndex)
	upToDate := args.LastLogTerm > lastTerm || (args.LastLogTerm == lastTerm && args.LastLogIndex >= lastIndex)

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && upToDate {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		rf.lastHeard = rf.clk.Now()
		rf.electionTimeout = rf.newElectionTimeout()
		rf.trace("vote_granted", map[string]any{"to": args.CandidateId, "candidate": args.CandidateId})
	} else {
		reason := "already voted"
		if !upToDate {
			reason = "log not up to date"
		}
		rf.trace("vote_denied", map[string]any{"to": args.CandidateId, "candidate": args.CandidateId, "reason": reason})
	}
	rf.persist()
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The rpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../rpc/rpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	return rf.peers[server].Call("Raft.RequestVote", args, reply)
}

type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
	XTerm   int
	XIndex  int
	XLen    int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}

	rf.state = RaftState_Follower
	rf.lastHeard = rf.clk.Now()
	rf.electionTimeout = rf.newElectionTimeout()
	reply.Term = rf.currentTerm
	reply.Success = false

	if args.PrevLogIndex < rf.lastIncludedIndex {
		reply.XTerm = -1
		reply.XLen = rf.lastIncludedIndex + 1
		rf.persist()
		rf.trace("append_entries_reject", map[string]any{"to": args.LeaderID, "xterm": reply.XTerm, "xindex": reply.XIndex, "xlen": reply.XLen})
		return
	}

	if args.PrevLogIndex > rf.lastIndex() {
		reply.XTerm = -1
		reply.XLen = rf.lastIndex() + 1
		rf.persist()
		rf.trace("append_entries_reject", map[string]any{"to": args.LeaderID, "xterm": reply.XTerm, "xindex": reply.XIndex, "xlen": reply.XLen})
		return
	}

	if rf.termAt(args.PrevLogIndex) != args.PrevLogTerm {
		reply.XTerm = rf.termAt(args.PrevLogIndex)
		i := args.PrevLogIndex
		for i > rf.lastIncludedIndex+1 && rf.termAt(i-1) == reply.XTerm {
			i--
		}
		reply.XIndex = i
		rf.persist()
		rf.trace("append_entries_reject", map[string]any{"to": args.LeaderID, "xterm": reply.XTerm, "xindex": reply.XIndex, "xlen": reply.XLen})
		return
	}

	for k, entry := range args.Entries {
		pos := args.PrevLogIndex + 1 + k

		if pos > rf.lastIndex() {
			rf.log = append(rf.log, args.Entries[k:]...)
			break
		}

		if rf.termAt(pos) != entry.Term {
			rf.log = append(rf.log[:pos-rf.lastIncludedIndex], args.Entries[k:]...)
			break
		}
	}
	if len(args.Entries) > 0 {
		rf.trace("log_append", map[string]any{"from_leader": args.LeaderID, "count": len(args.Entries), "last_index": rf.lastIndex()})
	}

	if args.LeaderCommit > rf.commitIndex {
		lastNew := args.PrevLogIndex + len(args.Entries)
		if args.LeaderCommit < lastNew {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = lastNew
		}
		rf.trace("commit_index_advance", map[string]any{"commit_index": rf.commitIndex})
		rf.applyCond.Broadcast()
	}

	reply.Success = true
	rf.trace("append_entries_ok", map[string]any{"to": args.LeaderID, "match": args.PrevLogIndex + len(args.Entries)})
	rf.persist()
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	return rf.peers[server].Call("Raft.AppendEntries", args, reply)
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderID          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}

	rf.state = RaftState_Follower
	rf.lastHeard = rf.clk.Now()
	rf.electionTimeout = rf.newElectionTimeout()
	reply.Term = rf.currentTerm

	if args.LastIncludedIndex <= rf.commitIndex {
		rf.persist()
		return
	}

	newLog := []LogEntry{{Term: args.LastIncludedTerm}}
	if args.LastIncludedIndex < rf.lastIndex() && rf.termAt(args.LastIncludedIndex) == args.LastIncludedTerm {
		newLog = append(newLog, rf.entriesFrom(args.LastIncludedIndex+1)...)
	}

	rf.log = newLog
	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm
	rf.snapshot = args.Data
	rf.commitIndex = args.LastIncludedIndex
	rf.pendingSnapshot = true
	rf.persist()
	rf.trace("install_snapshot_applied", map[string]any{"last_included_index": args.LastIncludedIndex, "last_included_term": args.LastIncludedTerm})
	rf.applyCond.Broadcast()
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	return rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
}

func (rf *Raft) applier() {
	for {
		rf.mu.Lock()
		for !rf.killed() && !rf.pendingSnapshot && rf.lastApplied >= rf.commitIndex {
			rf.applyCond.Wait()
		}
		if rf.killed() {
			rf.mu.Unlock()
			return
		}

		if rf.pendingSnapshot {
			msg := raftapi.ApplyMsg{
				SnapshotValid: true,
				Snapshot:      rf.snapshot,
				SnapshotTerm:  rf.lastIncludedTerm,
				SnapshotIndex: rf.lastIncludedIndex,
			}
			rf.pendingSnapshot = false
			if rf.lastApplied < rf.lastIncludedIndex {
				rf.lastApplied = rf.lastIncludedIndex
			}
			rf.mu.Unlock()
			rf.applyCh <- msg
			continue
		}

		msgs := make([]raftapi.ApplyMsg, 0, rf.commitIndex-rf.lastApplied)
		for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
			msgs = append(msgs, raftapi.ApplyMsg{
				CommandValid: true,
				Command:      rf.entryAt(i).Command,
				CommandIndex: i,
			})
		}
		rf.lastApplied = rf.commitIndex
		rf.mu.Unlock()

		for _, msg := range msgs {
			rf.trace("apply", map[string]any{"index": msg.CommandIndex, "command": fmt.Sprintf("%v", msg.Command)})
			rf.applyCh <- msg
		}
	}
}

func (rf *Raft) stepDown(term int) {
	rf.currentTerm = term
	rf.state = RaftState_Follower
	rf.votedFor = -1
	rf.lastHeard = rf.clk.Now()
	rf.electionTimeout = rf.newElectionTimeout()
	rf.persist()
	rf.trace("step_down", map[string]any{"new_term": term})
}

func (rf *Raft) StartHeartBeat() {
	for {
		rf.mu.Lock()
		if rf.killed() || rf.state != RaftState_Leader {
			rf.mu.Unlock()
			return
		}

		for i := range rf.peers {
			if i == rf.me {
				continue
			}

			if rf.nextIndex[i] <= rf.lastIncludedIndex {
				args := &InstallSnapshotArgs{
					Term:              rf.currentTerm,
					LeaderID:          rf.me,
					LastIncludedIndex: rf.lastIncludedIndex,
					LastIncludedTerm:  rf.lastIncludedTerm,
					Data:              rf.snapshot,
				}
				rf.trace("install_snapshot_sent", map[string]any{"to": i, "last_included_index": args.LastIncludedIndex})
				go rf.replicateSnapshot(i, args)
				continue
			}

			prevIndex := rf.nextIndex[i] - 1
			args := &AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderID:     rf.me,
				PrevLogIndex: prevIndex,
				PrevLogTerm:  rf.termAt(prevIndex),
				Entries:      rf.entriesFrom(rf.nextIndex[i]),
				LeaderCommit: rf.commitIndex,
			}
			if len(args.Entries) == 0 {
				rf.trace("heartbeat_sent", map[string]any{"to": i})
			} else {
				rf.trace("append_entries_sent", map[string]any{"to": i, "count": len(args.Entries), "prev_index": prevIndex})
			}
			go rf.replicateEntries(i, args)
		}
		rf.mu.Unlock()

		select {
		case <-rf.clk.After(100 * time.Millisecond):
		case <-rf.replicateCh:
		}
	}
}

func (rf *Raft) replicateEntries(i int, args *AppendEntriesArgs) {
	reply := &AppendEntriesReply{}
	if ok := rf.sendAppendEntries(i, args, reply); !ok {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if reply.Term > rf.currentTerm {
		rf.stepDown(reply.Term)
		return
	}

	if rf.state != RaftState_Leader || rf.currentTerm != args.Term {
		return
	}

	if reply.Success {
		match := args.PrevLogIndex + len(args.Entries)
		if match > rf.matchIndex[i] {
			rf.matchIndex[i] = match
		}
		rf.nextIndex[i] = rf.matchIndex[i] + 1
		rf.trace("next_index_update", map[string]any{"peer": i, "next_index": rf.nextIndex[i], "match_index": rf.matchIndex[i]})
		rf.advanceCommitIndex()
		return
	}

	if reply.XTerm == -1 {
		rf.nextIndex[i] = reply.XLen
	} else {
		rf.nextIndex[i] = reply.XIndex
		for k := rf.lastIndex(); k > rf.lastIncludedIndex; k-- {
			if rf.termAt(k) == reply.XTerm {
				rf.nextIndex[i] = k + 1
				break
			}
		}
	}

	if rf.nextIndex[i] < 1 {
		rf.nextIndex[i] = 1
	}
	if rf.nextIndex[i] > rf.lastIndex()+1 {
		rf.nextIndex[i] = rf.lastIndex() + 1
	}
	rf.trace("next_index_update", map[string]any{"peer": i, "next_index": rf.nextIndex[i], "match_index": rf.matchIndex[i]})
}

func (rf *Raft) replicateSnapshot(i int, args *InstallSnapshotArgs) {
	reply := &InstallSnapshotReply{}
	if ok := rf.sendInstallSnapshot(i, args, reply); !ok {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if reply.Term > rf.currentTerm {
		rf.stepDown(reply.Term)
		return
	}

	if rf.state != RaftState_Leader || rf.currentTerm != args.Term {
		return
	}

	if args.LastIncludedIndex > rf.matchIndex[i] {
		rf.matchIndex[i] = args.LastIncludedIndex
	}
	if rf.nextIndex[i] < rf.matchIndex[i]+1 {
		rf.nextIndex[i] = rf.matchIndex[i] + 1
	}
	rf.advanceCommitIndex()
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != RaftState_Leader {
		return -1, rf.currentTerm, false
	}

	rf.log = append(rf.log, LogEntry{Term: rf.currentTerm, Command: command})
	index := rf.lastIndex()
	rf.matchIndex[rf.me] = index
	rf.persist()
	rf.trace("log_append", map[string]any{"from_leader": rf.me, "count": 1, "last_index": index})

	select {
	case rf.replicateCh <- struct{}{}:
	default:
	}

	return index, rf.currentTerm, true
}

// call when lock is held
func (rf *Raft) advanceCommitIndex() {
	initialCommitIndex := rf.commitIndex
	for n := rf.commitIndex + 1; n <= rf.lastIndex(); n++ {
		if rf.termAt(n) != rf.currentTerm {
			continue
		}
		matchCount := 0
		for _, m := range rf.matchIndex {
			if m >= n {
				matchCount++
			}
		}
		if matchCount > len(rf.peers)/2 {
			rf.commitIndex = n
		}
	}

	if initialCommitIndex != rf.commitIndex {
		rf.trace("commit_index_advance", map[string]any{"commit_index": rf.commitIndex})
		rf.applyCond.Broadcast()
	}
}

func (rf *Raft) newElectionTimeout() clock.Duration {
	return clock.Duration(300+(rf.clk.Int63()%300)) * time.Millisecond
}

func (rf *Raft) startElections() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state == RaftState_Leader || rf.clk.Since(rf.lastHeard) <= rf.electionTimeout {
		return
	}
	rf.trace("election_timeout", nil)
	rf.currentTerm++
	rf.state = RaftState_Candidate
	voteCount := 1
	rf.votedFor = rf.me
	rf.lastHeard = rf.clk.Now()
	rf.electionTimeout = rf.newElectionTimeout()
	rf.persist()
	rf.trace("become_candidate", nil)

	lastLogIndex := rf.lastIndex()
	req := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  rf.termAt(lastLogIndex),
	}

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		rf.trace("request_vote_sent", map[string]any{"to": i})

		go func(i int) {
			resp := &RequestVoteReply{}
			if ok := rf.sendRequestVote(i, req, resp); !ok {
				return
			}
			rf.mu.Lock()
			defer rf.mu.Unlock()

			if resp.Term > rf.currentTerm {
				rf.stepDown(resp.Term)
				return
			}

			if rf.state != RaftState_Candidate || rf.currentTerm != req.Term {
				return
			}
			if resp.VoteGranted {
				voteCount++
			}

			if voteCount > len(rf.peers)/2 {
				rf.state = RaftState_Leader
				rf.nextIndex = make([]int, len(rf.peers))
				rf.matchIndex = make([]int, len(rf.peers))
				for p := range rf.peers {
					rf.nextIndex[p] = rf.lastIndex() + 1
					rf.matchIndex[p] = 0
				}
				rf.matchIndex[rf.me] = rf.lastIndex()
				rf.trace("become_leader", map[string]any{"votes": voteCount})
				go rf.StartHeartBeat()
			}
		}(i)
	}
}

func (rf *Raft) ticker() {
	for !rf.killed() {

		// Check if a leader election should be started.
		rf.mu.Lock()
		if rf.state != RaftState_Leader && rf.clk.Since(rf.lastHeard) > rf.electionTimeout {
			go rf.startElections()
		}
		rf.mu.Unlock()
		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rf.clk.Int63() % 300)
		rf.clk.Sleep(clock.Duration(ms) * time.Millisecond)
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
// clk, sink and node are added relative to the original: the virtual
// clock/RNG this peer uses, where it emits trace events, and the node id
// reported on them.
func Make(peers []*network.ClientEnd, me int,
	persister *cluster.Persister, applyCh chan raftapi.ApplyMsg,
	clk *clock.Clock, sink trace.Sink, node int) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.clk = clk
	rf.sink = sink
	rf.node = node
	rf.mu.C = clk

	rf.log = []LogEntry{{Term: 0}}
	rf.votedFor = -1
	rf.electionTimeout = rf.newElectionTimeout()
	rf.lastHeard = rf.clk.Now()
	rf.state = RaftState_Follower
	rf.applyCh = applyCh
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.applyCond = sync.NewCond(&rf.mu)
	rf.replicateCh = make(chan struct{}, 1)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.snapshot = persister.ReadSnapshot()
	rf.commitIndex = rf.lastIncludedIndex
	rf.lastApplied = rf.lastIncludedIndex

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.applier()

	return rf
}
