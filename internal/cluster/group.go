package cluster

// Tgid identifies a replica group. kvraft.StartKVServer accepts one per
// its original multi-group (sharded) design; this app only ever runs a
// single group.
type Tgid int

// UseRaftStateMachine lets kvraft/rsm swap in an alternate Raft
// implementation. Always false here: this app only uses internal/raft.
var UseRaftStateMachine bool
