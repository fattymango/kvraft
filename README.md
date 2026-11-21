# kvapp

A 5-node, Raft-replicated key/value store with an HTTP gateway: real TCP
between nodes, on-disk persistence, Docker Compose for deployment. One Go
module, no external dependencies besides a linearizability-checking test
library pulled in transitively.

## Run it

```bash
cd app
docker compose up --build
```

The gateway listens on `http://localhost:18080`:

```bash
curl -X PUT localhost:18080/kv/foo -d '{"value":"bar","version":0}'
curl localhost:18080/kv/foo
curl -X POST localhost:18080/txn -d '{
  "compares":[{"key":"foo","version":1}],
  "then":[{"type":"Put","key":"foo","value":"baz"}],
  "else":[]
}'
```

`./smoke.sh` runs this plus a kill/restart cycle end to end (needs the stack
already built; it runs `docker compose up --build` itself).

Kill or restart a node:

```bash
docker compose stop kvnode0      # any 2 of 5 can go down, the group keeps quorum
docker compose start kvnode0
docker compose restart           # bounce everything; data is read back from disk
```

Each `kvnodeN` has its own named volume (`kvnodeN-data`) holding
`raftstate.bin` and `snapshot.bin`.

## Sharded mode (shardkv + cross-shard 2PC transactions)

`docker-compose.shard.yml` runs the sharded service: a shard controller
(`shardctrl`, a kvsrv holding the shard config and 2PC decisions), two 3-node
replica groups (`shardnode`), and `shardgateway` on `localhost:18081`. Group 1
owns all 12 shards at first.

```bash
docker compose -f docker-compose.shard.yml up --build
curl -X POST localhost:18081/admin/join -d '{"gid":2,"servers":["g2n0:9000","g2n1:9000","g2n2:9000"]}'
curl localhost:18081/admin/config
curl -X POST localhost:18081/txn -d '{
  "compares":[{"Key":"alice","Version":1},{"Key":"carol","Version":1}],
  "then":[{"Type":"Put","Key":"alice","Value":"70"},{"Type":"Put","Key":"carol","Value":"130"}],
  "else":[]
}'
curl -X POST localhost:18081/admin/leave -d '{"gid":2}'
```

Transactions touching several shards use two-phase commit with two-phase
locking across groups; see `internal/shardkv`.

## Layout

```
app/
  go.mod                     module kvapp
  cmd/
    kvnode/                  the node binary
    kvgateway/                the HTTP gateway binary
  internal/
    raft/                     Raft consensus: leader election, log replication, snapshots
    raftapi/                  the Raft interface + ApplyMsg, the contract store/rsm depends on
    kv/
      store/                  the KVServer state machine: Get, versioned Put, Txn
        rsm/                  replicates store's operations through raft via Submit()
      proto/                  wire types for Get/Put/Txn requests and replies
    rpc/                      the RPC engine: ClientEnd/Server/Service, pluggable transport (SetCall)
    demux/                    multiplexes concurrent RPC calls over one connection
    codec/                    gob encode/decode wrapper rpc/ and raft/ use for messages
    cluster/                  shared runtime types: Persister (persisted state), group id
    transport/                 real TCP transport plugged into rpc's SetCall hook
    kvsrv/                    single-server versioned KV; the shard controller's storage
    shardkv/                  sharded KV clerk; Txn is the 2PC coordinator
      shardcfg/               shard -> group config, Join/Leave rebalancing
      shardctrler/            moves shards between groups on config changes
      shardgrp/               a replica group's server: per-shard data, locks, prepared txns
        shardrpc/             Freeze/Install/Delete shard and TxnPrepare/TxnFinish wire types
    persist/                  mirrors cluster.Persister to disk so state survives a restart
    client/                   a Get/Put/Txn client over transport, used by kvgateway
  Dockerfile, .dockerignore   multi-stage build, context is app/ itself
  docker-compose.yml          5 kvnodes + 1 kvgateway, each node its own volume
  smoke.sh                    end-to-end check described above
```

## How it fits together

`rpc.ClientEnd` has a `SetCall(f Fcall)` hook: swap the simulated network
for something real without touching `raft`/`store`. `store.StartKVServer`
already returns the `*KVServer` and the `raftapi.Raft` it drives; both have
plain `(args *A, reply *R)` RPC-shaped methods, so `rpc.MakeService` +
`rpc.Server.Dispatch` can serve them from a real listener.

- `internal/transport`: TCP backend for that hook. `Peer.Call` is an
  `rpc.Fcall` — lazily dials a peer and reconnects after any error, with a
  bounded timeout so a dead peer makes `Call` return `false` instead of
  hanging, same as the simulated network. `Server` is the listener side:
  accepts TCP connections and dispatches to an `rpc.Server`. Framing and
  request/reply multiplexing over one TCP connection is `internal/demux`.
- `internal/persist`: `cluster.Persister` only keeps raft state and the
  snapshot in memory. A node loads them from disk at startup (before calling
  `StartKVServer`, via `persister.Save(...)`, both exported) and a
  background poller (`persist.Watch`) copies any change out to disk
  atomically (temp file + fsync + rename). `cmd/kvnode` also flushes once on
  SIGTERM/SIGINT so a `docker compose stop` doesn't lose the last few
  milliseconds of state.
  - *Shortcut*: the poller runs every 20ms rather than on every raft
    `persist()` call, since there's no hook into `Persister.Save` to
    intercept it. A crash in that 20ms window can lose the very latest raft
    term/log entry; upgrade by adding a save hook to `cluster.Persister` if
    that window matters.
- `internal/client`: a small Get/Put/Txn client with a leader-sticky retry
  loop over `internal/transport`, calling the same `"KVServer.Get"` /
  `.Put` / `.Txn` RPCs `store`'s own server methods expose.
- `cmd/kvnode`: one group member. Loads persisted state, builds 5
  `rpc.ClientEnd`s (one `SetCall`-wired `transport.Peer` per other server),
  calls `store.StartKVServer`, registers the two services it returns on an
  `rpc.Server`, and serves that over `transport.Listen`.
- `cmd/kvgateway`: HTTP in front of `internal/client`.

`internal/raft`, `internal/raftapi`, `internal/kv/store` (+ `rsm`),
`internal/kv/proto`, `internal/rpc`, `internal/demux`, `internal/codec`, and
`internal/cluster` started from the MIT 6.5840 (Raft/kvraft) lab assignment;
they're trimmed to only what this app needs to build and run (no test
harness, no daemon-process scaffolding, no sharding) and organized as
regular internal packages of this one module.

## Verified

Ran the actual `docker compose up --build` cluster (not just the binaries
locally): Get/Put/Txn through the gateway, stopping 2 of 5 nodes while the
group kept serving reads and writes on its remaining quorum, and a full
`docker compose restart` of all 5 nodes plus the gateway with the data still
there afterward. `docker compose config` also confirms the compose file
parses and resolves correctly.
