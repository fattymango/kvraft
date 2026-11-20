// kvnode runs one server of a Raft-replicated KV store group: real TCP
// between peers (internal/transport) and raft state persisted to disk
// (internal/persist).
package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"kvapp/internal/cluster"
	"kvapp/internal/kv/store"
	"kvapp/internal/persist"
	"kvapp/internal/rpc"
	"kvapp/internal/transport"
)

func main() {
	id := flag.Int("id", -1, "this server's index into --peers")
	peersFlag := flag.String("peers", "", "comma-separated host:port of all servers in the group, in raft-peer order")
	data := flag.String("data", "/data", "directory for raft state and snapshot")
	maxraftstate := flag.Int("maxraftstate", -1, "snapshot once the raft log exceeds this many bytes; -1 disables snapshotting")
	flag.Parse()

	peerAddrs := strings.Split(*peersFlag, ",")
	if *id < 0 || *id >= len(peerAddrs) {
		log.Fatalf("--id %d out of range for %d --peers", *id, len(peerAddrs))
	}
	if err := os.MkdirAll(*data, 0o755); err != nil {
		log.Fatalf("mkdir %s: %v", *data, err)
	}

	raftstate, snapshot := persist.Load(*data)
	persister := cluster.MakePersister()
	persister.Save(raftstate, snapshot)

	rpcNet := rpc.MakeNetwork()
	ends := make([]*rpc.ClientEnd, len(peerAddrs))
	for i, addr := range peerAddrs {
		ends[i] = rpcNet.MakeEnd(fmt.Sprintf("peer-%d", i))
		if i != *id {
			ends[i].SetCall(transport.NewPeer(peerAddrs[*id], addr).Call)
		}
	}

	ret := store.StartKVServer(ends, cluster.Tgid(1), *id, persister, *maxraftstate)
	rpcSrv := rpc.MakeServer()
	rpcSrv.AddService(rpc.MakeService(ret[0]))
	rpcSrv.AddService(rpc.MakeService(ret[1]))

	// Listen on all interfaces rather than the peer hostname: the hostname
	// is how other nodes dial in, not necessarily how we must bind here.
	_, port, err := net.SplitHostPort(peerAddrs[*id])
	if err != nil {
		log.Fatalf("--peers entry %q for --id %d: %v", peerAddrs[*id], *id, err)
	}
	if _, err := transport.Listen(peerAddrs[*id], ":"+port, rpcSrv); err != nil {
		log.Fatalf("listen on :%s: %v", port, err)
	}
	log.Printf("kvnode %d listening on :%s, peers %v, data %s", *id, port, peerAddrs, *data)

	done := make(chan struct{})
	go persist.Watch(*data, persister, 20*time.Millisecond, done)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	close(done)
	if err := persist.Flush(*data, persister); err != nil {
		log.Printf("final flush: %v", err)
	}
}
