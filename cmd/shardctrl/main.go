// shardctrl runs the kvsrv that the shard controller keeps its state in:
// the current/next shard config and the 2PC transaction decisions.
//
// Like the lab's kvsrv it is a single in-memory server: restarting it loses
// the config and decisions.
package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"kvapp/internal/kvsrv"
	"kvapp/internal/rpc"
	"kvapp/internal/transport"
)

func main() {
	addr := flag.String("addr", ":9100", "listen address")
	flag.Parse()

	rpcSrv := rpc.MakeServer()
	rpcSrv.AddService(rpc.MakeService(kvsrv.MakeKVServer()))
	if _, err := transport.Listen("shardctrl", *addr, rpcSrv); err != nil {
		log.Fatalf("listen on %s: %v", *addr, err)
	}
	log.Printf("shardctrl kvsrv listening on %s", *addr)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
}
