// shardgateway exposes the sharded KV service over HTTP:
//
//	GET  /kv/{key}         -> {"value":"...","version":N}, 404 if missing
//	PUT  /kv/{key}         <- {"value":"...","version":N}
//	POST /txn              <- {"compares":[...],"then":[...],"else":[...]}  (cross-shard, 2PC)
//	GET  /admin/config     -> current shard config
//	POST /admin/join       <- {"gid":N,"servers":["host:port",...]}
//	POST /admin/leave      <- {"gid":N}
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync"

	"kvapp/internal/cluster"
	"kvapp/internal/kv/proto"
	"kvapp/internal/shardkv"
	"kvapp/internal/shardkv/shardcfg"
	"kvapp/internal/shardkv/shardctrler"
)

func main() {
	addr := flag.String("addr", ":8080", "HTTP listen address")
	ctrl := flag.String("ctrl", "", "host:port of the shardctrl kvsrv")
	group1 := flag.String("group1", "", "comma-separated host:port of group 1, which owns all shards in the first config")
	flag.Parse()

	clnt := cluster.MakeClnt("shardgateway")
	sck := shardctrler.MakeShardCtrler(clnt, *ctrl)

	// finish any config change a previous controller left half done, then
	// install the first config if there is none yet (as the lab tester does)
	sck.InitController()
	if _, _, err := sck.Get(shardctrler.CONFIG_KEY); err == proto.ErrNoKey {
		scfg := shardcfg.MakeShardConfig()
		scfg.JoinBalance(map[cluster.Tgid][]string{shardcfg.Gid1: strings.Split(*group1, ",")})
		sck.InitConfig(scfg)
	}

	// the clerk keeps per-call state (txn sequence, group clerks), so one
	// request at a time uses it; config changes are serialized separately
	var ckMu, adminMu sync.Mutex
	ck := shardkv.MakeClerk(clnt, sck).(*shardkv.Clerk)

	http.HandleFunc("/kv/", func(w http.ResponseWriter, r *http.Request) {
		key := strings.TrimPrefix(r.URL.Path, "/kv/")
		if key == "" {
			http.Error(w, "missing key", http.StatusBadRequest)
			return
		}
		switch r.Method {
		case http.MethodGet:
			ckMu.Lock()
			value, version, err := ck.Get(key)
			ckMu.Unlock()
			if err == proto.ErrNoKey {
				http.Error(w, string(err), http.StatusNotFound)
				return
			}
			writeJSON(w, http.StatusOK, map[string]any{"value": value, "version": version, "shard": shardcfg.Key2Shard(key)})
		case http.MethodPut:
			var body struct {
				Value   string         `json:"value"`
				Version proto.Tversion `json:"version"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			ckMu.Lock()
			err := ck.Put(key, body.Value, body.Version)
			ckMu.Unlock()
			switch err {
			case proto.OK:
				writeJSON(w, http.StatusOK, map[string]any{"err": err})
			case proto.ErrVersion:
				writeJSON(w, http.StatusConflict, map[string]any{"err": err})
			case proto.ErrMaybe:
				writeJSON(w, http.StatusAccepted, map[string]any{"err": err})
			default:
				writeJSON(w, http.StatusInternalServerError, map[string]any{"err": err})
			}
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	})

	http.HandleFunc("/txn", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var body struct {
			Compares []proto.TxnCompare `json:"compares"`
			Then     []proto.TxnOp      `json:"then"`
			Else     []proto.TxnOp      `json:"else"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		ckMu.Lock()
		reply := ck.Txn(body.Compares, body.Then, body.Else)
		ckMu.Unlock()
		writeJSON(w, http.StatusOK, reply)
	})

	http.HandleFunc("/admin/config", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, sck.Query())
	})

	http.HandleFunc("/admin/join", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Gid     cluster.Tgid `json:"gid"`
			Servers []string     `json:"servers"`
		}
		if r.Method != http.MethodPost || json.NewDecoder(r.Body).Decode(&body) != nil || body.Gid <= 0 || len(body.Servers) == 0 {
			http.Error(w, `POST {"gid":N,"servers":["host:port",...]}`, http.StatusBadRequest)
			return
		}
		adminMu.Lock()
		defer adminMu.Unlock()
		cfg := sck.Query()
		// shardcfg.Join log.Fatal's on these, which would kill the gateway
		if _, ok := cfg.Groups[body.Gid]; ok {
			http.Error(w, fmt.Sprintf("group %d already joined", body.Gid), http.StatusConflict)
			return
		}
		for gid, srvs := range cfg.Groups {
			for _, s := range srvs {
				for _, n := range body.Servers {
					if s == n {
						http.Error(w, fmt.Sprintf("server %s already in group %d", n, gid), http.StatusConflict)
						return
					}
				}
			}
		}
		newcfg := cfg.Copy()
		newcfg.JoinBalance(map[cluster.Tgid][]string{body.Gid: body.Servers})
		sck.ChangeConfigTo(newcfg)
		writeJSON(w, http.StatusOK, sck.Query())
	})

	http.HandleFunc("/admin/leave", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Gid cluster.Tgid `json:"gid"`
		}
		if r.Method != http.MethodPost || json.NewDecoder(r.Body).Decode(&body) != nil {
			http.Error(w, `POST {"gid":N}`, http.StatusBadRequest)
			return
		}
		adminMu.Lock()
		defer adminMu.Unlock()
		cfg := sck.Query()
		if _, ok := cfg.Groups[body.Gid]; !ok {
			http.Error(w, fmt.Sprintf("group %d is not joined", body.Gid), http.StatusConflict)
			return
		}
		if len(cfg.Groups) == 1 {
			http.Error(w, "cannot remove the last group", http.StatusConflict)
			return
		}
		newcfg := cfg.Copy()
		newcfg.LeaveBalance([]cluster.Tgid{body.Gid})
		sck.ChangeConfigTo(newcfg)
		writeJSON(w, http.StatusOK, sck.Query())
	})

	log.Printf("shardgateway listening on %s, ctrl %s", *addr, *ctrl)
	log.Fatal(http.ListenAndServe(*addr, nil))
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}
