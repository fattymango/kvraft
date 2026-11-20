// kvgateway exposes a Raft-replicated KV store group over HTTP:
//
//	GET  /kv/{key}         -> {"value":"...","version":N}, 404 if missing
//	PUT  /kv/{key}         <- {"value":"...","version":N}
//	POST /txn              <- {"compares":[...],"then":[...],"else":[...]}
package main

import (
	"encoding/json"
	"flag"
	"log"
	"net/http"
	"strings"

	"kvapp/internal/kv/proto"

	"kvapp/internal/client"
)

func main() {
	addr := flag.String("addr", ":8080", "HTTP listen address")
	peersFlag := flag.String("peers", "", "comma-separated host:port of all servers in the group")
	flag.Parse()

	ck := client.New(strings.Split(*peersFlag, ","))

	http.HandleFunc("/kv/", func(w http.ResponseWriter, r *http.Request) {
		key := strings.TrimPrefix(r.URL.Path, "/kv/")
		if key == "" {
			http.Error(w, "missing key", http.StatusBadRequest)
			return
		}
		switch r.Method {
		case http.MethodGet:
			value, version, err := ck.Get(key)
			if err == proto.ErrNoKey {
				http.Error(w, string(err), http.StatusNotFound)
				return
			}
			writeJSON(w, http.StatusOK, map[string]any{"value": value, "version": version})
		case http.MethodPut:
			var body struct {
				Value   string         `json:"value"`
				Version proto.Tversion `json:"version"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			switch err := ck.Put(key, body.Value, body.Version); err {
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
		reply := ck.Txn(body.Compares, body.Then, body.Else)
		writeJSON(w, http.StatusOK, reply)
	})

	log.Printf("kvgateway listening on %s, group %v", *addr, *peersFlag)
	log.Fatal(http.ListenAndServe(*addr, nil))
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}
