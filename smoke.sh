#!/usr/bin/env bash
# Smoke test for the dockerized kvraft1 cluster: brings it up, exercises
# Get/Put/Txn, kills two nodes to show the group still has a majority, then
# restarts the whole cluster and checks the data survived.
set -euo pipefail
cd "$(dirname "$0")"

GW=http://localhost:18080

curl_json() { curl -sS -m 5 "$@"; }

wait_for_gateway() {
	echo "waiting for gateway..."
	for i in $(seq 1 60); do
		if curl_json -o /dev/null "$GW/kv/__probe__"; then
			return 0
		fi
		sleep 1
	done
	echo "gateway never came up" >&2
	exit 1
}

get() { curl_json "$GW/kv/$1"; echo; }
put() { curl_json -X PUT "$GW/kv/$1" -d "{\"value\":\"$2\",\"version\":$3}"; echo; }

echo "== docker compose up --build =="
docker compose up -d --build

wait_for_gateway

echo "== Put foo=bar =="
put foo bar 0

echo "== Get foo =="
get foo

echo "== Txn: foo -> baz if version 1 =="
curl_json -X POST "$GW/txn" -d '{"compares":[{"key":"foo","version":1}],"then":[{"type":"Put","key":"foo","value":"baz"}],"else":[]}'
echo

echo "== Put qux=hi (new key) =="
put qux hi 0

echo "== stop kvnode0 and kvnode1 (group keeps its majority of 3/5) =="
docker compose stop kvnode0 kvnode1

echo "== Get foo and qux still work =="
get foo
get qux

echo "== Put during partial outage =="
put during-outage ok 0

echo "== bring kvnode0 and kvnode1 back =="
docker compose start kvnode0 kvnode1
sleep 2

echo "== docker compose restart (full cluster bounce) =="
docker compose restart

wait_for_gateway

echo "== data survived the restart =="
get foo
get qux
get during-outage

echo "smoke test passed"
