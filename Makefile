.PHONY: viz

# Builds the Raft Under Fire simulator: the WASM binary, then the Vite
# frontend around it, into a single static viz/dist/ site.
viz:
	cp "$$(go env GOROOT)/lib/wasm/wasm_exec.js" viz/web/public/wasm_exec.js
	GOOS=js GOARCH=wasm go build -o viz/web/public/raftviz.wasm ./viz/sim/wasm/
	cd viz/web && npm install && npm run build
