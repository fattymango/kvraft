import { defineConfig } from "vite";

// Build output lands in viz/dist (sibling of viz/web), alongside the WASM
// binary make viz places in public/ before this runs, so viz/dist ends up
// a single servable static site per the spec's "make viz builds ... into
// viz/dist/".
export default defineConfig({
  build: {
    outDir: "../dist",
    emptyOutDir: true,
  },
});
