// Thin typed wrapper around window.raftUnderFire, exported by
// viz/sim/wasm/main.go. The Go side owns no timer loop: step() must be
// called repeatedly (see main.ts's requestAnimationFrame loop) to make
// virtual time advance at all.

export interface InitConfig {
  numNodes?: number;
  maxRaftState?: number;
  latencyMs?: number;
  jitterMs?: number;
  dropRate?: number;
  dupRate?: number;
}

export interface NodeView {
  ID: number;
  Alive: boolean;
  Term: number;
  Role: string;
  VotedFor: number;
  LogLen: number;
  CommitIndex: number;
  LastApplied: number;
  LastIncludedIndex: number;
}

export interface Violation {
  Kind: string;
  Detail: string;
  At: number;
}

export interface ClusterSnapshot {
  Now: number; // virtual nanoseconds
  Safe: boolean;
  Stuck: boolean;
  Nodes: NodeView[];
  Violations: Violation[];
  EventCount: number;
  AliveCount: number;
  Serving: boolean;
}

export interface TraceEvent {
  t: number;
  node: number;
  term: number;
  kind: string;
  fn: string;
  file: string;
  line: number;
  data?: Record<string, unknown>;
}

export interface RaftUnderFireAPI {
  init(seed: number, config?: InitConfig): void;
  step(): boolean;
  hasPending(): boolean;
  now(): number;
  safe(): boolean;
  stuck(): boolean;
  violations(): Violation[];
  eventsSince(n: number): TraceEvent[];
  getState(): ClusterSnapshot;
  crash(node: number): void;
  restart(node: number): void;
  wipe(node: number): void;
  partition(groups: number[][]): void;
  heal(): void;
  setNetwork(config: InitConfig): void;
  submitGet(key: string): number;
  submitPut(key: string, value: string): number;
  result(id: number): { Done: boolean; Value: string; Version: number; Err: string } | null;
  setTraffic(on: boolean): void;
  checkLinearizable(timeoutMs?: number): { Result: "Ok" | "Illegal" | "Unknown"; OpCount: number; PendingCount: number };
}

declare global {
  interface Window {
    raftUnderFire: RaftUnderFireAPI;
    Go: new () => {
      importObject: WebAssembly.Imports;
      run(instance: WebAssembly.Instance): Promise<void>;
    };
  }
}

export async function loadWasm(url: string): Promise<RaftUnderFireAPI> {
  const go = new window.Go();
  const result = await WebAssembly.instantiateStreaming(fetch(url), go.importObject);
  // main()'s `select {}` keeps the instance alive forever, so don't await this.
  void go.run(result.instance);
  return window.raftUnderFire;
}
