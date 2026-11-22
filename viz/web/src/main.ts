import "./style.css";
import { loadWasm, type RaftUnderFireAPI, type TraceEvent, type ClusterSnapshot } from "./wasm";
import { Bomber, type ChaosMode } from "./bomber";
import { Sound } from "./sound";
import { ElectionDirector } from "./election";

const ROLE_COLOR: Record<string, string> = {
  leader: "#fde047",
  candidate: "#fbbf24",
  follower: "#60a5fa",
};

// Every RPC send is drawn as a flying message, each kind its own color, so
// the server-to-server traffic is actually visible -- but election traffic
// (vote requests, snapshots) is drawn bigger/brighter with a longer tail so
// it still reads as the moment that matters over routine heartbeat noise.
// `hollow` messages are replies: drawn as a ring instead of a filled dot, so
// request vs. response direction is visible without doubling the legend.
const MESSAGE_STYLE: Record<string, { color: string; r: number; tail: number; hollow?: boolean }> = {
  request_vote_sent: { color: "#38bdf8", r: 5, tail: 5 },
  vote_granted: { color: "#38bdf8", r: 4, tail: 3, hollow: true },
  vote_denied: { color: "#f87171", r: 4, tail: 3, hollow: true },
  install_snapshot_sent: { color: "#f472b6", r: 5, tail: 5 },
  append_entries_sent: { color: "#fbbf24", r: 3.5, tail: 3 },
  append_entries_ok: { color: "#fbbf24", r: 3, tail: 2, hollow: true },
  append_entries_reject: { color: "#f87171", r: 3, tail: 2, hollow: true },
  heartbeat_sent: { color: "#34d399", r: 2.5, tail: 2 },
};

// extends the same color language to kinds that never fly as messages, so
// the mission log reads with the same palette as the stage
const KIND_COLOR: Record<string, string> = {
  become_leader: "#fde047",
  become_candidate: "#fbbf24",
  election_timeout: "#fb923c",
  step_down: "#94a3b8",
  crash: "#ef4444",
  snapshot_taken: "#f472b6",
  commit_index_advance: "#2dd4bf",
};
function kindColor(kind: string): string {
  return KIND_COLOR[kind] ?? MESSAGE_STYLE[kind]?.color ?? "#94a3b8";
}

interface FlyingMessage {
  from: number;
  to: number;
  kind: string;
  start: number; // ms, real time (animation only; does not affect the sim)
}

const NUM_NODES = 5;

type NodeSnap = ClusterSnapshot["Nodes"][number];

// During a partition an old leader can keep believing it leads (in an older
// term) until it hears from the majority. The real leader is the one with
// the highest term; a lower-term "leader" is stale and can't commit anything.
// A leader frozen off the network (cutOff) can't lead either.
function realLeader(snap: ClusterSnapshot, cutOff: { has(id: number): boolean }): NodeSnap | undefined {
  let best: NodeSnap | undefined;
  for (const n of snap.Nodes) {
    if (n.Alive && n.Role === "leader" && !cutOff.has(n.ID) && (!best || n.Term > best.Term)) best = n;
  }
  return best;
}
function isStaleLeader(n: NodeSnap, snap: ClusterSnapshot, cutOff: { has(id: number): boolean }): boolean {
  return n.Alive && n.Role === "leader" && realLeader(snap, cutOff)?.ID !== n.ID;
}

// the real Go gopher (viz/web/public/gopher.png), fetched from
// https://go.dev/blog/gopher/gopher.png -- designed by Renée French,
// Creative Commons Attribution 4.0 (credit: go.dev/blog/gopher). Not a
// redrawn approximation: this is the official artwork, composited with
// state overlays (crown, glow, grayscale+tilt, bandages) at render time.
const GOPHER_ASPECT = 300 / 245;

// comic pop-up word shown over a node when it does something notable
const POPUP: Record<string, { text: string; color: string }> = {
  become_candidate: { text: "ELECTION!", color: "#fbbf24" },
  vote_granted: { text: "VoteGranted ✓", color: "#4ade80" },
  vote_denied: { text: "NOPE", color: "#f87171" },
  become_leader: { text: "LEADER", color: "#fde047" },
  append_entries_ok: { text: "Success ✓", color: "#4ade80" },
  append_entries_reject: { text: "Success ✗", color: "#f87171" },
  commit_index_advance: { text: "commitIndex ↑", color: "#34d399" },
  step_down: { text: "FOLLOWER", color: "#cbd5e1" },
  crash: { text: "OUCH!", color: "#ef4444" },
  restart: { text: "I'M BACK!", color: "#4ade80" },
};
const POPUP_MS = 1100;
const ELECTION_ACTION = new Set(["become_candidate", "request_vote_sent", "vote_granted", "vote_denied", "become_leader"]);
const MAX_POPUPS = 3;
// election title card colors per chaos theme: danger = leader down, gold = new leader
type BannerPal = { light: string; base: string; dark: string; edge: string; glow: string; subText: string };
const BANNER_THEME: Record<ChaosMode, { danger: BannerPal; gold: BannerPal }> = {
  calm: {
    danger: { light: "#ff7a5c", base: "#c2362a", dark: "#8f2219", edge: "#4a120c", glow: "#ff9b7a", subText: "#ffd8c8" },
    gold: { light: "#ffd166", base: "#e08a12", dark: "#a8600a", edge: "#5a3104", glow: "#ffe08a", subText: "#fff1c2" },
  },
  gremlin: {
    danger: { light: "#c084fc", base: "#7e22ce", dark: "#581c87", edge: "#2e0a4a", glow: "#e9a8ff", subText: "#f3d9ff" },
    gold: { light: "#fdba74", base: "#ea580c", dark: "#9a3412", edge: "#431407", glow: "#fed7aa", subText: "#ffedd5" },
  },
  storm: {
    danger: { light: "#7dd3fc", base: "#1e5f8c", dark: "#164063", edge: "#0b1d30", glow: "#bae6fd", subText: "#dbeafe" },
    gold: { light: "#fde68a", base: "#ca8a04", dark: "#854d0e", edge: "#3b2304", glow: "#fef3c7", subText: "#fef9c3" },
  },
  nightmare: {
    danger: { light: "#dc2626", base: "#7f1d1d", dark: "#4c0d0d", edge: "#1a0404", glow: "#ff3b3b", subText: "#fecaca" },
    gold: { light: "#fb923c", base: "#9a3412", dark: "#5c1d08", edge: "#1f0a03", glow: "#fdba74", subText: "#fed7aa" },
  },
};
// scenery + crowd lighting per chaos mode (multiplied over the daytime art)
const THEME_TINT: Record<string, string | undefined> = { gremlin: "#7b5aa6", storm: "#5d6b7c", nightmare: "#6a2a30" }; // words stacked on one gopher at once
const LOG_LINES = 120;
const STEP_BUDGET_MS = 8;

const svg = (body: string) => `<svg viewBox="0 0 24 24" width="22" height="22" fill="currentColor" stroke="currentColor" stroke-linejoin="round" stroke-linecap="round">${body}</svg>`;
const MODE_ICON = {
  calm: `<svg viewBox="0 0 24 24" width="22" height="22"><path d="M12 21V11" stroke="#14532d" stroke-width="2.2" stroke-linecap="round"/><path d="M12 13C12 8 8 6 4 6c0 4 3 7 8 7z" fill="#4ade80" stroke="#14532d" stroke-width="1.5"/><path d="M12 11c0-4 3-6.5 7.5-6.5 0 4-3 6.5-7.5 6.5z" fill="#86efac" stroke="#14532d" stroke-width="1.5"/></svg>`,
  gremlin: `<svg viewBox="0 0 24 24" width="22" height="22"><path d="M5 9 2 3l6 3M19 9l3-6-6 3" fill="#a855f7" stroke="#2e1065" stroke-width="1.4" stroke-linejoin="round"/><ellipse cx="12" cy="13" rx="8" ry="7.5" fill="#a855f7" stroke="#2e1065" stroke-width="1.6"/><path d="M7.5 10.5l3 1.5M16.5 10.5l-3 1.5" stroke="#2e1065" stroke-width="1.6" stroke-linecap="round"/><circle cx="9" cy="12.6" r="1.2" fill="#fde047"/><circle cx="15" cy="12.6" r="1.2" fill="#fde047"/><path d="M8 16q4 3.5 8 0" fill="#2e1065"/><path d="M9.5 16.6l.8 1.3.8-1.1M12.9 16.8l.8 1.1.8-1.3" fill="#fff"/></svg>`,
  storm: `<svg viewBox="0 0 24 24" width="22" height="22"><path d="M6.5 15.5a4.5 4.5 0 0 1-.4-9 6 6 0 0 1 11.4 1.5 3.8 3.8 0 0 1-.5 7.5z" fill="#475569" stroke="#0f172a" stroke-width="1.5"/><path d="M12.5 13.5 10 18h3l-2 4.5" fill="none" stroke="#fde047" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/><path d="M7 18l-1 2.5M17 18l-1 2.5" stroke="#7dd3fc" stroke-width="1.8" stroke-linecap="round"/></svg>`,
  nightmare: `<svg viewBox="0 0 24 24" width="22" height="22"><path d="M12 2.5c-5 0-8.5 3.4-8.5 8 0 2.8 1.4 4.6 3 5.6V19a1.5 1.5 0 0 0 1.5 1.5h8a1.5 1.5 0 0 0 1.5-1.5v-2.9c1.6-1 3-2.8 3-5.6 0-4.6-3.5-8-8.5-8z" fill="#fee2e2" stroke="#450a0a" stroke-width="1.6"/><path d="M6.8 10.5c0-1.4 1.2-2.2 2.5-2.2s2.2 1 2 2.4c-.2 1.4-1.4 2.2-2.6 2-1.2-.1-1.9-1-1.9-2.2zM17.2 10.5c0-1.4-1.2-2.2-2.5-2.2s-2.2 1-2 2.4c.2 1.4 1.4 2.2 2.6 2 1.2-.1 1.9-1 1.9-2.2z" fill="#dc2626"/><path d="M12 13.5l-1.2 2.3h2.4z" fill="#450a0a"/><path d="M9.5 18v2.5M12 18v2.5M14.5 18v2.5" stroke="#450a0a" stroke-width="1.2"/></svg>`,
};

const ICON = {
  play: svg('<path d="M7 4.5v15l12.5-7.5z" stroke-width="1.5"/>'),
  pause: svg('<rect x="5.5" y="4.5" width="4.5" height="15" rx="1"/><rect x="14" y="4.5" width="4.5" height="15" rx="1"/>'),
  step: svg('<path d="M4.5 4.5v15l10-7.5z" stroke-width="1.5"/><rect x="16.5" y="4.5" width="3.5" height="15" rx="1"/>'),
  soundOn: svg('<path d="M3.5 9h4l5-4.5v15l-5-4.5h-4z" stroke-width="1.5"/><path d="M16 8.5a5 5 0 0 1 0 7M18.5 6a8.5 8.5 0 0 1 0 12" fill="none" stroke-width="2"/>'),
  soundOff: svg('<path d="M3.5 9h4l5-4.5v15l-5-4.5h-4z" stroke-width="1.5"/><path d="M16 9l5 6M21 9l-5 6" fill="none" stroke-width="2"/>'),
};
const ICE_MS = 7000; // a frozen node thaws on its own after this long

// original tool icons (also used as the canvas cursor while a tool is armed)
type Tool = "meteor" | "ice" | "amnesia";
const TOOL_SVG: Record<Tool, string> = {
  meteor: `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 32 32"><path d="M31 1 L13 15 L17 19 Z" fill="#fde047"/><path d="M27 1 L11 17 L15 21 Z" fill="#fb923c"/><path d="M31 5 L15 19 L17 21 Z" fill="#f97316"/><circle cx="11" cy="21" r="8" fill="#7c3a12" stroke="#2a1205" stroke-width="2"/><circle cx="9" cy="19" r="3" fill="#b45309"/><circle cx="14" cy="24" r="1.6" fill="#4a1d07"/></svg>`,
  ice: `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 32 32"><path d="M5 11 L15 6 L28 10 L18 15 Z" fill="#e0f2fe" stroke="#0c4a6e" stroke-width="1.6" stroke-linejoin="round"/><path d="M5 11 L18 15 L18 29 L5 25 Z" fill="#7dd3fc" stroke="#0c4a6e" stroke-width="1.6" stroke-linejoin="round"/><path d="M18 15 L28 10 L28 24 L18 29 Z" fill="#38bdf8" stroke="#0c4a6e" stroke-width="1.6" stroke-linejoin="round"/><path d="M8 15 L12 16.5 M8 19 L10.5 20 M21 18 L25 16" stroke="#fff" stroke-width="1.6" stroke-linecap="round"/><path d="M9 25.5 L9.5 28 L10 26 M14 27 L14.4 29.5 L15 27.4" stroke="#bae6fd" stroke-width="1.2" fill="none"/></svg>`,
  amnesia: `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 32 32"><path d="M10 27 C4 27 2.5 20 6 18 C3 13.5 6 8.5 10.5 9.5 C11.5 5 17.5 4.5 18.5 8.5 C21.5 5.5 27.5 7.5 26.5 12.5 C30.5 14.5 29.5 21.5 25.5 22.5 C25.5 27.5 18.5 28.5 16.5 25.5 C14.5 27.5 11.5 28 10 27 Z" fill="#f9a8d4" stroke="#831843" stroke-width="1.8"/><path d="M12 13.5 C14 11.5 16.5 13.5 15.5 15.5 M19.5 12.5 C21.5 14.5 19.5 17.5 21.5 18.5 M11 20 C13 19 15 22 17.5 21" stroke="#831843" stroke-width="1.4" fill="none" stroke-linecap="round"/><circle cx="25" cy="6.5" r="5" fill="#a855f7" stroke="#3b0764" stroke-width="1.3"/><path d="M23.2 5.2 C23.2 3.4 26.8 3.4 26.8 5.4 C26.8 6.8 25 6.8 25 8.2" stroke="#fff" stroke-width="1.5" fill="none" stroke-linecap="round"/><circle cx="25" cy="10" r="0.9" fill="#fff"/></svg>`,
};
const TOOL_HINT: Record<Tool, string> = {
  meteor: "Meteor: crash a node",
  ice: "Ice block: freeze a node off the network (click to smash, or it melts)",
  amnesia: "Amnesia: crash a node and wipe its saved Raft state",
};

// RPCs are labeled on the node receiving them (TigerBeetle-style), using the
// real RPC / reply-field names from viz/sim/raft and the KV service
const RPC_POPUP: Record<string, { text: string; color: string }> = {
  request_vote_sent: { text: "RequestVote", color: "#60a5fa" },
  append_entries_sent: { text: "AppendEntries", color: "#fbbf24" },
  install_snapshot_sent: { text: "InstallSnapshot", color: "#f472b6" },
};

function popupFor(ev: TraceEvent): { node: number; text: string; color: string } | null {
  if (ev.kind === "client_request") {
    const op = String(ev.data?.op ?? "");
    const color = op === "Get" ? "#38bdf8" : op === "Put" ? "#fb923c" : "#c084fc";
    return { node: ev.node, text: op, color };
  }
  const rpc = RPC_POPUP[ev.kind];
  if (rpc && typeof ev.data?.to === "number") {
    return { node: ev.data.to as number, ...rpc };
  }
  if (ev.kind === "client_reply" && ev.data?.err === "ErrWrongLeader") {
    return { node: ev.node, text: "ErrWrongLeader", color: "#cbd5e1" };
  }
  const p = POPUP[ev.kind];
  return p ? { node: ev.node, ...p } : null;
}

class App {
  private api!: RaftUnderFireAPI;
  private bomber!: Bomber;
  private canvas: HTMLCanvasElement;
  private ctx: CanvasRenderingContext2D;
  private playing = false;
  private frozen = false;
  private readonly stepsPerFrame = 6.8; // fixed; 15% under the old 8 (fractions carry to the next frame)
  private stepCarry = 0;
  private logBuffer: TraceEvent[] = [];
  private nextElectionStepAt = 0;
  private trafficPausedForElection = false;
  private lastEventIdx = 0;
  private messages: FlyingMessage[] = [];
  private snapshot: ClusterSnapshot | null = null;
  private eventLogEl: HTMLDivElement;
  private safetyEl: HTMLDivElement;
  private pendingReqID: number | null = null;
  private pendingReqKind: "Get" | "Put" | null = null;
  private sound = new Sound();
  private gopherImg!: HTMLImageElement;
  private tintCanvas = document.createElement("canvas");
  private election = new ElectionDirector();
  private shakeStart = -Infinity;
  private readonly shakeDurationMs = 300;
  private selectedNode: number | null = null;
  private prevAlive: boolean[] = Array(NUM_NODES).fill(true);
  private recoveringUntil: number[] = Array(NUM_NODES).fill(-Infinity);
  // per node: words still on screen, newest last; they pile up instead of replacing each other
  private popups: { text: string; color: string; start: number; dx: number; dy: number; rot: number }[][] =
    Array.from({ length: NUM_NODES }, () => []);
  private bgCache: HTMLCanvasElement | null = null;
  private crowdSprites: HTMLCanvasElement[] = [];
  private crowdScaled: HTMLCanvasElement[] = [];
  private crowdScaledW = 0;
  private cachesLost = false; // set when the browser discarded a cached canvas; rebuilt next frame
  private tool: Tool | null = null;
  private isolated = new Map<number, number>(); // node -> when its bubble went up
  private fx: { kind: Tool | "shatter"; node: number; start: number; done: boolean }[] = [];
  private crowd: { x: number; y: number; sprite: number; phase: number; flag: string | null }[] | null = null;
  private cheerAt = -Infinity;
  private gaspAt = -Infinity;

  constructor() {
    const app = document.getElementById("app")!;
    app.innerHTML = `
      <div class="layout">
        <div class="main">
          <div class="leftside">
            <div class="panel-card left-controls">
              <div class="panel-card-title">Controls <span id="safety" class="safety safe">SAFETY ✓</span></div>
              <div class="control-row">
                <button id="btnPlay" class="icon-btn" title="Play / pause" aria-label="Play">${ICON.play}</button>
                <button id="btnStep" class="icon-btn" title="Step" aria-label="Step">${ICON.step}</button>
                <button id="btnSound" class="icon-btn" title="Sound" aria-label="Sound off">${ICON.soundOff}</button>
              </div>
              <div class="chaos-select" id="chaosMode">
                <button data-mode="calm" class="active">${MODE_ICON.calm}<span>Calm</span></button>
                <button data-mode="gremlin">${MODE_ICON.gremlin}<span>Gremlin</span></button>
                <button data-mode="storm">${MODE_ICON.storm}<span>Storm</span></button>
                <button data-mode="nightmare">${MODE_ICON.nightmare}<span>Nightmare</span></button>
              </div>
            </div>
            <div class="panel-card inspector-card" id="inspectorCard">
              <div class="panel-card-title inspector-title">Selected node <span id="inspectorId"></span></div>
              <div class="portrait-frame"><canvas id="portrait" width="220" height="200"></canvas></div>
              <div id="inspectorBody" class="inspector-body"></div>
              <div class="inspector-hint">click a gopher to inspect it</div>
            </div>
          </div>
          <div class="center">
            <div class="stage-frame">
              <canvas id="stage"></canvas>
              <div class="toolbar" id="toolbar">
                ${(Object.keys(TOOL_SVG) as Tool[]).map((t) => `<button data-tool="${t}" title="${TOOL_HINT[t]}">${TOOL_SVG[t]}</button>`).join("")}
              </div>
              <span class="gopher-credit">Go gopher by Renée French, CC BY 4.0</span>
            </div>
          </div>
          <div class="side">
            <div class="controls">
              <div class="panel-card">
                <div class="panel-card-title">Client request</div>
                <div class="manual-request">
                  <input id="reqKey" type="text" placeholder="key" value="alpha">
                  <input id="reqValue" type="text" placeholder="value" value="hello">
                  <button id="btnGet">Get</button>
                  <button id="btnPut">Put</button>
                </div>
                <div id="reqResult" class="req-result"></div>
              </div>
              <div class="panel-card">
                <div class="panel-card-title">Linearizability</div>
                <button id="btnLinearizability">Check linearizability</button>
                <div id="linResult" class="lin-result"></div>
              </div>
            </div>
            <!-- mission log disabled
            <div class="panel-card eventlog-card">
              <div class="panel-card-title">Mission log</div>
              <div class="eventlog" id="eventlog"></div>
            </div>
            -->
          </div>
        </div>
      </div>
    `;
    this.canvas = document.getElementById("stage") as HTMLCanvasElement;
    this.ctx = this.canvas.getContext("2d")!;
    this.eventLogEl = document.getElementById("eventlog") as HTMLDivElement;
    this.safetyEl = document.getElementById("safety") as HTMLDivElement;

    document.getElementById("btnPlay")!.addEventListener("click", () => this.togglePlay());
    document.getElementById("btnStep")!.addEventListener("click", () => this.doSteps(1));
    const chaosDiv = document.getElementById("chaosMode")!;
    chaosDiv.addEventListener("click", (e) => {
      const btn = (e.target as HTMLElement).closest("button");
      if (!btn) return;
      for (const b of chaosDiv.querySelectorAll("button")) b.classList.remove("active");
      btn.classList.add("active");
      this.bomber.setMode(btn.dataset.mode as ChaosMode);
      this.bgCache = null; // repaint the scenery and crowd for the new mode's theme
      this.crowdScaledW = 0;
    });
    document.getElementById("btnSound")!.addEventListener("click", (e) => {
      this.sound.enabled = !this.sound.enabled;
      const btn = e.currentTarget as HTMLButtonElement;
      btn.innerHTML = this.sound.enabled ? ICON.soundOn : ICON.soundOff;
      btn.setAttribute("aria-label", this.sound.enabled ? "Sound on" : "Sound off");
    });
    document.getElementById("toolbar")!.addEventListener("click", (e) => {
      const btn = (e.target as HTMLElement).closest("button");
      if (btn) this.setTool(this.tool === btn.dataset.tool ? null : (btn.dataset.tool as Tool));
    });
    window.addEventListener("keydown", (e) => {
      if (e.key === "Escape") this.setTool(null);
    });
    document.getElementById("btnLinearizability")!.addEventListener("click", () => {
      const r = this.api.checkLinearizable(1000);
      const el = document.getElementById("linResult")!;
      el.textContent = `${r.Result} (${r.OpCount} ops checked, ${r.PendingCount} in flight)`;
      el.className = `lin-result ${r.Result === "Illegal" ? "unsafe" : "safe"}`;
    });
    document.getElementById("btnGet")!.addEventListener("click", () => {
      const key = (document.getElementById("reqKey") as HTMLInputElement).value;
      this.pendingReqID = this.api.submitGet(key);
      this.pendingReqKind = "Get";
      this.renderReqResult("Get", "pending...", "pending");
    });
    document.getElementById("btnPut")!.addEventListener("click", () => {
      const key = (document.getElementById("reqKey") as HTMLInputElement).value;
      const value = (document.getElementById("reqValue") as HTMLInputElement).value;
      this.pendingReqID = this.api.submitPut(key, value);
      this.pendingReqKind = "Put";
      this.renderReqResult("Put", "pending...", "pending");
    });
    this.canvas.addEventListener("click", (e) => {
      const [mx, my] = this.toCanvasCoords(e);
      const hit = this.nodeAt(mx, my);
      if (this.tool && hit !== null) {
        this.useTool(this.tool, hit);
      } else {
        this.selectedNode = hit !== null && hit === this.selectedNode ? null : hit;
      }
    });
  }

  async start() {
    this.gopherImg = new Image();
    // load event, not decode(): decode() can stall indefinitely in a background tab
    await new Promise((res, rej) => {
      this.gopherImg.onload = res;
      this.gopherImg.onerror = rej;
      this.gopherImg.src = "/gopher.png";
    });
    this.tintCanvas.width = this.gopherImg.naturalWidth;
    this.tintCanvas.height = this.gopherImg.naturalHeight;
    this.buildCrowdSprites();
    // the browser may drop canvas contents (GPU reset, sleep, memory pressure)
    this.canvas.addEventListener("contextrestored", () => (this.cachesLost = true));
    this.api = await loadWasm("/raftviz.wasm");
    this.bomber = new Bomber(this.api);
    // snapshots keep the log short; without them every step got slower as the log grew
    this.api.init(Date.now() & 0xffffffff, { numNodes: NUM_NODES, latencyMs: 20, jitterMs: 10, maxRaftState: 4000 });
    this.api.setTraffic(true); // background client traffic is always on
    this.snapshot = this.api.getState();
    this.resizeCanvas();
    window.addEventListener("resize", () => this.resizeCanvas());
    requestAnimationFrame((t) => this.frame(t));
  }

  togglePlay() {
    this.setPlaying(!this.playing);
  }

  setPlaying(playing: boolean) {
    this.playing = playing;
    const btn = document.getElementById("btnPlay")!;
    btn.innerHTML = playing ? ICON.pause : ICON.play;
    btn.setAttribute("aria-label", playing ? "Pause" : "Play");
  }

  toCanvasCoords(e: MouseEvent): [number, number] {
    const rect = this.canvas.getBoundingClientRect();
    const scaleX = this.canvas.width / rect.width;
    const scaleY = this.canvas.height / rect.height;
    return [(e.clientX - rect.left) * scaleX, (e.clientY - rect.top) * scaleY];
  }


  /** Returns the node ID whose body the given canvas coords land in, or null. */
  nodeAt(mx: number, my: number): number | null {
    for (let i = 0; i < NUM_NODES; i++) {
      const [x, y] = this.nodePos(i);
      if (Math.hypot(mx - x, my - y) < this.nodeR()) return i;
    }
    return null;
  }

  // Drag a line across the node ring to partition the cluster: nodes fall
  // into two groups depending on which side of the line they're on.
  setTool(tool: Tool | null) {
    this.tool = tool;
    for (const b of document.querySelectorAll<HTMLButtonElement>("#toolbar button")) {
      b.classList.toggle("active", b.dataset.tool === tool);
    }
    this.canvas.style.cursor = tool
      ? `url("data:image/svg+xml,${encodeURIComponent(TOOL_SVG[tool].replace("<svg ", '<svg width="40" height="40" '))}") 20 20, crosshair`
      : "";
  }

  useTool(tool: Tool, i: number) {
    const n = this.snapshot?.Nodes[i];
    if (!n) return;
    const now = performance.now();
    if (tool === "meteor") {
      if (!n.Alive || this.fx.some((f) => f.kind === "meteor" && f.node === i && !f.done)) return;
      this.fx.push({ kind: tool, node: i, start: now, done: false }); // crash lands on impact
      this.sound.meteorFall(650);
    } else if (tool === "ice") {
      if (this.isolated.has(i)) this.thaw(i, true);
      else if (n.Alive) {
        this.isolated.set(i, now);
        this.applyIsolation();
        this.addPopup(i, "FROZEN!", "#7dd3fc");
        this.sound.freeze();
      }
    } else {
      if (!n.Alive || this.fx.some((f) => f.kind === "amnesia" && f.node === i)) return;
      this.addPopup(i, "WHO AM I?", "#c084fc");
      this.sound.amnesia();
      this.fx.push({ kind: tool, node: i, start: now, done: false }); // wipe lands when the swirl peaks
    }
  }

  // ice melts on its own (the cluster heals); smashing it just ends it early
  thaw(i: number, smashed: boolean) {
    if (!this.isolated.delete(i)) return;
    this.applyIsolation();
    const now = performance.now();
    this.fx.push({ kind: "shatter", node: i, start: now, done: true });
    this.addPopup(i, smashed ? "SMASH!" : "THAWED!", "#7dd3fc");
    this.sound.shatter();
  }

  // a chunky ice cube around a frozen (cut-off) gopher; it sags and drips as it melts
  drawIce(i: number, gx: number, gy: number, r: number, now: number) {
    const since = this.isolated.get(i);
    if (since === undefined) return;
    const ctx = this.ctx;
    const age = now - since;
    const grow = Math.min(1, age / 180);
    const melt = Math.max(0, (age - (ICE_MS - 1500)) / 1500); // last 1.5s: melting
    const W = r * 2.3 * (grow < 1 ? 0.6 + grow * 0.45 : 1);
    const H = r * 3.0 * (1 - melt * 0.35) * (grow < 1 ? 0.6 + grow * 0.45 : 1);
    const dx = W * 0.28; // iso depth offset of the back faces
    const dyy = -W * 0.2;
    const x0 = gx - W / 2 - dx / 2;
    const yb = gy + r * 0.32;
    const yt = yb - H;
    const poly = (pts: number[][], fill: string) => {
      ctx.beginPath();
      pts.forEach(([x, y], k) => (k ? ctx.lineTo(x, y) : ctx.moveTo(x, y)));
      ctx.closePath();
      ctx.fillStyle = fill;
      ctx.fill();
      ctx.stroke();
    };
    ctx.save();
    ctx.globalAlpha = 1 - melt * 0.5;
    ctx.lineJoin = "round";
    ctx.strokeStyle = "rgba(255, 255, 255, 0.95)";
    ctx.lineWidth = 2.5;
    ctx.shadowColor = "#7dd3fc";
    ctx.shadowBlur = 12;
    poly([[x0 + W, yt], [x0 + W + dx, yt + dyy], [x0 + W + dx, yb + dyy], [x0 + W, yb]], "rgba(56, 189, 248, 0.38)"); // right face
    poly([[x0, yt], [x0 + dx, yt + dyy], [x0 + W + dx, yt + dyy], [x0 + W, yt]], "rgba(224, 242, 254, 0.7)"); // top face
    poly([[x0, yt], [x0 + W, yt], [x0 + W, yb], [x0, yb]], "rgba(186, 230, 253, 0.42)"); // front face
    ctx.shadowBlur = 0;
    // glints and frost
    ctx.strokeStyle = "rgba(255, 255, 255, 0.85)";
    ctx.lineWidth = 3;
    ctx.lineCap = "round";
    ctx.beginPath();
    ctx.moveTo(x0 + W * 0.12, yt + H * 0.12);
    ctx.lineTo(x0 + W * 0.34, yt + H * 0.04);
    ctx.moveTo(x0 + W * 0.1, yt + H * 0.24);
    ctx.lineTo(x0 + W * 0.2, yt + H * 0.2);
    ctx.stroke();
    ctx.fillStyle = "rgba(255, 255, 255, 0.7)";
    for (let k = 0; k < 14; k++) {
      const fx = x0 + ((k * 37) % 100) / 100 * W;
      const fy = yt + ((k * 61) % 100) / 100 * H;
      ctx.beginPath();
      ctx.arc(fx, fy, 1.4, 0, Math.PI * 2);
      ctx.fill();
    }
    // icicles along the front bottom edge
    ctx.fillStyle = "rgba(224, 242, 254, 0.9)";
    for (let k = 0; k < 5; k++) {
      const ix = x0 + W * (0.12 + k * 0.19);
      const len = r * (0.18 + (k % 2) * 0.14) * (1 + melt);
      ctx.beginPath();
      ctx.moveTo(ix - 4, yb);
      ctx.lineTo(ix + 4, yb);
      ctx.lineTo(ix, yb + len);
      ctx.closePath();
      ctx.fill();
    }
    if (melt > 0) {
      ctx.fillStyle = "rgba(125, 211, 252, 0.45)";
      ctx.beginPath();
      ctx.ellipse(gx, gy + r * 0.2, W * (0.6 + melt * 0.5), W * 0.22, 0, 0, Math.PI * 2);
      ctx.fill();
    }
    ctx.restore();
  }


  // every frozen node sits alone in its own partition; everyone else stays together
  applyIsolation() {
    if (this.isolated.size === 0) {
      this.api.heal();
      return;
    }
    const rest = [...Array(NUM_NODES).keys()].filter((i) => !this.isolated.has(i));
    const groups = [...this.isolated.keys()].map((i) => [i]);
    if (rest.length) groups.push(rest);
    this.api.partition(groups);
  }

  drawToolFx(now: number) {
    const ctx = this.ctx;
    const r = this.nodeR();
    // an effect that has not landed yet (crash/wipe pending) is never dropped, even if frames were skipped
    this.fx = this.fx.filter((f) => !f.done || now - f.start < (f.kind === "meteor" ? 1300 : f.kind === "amnesia" ? 1100 : 600));
    for (const f of this.fx) {
      const [gx, gy] = this.groundPos(f.node);
      const t = (now - f.start) / (f.kind === "meteor" ? 650 : 1100);
      ctx.save();
      if (f.kind === "meteor") {
        if (t < 1) {
          const e = t * t;
          const sx = gx + this.canvas.width * 0.3;
          const sy = -80;
          const x = sx + (gx - sx) * e;
          const y = sy + (gy - r * 0.5 - sy) * e;
          const tail = ctx.createLinearGradient(x, y, x + 140, y - 160);
          tail.addColorStop(0, "rgba(253, 186, 116, 0.95)");
          tail.addColorStop(0.4, "rgba(249, 115, 22, 0.6)");
          tail.addColorStop(1, "rgba(253, 224, 71, 0)");
          ctx.strokeStyle = tail;
          ctx.lineCap = "round";
          ctx.lineWidth = r * 0.55;
          ctx.beginPath();
          ctx.moveTo(x, y);
          ctx.lineTo(x + 140, y - 160);
          ctx.stroke();
          ctx.shadowColor = "#f97316";
          ctx.shadowBlur = 25;
          ctx.fillStyle = "#7c3a12";
          ctx.strokeStyle = "#2a1205";
          ctx.lineWidth = 2;
          ctx.beginPath();
          ctx.arc(x, y, r * 0.38, 0, Math.PI * 2);
          ctx.fill();
          ctx.stroke();
          ctx.shadowBlur = 0;
          ctx.fillStyle = "#b45309";
          ctx.beginPath();
          ctx.arc(x - r * 0.1, y - r * 0.1, r * 0.13, 0, Math.PI * 2);
          ctx.fill();
          // target marker on the ground
          ctx.strokeStyle = `rgba(239, 68, 68, ${0.5 + 0.5 * Math.sin(now / 60)})`;
          ctx.lineWidth = 3;
          ctx.setLineDash([8, 6]);
          ctx.beginPath();
          ctx.ellipse(gx, gy, r * 1.25, r * 0.5, 0, 0, Math.PI * 2);
          ctx.stroke();
        } else {
          if (!f.done) {
            f.done = true;
            this.api.crash(f.node);
            this.shakeStart = now;
            this.sound.meteorImpact();
          }
          const k = Math.min(1, (t - 1) / 1);
          ctx.globalAlpha = 1 - k;
          ctx.fillStyle = "#fde047";
          ctx.beginPath();
          ctx.ellipse(gx, gy - r * 0.4, r * (0.8 + k * 1.6), r * (0.6 + k * 1.1), 0, 0, Math.PI * 2);
          ctx.fill();
          ctx.fillStyle = "#f97316";
          ctx.beginPath();
          ctx.ellipse(gx, gy - r * 0.4, r * (0.5 + k * 1.2), r * (0.4 + k * 0.8), 0, 0, Math.PI * 2);
          ctx.fill();
          ctx.strokeStyle = "#7c2d12";
          ctx.lineWidth = 4;
          ctx.beginPath();
          ctx.ellipse(gx, gy, r * (1 + k * 2), r * (0.4 + k * 0.8), 0, 0, Math.PI * 2);
          ctx.stroke();
        }
      } else if (f.kind === "shatter") {
        // bubble shatters into shards
        const k = (now - f.start) / 600;
        ctx.globalAlpha = Math.max(0, 1 - k);
        ctx.fillStyle = "#e0f2fe";
        ctx.strokeStyle = "#0284c7";
        ctx.lineWidth = 1.5;
        for (let s2 = 0; s2 < 14; s2++) {
          const a = (s2 / 14) * Math.PI * 2;
          const d = r * (1 + k * 2.2);
          const x = gx + Math.cos(a) * d;
          const y = gy - r * 1.4 + Math.sin(a) * d * 0.8 + k * k * r;
          ctx.save();
          ctx.translate(x, y);
          ctx.rotate(a + k * 6);
          ctx.beginPath();
          ctx.moveTo(0, -6);
          ctx.lineTo(5, 4);
          ctx.lineTo(-4, 3);
          ctx.closePath();
          ctx.fill();
          ctx.stroke();
          ctx.restore();
        }
      } else if (f.kind === "amnesia") {
        // memories spiral out of its head, then the wipe lands
        const [hx, hy] = [gx, gy - r * 2.3];
        if (t >= 0.6 && !f.done) {
          f.done = true;
          this.api.wipe(f.node);
          this.shakeStart = now;
        }
        ctx.globalAlpha = t > 0.8 ? (1 - t) / 0.2 : 1;
        ctx.lineCap = "round";
        for (let arm = 0; arm < 3; arm++) {
          ctx.strokeStyle = ["#a855f7", "#ec4899", "#c084fc"][arm];
          ctx.lineWidth = 4;
          ctx.shadowColor = "#a855f7";
          ctx.shadowBlur = 10;
          ctx.beginPath();
          for (let k = 0; k <= 40; k++) {
            const u = k / 40;
            const a2 = u * Math.PI * 3 + (arm * Math.PI * 2) / 3 + now / 120;
            const rad = u * r * (0.4 + t * 0.9);
            const px = hx + Math.cos(a2) * rad;
            const py = hy + Math.sin(a2) * rad * 0.55;
            if (k) ctx.lineTo(px, py);
            else ctx.moveTo(px, py);
          }
          ctx.stroke();
        }
        ctx.shadowBlur = 0;
        // floating "?" and fragments of its old log drifting away
        ctx.font = `900 ${Math.round(r * 0.45)}px "Arial Black", Impact, sans-serif`;
        ctx.textAlign = "center";
        ctx.lineJoin = "round";
        for (let q = 0; q < 4; q++) {
          const a2 = (q / 4) * Math.PI * 2 + t * 2;
          const qx = hx + Math.cos(a2) * r * (0.6 + t * 1.2);
          const qy = hy + Math.sin(a2) * r * 0.5 - t * r * 0.8;
          const label = q % 2 ? "?" : ["term", "log", "vote"][q % 3];
          ctx.strokeStyle = "#3b0764";
          ctx.lineWidth = 4;
          ctx.strokeText(label, qx, qy);
          ctx.fillStyle = q % 2 ? "#f0abfc" : "#e9d5ff";
          ctx.fillText(label, qx, qy);
        }

      }
      ctx.restore();
    }
  }

  doSteps(n: number) {
    // time budget per frame: on a slow machine run fewer sim steps rather
    // than drop frames, so animations stay smooth
    const deadline = performance.now() + STEP_BUDGET_MS;
    for (let i = 0; i < n && (i === 0 || performance.now() < deadline); i++) {
      if (!this.api.step()) break;
      // stop the batch the moment an election starts, so it can't start and
      // finish inside one frame -- the slow-motion takes over from here
      if (this.pullEvents() && n > 1) break;
    }
    this.snapshot = this.api.getState();
    const now = performance.now();
    for (const node of this.snapshot.Nodes) {
      if (node.Alive && !this.prevAlive[node.ID]) {
        this.recoveringUntil[node.ID] = now + 5500; // just restarted -- patched up for a bit
      }
      this.prevAlive[node.ID] = node.Alive;
    }
    if (!this.snapshot.Safe && !this.frozen) {
      // "If anything is violated, freeze the simulation and show exactly
      // what broke" -- stop driving virtual time forward at all.
      this.frozen = true;
      this.setPlaying(false);
    }
  }

  pollPendingRequest() {
    if (this.pendingReqID === null) return;
    const r = this.api.result(this.pendingReqID);
    if (!r || !r.Done) return;
    const reqKind = this.pendingReqKind;
    this.pendingReqID = null;
    this.pendingReqKind = null;
    const body = reqKind === "Get" && r.Err === "OK" ? `"${r.Value}" (v${r.Version})` : r.Err;
    this.renderReqResult(reqKind, body, r.Err === "OK" ? "ok" : "err");
  }

  renderReqResult(reqKind: "Get" | "Put" | null, text: string, kind: "pending" | "ok" | "err") {
    const el = document.getElementById("reqResult")!;
    el.textContent = `${reqKind ?? ""} -> ${text}`.trim();
    el.className = `req-result ${kind}`;
  }

  renderInspector(snap: ClusterSnapshot) {
    // nothing clicked yet: follow the leader, like a spectator camera
    const id = this.selectedNode ?? realLeader(snap, this.isolated)?.ID ?? 0;
    const n = snap.Nodes[id];
    const now = performance.now();
    const recovering = n.Alive && now < (this.recoveringUntil[n.ID] ?? -Infinity);
    const electing = n.Alive && this.election.active() && (n.Role === "candidate" || this.election.isCandidate(n.ID));
    const [status, statusColor] = !n.Alive
      ? ["Crashed", "#f87171"]
      : recovering
        ? ["Recovering", "#4ade80"]
        : electing
          ? ["Voting", "#fbbf24"]
          : ["Normal", "#ffffff"];
    const role = n.Alive && n.Role ? n.Role.charAt(0).toUpperCase() + n.Role.slice(1) : "Down";
    const roleText = isStaleLeader(n, snap, this.isolated) ? "Stale leader" : role;
    const pct = n.LogLen > 0 ? Math.min(100, (n.CommitIndex / n.LogLen) * 100) : 0;
    const row = (k: string, v: string | number, color = "#ffffff") =>
      `<div class="kv"><span>${k}:</span><b style="color:${color}">${v}</b></div>`;

    document.getElementById("inspectorId")!.textContent = `n${n.ID}`;
    this.renderPortrait(n, now, recovering, electing);
    document.getElementById("inspectorBody")!.innerHTML = `
      ${row("Role", roleText, n.Alive && !isStaleLeader(n, snap, this.isolated) ? ROLE_COLOR[n.Role] ?? "#ffffff" : "#94a3b8")}
      ${row("Status", status, statusColor)}
      ${row("Term", n.Term)}
      ${row("Voted for", n.VotedFor < 0 ? "-" : "n" + n.VotedFor)}
      ${row("Log", n.LogLen)}
      ${row("Commit", `${n.CommitIndex}/${n.LogLen}`)}
      ${row("Applied", n.LastApplied)}
      ${row("Snapshot", n.LastIncludedIndex)}
      <div class="kv-sub">COMMITTED</div>
      <div class="meter"><i style="width:${pct}%"></i></div>
    `;
  }

  // the inspected gopher, drawn live with the same state art as the stage
  renderPortrait(n: ClusterSnapshot["Nodes"][number], now: number, recovering: boolean, electing: boolean) {
    const pc = document.getElementById("portrait") as HTMLCanvasElement;
    const pctx = pc.getContext("2d")!;
    const stage = this.ctx;
    this.ctx = pctx; // reuse the stage drawing helpers on the portrait canvas
    pctx.clearRect(0, 0, pc.width, pc.height);
    const glow = !n.Alive ? "#ef4444" : ROLE_COLOR[n.Role] ?? "#64748b";
    const bg = pctx.createRadialGradient(110, 115, 10, 110, 115, 120);
    bg.addColorStop(0, glow + "55");
    bg.addColorStop(1, "transparent");
    pctx.fillStyle = bg;
    pctx.fillRect(0, 0, pc.width, pc.height);
    const r = 50;
    const [x, y] = [110, 118];
    pctx.fillStyle = "rgba(15, 23, 42, 0.45)";
    pctx.beginPath();
    pctx.ellipse(x, y + r, r * (n.Alive ? 0.75 : 1.2), r * 0.22, 0, 0, Math.PI * 2);
    pctx.fill();
    this.drawCritter(x, y, r, {
      glowColor: glow,
      dead: !n.Alive,
      glowBlur: electing ? 20 : 0,
      happy: n.Alive && n.Role === "leader" && !isStaleLeader(n, this.snapshot!, this.isolated),
      recovering,
      now,
    });
    if (n.Alive && n.Role === "leader") {
      const { dy, w } = this.gopherBounds(x, y, r);
      this.drawCrown(x, dy - w * 0.08, w * 0.72, now, isStaleLeader(n, this.snapshot!, this.isolated));
    }
    this.ctx = stage;
  }

  /** Returns true if a node became a candidate (an election just started). */
  pullEvents(): boolean {
    const events = this.api.eventsSince(this.lastEventIdx);
    this.lastEventIdx += events.length;
    const now = performance.now();
    let electionStarted = false;
    // who is down right now, kept current within this batch of events
    const down = new Set(this.snapshot?.Nodes.filter((n) => !n.Alive).map((n) => n.ID) ?? []);
    for (const ev of events) {
      if (ev.kind === "become_candidate") electionStarted = true;
      if (ev.kind === "crash") {
        down.add(ev.node);
        this.popups[ev.node] = []; // a dead gopher drops whatever it was saying
      } else if (ev.kind === "restart") {
        down.delete(ev.node);
      }
      // this.logEvent(ev); // mission log disabled
      if (MESSAGE_STYLE[ev.kind] && typeof ev.data?.to === "number") {
        this.messages.push({ from: ev.node, to: ev.data.to as number, kind: ev.kind, start: now });
      }
      const pop = popupFor(ev);
      // an RPC labeled on its receiver only counts if it can actually arrive:
      // dead or frozen nodes never get the message
      const unreachable = pop && pop.node !== ev.node && (down.has(pop.node) || this.isolated.has(pop.node));
      if (pop && pop.node >= 0 && pop.node < NUM_NODES && !unreachable) this.addPopup(pop.node, pop.text, pop.color);
      if (ev.kind === "request_vote_sent" || ev.kind === "install_snapshot_sent") {
        this.sound.blip();
      } else if (ev.kind === "commit_index_advance") {
        this.sound.chime();
      } else if (ev.kind === "crash") {
        this.sound.boom();
        this.shakeStart = now;
        this.gaspAt = now;
      } else if (ev.kind === "become_leader") {
        this.cheerAt = now;
        this.sound.chime();
      }
    }
    return electionStarted;
  }

  logEvent(ev: TraceEvent) {
    this.logBuffer.push(ev);
    if (this.logBuffer.length > LOG_LINES) this.logBuffer.splice(0, this.logBuffer.length - LOG_LINES);
  }

  // one DOM write + one scroll per frame: touching layout per event was the
  // main thing making the whole window stutter
  flushLog() {
    if (this.logBuffer.length === 0) return;
    const frag = document.createDocumentFragment();
    for (const ev of this.logBuffer) {
      const line = document.createElement("div");
      const kindSpan = document.createElement("span");
      kindSpan.style.color = kindColor(ev.kind);
      kindSpan.textContent = ev.kind;
      line.append(`T=${(ev.t / 1_000_000).toFixed(0)}ms · n${ev.node} · term ${ev.term} · `, kindSpan);
      frag.appendChild(line);
    }
    this.logBuffer = [];
    this.eventLogEl.appendChild(frag);
    const extra = this.eventLogEl.childElementCount - LOG_LINES;
    for (let i = 0; i < extra; i++) this.eventLogEl.firstChild!.remove();
    this.eventLogEl.scrollTop = this.eventLogEl.scrollHeight;
  }

  frame(_t: number) {
    const now = performance.now();

    if (this.snapshot) {
      this.snapshot = this.api.getState(); // catch crashes from tools/bomber before stepping
      this.election.update(this.snapshot, now, this.isolated);
    }

    // background traffic floods the event queue, so pause it while an election
    // plays out -- otherwise the one-event-at-a-time slow motion is all traffic
    const electing = this.election.active();
    if (electing !== this.trafficPausedForElection) {
      this.trafficPausedForElection = electing;
      this.api.setTraffic(!electing);
    }
    if (!this.frozen && this.playing) {
      if (electing) {
        // cinematic pacing: quiet steps (followers waiting to time out) go by
        // quickly; a step carrying election action holds so each vote is seen
        if (now >= this.nextElectionStepAt) {
          const before = this.lastEventIdx;
          this.doSteps(1);
          const action = this.api.eventsSince(before).some((e) => ELECTION_ACTION.has(e.kind));
          this.nextElectionStepAt = now + (action ? 220 : 25);
        }
      } else {
        this.stepCarry += this.stepsPerFrame;
        const n = Math.floor(this.stepCarry);
        this.stepCarry -= n;
        if (n > 0) this.doSteps(n);
      }
    }
    // bomber only acts while the sim advances: a paused snapshot is stale
    if (this.snapshot && !this.frozen && this.playing) {
      this.bomber.heal(now, this.snapshot);
      if (this.election.active()) this.bomber.holdOff(now);
      else this.bomber.tick(now, this.snapshot, this.isolated, (tool, node) => this.useTool(tool, node));
    }
    for (const [i, since] of this.isolated) if (now - since > ICE_MS) this.thaw(i, false);
    this.pollPendingRequest();
    this.render();
    requestAnimationFrame((t) => this.frame(t));
  }

  resizeCanvas() {
    this.canvas.width = this.canvas.clientWidth;
    this.canvas.height = this.canvas.clientHeight;
    this.bgCache = null;
    this.crowd = null;
  }

  // isometric layout: nodes stand on an ellipse (a circle seen at an angle)
  // inside a diamond-shaped stone plaza
  plaza() {
    const W = this.canvas.width;
    const H = this.canvas.height;
    // narrower than the window: the floating side panels cover the edges.
    // On phones the panels sit above and below, so the plaza takes the width.
    if (this.narrow()) {
      const rx = Math.min(W * 0.36, H * 0.3);
      const ry = rx * 0.52;
      return { cx: W / 2, cy: H * 0.56, rx, ry, px: rx * 1.5, py: ry * 1.5 };
    }
    const rx = Math.min(W * 0.25, H * 0.6);
    const ry = rx * 0.52;
    return { cx: W / 2, cy: H * 0.55, rx, ry, px: rx * 1.5, py: ry * 1.5 };
  }

  narrow(): boolean {
    return this.canvas.width < 700;
  }

  nodeR(): number {
    return Math.max(30, Math.min(52, this.plaza().rx * 0.14));
  }

  // where node i's feet touch the plaza
  groundPos(i: number): [number, number] {
    const { cx, cy, rx, ry } = this.plaza();
    const angle = (i / NUM_NODES) * Math.PI * 2 - Math.PI / 2;
    return [cx + rx * Math.cos(angle), cy + ry * Math.sin(angle)];
  }

  // node body center (what clicks and labels anchor to)
  nodePos(i: number): [number, number] {
    const [gx, gy] = this.groundPos(i);
    return [gx, gy - this.nodeR()];
  }

  // iso grid (a,b in [-1,1]) <-> screen
  isoToScreen(a: number, b: number): [number, number] {
    const { cx, cy, px, py } = this.plaza();
    return [cx + ((a - b) / 2) * px, cy + ((a + b) / 2) * py];
  }

  screenToIso(x: number, y: number): [number, number] {
    const { cx, cy, px, py } = this.plaza();
    const X = ((x - cx) * 2) / px;
    const Y = ((y - cy) * 2) / py;
    return [(X + Y) / 2, (Y - X) / 2];
  }

  // cable between two nodes: an elbow along the plaza's iso axes, like a trace on a board
  route(i: number, j: number): [number, number][] {
    const [lo, hi] = i < j ? [i, j] : [j, i];
    const A = this.groundPos(lo);
    const B = this.groundPos(hi);
    const [aA] = this.screenToIso(...A);
    const [, bB] = this.screenToIso(...B);
    const pts: [number, number][] = [A, this.isoToScreen(aA, bB), B];
    return i < j ? pts : pts.reverse();
  }

  pointAlong(pts: [number, number][], frac: number): [number, number] {
    const lens = pts.slice(1).map((p, k) => Math.hypot(p[0] - pts[k][0], p[1] - pts[k][1]));
    let d = frac * lens.reduce((s, l) => s + l, 0);
    for (let k = 0; k < lens.length; k++) {
      if (d <= lens[k] || k === lens.length - 1) {
        const t = lens[k] ? Math.min(1, d / lens[k]) : 0;
        return [pts[k][0] + (pts[k + 1][0] - pts[k][0]) * t, pts[k][1] + (pts[k + 1][1] - pts[k][1]) * t];
      }
      d -= lens[k];
    }
    return pts[pts.length - 1];
  }

  buildBackground(): HTMLCanvasElement {
    const W = this.canvas.width;
    const H = this.canvas.height;
    const c = this.cacheCanvas(W, H);
    const g = c.getContext("2d")!;
    let seed = 1337;
    const rnd = () => (seed = (seed * 16807) % 2147483647) / 2147483647;
    const { py } = this.plaza();
    const iso = (a: number, b: number) => this.isoToScreen(a, b);

    // grass: bright, saturated, with sunny dapples
    const grass = g.createLinearGradient(0, 0, 0, H);
    grass.addColorStop(0, "#94d66c");
    grass.addColorStop(1, "#5fb043");
    g.fillStyle = grass;
    g.fillRect(0, 0, W, H);
    for (let k = 0; k < 18; k++) {
      g.fillStyle = rnd() < 0.5 ? "rgba(190, 240, 130, 0.25)" : "rgba(60, 140, 50, 0.18)";
      g.beginPath();
      g.ellipse(rnd() * W, rnd() * H, 60 + rnd() * 140, 30 + rnd() * 60, 0, 0, Math.PI * 2);
      g.fill();
    }
    g.lineCap = "round";
    for (let k = 0; k < (W * H) / 650; k++) {
      const x = rnd() * W;
      const y = rnd() * H;
      g.strokeStyle = rnd() < 0.55 ? "rgba(52, 125, 40, 0.45)" : "rgba(210, 245, 150, 0.5)";
      g.lineWidth = 1.5;
      g.beginPath();
      g.moveTo(x - 2, y - 4);
      g.lineTo(x, y);
      g.lineTo(x + 2, y - 5);
      g.stroke();
    }

    const isoMax = (x: number, y: number) => {
      const [a, b] = this.screenToIso(x, y);
      return Math.max(Math.abs(a), Math.abs(b));
    };

    // warm sandy ground the plaza and crowd sit on, with a ragged edge
    g.fillStyle = "#ecc78f";
    g.beginPath();
    const P = 120;
    for (let k = 0; k <= P; k++) {
      const t = (k / P) * 4;
      const side = Math.floor(t) % 4;
      const f = t - Math.floor(t);
      const s = 1.5 + (rnd() - 0.5) * 0.08;
      const [a, b] = [[-s + 2 * s * f, -s], [s, -s + 2 * s * f], [s - 2 * s * f, s], [-s, s - 2 * s * f]][side];
      const [x, y] = iso(a, b);
      if (k) g.lineTo(x, y);
      else g.moveTo(x, y);
    }
    g.closePath();
    g.fill();
    g.strokeStyle = "rgba(196, 140, 80, 0.6)";
    g.lineWidth = 3;
    g.stroke();
    for (let k = 0; k < (W * H) / 900; k++) {
      const x = rnd() * W;
      const y = rnd() * H;
      if (isoMax(x, y) > 1.45) continue;
      g.fillStyle = rnd() < 0.6 ? "rgba(190, 135, 75, 0.35)" : "rgba(255, 240, 210, 0.6)";
      g.beginPath();
      g.ellipse(x, y, 1.5 + rnd() * 2, 1 + rnd(), 0, 0, Math.PI * 2);
      g.fill();
    }

    // flowers on the grass
    for (let k = 0; k < (W * H) / 5000; k++) {
      const x = rnd() * W;
      const y = rnd() * H;
      if (isoMax(x, y) < 1.6) continue;
      const col = ["#f9a8d4", "#fde047", "#ffffff", "#c4b5fd"][Math.floor(rnd() * 4)];
      g.fillStyle = col;
      for (let p = 0; p < 5; p++) {
        const a = (p / 5) * Math.PI * 2;
        g.beginPath();
        g.arc(x + Math.cos(a) * 2.6, y + Math.sin(a) * 1.8, 1.8, 0, Math.PI * 2);
        g.fill();
      }
      g.fillStyle = "#f59e0b";
      g.beginPath();
      g.arc(x, y, 1.4, 0, Math.PI * 2);
      g.fill();
    }

    // bushes, rocks and blossom trees, far enough out to leave room for the crowd
    const decor: { x: number; y: number; kind: number; s: number }[] = [];
    for (let k = 0; k < 90; k++) {
      const x = rnd() * W;
      const y = rnd() * H;
      const m = isoMax(x, y);
      if (m < 1.75) continue;
      const roll = rnd();
      decor.push({ x, y, kind: roll < 0.35 ? 2 : roll < 0.75 ? 0 : 1, s: 10 + rnd() * 16 });
    }
    decor.sort((p, q) => p.y - q.y);
    for (const { x, y, kind, s } of decor) {
      if (kind === 0) {
        g.fillStyle = "rgba(30, 80, 20, 0.3)";
        g.beginPath();
        g.ellipse(x, y + s * 0.5, s * 1.3, s * 0.45, 0, 0, Math.PI * 2);
        g.fill();
        for (const [ox, oy, rr, col] of [
          [-s * 0.6, 0, s * 0.7, "#3c9a37"],
          [s * 0.6, 0, s * 0.7, "#3c9a37"],
          [0, -s * 0.4, s * 0.85, "#4fb446"],
          [-s * 0.15, -s * 0.6, s * 0.4, "#86dc63"],
        ] as const) {
          g.fillStyle = col;
          g.beginPath();
          g.arc(x + ox, y + oy, rr, 0, Math.PI * 2);
          g.fill();
        }
      } else if (kind === 1) {
        g.fillStyle = "rgba(30, 60, 20, 0.3)";
        g.beginPath();
        g.ellipse(x, y + s * 0.3, s * 0.9, s * 0.3, 0, 0, Math.PI * 2);
        g.fill();
        g.fillStyle = "#a3abb5";
        g.beginPath();
        g.ellipse(x, y, s * 0.8, s * 0.5, 0, 0, Math.PI * 2);
        g.fill();
        g.fillStyle = "#d5dae0";
        g.beginPath();
        g.ellipse(x - s * 0.15, y - s * 0.15, s * 0.45, s * 0.25, 0, 0, Math.PI * 2);
        g.fill();
      } else {
        const S = s * 2.2;
        g.fillStyle = "rgba(30, 70, 20, 0.3)";
        g.beginPath();
        g.ellipse(x, y, S * 0.8, S * 0.25, 0, 0, Math.PI * 2);
        g.fill();
        g.fillStyle = "#7a4a2a";
        g.fillRect(x - S * 0.08, y - S * 0.9, S * 0.16, S * 0.9);
        g.fillStyle = "#5c341c";
        g.fillRect(x, y - S * 0.9, S * 0.08, S * 0.9);
        for (const [ox, oy, rr, col] of [
          [-S * 0.45, -S * 1.0, S * 0.45, "#ec4899"],
          [S * 0.45, -S * 1.0, S * 0.45, "#ec4899"],
          [0, -S * 1.25, S * 0.55, "#f472b6"],
          [-S * 0.3, -S * 1.15, S * 0.38, "#f9a8d4"],
          [S * 0.25, -S * 1.4, S * 0.3, "#fbcfe8"],
        ] as const) {
          g.fillStyle = col;
          g.beginPath();
          g.arc(x + ox, y + oy, rr, 0, Math.PI * 2);
          g.fill();
        }
        // fallen petals
        g.fillStyle = "#f9a8d4";
        for (let p = 0; p < 14; p++) {
          g.beginPath();
          g.ellipse(x + (rnd() - 0.5) * S * 2.4, y + (rnd() - 0.3) * S * 0.6, 2, 1.2, rnd() * 3, 0, Math.PI * 2);
          g.fill();
        }
      }
    }

    // plaza: drop shadow, thick stone sides, tiled top
    const T = Math.max(14, py * 0.06);
    const top = iso(-1, -1), right = iso(1, -1), bottom = iso(1, 1), left = iso(-1, 1);
    g.fillStyle = "rgba(120, 75, 30, 0.35)";
    g.beginPath();
    g.moveTo(left[0] - 6, left[1] + T + 8);
    g.lineTo(bottom[0], bottom[1] + T + 16);
    g.lineTo(right[0] + 6, right[1] + T + 8);
    g.lineTo(right[0], right[1]);
    g.lineTo(left[0], left[1]);
    g.fill();
    g.fillStyle = "#a7b0bc";
    g.beginPath();
    g.moveTo(...left);
    g.lineTo(...bottom);
    g.lineTo(bottom[0], bottom[1] + T);
    g.lineTo(left[0], left[1] + T);
    g.fill();
    g.fillStyle = "#8b95a3";
    g.beginPath();
    g.moveTo(...bottom);
    g.lineTo(...right);
    g.lineTo(right[0], right[1] + T);
    g.lineTo(bottom[0], bottom[1] + T);
    g.fill();
    // brick courses on the sides
    g.strokeStyle = "rgba(60, 68, 82, 0.35)";
    g.lineWidth = 1;
    for (const [p, q] of [[left, bottom], [bottom, right]] as const) {
      g.beginPath();
      g.moveTo(p[0], p[1] + T / 2);
      g.lineTo(q[0], q[1] + T / 2);
      g.stroke();
      for (let k = 0; k <= 24; k++) {
        const t = k / 24;
        const x = p[0] + (q[0] - p[0]) * t;
        const y = p[1] + (q[1] - p[1]) * t;
        const off = k % 2 ? 0 : T / 2;
        g.beginPath();
        g.moveTo(x, y + off);
        g.lineTo(x, y + off + T / 2);
        g.stroke();
      }
    }

    const N = 12;
    for (let u = 0; u < N; u++) {
      for (let v = 0; v < N; v++) {
        const a0 = -1 + (2 * u) / N, a1 = -1 + (2 * (u + 1)) / N;
        const b0 = -1 + (2 * v) / N, b1 = -1 + (2 * (v + 1)) / N;
        const corners = [iso(a0, b0), iso(a1, b0), iso(a1, b1), iso(a0, b1)];
        const l = 84 + rnd() * 7 - (rnd() < 0.08 ? 7 : 0);
        g.fillStyle = `hsl(212, 14%, ${l}%)`;
        g.beginPath();
        corners.forEach((p, k) => (k ? g.lineTo(...p) : g.moveTo(...p)));
        g.closePath();
        g.fill();
        g.strokeStyle = "rgba(120, 132, 150, 0.55)";
        g.lineWidth = 1.5;
        g.stroke();
        // light catching the top edges
        g.strokeStyle = "rgba(255, 255, 255, 0.35)";
        g.lineWidth = 1;
        g.beginPath();
        g.moveTo(corners[3][0], corners[3][1] - 1);
        g.lineTo(corners[0][0], corners[0][1] - 1);
        g.lineTo(corners[1][0], corners[1][1] - 1);
        g.stroke();
        if (rnd() < 0.12) {
          const [sx, sy] = iso(a0 + (a1 - a0) * rnd(), b0 + (b1 - b0) * rnd());
          g.strokeStyle = "rgba(80, 85, 95, 0.5)";
          g.beginPath();
          g.moveTo(sx, sy);
          g.lineTo(sx + (rnd() - 0.5) * 16, sy + (rnd() - 0.5) * 8);
          g.lineTo(sx + (rnd() - 0.5) * 22, sy + (rnd() - 0.5) * 12);
          g.stroke();
        }
      }
    }
    // rim
    g.strokeStyle = "#ffffff";
    g.lineWidth = 3.5;
    g.beginPath();
    [top, right, bottom, left].forEach((p, k) => (k ? g.lineTo(...p) : g.moveTo(...p)));
    g.closePath();
    g.stroke();

    const mode = this.bomber?.mode ?? "calm";
    if (mode === "calm") {
      // warm afternoon sun from the top-left
      const sun = g.createRadialGradient(W * 0.15, -H * 0.1, 0, W * 0.15, -H * 0.1, Math.max(W, H));
      sun.addColorStop(0, "rgba(255, 236, 180, 0.28)");
      sun.addColorStop(1, "rgba(255, 236, 180, 0)");
      g.fillStyle = sun;
      g.fillRect(0, 0, W, H);
    } else {
      this.paintTheme(g, mode, W, H, rnd);
    }
    return c;
  }

  // spectators packed on the grass along the plaza's edges, back rows first
  buildCrowd() {
    let seed = 4242;
    const rnd = () => (seed = (seed * 16807) % 2147483647) / 2147483647;
    const W = this.canvas.width;
    const H = this.canvas.height;
    const spots: NonNullable<App["crowd"]> = [];
    const flags = ["#fde047", "#60a5fa", "#f87171", "#4ade80"];
    const add = (a: number, b: number) => {
      if (rnd() < 0.18) return; // gaps so it reads as people, not a fence
      const [x, y] = this.isoToScreen(a, b);
      if (x < 20 || x > W - 20 || y < 40 || y > H - 8) return;
      spots.push({
        x: x + (rnd() - 0.5) * 8,
        y: y + (rnd() - 0.5) * 4,
        sprite: Math.floor(rnd() * this.crowdSprites.length),
        phase: rnd() * Math.PI * 2,
        flag: rnd() < 0.25 ? flags[Math.floor(rnd() * flags.length)] : null,
      });
    };
    for (const row of [1.2, 1.4, 1.6]) {
      for (let t = -1.05; t <= 1.05; t += 0.15) {
        add(-row, t); // back-left edge
        add(t, -row); // back-right edge
      }
    }
    for (let t = -0.9; t <= 0.9; t += 0.18) {
      add(1.38, t); // front edges, one row, clear of the plaza wall
      add(t, 1.38);
    }
    return spots.sort((p, q) => p.y - q.y);
  }

  // pre-tinted spectator variants so the crowd isn't a wall of clones
  buildCrowdSprites() {
    this.crowdSprites = [null, "#60a5fa", "#f472b6", "#a78bfa", "#34d399", "#fb923c"].map((col) => {
      const c = this.cacheCanvas(this.tintCanvas.width, this.tintCanvas.height);
      c.getContext("2d")!.drawImage(col ? this.tintedGopher(col) : this.gopherImg, 0, 0);
      return c;
    });
  }

  // an offscreen canvas that flags the caches for rebuild if the browser drops its pixels
  cacheCanvas(w: number, h: number): HTMLCanvasElement {
    const c = document.createElement("canvas");
    c.width = w;
    c.height = h;
    const lost = () => (this.cachesLost = true);
    c.addEventListener("contextlost", lost);
    c.addEventListener("contextrestored", lost);
    return c;
  }

  // rebuild every cached canvas if any was discarded; isContextLost catches a
  // loss whose event fired while the tab was asleep
  ensureCaches() {
    const lost = (c: HTMLCanvasElement | null | undefined) =>
      !!c && (c.getContext("2d") as CanvasRenderingContext2D & { isContextLost?: () => boolean }).isContextLost?.() === true;
    if (!this.cachesLost && !lost(this.bgCache) && !lost(this.crowdScaled[0]) && !lost(this.crowdSprites[0])) return;
    if ([this.bgCache, this.crowdScaled[0], this.crowdSprites[0]].some(lost)) return; // still lost: wait for restore
    this.cachesLost = false;
    this.bgCache = null;
    this.crowdScaledW = 0;
    this.buildCrowdSprites();
  }

  drawCrowd(now: number) {
    if (this.crowdSprites.length === 0) return;
    this.crowd ??= this.buildCrowd();
    const ctx = this.ctx;
    const w = this.nodeR() * 0.8;
    const h = w * GOPHER_ASPECT;
    // pre-shrunk sprites: scaling ~150 full-size images every frame was costly
    if (this.crowdScaledW !== w) {
      this.crowdScaledW = w;
      this.crowdScaled = this.crowdSprites.map((src) => {
        const c = this.cacheCanvas(Math.ceil(w), Math.ceil(h));
        const g = c.getContext("2d")!;
        g.imageSmoothingQuality = "high";
        g.drawImage(src, 0, 0, w, h);
        const tint = THEME_TINT[this.bomber.mode];
        if (tint) {
          // match the crowd to the mode's lighting, clipped to the sprite
          g.globalCompositeOperation = "multiply";
          g.fillStyle = tint;
          g.fillRect(0, 0, c.width, c.height);
          g.globalCompositeOperation = "destination-in";
          g.drawImage(src, 0, 0, w, h);
        }
        return c;
      });
    }
    const sinceCheer = now - this.cheerAt;
    const sinceGasp = now - this.gaspAt;
    // all ground shadows in one path, one fill
    ctx.fillStyle = "rgba(20, 35, 15, 0.35)";
    ctx.beginPath();
    for (const p of this.crowd) {
      ctx.moveTo(p.x + w * 0.4, p.y);
      ctx.ellipse(p.x, p.y, w * 0.4, w * 0.13, 0, 0, Math.PI * 2);
    }
    ctx.fill();
    for (const p of this.crowd) {
      let jump = Math.abs(Math.sin(now / 420 + p.phase)) * 1.5;
      if (sinceCheer < 2200) jump += Math.abs(Math.sin(sinceCheer / 110 + p.phase)) * h * 0.35 * (1 - sinceCheer / 2200);
      const shake = sinceGasp < 700 ? Math.sin(now / 22 + p.phase) * 2.5 * (1 - sinceGasp / 700) : 0;
      const x = p.x + shake;
      ctx.drawImage(this.crowdScaled[p.sprite], Math.round(x - w / 2), Math.round(p.y - h - jump));
      if (p.flag) {
        const hx = x + w * 0.42;
        const hy = p.y - h * 0.45 - jump;
        const top = hy - h * 0.75;
        const wave = Math.sin(now / 140 + p.phase) * (sinceCheer < 2200 ? 6 : 2);
        ctx.strokeStyle = "#5b3a1e";
        ctx.lineWidth = 2;
        ctx.beginPath();
        ctx.moveTo(hx, hy);
        ctx.lineTo(hx, top);
        ctx.stroke();
        ctx.fillStyle = p.flag;
        ctx.strokeStyle = "#1e293b";
        ctx.lineWidth = 1.2;
        ctx.beginPath();
        ctx.moveTo(hx, top);
        ctx.lineTo(hx + w * 0.55, top + h * 0.1 + wave * 0.3);
        ctx.lineTo(hx, top + h * 0.22);
        ctx.closePath();
        ctx.fill();
        ctx.stroke();
      }
    }
  }

  // darken/recolor the scenery for a chaos mode and add its props
  paintTheme(g: CanvasRenderingContext2D, mode: ChaosMode, W: number, H: number, rnd: () => number) {
    const tint = THEME_TINT[mode]!;
    g.save();
    g.globalCompositeOperation = "multiply";
    g.fillStyle = tint;
    g.fillRect(0, 0, W, H);
    g.restore();

    const offPlaza = (x: number, y: number) => {
      const [a, b] = this.screenToIso(x, y);
      return Math.max(Math.abs(a), Math.abs(b)) > 1.75;
    };
    const spots = (n: number) => {
      const out: [number, number][] = [];
      for (let k = 0; k < n * 6 && out.length < n; k++) {
        const x = rnd() * W, y = H * 0.12 + rnd() * H * 0.88;
        if (offPlaza(x, y)) out.push([x, y]);
      }
      return out.sort((p, q) => p[1] - q[1]);
    };

    if (mode === "nightmare") {
      // blood moon
      const mx = W * 0.82, my = H * 0.14, mr = Math.min(W, H) * 0.07;
      const glow = g.createRadialGradient(mx, my, mr * 0.5, mx, my, mr * 3);
      glow.addColorStop(0, "rgba(255, 70, 50, 0.45)");
      glow.addColorStop(1, "rgba(255, 70, 50, 0)");
      g.fillStyle = glow;
      g.fillRect(mx - mr * 3, my - mr * 3, mr * 6, mr * 6);
      g.fillStyle = "#e0442f";
      g.beginPath();
      g.arc(mx, my, mr, 0, Math.PI * 2);
      g.fill();
      g.fillStyle = "rgba(120, 20, 15, 0.35)";
      for (const [ox, oy, rr] of [[-0.3, -0.2, 0.22], [0.25, 0.15, 0.16], [-0.05, 0.35, 0.12]]) {
        g.beginPath();
        g.arc(mx + ox * mr, my + oy * mr, rr * mr, 0, Math.PI * 2);
        g.fill();
      }
      for (const [x, y] of spots(14)) this.drawTombstone(g, x, y, 14 + rnd() * 10, rnd);
    }
    if (mode === "nightmare" || mode === "gremlin") {
      for (const [x, y] of spots(mode === "nightmare" ? 10 : 14)) this.drawPumpkin(g, x, y, 9 + rnd() * 8, mode === "nightmare");
    }
    if (mode === "storm") {
      // puddles
      for (const [x, y] of spots(16)) {
        g.fillStyle = "rgba(160, 190, 215, 0.35)";
        g.beginPath();
        g.ellipse(x, y, 18 + rnd() * 26, 5 + rnd() * 6, 0, 0, Math.PI * 2);
        g.fill();
      }
    }
  }

  drawPumpkin(g: CanvasRenderingContext2D, x: number, y: number, s: number, glowing: boolean) {
    g.fillStyle = "rgba(0, 0, 0, 0.3)";
    g.beginPath();
    g.ellipse(x, y + s * 0.8, s * 1.2, s * 0.35, 0, 0, Math.PI * 2);
    g.fill();
    for (const [ox, rr] of [[-0.55, 0.75], [0.55, 0.75], [0, 0.9]] as const) {
      g.fillStyle = ox === 0 ? "#f97316" : "#ea580c";
      g.beginPath();
      g.ellipse(x + ox * s, y, rr * s, s * 0.85, 0, 0, Math.PI * 2);
      g.fill();
    }
    g.fillStyle = "#4d7c0f";
    g.fillRect(x - s * 0.1, y - s * 1.15, s * 0.2, s * 0.4);
    // jack-o'-lantern face
    if (glowing) {
      g.shadowColor = "#fde047";
      g.shadowBlur = 10;
    }
    g.fillStyle = glowing ? "#fde047" : "#7c2d12";
    g.beginPath();
    g.moveTo(x - s * 0.5, y - s * 0.15); g.lineTo(x - s * 0.25, y - s * 0.45); g.lineTo(x - s * 0.1, y - s * 0.15);
    g.moveTo(x + s * 0.5, y - s * 0.15); g.lineTo(x + s * 0.25, y - s * 0.45); g.lineTo(x + s * 0.1, y - s * 0.15);
    g.fill();
    g.beginPath();
    g.moveTo(x - s * 0.55, y + s * 0.2);
    g.quadraticCurveTo(x, y + s * 0.75, x + s * 0.55, y + s * 0.2);
    g.quadraticCurveTo(x, y + s * 0.45, x - s * 0.55, y + s * 0.2);
    g.fill();
    g.shadowBlur = 0;
  }

  drawTombstone(g: CanvasRenderingContext2D, x: number, y: number, s: number, rnd: () => number) {
    g.save();
    g.translate(x, y);
    g.rotate((rnd() - 0.5) * 0.25);
    g.fillStyle = "rgba(0, 0, 0, 0.35)";
    g.beginPath();
    g.ellipse(0, 0, s * 1.1, s * 0.3, 0, 0, Math.PI * 2);
    g.fill();
    g.fillStyle = "#9ca3af";
    g.strokeStyle = "#374151";
    g.lineWidth = 2;
    g.beginPath();
    g.moveTo(-s * 0.7, 0);
    g.lineTo(-s * 0.7, -s * 1.3);
    g.arc(0, -s * 1.3, s * 0.7, Math.PI, 0);
    g.lineTo(s * 0.7, 0);
    g.closePath();
    g.fill();
    g.stroke();
    g.strokeStyle = "#4b5563";
    g.beginPath();
    g.moveTo(0, -s * 1.6); g.lineTo(0, -s * 0.8);
    g.moveTo(-s * 0.3, -s * 1.35); g.lineTo(s * 0.3, -s * 1.35);
    g.stroke();
    g.restore();
  }

  // live weather on top of everything: rain + lightning (storm), bats (gremlin/nightmare), fog (nightmare)
  drawWeather(now: number) {
    const mode = this.bomber.mode;
    if (mode === "calm") return;
    const ctx = this.ctx;
    const W = this.canvas.width, H = this.canvas.height;
    if (mode === "storm") {
      ctx.save();
      ctx.strokeStyle = "rgba(190, 215, 240, 0.45)";
      ctx.lineWidth = 1.3;
      ctx.beginPath();
      for (let i = 0; i < 220; i++) {
        const sx = (i * 97.13) % W;
        const speed = 0.9 + ((i * 37) % 10) / 20;
        const y = ((i * 211.7 + now * speed) % (H + 40)) - 20;
        const x = (sx + y * 0.25) % W;
        ctx.moveTo(x, y);
        ctx.lineTo(x - 4, y - 16);
      }
      ctx.stroke();
      ctx.restore();
      // lightning: a bright flash plus a jagged bolt, every few seconds
      const cycle = now % 5200;
      if (cycle < 260) {
        const flash = cycle < 90 ? 0.55 : cycle < 140 ? 0.15 : cycle < 200 ? 0.4 : 0;
        ctx.fillStyle = `rgba(235, 240, 255, ${flash})`;
        ctx.fillRect(0, 0, W, H);
        if (flash > 0.3) {
          const seed = Math.floor(now / 5200);
          let x = W * (0.2 + ((seed * 0.37) % 0.6)), y = 0;
          ctx.save();
          ctx.strokeStyle = "#fefce8";
          ctx.shadowColor = "#bfdbfe";
          ctx.shadowBlur = 18;
          ctx.lineWidth = 3;
          ctx.beginPath();
          ctx.moveTo(x, y);
          for (let k = 1; k <= 9; k++) {
            x += Math.sin(seed * 13 + k * 2.1) * 30;
            y = (H * 0.5 * k) / 9;
            ctx.lineTo(x, y);
          }
          ctx.stroke();
          ctx.restore();
        }
      }
    }
    if (mode === "nightmare") {
      ctx.save();
      for (let i = 0; i < 6; i++) {
        const fx = ((i * 331 + now * 0.012 * (i % 2 ? 1 : -1)) % (W + 600)) - 300;
        const fy = H * (0.35 + (i % 3) * 0.22);
        const fog = ctx.createRadialGradient(fx, fy, 10, fx, fy, 320);
        fog.addColorStop(0, "rgba(200, 190, 210, 0.12)");
        fog.addColorStop(1, "rgba(200, 190, 210, 0)");
        ctx.fillStyle = fog;
        ctx.fillRect(fx - 320, fy - 320, 640, 640);
      }
      ctx.restore();
    }
    if (mode === "gremlin" || mode === "nightmare") {
      ctx.save();
      ctx.fillStyle = "#120a16";
      const bats = mode === "nightmare" ? 9 : 6;
      for (let i = 0; i < bats; i++) {
        const t = now / 1000 + i * 1.7;
        const x = ((i * 271 + t * (60 + i * 9)) % (W + 120)) - 60;
        const y = H * 0.08 + ((i * 53) % (H * 0.3)) + Math.sin(t * 2 + i) * 18;
        const flap = Math.sin(t * 14 + i) * 0.8;
        const s = 7 + (i % 3) * 3;
        ctx.beginPath();
        ctx.ellipse(x, y, s * 0.35, s * 0.25, 0, 0, Math.PI * 2);
        ctx.moveTo(x, y);
        ctx.quadraticCurveTo(x - s * 0.8, y - s * flap, x - s * 1.6, y - s * 0.2 * flap);
        ctx.quadraticCurveTo(x - s * 0.9, y + s * 0.15, x, y);
        ctx.quadraticCurveTo(x + s * 0.9, y + s * 0.15, x + s * 1.6, y - s * 0.2 * flap);
        ctx.quadraticCurveTo(x + s * 0.8, y - s * flap, x, y);
        ctx.fill();
      }
      ctx.restore();
    }
  }

  drawCables(snap: ClusterSnapshot) {
    const ctx = this.ctx;
    ctx.save();
    ctx.lineCap = "round";
    ctx.lineJoin = "round";
    for (let i = 0; i < NUM_NODES; i++) {
      for (let j = i + 1; j < NUM_NODES; j++) {
        const pts = this.route(i, j);
        const live = snap.Nodes[i].Alive && snap.Nodes[j].Alive;
        const cut = this.isolated.has(i) || this.isolated.has(j);
        ctx.setLineDash(cut ? [4, 7] : []);
        for (const [w, col] of [
          [7, cut ? "rgba(14, 116, 144, 0.5)" : "rgba(70, 75, 86, 0.45)"],
          [4, cut ? "#bae6fd" : live ? "#f8fafc" : "#a8adb5"],
        ] as const) {
          ctx.strokeStyle = col;
          ctx.lineWidth = w;
          ctx.beginPath();
          pts.forEach((p, k) => (k ? ctx.lineTo(...p) : ctx.moveTo(...p)));
          ctx.stroke();
        }
        ctx.setLineDash([]);
        // little junction studs at the elbow
        ctx.fillStyle = "#f8fafc";
        ctx.strokeStyle = "rgba(70, 75, 86, 0.6)";
        ctx.lineWidth = 1.5;
        ctx.beginPath();
        ctx.arc(pts[1][0], pts[1][1], 3.5, 0, Math.PI * 2);
        ctx.fill();
        ctx.stroke();
      }
    }
    // a stone pad under each node where the cables plug in
    const r = this.nodeR();
    for (let i = 0; i < NUM_NODES; i++) {
      const [gx, gy] = this.groundPos(i);
      ctx.fillStyle = "#b9bec6";
      ctx.strokeStyle = "#eef0f2";
      ctx.lineWidth = 3;
      ctx.beginPath();
      ctx.ellipse(gx, gy, r * 1.05, r * 0.42, 0, 0, Math.PI * 2);
      ctx.fill();
      ctx.stroke();
    }
    ctx.restore();
  }

  addPopup(node: number, text: string, color: string) {
    const list = this.popups[node];
    const now = performance.now();
    // a word already on screen is refreshed in place rather than stacked again:
    // with steady traffic, Success/commitIndex every heartbeat piled into a smear
    const same = list.findIndex((p) => p.text === text);
    if (same >= 0) {
      const [p] = list.splice(same, 1);
      if (now - p.start > POPUP_MS * 0.5) p.start = now;
      list.push(p);
      return;
    }
    const r = this.nodeR();
    list.push({
      text,
      color,
      start: now,
      dx: (Math.random() - 0.5) * r * 0.7,
      dy: list.length ? -(Math.random() * r * 0.35) : 0,
      rot: (Math.random() - 0.5) * 0.24,
    });
    if (list.length > MAX_POPUPS) list.shift();
  }

  // draws every live word for node i, oldest first so the newest lands on top
  drawPopup(i: number, x: number, y: number, now: number) {
    const list = (this.popups[i] = this.popups[i].filter((p) => now - p.start < POPUP_MS));
    for (const p of list) this.drawOnePopup(p, x + p.dx, y + p.dy, now);
  }

  drawOnePopup(p: { text: string; color: string; start: number; rot: number }, x: number, y: number, now: number) {
    const t = (now - p.start) / POPUP_MS;
    const ctx = this.ctx;
    const scale = t < 0.1 ? 0.4 + (t / 0.1) * 0.85 : t < 0.18 ? 1.25 - ((t - 0.1) / 0.08) * 0.25 : 1;
    ctx.save();
    ctx.globalAlpha = t > 0.75 ? (1 - t) / 0.25 : 1;
    ctx.translate(x, y - t * 16);
    ctx.rotate(-0.06 + p.rot);
    ctx.scale(scale, scale);
    const size = Math.round((this.nodeR() * 0.5 + 6) * Math.min(1, 9 / p.text.length));
    ctx.font = `900 ${Math.max(12, size)}px "Arial Black", Impact, system-ui, sans-serif`;
    ctx.textAlign = "center";
    ctx.textBaseline = "alphabetic";
    ctx.lineJoin = "round";
    ctx.strokeStyle = "#1e293b";
    ctx.lineWidth = 7;
    ctx.strokeText(p.text, 0, 0);
    ctx.fillStyle = p.color;
    ctx.fillText(p.text, 0, 0);
    ctx.restore();
  }

  // white text with a dark outline: readable on stone, grass, or the dimmed overlay
  // chunky ribbon at the top of the stage so viewers know what they're watching
  // game title card: a ribbon with folded tails that drops in from the top,
  // colored to match the current chaos theme
  renderElectionBanner(now: number) {
    const banner = this.election.banner(now);
    if (!banner) return;
    const ctx = this.ctx;
    const pal = BANNER_THEME[this.bomber.mode][banner.tone];
    const titleFont = '900 30px "Arial Black", Impact, system-ui, sans-serif';
    const subFont = '800 13px "Trebuchet MS", system-ui, sans-serif';

    // drop in with a little overshoot, then float
    const t = Math.min(1, banner.age / 380);
    const ease = 1 - Math.pow(1 - t, 3);
    const overshoot = t < 1 ? Math.sin(t * Math.PI) * 10 : 0;
    const cx = this.canvas.width / 2;
    // on phones the HTML cards cover the top, so the ribbon rises from the bottom
    // into the strip above the client request card
    const H = this.canvas.height;
    const [from, rest] = this.narrow() ? [H + 60, H - 200] : [-60, 62];
    const dir = Math.sign(rest - from);
    const cy = from + (rest - from) * ease + dir * overshoot + Math.sin(now / 500) * 2;

    ctx.save();
    ctx.font = titleFont;
    const w = Math.max(340, ctx.measureText(banner.title).width + 120);
    const h = 64;
    const tail = 44;
    ctx.translate(cx, cy);
    const fit = Math.min(1, (this.canvas.width - 12) / (w + tail * 2));
    ctx.scale(fit, fit);

    // folded tails behind the ribbon
    for (const side of [-1, 1]) {
      const x0 = side * (w / 2 - 14);
      const x1 = side * (w / 2 + tail);
      ctx.fillStyle = pal.dark;
      ctx.beginPath();
      ctx.moveTo(x0, -h / 2 + 14);
      ctx.lineTo(x1, -h / 2 + 14);
      ctx.lineTo(x1 - side * 16, 8);
      ctx.lineTo(x1, h / 2 + 12);
      ctx.lineTo(x0, h / 2 + 12);
      ctx.closePath();
      ctx.fill();
      ctx.strokeStyle = pal.edge;
      ctx.lineWidth = 3;
      ctx.stroke();
      // fold shadow
      ctx.fillStyle = "rgba(0, 0, 0, 0.35)";
      ctx.beginPath();
      ctx.moveTo(side * (w / 2 - 14), h / 2);
      ctx.lineTo(side * (w / 2), h / 2);
      ctx.lineTo(side * (w / 2 - 14), h / 2 + 12);
      ctx.closePath();
      ctx.fill();
    }

    // drop shadow + main ribbon
    ctx.fillStyle = "rgba(0, 0, 0, 0.4)";
    ctx.beginPath();
    ctx.roundRect(-w / 2 + 4, -h / 2 + 7, w, h, 12);
    ctx.fill();
    const grad = ctx.createLinearGradient(0, -h / 2, 0, h / 2);
    grad.addColorStop(0, pal.light);
    grad.addColorStop(1, pal.base);
    ctx.fillStyle = grad;
    ctx.beginPath();
    ctx.roundRect(-w / 2, -h / 2, w, h, 12);
    ctx.fill();
    ctx.lineWidth = 4;
    ctx.strokeStyle = pal.edge;
    ctx.stroke();
    // inner cream trim, like the panels
    ctx.lineWidth = 2;
    ctx.strokeStyle = "rgba(255, 244, 220, 0.75)";
    ctx.beginPath();
    ctx.roundRect(-w / 2 + 6, -h / 2 + 6, w - 12, h - 12, 8);
    ctx.stroke();
    // glossy top
    ctx.fillStyle = "rgba(255, 255, 255, 0.18)";
    ctx.beginPath();
    ctx.roundRect(-w / 2 + 8, -h / 2 + 8, w - 16, h * 0.32, 6);
    ctx.fill();
    // rivets
    ctx.fillStyle = "rgba(255, 244, 220, 0.9)";
    for (const side of [-1, 1]) {
      ctx.beginPath();
      ctx.arc(side * (w / 2 - 20), 0, 4, 0, Math.PI * 2);
      ctx.fill();
    }
    ctx.restore();

    // title + subtitle plate
    const pulse = banner.tone === "danger" ? 0.6 + 0.4 * Math.sin(now / 140) : 1;
    ctx.save();
    ctx.shadowColor = pal.glow;
    ctx.shadowBlur = 14 * pulse;
    this.outlinedText(banner.title, cx, cy + 9, titleFont, "#fff4dc");
    ctx.restore();
    ctx.save();
    ctx.font = subFont;
    const sw = ctx.measureText(banner.subtitle).width + 26;
    ctx.fillStyle = pal.edge;
    ctx.beginPath();
    ctx.roundRect(cx - sw / 2, cy + h / 2 - 4, sw, 20, 10);
    ctx.fill();
    ctx.strokeStyle = "rgba(255, 244, 220, 0.8)";
    ctx.lineWidth = 1.5;
    ctx.stroke();
    ctx.restore();
    this.outlinedText(banner.subtitle, cx, cy + h / 2 + 10.5, subFont, pal.subText);
  }

  outlinedText(text: string, x: number, y: number, font: string, fill = "#ffffff") {
    const ctx = this.ctx;
    ctx.save();
    ctx.font = font;
    ctx.textAlign = "center";
    ctx.lineJoin = "round";
    ctx.strokeStyle = "#1e293b";
    ctx.lineWidth = 4;
    ctx.strokeText(text, x, y);
    ctx.fillStyle = fill;
    ctx.fillText(text, x, y);
    ctx.restore();
  }


  drawNode(n: ClusterSnapshot["Nodes"][number], now: number) {
    const ctx = this.ctx;
    const r = this.nodeR();
    const [gx, gy] = this.groundPos(n.ID);
    const [x, y] = [gx, gy - r];
    const selected = this.selectedNode === n.ID;
    const recovering = n.Alive && now < (this.recoveringUntil[n.ID] ?? -Infinity);
    const { dy } = this.gopherBounds(x, y, r);

    // contact shadow
    ctx.fillStyle = "rgba(25, 30, 40, 0.32)";
    ctx.beginPath();
    ctx.ellipse(gx, gy, r * (n.Alive ? 0.7 : 1.15), r * 0.24, 0, 0, Math.PI * 2);
    ctx.fill();

    // ground rings: lime for selection, pulsing green while patched up
    const ring = (color: string, width: number, scale: number) => {
      ctx.save();
      ctx.strokeStyle = color;
      ctx.lineWidth = width;
      ctx.beginPath();
      ctx.ellipse(gx, gy, r * 1.2 * scale, r * 0.48 * scale, 0, 0, Math.PI * 2);
      ctx.stroke();
      ctx.restore();
    };
    if (recovering) ring(`rgba(74, 222, 128, ${0.5 + 0.5 * Math.sin(now / 160)})`, 4, 1.08);
    if (selected) ring("#d4f53c", 4.5, 1);

    const labelY = gy + r * 0.42 + 18;
    if (!n.Alive) {
      this.drawCritter(x, y, r, { glowColor: "transparent", dead: true, glowBlur: 0, happy: false, recovering: false, now });
      this.outlinedText(`n${n.ID} · DOWN`, gx, labelY, "800 12px system-ui, sans-serif", "#fca5a5");
      this.drawIce(n.ID, gx, gy, r, now);
      this.drawPopup(n.ID, gx, gy - r * 1.6, now);
      return;
    }

    const stale = isStaleLeader(n, this.snapshot!, this.isolated);
    const color = ROLE_COLOR[n.Role] ?? "#64748b";
    const isElecting = this.election.active() && (n.Role === "candidate" || this.election.isCandidate(n.ID));
    const isNewLeader = this.election.phase === "crowning" && this.election.newLeader === n.ID;
    const pulse = 0.6 + 0.4 * Math.sin(now / 220);

    this.drawCritter(x, y, r, {
      glowColor: color,
      dead: false,
      glowBlur: isElecting || isNewLeader ? 24 * pulse : n.Role === "leader" && !stale ? 14 : 0,
      happy: n.Role === "leader" && !stale,
      recovering,
      now,
    });

    this.outlinedText(
      stale ? `n${n.ID} · T${n.Term} · stale leader` : `n${n.ID} · T${n.Term}`,
      gx, labelY, "800 12px system-ui, sans-serif",
      stale ? "#cbd5e1" : n.Role === "leader" ? "#fde047" : "#ffffff",
    );

    let topY = dy;
    if (n.Role === "leader") {
      const { w } = this.gopherBounds(x, y, r);
      this.drawCrown(x, dy - w * 0.08, w * 0.72, now, stale);
      topY = dy - w * 0.5;
    }
    if (this.popups[n.ID].length === 0 && (isElecting || n.Role === "candidate")) {
      this.outlinedText(isElecting ? "voting..." : "running...", x, topY - 6, "700 11px system-ui, sans-serif", "#fde68a");
    }
    this.drawIce(n.ID, gx, gy, r, now);
    this.drawPopup(n.ID, x, topY - 8, now);
  }

  // the drawn bounding box for the real Go gopher image at a given node
  // position/size, shared by drawCritter and drawNode's crown/label placement
  gopherBounds(x: number, y: number, r: number) {
    const w = r * 1.9;
    const h = w * GOPHER_ASPECT;
    return { dx: x - w / 2, dy: y - h * 0.56, w, h };
  }

  // the gopher artwork recolored on an offscreen canvas (source-atop only
  // touches the sprite's own pixels, never the stage behind it)
  // "color" blend keeps the art's light/dark detail (eyes, outline) and only
  // swaps hue; destination-in then clips back to the sprite's own alpha
  tintedGopher(color: string): HTMLCanvasElement {
    const tctx = this.tintCanvas.getContext("2d")!;
    const { width: W, height: H } = this.tintCanvas;
    tctx.clearRect(0, 0, W, H);
    tctx.drawImage(this.gopherImg, 0, 0);
    tctx.globalCompositeOperation = "color";
    tctx.fillStyle = color;
    tctx.fillRect(0, 0, W, H);
    tctx.globalCompositeOperation = "destination-in";
    tctx.drawImage(this.gopherImg, 0, 0);
    tctx.globalCompositeOperation = "source-over";
    return this.tintCanvas;
  }

  // a Dragon Ball-style energy aura: spiky flame shapes radiating from the
  // leader's base, rotating and pulsing -- drawn behind the sprite.
  drawAura(x: number, y: number, h: number, now: number) {
    const ctx = this.ctx;
    const pulse = 0.75 + 0.25 * Math.sin(now / 150);
    const spikes = 12;
    const baseR = h * 0.4;
    ctx.save();
    ctx.translate(x, y + h * 0.12);
    ctx.shadowColor = "#fde047";
    ctx.shadowBlur = 18;
    for (let i = 0; i < spikes; i++) {
      const angle = (i / spikes) * Math.PI * 2 + now / 900;
      const len = baseR * (0.85 + 0.35 * Math.sin(now / 110 + i * 1.7));
      ctx.save();
      ctx.rotate(angle);
      ctx.beginPath();
      ctx.moveTo(-5, 0);
      ctx.lineTo(0, -len);
      ctx.lineTo(5, 0);
      ctx.closePath();
      ctx.fillStyle = `rgba(253, 224, 71, ${0.55 * pulse})`;
      ctx.fill();
      ctx.restore();
    }
    ctx.restore();
  }

  // mascot: the real Go gopher artwork (viz/web/public/gopher.png),
  // composited with state overlays -- not a redrawn approximation.
  drawCritter(
    x: number,
    y: number,
    r: number,
    opts: { glowColor: string; dead: boolean; glowBlur: number; happy: boolean; recovering: boolean; now: number },
  ) {
    const ctx = this.ctx;
    const { dx, dy, w, h } = this.gopherBounds(x, y, r);
    const groundY = y + r;

    if (opts.dead) {
      // knocked out flat on the plaza, red like it took a hit, stars circling
      ctx.save();
      ctx.translate(x + h * 0.5, groundY - w * 0.42);
      ctx.rotate(-Math.PI / 2);
      ctx.translate(-x, -(y + h * 0.44));
      ctx.filter = "saturate(0.7)";
      ctx.drawImage(this.tintedGopher("#dc2626"), dx, dy, w, h);
      ctx.filter = "none";
      ctx.restore();
      const hx = x - h * 0.3;
      const hy = groundY - w * 0.85;
      for (let i = 0; i < 3; i++) {
        const a = opts.now / 400 + (i / 3) * Math.PI * 2;
        this.drawKoStar(hx + Math.cos(a) * w * 0.32, hy + Math.sin(a) * w * 0.1, 7);
      }
      return;
    }

    ctx.save();
    if (opts.happy) this.drawAura(x, y, h, opts.now);
    ctx.shadowColor = opts.glowColor;
    ctx.shadowBlur = opts.glowBlur;
    ctx.drawImage(opts.happy ? this.tintedGopher("#facc15") : this.gopherImg, dx, dy, w, h);
    ctx.restore();

    if (opts.recovering) {
      // crossed band-aids on the forehead and one on the belly
      this.drawBandage(dx + w * 0.3, dy + h * 0.16, w * 0.1, -0.6);
      this.drawBandage(dx + w * 0.3, dy + h * 0.16, w * 0.1, 0.6);
      this.drawBandage(dx + w * 0.62, dy + h * 0.6, w * 0.09, -0.25);
    }
  }

  drawKoStar(x: number, y: number, size: number) {
    const ctx = this.ctx;
    ctx.save();
    ctx.beginPath();
    for (let i = 0; i < 10; i++) {
      const a = (i / 10) * Math.PI * 2 - Math.PI / 2;
      const rr = i % 2 ? size * 0.45 : size;
      ctx.lineTo(x + Math.cos(a) * rr, y + Math.sin(a) * rr);
    }
    ctx.closePath();
    ctx.fillStyle = "#fde047";
    ctx.strokeStyle = "#78350f";
    ctx.lineWidth = 1.5;
    ctx.fill();
    ctx.stroke();
    ctx.restore();
  }

  // a proper band-aid: rounded tan strip, lighter gauze pad with air holes
  drawBandage(x: number, y: number, u: number, angle: number) {
    const ctx = this.ctx;
    ctx.save();
    ctx.translate(x, y);
    ctx.rotate(angle);
    const L = u * 4;
    const Hh = u * 1.3;
    ctx.beginPath();
    ctx.roundRect(-L / 2, -Hh / 2, L, Hh, Hh / 2);
    ctx.fillStyle = "#f2b880";
    ctx.fill();
    ctx.strokeStyle = "#7a4319";
    ctx.lineWidth = Math.max(1.2, u * 0.16);
    ctx.stroke();
    ctx.fillStyle = "#fde7cf";
    ctx.beginPath();
    ctx.roundRect(-u * 0.65, -Hh * 0.32, u * 1.3, Hh * 0.64, u * 0.15);
    ctx.fill();
    ctx.fillStyle = "rgba(122, 67, 25, 0.45)";
    for (const sx of [-1, 1]) {
      for (const sy of [-1, 1]) {
        ctx.beginPath();
        ctx.arc(sx * L * 0.33, sy * Hh * 0.18, u * 0.09, 0, Math.PI * 2);
        ctx.fill();
      }
    }
    ctx.restore();
  }

  // a chunky gold crown with jewels, sized to the gopher's head
  drawCrown(x: number, y: number, size: number, now: number, stale = false) {
    const ctx = this.ctx;
    const crowning = this.election.phase === "crowning";
    const bob = crowning ? Math.sin(now / 90) * 4 : Math.sin(now / 600) * 1.5;
    const s = size / 2;
    ctx.save();
    ctx.translate(x + (stale ? s * 0.5 : 0), y + bob + (stale ? s * 0.25 : 0));
    ctx.rotate(stale ? 0.55 : -0.12); // a stale leader's crown is knocked askew and dull
    if (stale) {
      ctx.filter = "grayscale(1) brightness(0.85)";
      ctx.globalAlpha = 0.8;
    }
    ctx.shadowColor = "#fde047";
    ctx.shadowBlur = stale ? 0 : crowning ? 22 : 8;
    const grad = ctx.createLinearGradient(0, -s, 0, s * 0.3);
    grad.addColorStop(0, "#fff3a3");
    grad.addColorStop(0.5, "#facc15");
    grad.addColorStop(1, "#b7791f");
    ctx.beginPath();
    ctx.moveTo(-s, 0);
    ctx.lineTo(-s * 1.05, -s * 0.85);
    ctx.lineTo(-s * 0.5, -s * 0.35);
    ctx.lineTo(0, -s * 1.1);
    ctx.lineTo(s * 0.5, -s * 0.35);
    ctx.lineTo(s * 1.05, -s * 0.85);
    ctx.lineTo(s, 0);
    ctx.closePath();
    ctx.fillStyle = grad;
    ctx.fill();
    ctx.shadowBlur = 0;
    ctx.strokeStyle = "#713f12";
    ctx.lineWidth = 2.5;
    ctx.lineJoin = "round";
    ctx.stroke();
    // band
    ctx.beginPath();
    ctx.roundRect(-s * 1.02, -s * 0.05, s * 2.04, s * 0.38, s * 0.08);
    ctx.fillStyle = "#eab308";
    ctx.fill();
    ctx.stroke();
    // tip balls
    for (const [tx, ty] of [[-s * 1.05, -s * 0.85], [0, -s * 1.1], [s * 1.05, -s * 0.85]]) {
      ctx.beginPath();
      ctx.arc(tx, ty, s * 0.14, 0, Math.PI * 2);
      ctx.fillStyle = "#fef9c3";
      ctx.fill();
      ctx.stroke();
    }
    // jewels
    for (const [jx, col] of [[-s * 0.55, "#3b82f6"], [0, "#ef4444"], [s * 0.55, "#22c55e"]] as const) {
      ctx.beginPath();
      ctx.arc(jx, s * 0.14, s * 0.12, 0, Math.PI * 2);
      ctx.fillStyle = col;
      ctx.fill();
      ctx.lineWidth = 1.5;
      ctx.stroke();
      ctx.fillStyle = "rgba(255,255,255,0.7)";
      ctx.beginPath();
      ctx.arc(jx - s * 0.04, s * 0.1, s * 0.04, 0, Math.PI * 2);
      ctx.fill();
    }
    ctx.restore();
  }

  render() {
    const ctx = this.ctx;
    const snap = this.snapshot;
    if (!snap) {
      ctx.fillStyle = "#1b2433";
      ctx.fillRect(0, 0, this.canvas.width, this.canvas.height);
      return;
    }
    // always wipe the frame first: if the cached background is ever blank,
    // drawing it would no longer cover the last frame and everything smears
    ctx.fillStyle = "#5fb043";
    ctx.fillRect(0, 0, this.canvas.width, this.canvas.height);
    this.ensureCaches();
    this.bgCache ??= this.buildBackground();

    // screen shake: a crash is a sudden, violent moment -- a brief
    // decaying jolt sells that better than any static icon can
    const now0 = performance.now();
    const shakeElapsed = now0 - this.shakeStart;
    ctx.save();
    if (shakeElapsed < this.shakeDurationMs) {
      const decay = 1 - shakeElapsed / this.shakeDurationMs;
      ctx.translate((Math.random() - 0.5) * 10 * decay, (Math.random() - 0.5) * 10 * decay);
    }
    ctx.drawImage(this.bgCache, 0, 0);
    this.drawCrowd(now0);
    this.drawCables(snap);

    // messages travel along the cables at ground level
    const now = performance.now();
    const FLY_MS = 550;
    this.messages = this.messages.filter((m) => now - m.start < FLY_MS);
    for (const m of this.messages) {
      const pts = this.route(m.from, m.to);
      const frac = (now - m.start) / FLY_MS;
      const style = MESSAGE_STYLE[m.kind] ?? { color: "#94a3b8", r: 3, tail: 3 };
      const R = style.r * 1.7;
      for (let i = style.tail - 1; i >= 0; i--) {
        const [x, y] = this.pointAlong(pts, Math.max(0, frac - i * 0.03));
        const radius = R - i * (R / style.tail / 1.5);
        ctx.globalAlpha = 1 - i * (1 / style.tail);
        ctx.beginPath();
        ctx.arc(x, y, radius, 0, Math.PI * 2);
        ctx.lineWidth = 2;
        ctx.strokeStyle = "#1e293b";
        if (style.hollow) {
          ctx.fillStyle = "#ffffff";
          ctx.fill();
          ctx.stroke();
          ctx.beginPath();
          ctx.arc(x, y, radius * 0.6, 0, Math.PI * 2);
          ctx.strokeStyle = style.color;
          ctx.lineWidth = 2.5;
          ctx.stroke();
        } else {
          ctx.fillStyle = style.color;
          ctx.fill();
          ctx.stroke();
        }
      }
      ctx.globalAlpha = 1;
    }

    // back-to-front so nearer gophers overlap farther ones
    for (const n of [...snap.Nodes].sort((p, q) => this.groundPos(p.ID)[1] - this.groundPos(q.ID)[1])) {
      this.drawNode(n, now);
    }
    this.drawToolFx(now);
    this.drawWeather(now);
    this.renderElectionBanner(now);

    ctx.restore();

    this.renderInspector(snap);
    // this.flushLog(); // mission log disabled

    this.safetyEl.textContent = snap.Safe ? "SAFETY ✓" : `SAFETY ✗ ${snap.Violations.at(-1)?.Detail ?? ""}`;
    this.safetyEl.className = `safety ${snap.Safe ? "safe" : "unsafe"}`;
  }
}

new App().start();
