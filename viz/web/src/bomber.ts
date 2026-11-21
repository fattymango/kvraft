import type { ClusterSnapshot, RaftUnderFireAPI } from "./wasm";

export type ChaosMode = "calm" | "gremlin" | "storm" | "nightmare";

export interface Bomb {
  node: number;
  plantedAt: number; // performance.now() ms
  fuseMs: number;
  defused: boolean;
}

const MODE_INTERVAL_MS: Record<ChaosMode, number> = {
  calm: Infinity,
  gremlin: 4000,
  storm: 2200,
  nightmare: 3000,
};

/**
 * Bomber is a purely client-side game-layer character: it decides *when*
 * and *which* node to threaten, then drives the simulation through the
 * same crash()/setNetwork() calls a player's own buttons use. It never
 * touches simulation state directly, so it adds no new safety-relevant
 * code to the Go/WASM side.
 */
export class Bomber {
  mode: ChaosMode = "calm";
  bomb: Bomb | null = null;
  wanderNode = 0;

  private lastActionAt = 0;
  private lastWanderAt = 0;
  private lastSeenAt = 0;
  private api: RaftUnderFireAPI;
  private deadSince = new Map<number, number>();
  private readonly AUTO_RESTART_DELAY_MS = 3500;

  constructor(api: RaftUnderFireAPI) {
    this.api = api;
  }

  setMode(mode: ChaosMode) {
    this.mode = mode;
    this.deadSince.clear(); // avoid an instant restart from a stale timer after switching modes
    if (mode === "storm") {
      this.api.setNetwork({ latencyMs: 25, jitterMs: 40, dropRate: 0.2, dupRate: 0.05 });
    } else {
      this.api.setNetwork({ latencyMs: 20, jitterMs: 10, dropRate: 0, dupRate: 0 });
    }
  }

  /**
   * Auto-heal crashed nodes in every mode: the cluster always self-heals.
   * Must run even while an election is in progress: with a dead majority
   * the election can never finish, so gating this on it deadlocks.
   */
  heal(now: number, snap: ClusterSnapshot) {
    // not called while paused: push every timer forward by the pause so a
    // bomb doesn't detonate (or a node restart) the instant play resumes
    const gap = now - this.lastSeenAt;
    if (this.lastSeenAt > 0 && gap > 1000) {
      this.lastActionAt += gap;
      this.lastWanderAt += gap;
      if (this.bomb) this.bomb.plantedAt += gap;
      for (const [id, t] of this.deadSince) this.deadSince.set(id, t + gap);
    }
    this.lastSeenAt = now;

    {
      for (const n of snap.Nodes) {
        if (!n.Alive) {
          const since = this.deadSince.get(n.ID);
          if (since === undefined) {
            this.deadSince.set(n.ID, now);
          } else if (now - since > this.AUTO_RESTART_DELAY_MS) {
            this.api.restart(n.ID);
            this.deadSince.delete(n.ID);
          }
        } else {
          this.deadSince.delete(n.ID);
        }
      }
    }
  }

  /** Call once per frame with real (performance.now()) time and the latest snapshot. */
  tick(now: number, snap: ClusterSnapshot) {
    if (now - this.lastWanderAt > 900) {
      this.lastWanderAt = now;
      const alive = snap.Nodes.filter((n) => n.Alive);
      if (alive.length > 0) {
        this.wanderNode = alive[Math.floor(Math.random() * alive.length)].ID;
      }
    }

    if (this.bomb) {
      if (this.bomb.defused) {
        this.bomb = null;
      } else if (now - this.bomb.plantedAt >= this.bomb.fuseMs) {
        // re-check at detonation: others may have died since the bomb was planted
        if (snap.Nodes.filter((n) => n.Alive).length > 1) this.api.crash(this.bomb.node);
        this.bomb = null;
      }
      return; // one bomb at a time
    }

    const interval = MODE_INTERVAL_MS[this.mode];
    if (!isFinite(interval) || now - this.lastActionAt < interval) return;

    const alive = snap.Nodes.filter((n) => n.Alive);
    if (alive.length <= 1) return; // leave at least one node standing

    let target: number;
    if (this.mode === "nightmare") {
      const leader = alive.find((n) => n.Role === "leader");
      target = leader ? leader.ID : alive[Math.floor(Math.random() * alive.length)].ID;
    } else {
      target = alive[Math.floor(Math.random() * alive.length)].ID;
    }

    this.lastActionAt = now;
    this.bomb = { node: target, plantedAt: now, fuseMs: 2200, defused: false };
  }

  /** Returns true if a live bomb was defused. */
  defuse(): boolean {
    if (this.bomb && !this.bomb.defused) {
      this.bomb.defused = true;
      return true;
    }
    return false;
  }
}
