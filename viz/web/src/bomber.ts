import type { ClusterSnapshot, RaftUnderFireAPI } from "./wasm";

export type ChaosMode = "calm" | "gremlin" | "storm" | "nightmare";

export type Attack = "meteor" | "ice";

const MODE_INTERVAL_MS: Record<ChaosMode, number> = {
  calm: Infinity,
  gremlin: 4000,
  storm: 4500,
  nightmare: 6000,
};

// how many gophers may be down (crashed or frozen) before the bomber waits:
// one failure at a time keeps Storm/Nightmare followable
const MAX_DOWN: Record<ChaosMode, number> = { calm: 0, gremlin: 2, storm: 1, nightmare: 1 };

/**
 * Bomber is a purely client-side game-layer character: it decides *when*
 * and *which* node to attack, then uses the same Meteor / Ice tools the
 * player has (plus setNetwork for Storm). It never
 * touches simulation state directly, so it adds no new safety-relevant
 * code to the Go/WASM side.
 */
export class Bomber {
  mode: ChaosMode = "calm";

  private lastActionAt = 0;
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
    this.lastActionAt = performance.now(); // a full interval of grace before the first attack
    if (mode === "storm") {
      this.api.setNetwork({ latencyMs: 25, jitterMs: 40, dropRate: 0.08, dupRate: 0.02 });
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
    // node restart or attack doesn't fire the instant play resumes
    const gap = now - this.lastSeenAt;
    if (this.lastSeenAt > 0 && gap > 1000) {
      this.lastActionAt += gap;
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

  /** Restart the attack timer: called during elections, so the new leader gets a full interval to settle. */
  holdOff(now: number) {
    this.lastActionAt = now;
  }

  /**
   * Call once per frame with real (performance.now()) time and the latest
   * snapshot. When it's time, attacks one node with a randomly chosen tool
   * (the same Meteor / Ice the player uses); `unavailable` are nodes already
   * frozen, which it won't pick or count as standing.
   */
  tick(now: number, snap: ClusterSnapshot, unavailable: { has(id: number): boolean }, attack: (tool: Attack, node: number) => void) {
    const interval = MODE_INTERVAL_MS[this.mode];
    if (!isFinite(interval) || now - this.lastActionAt < interval) return;

    const standing = snap.Nodes.filter((n) => n.Alive && !unavailable.has(n.ID));
    if (standing.length <= 1) return; // leave at least one node standing
    if (snap.Nodes.length - standing.length >= MAX_DOWN[this.mode]) return;

    let target: number;
    if (this.mode === "nightmare") {
      const leader = standing.find((n) => n.Role === "leader");
      target = leader ? leader.ID : standing[Math.floor(Math.random() * standing.length)].ID;
    } else {
      target = standing[Math.floor(Math.random() * standing.length)].ID;
    }

    this.lastActionAt = now;
    attack(Math.random() < 0.5 ? "meteor" : "ice", target);
  }
}
