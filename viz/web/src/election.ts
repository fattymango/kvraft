import type { ClusterSnapshot } from "./wasm";

export type ElectionPhase = "idle" | "inProgress" | "crowning";

const CROWNING_HOLD_MS = 1000;

/**
 * Drives the "leader dies -> election -> new leader" cinematic: the
 * centerpiece interaction this UI exists to show off. Purely a frontend
 * state machine watching getState() snapshots (and, loosely, the trace
 * stream) -- it never touches the simulation's own correctness, only how
 * the frontend paces itself and what it draws while watching an election
 * unfold.
 */
export class ElectionDirector {
  phase: ElectionPhase = "idle";

  private lastLeader: number | null = null;
  private phaseStart = 0;
  private candidateIds = new Set<number>();
  newLeader: number | null = null;
  newTerm = 0;

  update(
    snap: ClusterSnapshot,
    now: number,
    cutOff: { has(id: number): boolean },
  ) {
    // highest-term leader that can still reach the cluster: a stale one, or
    // one frozen off the network, doesn't count -- the rest must elect anew
    const leader = snap.Nodes
      .filter((n) => n.Alive && n.Role === "leader" && !cutOff.has(n.ID))
      .sort((a, b) => b.Term - a.Term)[0];
    const currentLeader = leader ? leader.ID : null;

    if (this.phase === "idle") {
      if (this.lastLeader !== null && currentLeader === null) {
        this.phase = "inProgress";
        this.phaseStart = now;
      }
    } else if (this.phase === "inProgress") {
      if (currentLeader !== null) {
        this.phase = "crowning";
        this.phaseStart = now;
        this.newLeader = currentLeader;
        this.newTerm = leader!.Term;
      }
    } else if (this.phase === "crowning") {
      if (now - this.phaseStart > CROWNING_HOLD_MS) {
        this.phase = "idle";
      }
    }

    this.candidateIds = this.phase === "inProgress"
      ? new Set(snap.Nodes.filter((n) => n.Alive && n.Role === "candidate").map((n) => n.ID))
      : new Set();

    this.lastLeader = currentLeader;
  }

  isCandidate(id: number): boolean {
    return this.candidateIds.has(id);
  }

  active(): boolean {
    return this.phase !== "idle";
  }

  banner(): { text: string; tone: "danger" | "gold" } | null {
    if (this.phase === "inProgress") return { text: "⚡ LEADER DOWN — ELECTION IN PROGRESS ⚡", tone: "danger" };
    if (this.phase === "crowning") return { text: `👑 n${this.newLeader} ELECTED — TERM ${this.newTerm}`, tone: "gold" };
    return null;
  }

}
