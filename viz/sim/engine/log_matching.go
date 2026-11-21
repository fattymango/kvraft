package engine

import "fmt"

// checkLogMatching checks the Raft paper's Log Matching Property across
// every pair of currently-alive nodes: if two logs ever share an entry at
// the same (index, term), earlier entries up to that index should never
// disagree. Called after a Step() has fully quiesced (never from inside a
// trace() callback -- see raft.Raft.LogTerms's comment on why that would
// deadlock).
func (e *Engine) checkLogMatching() {
	e.mu.Lock()
	nodes := append([]*node{}, e.nodes...)
	e.mu.Unlock()

	type logView struct {
		id    int
		start int
		terms []int
	}
	var views []logView
	for i, n := range nodes {
		if !n.alive {
			continue
		}
		start, terms := n.rf.LogTerms()
		views = append(views, logView{id: i, start: start, terms: terms})
	}

	for i := 0; i < len(views); i++ {
		for j := i + 1; j < len(views); j++ {
			a, b := views[i], views[j]
			lo := max(a.start, b.start)
			hi := min(a.start+len(a.terms)-1, b.start+len(b.terms)-1)

			mismatchSeen := false
			for idx := lo; idx <= hi; idx++ {
				ta := a.terms[idx-a.start]
				tb := b.terms[idx-b.start]
				if ta != tb {
					mismatchSeen = true
					continue
				}
				if mismatchSeen {
					e.mu.Lock()
					e.violations = append(e.violations, Violation{
						Kind: "log_matching",
						Detail: fmt.Sprintf(
							"node%d and node%d logs match again at index %d (term %d) after disagreeing at an earlier index",
							a.id, b.id, idx, ta),
						At: e.clk.Now(),
					})
					e.mu.Unlock()
					return // one confirmed violation is enough to freeze the UI
				}
			}
		}
	}
}
