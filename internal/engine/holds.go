package engine

import (
	"fmt"
	"io"
	"maps"
	"sync"
	"time"

	"github.com/danielmaier42/claudeq/internal/schedule"
)

// HoldLogAfter is how long a due task may sit without starting before the
// daemon writes down why. Short waits are the queue working normally; a long
// one is what someone asks about afterwards, when the queue no longer shows it.
const HoldLogAfter = 5 * time.Minute

// holds remembers, per task, why the last tick did not start it although it
// was due. It has its own lock because the dashboard reads it while a tick is
// under way.
type holds struct {
	mu     sync.Mutex
	byTask map[string]schedule.Hold
	// logged is the reason last written to the log per task, so a long wait is
	// logged once, and again only when its reason changes.
	logged map[string]string
	// announced holds the tasks whose current wait was notified, so a wait is
	// announced once however long it goes on. counting is since when each
	// task has waited for a reason worth announcing: time held for one that is
	// not (no network, out of allowance) does not count towards the limit.
	announced map[string]bool
	counting  map[string]time.Time
}

// longHold is a wait that has just gone on long enough to be announced.
type longHold struct {
	taskID string
	reason string
	waited time.Duration
}

// update replaces the holds with one tick's verdicts. A task that keeps
// waiting keeps its start time even when the reason changes, because to the
// person looking at the queue it is still the same wait. Tasks missing from
// reasons started, or are not due any more, and are forgotten.
//
// limit says how long each task may wait before its wait is announced, zero
// for never (nil: nothing is announced); the waits that just crossed it are
// returned, each once per wait.
func (h *holds) update(reasons map[string]string, now time.Time, log io.Writer, limit func(id string) time.Duration) []longHold {
	h.mu.Lock()
	defer h.mu.Unlock()
	next := make(map[string]schedule.Hold, len(reasons))
	logged := make(map[string]string, len(reasons))
	announced := make(map[string]bool, len(h.announced))
	counting := make(map[string]time.Time, len(h.counting))
	var long []longHold
	for id, reason := range reasons {
		since := now
		if prev, ok := h.byTask[id]; ok {
			since = prev.Since
		}
		next[id] = schedule.Hold{Reason: reason, Since: since}
		if last, ok := h.logged[id]; ok {
			logged[id] = last
		}
		waited := now.Sub(since)
		if waited >= HoldLogAfter && logged[id] != reason {
			fmt.Fprintf(log, "dispatchqd: task %q has been due for %s and does not start: %s\n",
				id, waited.Round(time.Minute), reason)
			logged[id] = reason
		}
		if h.announced[id] {
			announced[id] = true
			continue
		}
		after := time.Duration(0)
		if limit != nil {
			after = limit(id)
		}
		if after <= 0 {
			continue
		}
		from, ok := h.counting[id]
		if !ok {
			from = now
		}
		counting[id] = from
		if counted := now.Sub(from); counted >= after {
			fmt.Fprintf(log, "dispatchqd: task %q has waited %s; notifying\n", id, counted.Round(time.Minute))
			long = append(long, longHold{taskID: id, reason: reason, waited: counted})
			announced[id] = true
		}
	}
	h.byTask, h.logged, h.announced, h.counting = next, logged, announced, counting
	return long
}

// snapshot returns a copy of the current holds.
func (h *holds) snapshot() map[string]schedule.Hold {
	h.mu.Lock()
	defer h.mu.Unlock()
	return maps.Clone(h.byTask)
}
