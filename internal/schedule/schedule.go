// Package schedule contains the pure scheduling logic: when a task is due, and
// which due tasks may start given the current concurrency state. It has no I/O
// so it is fully unit-testable (PLAN.md §7, FA-11/12/13/14).
package schedule

import (
	"fmt"
	"time"

	"github.com/danielmaier42/claudeq/internal/task"
)

// Inputs are the per-task facts needed to decide whether a task is due.
type Inputs struct {
	// Now is the evaluation time.
	Now time.Time
	// Running is true if a run of this task is currently active. A task never
	// starts a second concurrent run (one-shot guard and cron overlap-skip).
	Running bool
	// CompletedOnce is true if a one-shot (asap/fixed) task has already run.
	CompletedOnce bool
	// CronAnchor is the time from which the next cron occurrence is computed:
	// the last run's start, or the task's first-seen time. Required for cron.
	CronAnchor time.Time
	// DependenciesMet is false while a task is still waiting for a job it
	// depends on. It is true for a task that depends on nothing.
	DependenciesMet bool
}

// Due reports whether the task should start now.
func Due(t task.Task, in Inputs) (bool, error) {
	if !t.Enabled || in.Running {
		return false, nil
	}
	// A job that waits for others is not due, whatever its trigger says. The
	// trigger decides the earliest it may start; the dependencies decide whether
	// its input exists yet.
	if len(t.DependsOn) > 0 && !in.DependenciesMet {
		return false, nil
	}

	switch t.Trigger {
	case task.TriggerASAP:
		return !in.CompletedOnce, nil
	case task.TriggerFixed:
		return !in.CompletedOnce && !in.Now.Before(t.FixedAt), nil
	case task.TriggerCron:
		sched, err := t.CronSchedule()
		if err != nil {
			return false, fmt.Errorf("task %q: %w", t.ID, err)
		}
		next := sched.Next(in.CronAnchor)
		return !next.After(in.Now), nil
	default:
		return false, fmt.Errorf("task %q: unknown trigger %q", t.ID, t.Trigger)
	}
}

// Running summarizes what is currently executing, for concurrency decisions.
type Running struct {
	// NonParallel is true if an exclusive (non-parallel) task is running.
	NonParallel bool
	// Parallel is true if at least one parallel task is running.
	Parallel bool
}

// ExclusiveFairWait is how long a due task that runs alone may wait behind
// higher-priority parallel work before it gets its turn. Parallel tasks that
// come due every minute would otherwise always keep a slot busy, and the task
// that needs the queue to itself would never find it empty.
const ExclusiveFairWait = 5 * time.Minute

// Overdue picks the due tasks that run alone and have waited at least
// [ExclusiveFairWait], from when each one became due (since). Backfill work is
// never among them: it only soaks up allowance nothing else wants, so it does
// not get to empty the queue for itself.
func Overdue(due []task.Task, since map[string]time.Time, now time.Time) map[string]bool {
	out := map[string]bool{}
	for _, t := range due {
		at, ok := since[t.ID]
		if !t.Parallel && !t.Backfill && ok && now.Sub(at) >= ExclusiveFairWait {
			out[t.ID] = true
		}
	}
	return out
}

// fair moves the overdue tasks to the front of due, in their own priority
// order, and leaves the rest as they are. An overdue task at the front makes
// the queue drain: nothing new starts until it has run (see Select).
func fair(due []task.Task, overdue map[string]bool) []task.Task {
	if len(overdue) == 0 {
		return due
	}
	out := make([]task.Task, 0, len(due))
	for _, t := range due {
		if overdue[t.ID] {
			out = append(out, t)
		}
	}
	for _, t := range due {
		if !overdue[t.ID] {
			out = append(out, t)
		}
	}
	return out
}

// Select chooses which of the due tasks to start this tick. due must be in
// priority order (highest first). The rules (PLAN.md §7):
//
//   - An exclusive (non-parallel) task runs alone.
//   - Parallel tasks may run together with other parallel tasks.
//   - Priority is preserved: we never start a lower-priority task ahead of a
//     higher-priority due task that is merely waiting for exclusivity.
//   - Fair drain: an exclusive task in overdue (see [Overdue]) counts as the
//     highest priority. No new task starts while the running ones finish, and
//     once the queue is empty it starts alone. Nothing is skipped meanwhile: a
//     task held back stays due and starts after it.
func Select(due []task.Task, running Running, overdue map[string]bool) []task.Task {
	if running.NonParallel {
		return nil // the exclusive slot is taken
	}

	var start []task.Task
	for _, t := range fair(due, overdue) {
		if t.Parallel {
			start = append(start, t)
			continue
		}
		// A non-parallel task needs the exclusive slot: nothing running and
		// nothing already selected this tick. If it can't have it, stop —
		// starting a lower-priority task ahead of it would break priority.
		if running.Parallel || len(start) > 0 {
			break
		}
		return []task.Task{t}
	}
	return start
}

// Hold is why a due task did not start, and since when it has been waiting.
// The queue shows it, so a task that sits there is not a mystery.
type Hold struct {
	Reason string    `json:"reason"`
	Since  time.Time `json:"since"`
}

// Held explains, for each due task [Select] leaves out, why the concurrency
// rules hold it back. due, running and overdue must be what Select was given.
func Held(due []task.Task, running Running, overdue map[string]bool) map[string]string {
	started := map[string]bool{}
	for _, t := range Select(due, running, overdue) {
		started[t.ID] = true
	}
	out := map[string]string{}
	exclusiveAhead, drain := false, false
	for _, t := range fair(due, overdue) {
		switch {
		case started[t.ID]:
		case running.NonParallel:
			out[t.ID] = "A task that runs alone is running; nothing else starts until it finishes."
		case drain:
			out[t.ID] = "Lets a task that runs alone and has waited longer go first; starts after it."
		case exclusiveAhead:
			out[t.ID] = "Waits behind a higher-priority task that runs alone."
		case overdue[t.ID]:
			out[t.ID] = "Runs alone and has waited long enough: nothing new starts until the running tasks finish."
		default:
			out[t.ID] = "Runs alone, so it waits until the running tasks finish."
		}
		if !t.Parallel {
			exclusiveAhead = true
			drain = drain || overdue[t.ID]
		}
	}
	return out
}
