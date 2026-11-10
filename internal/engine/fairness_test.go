package engine

import (
	"context"
	"testing"
	"time"

	"github.com/danielmaier42/claudeq/internal/clock"
	"github.com/danielmaier42/claudeq/internal/schedule"
	"github.com/danielmaier42/claudeq/internal/task"
)

// A task that runs alone sits below a parallel watcher that comes due every
// minute. Strict priority alone never found the queue empty for it: each tick
// started the watcher first and the exclusive task waited behind it for good.
// Priority still decides while the wait is short; once the exclusive task has
// waited schedule.ExclusiveFairWait, the watcher holds back and the exclusive
// task runs alone. The watcher is only delayed, never skipped.
func TestExclusiveTaskGetsItsTurnBehindFrequentParallelWork(t *testing.T) {
	fc := clock.NewFake(time.Date(2026, 11, 10, 9, 0, 30, 0, time.UTC))
	r := &stub{}
	e, st := newTestEngine(t, r, fc)
	watch := asapTask("watch", true)
	watch.Trigger, watch.Cron = task.TriggerCron, "* * * * *"
	review := asapTask("review", false)
	// The watcher is in the queue first, so it is due on every tick from the
	// moment the exclusive task is added below it.
	saveTasks(t, st, watch)
	if err := e.Tick(context.Background()); err != nil {
		t.Fatal(err)
	}
	fc.Advance(time.Minute)
	saveTasks(t, st, watch, review)

	reviewAt := -1
	watchRuns := 0
	for minute := 0; minute <= 10 && reviewAt < 0; minute++ {
		if err := e.Tick(context.Background()); err != nil {
			t.Fatalf("Tick at minute %d: %v", minute, err)
		}
		e.WaitIdle()
		watchRuns, reviewAt = 0, -1
		for _, req := range r.reqs {
			switch req.Task.ID {
			case "watch":
				watchRuns++
			case "review":
				reviewAt = minute
			}
		}
		fc.Advance(time.Minute)
	}
	if reviewAt < 0 {
		t.Fatalf("the exclusive task never started; the watcher ran %d times", watchRuns)
	}
	if wait := time.Duration(reviewAt) * time.Minute; wait < schedule.ExclusiveFairWait {
		t.Fatalf("the exclusive task jumped the queue after %s; priority holds until %s", wait, schedule.ExclusiveFairWait)
	}
	if watchRuns < 4 {
		t.Fatalf("the higher-priority watcher ran %d times before the exclusive task, want it to keep running until the fair wait", watchRuns)
	}
	if h, ok := e.Holds()["watch"]; !ok || h.Reason == "" {
		t.Fatalf("the watcher held back for the exclusive task must say why, got %+v", e.Holds())
	}

	// The watcher was only delayed: the next tick starts it again.
	fc.Advance(time.Minute)
	if err := e.Tick(context.Background()); err != nil {
		t.Fatal(err)
	}
	e.WaitIdle()
	if last := r.reqs[len(r.reqs)-1].Task.ID; last != "watch" {
		t.Fatalf("after the exclusive task the watcher runs again, last start was %q", last)
	}
}
