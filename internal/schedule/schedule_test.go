package schedule

import (
	"testing"
	"time"

	"github.com/danielmaier42/claudeq/internal/task"
)

func base(id string, tr task.Trigger) task.Task {
	return task.Task{
		ID: id, Name: id, Prompt: "p", WorkingDir: "/r",
		Trigger: tr, Enabled: true, Permissions: task.PermissionsDefault,
	}
}

func TestDueASAP(t *testing.T) {
	now := time.Now()
	tk := base("a", task.TriggerASAP)

	if due, _ := Due(tk, Inputs{Now: now}); !due {
		t.Fatal("fresh asap task should be due")
	}
	if due, _ := Due(tk, Inputs{Now: now, CompletedOnce: true}); due {
		t.Fatal("completed asap task should not be due")
	}
	if due, _ := Due(tk, Inputs{Now: now, Running: true}); due {
		t.Fatal("running task should not be due again")
	}
	tk.Enabled = false
	if due, _ := Due(tk, Inputs{Now: now}); due {
		t.Fatal("disabled task should not be due")
	}
}

func TestDueFixed(t *testing.T) {
	at := time.Date(2026, 7, 17, 20, 0, 0, 0, time.UTC)
	tk := base("f", task.TriggerFixed)
	tk.FixedAt = at

	if due, _ := Due(tk, Inputs{Now: at.Add(-time.Minute)}); due {
		t.Fatal("fixed task should not be due before its time")
	}
	if due, _ := Due(tk, Inputs{Now: at}); !due {
		t.Fatal("fixed task should be due at its time")
	}
	if due, _ := Due(tk, Inputs{Now: at.Add(time.Hour), CompletedOnce: true}); due {
		t.Fatal("completed fixed task should not re-run")
	}
}

func TestDueCron(t *testing.T) {
	tk := base("c", task.TriggerCron)
	tk.Cron = "0 20 * * *" // daily 20:00
	anchor := time.Date(2026, 7, 17, 8, 0, 0, 0, time.UTC)

	// Before 20:00, next occurrence after anchor is 20:00 today -> not due.
	if due, _ := Due(tk, Inputs{Now: time.Date(2026, 7, 17, 19, 0, 0, 0, time.UTC), CronAnchor: anchor}); due {
		t.Fatal("cron should not be due before its occurrence")
	}
	// At 20:00 -> due.
	if due, _ := Due(tk, Inputs{Now: time.Date(2026, 7, 17, 20, 0, 0, 0, time.UTC), CronAnchor: anchor}); !due {
		t.Fatal("cron should be due at its occurrence")
	}
	// Anchor already past today's occurrence -> next is tomorrow -> not due now.
	justRan := time.Date(2026, 7, 17, 20, 0, 1, 0, time.UTC)
	if due, _ := Due(tk, Inputs{Now: time.Date(2026, 7, 17, 21, 0, 0, 0, time.UTC), CronAnchor: justRan}); due {
		t.Fatal("cron should wait for the next occurrence after the last run")
	}
}

func TestSelectExclusiveRunsAlone(t *testing.T) {
	due := []task.Task{base("hi", task.TriggerASAP)} // non-parallel by default
	got := Select(due, Running{}, nil)
	if len(got) != 1 || got[0].ID != "hi" {
		t.Fatalf("expected [hi], got %v", ids(got))
	}
}

func TestSelectNothingWhenExclusiveRunning(t *testing.T) {
	due := []task.Task{base("a", task.TriggerASAP)}
	if got := Select(due, Running{NonParallel: true}, nil); got != nil {
		t.Fatalf("expected nothing while exclusive running, got %v", ids(got))
	}
}

func TestSelectBatchesParallelInPriorityOrder(t *testing.T) {
	p1 := base("p1", task.TriggerASAP)
	p1.Parallel = true
	p2 := base("p2", task.TriggerASAP)
	p2.Parallel = true
	excl := base("e", task.TriggerASAP)

	// [p1, p2, e]: both parallels start; the exclusive waits behind them.
	got := Select([]task.Task{p1, p2, excl}, Running{}, nil)
	if want := []string{"p1", "p2"}; !equal(ids(got), want) {
		t.Fatalf("got %v, want %v", ids(got), want)
	}
}

func TestSelectExclusiveFirstBlocksLowerParallel(t *testing.T) {
	excl := base("e", task.TriggerASAP) // highest priority, non-parallel
	p1 := base("p1", task.TriggerASAP)
	p1.Parallel = true

	// [e, p1]: e must run alone; p1 waits despite being parallel.
	got := Select([]task.Task{excl, p1}, Running{}, nil)
	if want := []string{"e"}; !equal(ids(got), want) {
		t.Fatalf("got %v, want %v", ids(got), want)
	}
}

func TestSelectAddsParallelWhileParallelRunning(t *testing.T) {
	p := base("p", task.TriggerASAP)
	p.Parallel = true
	excl := base("e", task.TriggerASAP)

	got := Select([]task.Task{p, excl}, Running{Parallel: true}, nil)
	if want := []string{"p"}; !equal(ids(got), want) {
		t.Fatalf("got %v, want %v (exclusive must wait for parallels to drain)", ids(got), want)
	}
}

func ids(ts []task.Task) []string {
	out := make([]string, len(ts))
	for i, t := range ts {
		out[i] = t.ID
	}
	return out
}

func equal(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestHeld(t *testing.T) {
	par := func(id string) task.Task { tk := base(id, task.TriggerASAP); tk.Parallel = true; return tk }
	excl := func(id string) task.Task { return base(id, task.TriggerASAP) }
	const (
		alone   = "Runs alone, so it waits until the running tasks finish."
		behind  = "Waits behind a higher-priority task that runs alone."
		blocked = "A task that runs alone is running; nothing else starts until it finishes."
		turn    = "Runs alone and has waited long enough: nothing new starts until the running tasks finish."
		first   = "Lets a task that runs alone and has waited longer go first; starts after it."
	)
	tests := []struct {
		name    string
		due     []task.Task
		running Running
		overdue map[string]bool
		want    map[string]string
	}{
		{"everything starts", []task.Task{par("a"), par("b")}, Running{}, nil, map[string]string{}},
		{"exclusive running holds all", []task.Task{par("a"), excl("b")}, Running{NonParallel: true}, nil,
			map[string]string{"a": blocked, "b": blocked}},
		{"exclusive waits for parallel", []task.Task{excl("a"), par("b")}, Running{Parallel: true}, nil,
			map[string]string{"a": alone, "b": behind}},
		{"exclusive after a started parallel", []task.Task{par("a"), excl("b"), par("c")}, Running{}, nil,
			map[string]string{"b": alone, "c": behind}},
		{"tasks behind a started exclusive", []task.Task{excl("a"), par("b")}, Running{}, nil,
			map[string]string{"b": behind}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := Held(tc.due, tc.running, tc.overdue)
			if len(got) != len(tc.want) {
				t.Fatalf("Held = %v, want %v", got, tc.want)
			}
			for id, reason := range tc.want {
				if got[id] != reason {
					t.Fatalf("Held[%s] = %q, want %q", id, got[id], reason)
				}
			}
		})
	}
}

// A task that runs alone sits below parallel tasks that come due every minute.
// Before it has waited ExclusiveFairWait priority decides and the parallel ones
// start; after that nothing new starts until the running ones finish, and then
// it runs alone.
func TestSelectFairDrainForStarvedExclusive(t *testing.T) {
	watch := base("watch", task.TriggerCron)
	watch.Parallel = true
	review := base("review", task.TriggerASAP)
	due := []task.Task{watch, review}
	now := time.Date(2026, 11, 10, 9, 0, 0, 0, time.UTC)

	fresh := Overdue(due, map[string]time.Time{"review": now.Add(-time.Minute)}, now)
	if got := Select(due, Running{Parallel: true}, fresh); !equal(ids(got), []string{"watch"}) {
		t.Fatalf("before the fair wait priority decides: got %v, want [watch]", ids(got))
	}

	overdue := Overdue(due, map[string]time.Time{"review": now.Add(-ExclusiveFairWait)}, now)
	if !overdue["review"] || overdue["watch"] {
		t.Fatalf("Overdue = %v, want only review", overdue)
	}
	if got := Select(due, Running{Parallel: true}, overdue); len(got) != 0 {
		t.Fatalf("while the queue drains nothing new starts, got %v", ids(got))
	}
	if got := Select(due, Running{}, overdue); !equal(ids(got), []string{"review"}) {
		t.Fatalf("once the queue is empty the overdue task runs alone, got %v", ids(got))
	}
}

func TestOverdueLeavesParallelAndBackfillAlone(t *testing.T) {
	now := time.Date(2026, 11, 10, 9, 0, 0, 0, time.UTC)
	par := base("par", task.TriggerASAP)
	par.Parallel = true
	fill := base("fill", task.TriggerASAP)
	fill.Backfill = true
	unseen := base("unseen", task.TriggerASAP)
	long := now.Add(-time.Hour)
	got := Overdue([]task.Task{par, fill, unseen}, map[string]time.Time{"par": long, "fill": long}, now)
	if len(got) != 0 {
		t.Fatalf("Overdue = %v, want none", got)
	}
}
