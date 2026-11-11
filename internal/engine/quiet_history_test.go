package engine

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/danielmaier42/claudeq/internal/clock"
	"github.com/danielmaier42/claudeq/internal/executor"
	"github.com/danielmaier42/claudeq/internal/provider"
	"github.com/danielmaier42/claudeq/internal/store"
	"github.com/danielmaier42/claudeq/internal/task"
)

// runQuietTask runs one quiet-history task to completion with the given
// outcome and returns the store to inspect.
func runQuietTask(t *testing.T, status store.RunStatus) *store.Store {
	t.Helper()
	fc := clock.NewFake(time.Date(2026, 9, 7, 8, 0, 0, 0, time.UTC))
	r := &stub{result: func(req executor.Request, _ int) provider.Result {
		return provider.Result{Status: status, SessionID: req.SessionID, ExitCode: 1, RetryAfter: time.Minute}
	}}
	e, st := newTestEngine(t, r, fc)
	watcher := asapTask("watch", false)
	watcher.Trigger = task.TriggerCron
	watcher.Cron = "*/15 * * * *"
	watcher.QuietHistory = true
	saveTasks(t, st, watcher)

	if err := e.RunTaskNow(context.Background(), "watch"); err != nil {
		t.Fatalf("RunTaskNow: %v", err)
	}
	e.WaitIdle()
	return st
}

// A quiet task's success is recorded, marked quiet, with its log kept, so the
// Log can show it on request; every other outcome is recorded like any other
// run's: not quiet, unread, and listed by default.
func TestQuietTaskRecordsSuccessAsQuietAndTheRestAsUsual(t *testing.T) {
	cases := []struct {
		status store.RunStatus
		quiet  bool
	}{
		{store.StatusSuccess, true},
		{store.StatusRateLimited, false}, // it holds up the whole provider: show it
		{store.StatusFailed, false},
		{store.StatusAuthError, false},
	}
	for _, c := range cases {
		t.Run(string(c.status), func(t *testing.T) {
			st := runQuietTask(t, c.status)
			runs, err := st.Runs()
			if err != nil {
				t.Fatal(err)
			}
			if len(runs) != 1 || runs[0].Status != c.status || runs[0].RunID != "run-1" || runs[0].Quiet != c.quiet {
				t.Fatalf("a quiet task's %s must be recorded with quiet=%t, got %+v", c.status, c.quiet, runs)
			}
			if _, err := os.Stat(st.LogPath("run-1")); err != nil {
				t.Fatalf("the recorded run's log must remain for inspection: %v", err)
			}
			if visible := store.VisibleRuns(runs); len(visible) == 1 == c.quiet {
				t.Fatalf("a %s of a quiet task: visible by default = %t, want %t", c.status, len(visible) == 1, !c.quiet)
			}
		})
	}
}

// Recording quiet successes must not let a frequent watcher take over the
// history: they are pruned on their own retention and do not count against
// the run history limit.
func TestQuietRunsDoNotCrowdOutRealHistory(t *testing.T) {
	fc := clock.NewFake(time.Date(2026, 9, 7, 8, 0, 0, 0, time.UTC))
	r := &stub{result: func(req executor.Request, _ int) provider.Result {
		if req.Task.ID == "real" {
			return provider.Result{Status: store.StatusFailed, ExitCode: 1}
		}
		return provider.Result{Status: store.StatusSuccess}
	}}
	e, st := newTestEngine(t, r, fc)
	watcher := asapTask("watch", false)
	watcher.Trigger, watcher.Cron = task.TriggerCron, "*/15 * * * *"
	watcher.QuietHistory = true
	saveTasks(t, st, asapTask("real", false), watcher)
	if err := st.UpdateConfig(func(c *store.Config) error {
		c.Settings.MaxRunHistory = 2
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	if err := e.RunTaskNow(context.Background(), "real"); err != nil {
		t.Fatal(err)
	}
	e.WaitIdle()
	// More ticks than the store lets expire before it rewrites history (its
	// prune slack of 20), so their expiry below is due as one batch.
	const ticks = 21
	for range ticks {
		fc.Advance(15 * time.Minute)
		if err := e.RunTaskNow(context.Background(), "watch"); err != nil {
			t.Fatal(err)
		}
		e.WaitIdle()
	}
	runs, _ := st.Runs()
	if v := store.VisibleRuns(runs); len(v) != 1 || v[0].TaskID != "real" {
		t.Fatalf("the real run must survive the quiet ticks under a limit of 2: %+v", runs)
	}
	if len(runs) != ticks+1 {
		t.Fatalf("all quiet ticks are within their retention: got %d runs", len(runs))
	}

	// Two days later the next tick finds the earlier ones expired.
	fc.Advance(store.QuietRunMaxAge + time.Minute)
	if err := e.RunTaskNow(context.Background(), "watch"); err != nil {
		t.Fatal(err)
	}
	e.WaitIdle()
	runs, _ = st.Runs()
	if len(runs) != 2 || runs[0].TaskID != "real" || !runs[1].Quiet {
		t.Fatalf("expired quiet runs must be pruned, the real one kept: %+v", runs)
	}
	if _, err := os.Stat(st.LogPath("run-2")); !os.IsNotExist(err) {
		t.Fatalf("the log of a pruned quiet run must go with it: %v", err)
	}
}

func TestQuietRunIsNotRecordedWhileRunning(t *testing.T) {
	fc := clock.NewFake(time.Now())
	r := &stub{block: make(chan struct{})}
	e, st := newTestEngine(t, r, fc)
	watcher := asapTask("watch", false)
	watcher.QuietHistory = true
	saveTasks(t, st, watcher)

	// Tick (not RunTaskNow, which waits for the run) so the run can be
	// observed while it is still in flight.
	if err := e.Tick(context.Background()); err != nil {
		t.Fatalf("Tick: %v", err)
	}
	waitFor(t, func() bool { return len(r.requests()) == 1 })
	if runs, _ := st.Runs(); len(runs) != 0 {
		t.Fatalf("a quiet run must not enter history while running: %+v", runs)
	}
	if ids := e.ActiveTaskIDs(); len(ids) != 1 || ids[0] != "watch" {
		t.Fatalf("the task still counts as running for the queue: %v", ids)
	}
	// History holds nothing for it, so the engine is what the Log and the
	// dashboard learn from that it runs.
	if live := e.ActiveRuns(); len(live) != 1 || live[0].RunID != "run-1" ||
		live[0].Status != store.StatusRunning || live[0].TaskID != "watch" {
		t.Fatalf("the quiet run in flight must be reported as running: %+v", live)
	}
	close(r.block)
	e.WaitIdle()
	if runs, _ := st.Runs(); len(runs) != 1 || !runs[0].Quiet || runs[0].Status != store.StatusSuccess {
		t.Fatalf("a finished quiet success is recorded as quiet: %+v", runs)
	}
	if live := e.ActiveRuns(); len(live) != 0 {
		t.Fatalf("a finished run is no longer in flight: %+v", live)
	}
}
