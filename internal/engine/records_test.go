//go:build unix

package engine

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/danielmaier42/claudeq/internal/clock"
	"github.com/danielmaier42/claudeq/internal/store"
)

// The incident: a one-shot ran, its completion was written, but the write that
// takes it out of the queue failed. It stayed listed, enabled, and never due
// again. The next start takes it out of the queue.
func TestRecoverRemovesACompletedOneShotStillQueued(t *testing.T) {
	fc := clock.NewFake(time.Date(2026, 11, 11, 9, 0, 0, 0, time.UTC))
	r := &stub{}
	e, st := newTestEngine(t, r, fc)
	e.procs = newFakeProcs()
	saveTasks(t, st, asapTask("r13-implementation", false), asapTask("next", false))
	if err := st.UpdateState(func(s *store.State) error {
		s.MarkCompletedOnce("r13-implementation")
		s.OweRemoval("r13-implementation") // the retire's removal failed
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	if _, _, err := e.Recover(); err != nil {
		t.Fatal(err)
	}
	cfg, err := st.LoadConfig()
	if err != nil {
		t.Fatal(err)
	}
	var queued []string
	for _, tk := range cfg.Tasks {
		queued = append(queued, tk.ID)
	}
	if !slices.Equal(queued, []string{"next"}) {
		t.Fatalf("queue after the start = %v, want only the task that has not run", queued)
	}
	if err := e.Tick(context.Background()); err != nil {
		t.Fatal(err)
	}
	e.WaitIdle()
	if reqs := r.requests(); len(reqs) != 1 || reqs[0].Task.ID != "next" {
		t.Fatalf("runs = %+v, want only the task that has not run", reqs)
	}
}

// The incident: a run ended while the system was out of file handles, and its
// outcome never reached history. The record is kept, and the next start
// writes it.
func TestRecoverWritesARunRecordAnEarlierDaemonKept(t *testing.T) {
	fc := clock.NewFake(time.Date(2026, 11, 11, 11, 13, 2, 0, time.UTC))
	e, st := newTestEngine(t, &stub{}, fc)
	e.procs = newFakeProcs()

	finished := fc.Now()
	rec := store.Run{
		RunID: "20261111T111302-0410a389", TaskID: "watch", TaskName: "watch",
		StartedAt: finished.Add(-time.Minute), FinishedAt: &finished,
		Status: store.StatusFailed, Error: "run failed (exit 1)",
	}
	kept := filepath.Join(st.Home(), "runs", rec.RunID+".pending.json")
	data := []byte(`{"run_id":"` + rec.RunID + `","task_id":"watch","task_name":"watch",` +
		`"started_at":"2026-11-11T11:12:02Z","finished_at":"2026-11-11T11:13:02Z",` +
		`"status":"failed","error":"run failed (exit 1)"}`)
	if err := os.WriteFile(kept, data, 0o644); err != nil {
		t.Fatal(err)
	}

	if _, _, err := e.Recover(); err != nil {
		t.Fatal(err)
	}
	got := latestRun(t, st, rec.RunID)
	if got.Status != store.StatusFailed || got.Error != rec.Error {
		t.Fatalf("run in history = %+v, want the kept outcome", got)
	}
	if _, err := os.Stat(kept); !os.IsNotExist(err) {
		t.Fatalf("kept record still on disk after it was written: %v", err)
	}
}

// refuseHistoryWrites makes history.jsonl readable but not writable, as a
// system out of file handles or a full disk leaves it for writes. The
// returned func lifts it.
func refuseHistoryWrites(t *testing.T, st *store.Store) func() {
	t.Helper()
	path := filepath.Join(st.Home(), "history.jsonl")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	_ = f.Close()
	if err := os.Chmod(path, 0o444); err != nil {
		t.Fatal(err)
	}
	return func() {
		if err := os.Chmod(path, 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

// The review finding: a start record history refused was kept and landed
// later, although the run never started, so a disk outage left one phantom
// "running" run per tick. A start that cannot be recorded leaves nothing.
func TestARefusedStartRecordLeavesNoPhantomRun(t *testing.T) {
	fc := clock.NewFake(time.Date(2026, 11, 11, 11, 13, 2, 0, time.UTC))
	r := &stub{}
	e, st := newTestEngine(t, r, fc)
	st.SetRetrySleep(func(time.Duration) {})
	saveTasks(t, st, asapTask("a", false))
	unblock := refuseHistoryWrites(t, st)

	for range 2 {
		if err := e.Tick(context.Background()); err == nil {
			t.Fatal("Tick started a run whose start could not be recorded")
		}
	}
	e.WaitIdle()
	if n := len(r.requests()); n != 0 {
		t.Fatalf("%d run(s) started without a record", n)
	}
	unblock()
	if err := e.Tick(context.Background()); err != nil {
		t.Fatal(err)
	}
	e.WaitIdle()
	runs, err := st.Runs()
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, r := range runs {
		got = append(got, r.RunID+":"+string(r.Status))
	}
	if !slices.Equal(got, []string{"run-3:success"}) {
		t.Fatalf("history = %v, want only the run that really ran", got)
	}
	if kept, _ := filepath.Glob(filepath.Join(st.Home(), "runs", "*.pending.json")); len(kept) != 0 {
		t.Fatalf("kept records left behind: %v", kept)
	}
}

// The review finding: cancelling a resume reported a failure when its record
// was only late, although the resume was canceled and the record lands later.
func TestCancelResumeSucceedsWhenItsRecordIsKept(t *testing.T) {
	fc := clock.NewFake(time.Date(2026, 7, 17, 22, 0, 0, 0, time.UTC))
	e, st, _ := rateLimitedTask(t, asapTask("a", false), fc)
	st.SetRetrySleep(func(time.Duration) {})
	unblock := refuseHistoryWrites(t, st)
	defer unblock()

	if err := e.CancelRun("run-1"); err != nil {
		t.Fatalf("CancelRun: %v", err)
	}
	if s, _ := st.LoadState(); len(s.PendingResumes) != 0 {
		t.Fatal("the resume is still planned")
	}
}

// The review finding: a run's process record was cleared before its outcome
// was written, so a quiet-history run whose outcome history could not take
// (or a daemon killed in between) left no trace at all. The process record
// stays until the outcome is written or kept: a start that cannot write it
// either keeps it too, and the first start that can records the run.
func TestAQuietRunWhoseOutcomeCannotBeWrittenIsRecordedAtTheNextStart(t *testing.T) {
	fc := clock.NewFake(time.Date(2026, 11, 11, 11, 13, 2, 0, time.UTC))
	procs := newFakeProcs()
	first := &startedStub{pid: 50311, procs: procs, block: make(chan struct{})}
	e, st := newTestEngine(t, first, fc)
	st.SetRetrySleep(func(time.Duration) {})
	e.procs = procs
	tk := asapTask("watch", false)
	tk.QuietHistory = true
	saveTasks(t, st, tk)
	if err := e.Tick(context.Background()); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool {
		s, _ := st.LoadState()
		return len(s.LiveRuns) == 1
	})

	// History refuses the outcome, and there is no room to keep it either.
	unblock := refuseHistoryWrites(t, st)
	runs := filepath.Join(st.Home(), "runs")
	if err := os.Chmod(runs, 0o555); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(runs, 0o755) })
	close(first.block)
	e.WaitIdle()
	if s, _ := st.LoadState(); len(s.LiveRuns) != 1 {
		t.Fatal("the process record went although the outcome was written nowhere")
	}
	procs.exit(50311)

	// restart is a daemon start in a new process: a store with nothing in
	// memory, on the same data directory.
	restart := func() *store.Store {
		t.Helper()
		next, err := store.Open(st.Home())
		if err != nil {
			t.Fatal(err)
		}
		next.SetRetrySleep(func(time.Duration) {})
		d, _ := newTestEngine(t, &stub{}, fc)
		d.store = next
		d.procs = procs
		if _, _, err := d.Recover(); err != nil {
			t.Fatal(err)
		}
		return next
	}
	// A start that cannot write the outcome either keeps the process record.
	if s, _ := restart().LoadState(); len(s.LiveRuns) != 1 {
		t.Fatal("a start that could not record the run forgot its process record")
	}

	unblock()
	if err := os.Chmod(runs, 0o755); err != nil {
		t.Fatal(err)
	}
	next := restart()
	if run := latestRun(t, next, "run-1"); run.Status != store.StatusFailed || run.Quiet {
		t.Fatalf("run = %s quiet=%v, want a visible failed row", run.Status, run.Quiet)
	}
	if s, _ := next.LoadState(); len(s.LiveRuns) != 0 {
		t.Fatal("the process record stayed after the run was recorded")
	}
}
