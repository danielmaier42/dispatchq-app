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
	unblock := refuseHistoryWrites(t, st)
	defer unblock()

	if err := e.CancelRun("run-1"); err != nil {
		t.Fatalf("CancelRun: %v", err)
	}
	if s, _ := st.LoadState(); len(s.PendingResumes) != 0 {
		t.Fatal("the resume is still planned")
	}
}
