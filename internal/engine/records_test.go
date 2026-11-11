//go:build unix

package engine

import (
	"context"
	"os"
	"path/filepath"
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
	if len(cfg.Tasks) != 1 || cfg.Tasks[0].ID != "next" {
		t.Fatalf("queue after the start = %+v, want only the task that has not run", cfg.Tasks)
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
