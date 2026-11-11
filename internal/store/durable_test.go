package store

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/danielmaier42/claudeq/internal/task"
)

// blockHistory makes history.jsonl unwritable the way a broken disk does: the
// path is taken by a directory, so opening it for appending fails. The
// returned func lifts the block again.
func blockHistory(t *testing.T, s *Store) func() {
	t.Helper()
	path := s.path(historyFile)
	if err := os.RemoveAll(path); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(path, 0o755); err != nil {
		t.Fatal(err)
	}
	return func() {
		if err := os.Remove(path); err != nil {
			t.Fatal(err)
		}
	}
}

func finishedRun(id string, status RunStatus) Run {
	at := time.Date(2026, 11, 11, 11, 13, 2, 0, time.UTC)
	return Run{RunID: id, TaskID: "t-" + id, TaskName: id, StartedAt: at, FinishedAt: &at, Status: status}
}

func historyIDs(t *testing.T, s *Store) []string {
	t.Helper()
	runs, err := s.Runs()
	if err != nil {
		t.Fatalf("Runs: %v", err)
	}
	var ids []string
	for _, r := range runs {
		ids = append(ids, r.RunID+":"+string(r.Status))
	}
	return ids
}

// A write that fails once (a system briefly out of file handles) is tried
// again, and the record lands.
func TestAppendRunRetriesATransientFailure(t *testing.T) {
	s := openTemp(t)
	unblock := blockHistory(t, s)
	waits := 0
	s.sleep = func(time.Duration) {
		if waits++; waits == 1 {
			unblock()
		}
	}
	if err := s.AppendRun(finishedRun("r1", StatusFailed)); err != nil {
		t.Fatalf("AppendRun after a transient failure: %v", err)
	}
	if got := historyIDs(t, s); strings.Join(got, ",") != "r1:failed" {
		t.Fatalf("history = %v, want the record", got)
	}
}

// A record history keeps refusing is kept, written ahead of the next record
// once history takes writes again, and its fallback file is removed then.
func TestAppendRunKeepsARecordHistoryRefuses(t *testing.T) {
	s := openTemp(t)
	s.sleep = func(time.Duration) {}
	unblock := blockHistory(t, s)

	err := s.AppendRun(finishedRun("r1", StatusFailed))
	if err == nil {
		t.Fatal("AppendRun into a refused history reported success")
	}
	if !strings.Contains(err.Error(), "kept in") {
		t.Fatalf("error %q does not say the record was kept", err)
	}
	if _, err := os.Stat(s.pendingPath("r1")); err != nil {
		t.Fatalf("no fallback file for the refused record: %v", err)
	}

	unblock()
	if err := s.AppendRun(finishedRun("r2", StatusSuccess)); err != nil {
		t.Fatalf("AppendRun: %v", err)
	}
	if got := historyIDs(t, s); strings.Join(got, ",") != "r1:failed,r2:success" {
		t.Fatalf("history = %v, want the kept record ahead of the new one", got)
	}
	if _, err := os.Stat(s.pendingPath("r1")); !os.IsNotExist(err) {
		t.Fatalf("fallback file still there after history took the record: %v", err)
	}
}

// A record a daemon kept back and never got into history is written by the
// next start, once, and its fallback file goes.
func TestReplayPendingRunsWritesWhatAnEarlierProcessKept(t *testing.T) {
	s := openTemp(t)
	s.sleep = func(time.Duration) {}
	if err := s.AppendRun(finishedRun("r0", StatusRunning)); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(s.path(historyFile))
	if err != nil {
		t.Fatal(err)
	}
	unblock := blockHistory(t, s)
	if err := s.AppendRun(finishedRun("r0", StatusFailed)); err == nil {
		t.Fatal("AppendRun into a refused history reported success")
	}
	unblock()
	if err := os.WriteFile(s.path(historyFile), data, 0o644); err != nil {
		t.Fatal(err)
	}

	next, err := Open(s.home)
	if err != nil {
		t.Fatal(err)
	}
	if got := historyIDs(t, next); strings.Join(got, ",") != "r0:running" {
		t.Fatalf("history before the replay = %v", got)
	}
	n, err := next.ReplayPendingRuns()
	if err != nil || n != 1 {
		t.Fatalf("ReplayPendingRuns = %d, %v; want 1 record", n, err)
	}
	if got := historyIDs(t, next); strings.Join(got, ",") != "r0:failed" {
		t.Fatalf("history after the replay = %v, want the kept outcome", got)
	}
	if _, err := os.Stat(next.pendingPath("r0")); !os.IsNotExist(err) {
		t.Fatalf("fallback file still there after the replay: %v", err)
	}
	if n, err := next.ReplayPendingRuns(); err != nil || n != 0 {
		t.Fatalf("second ReplayPendingRuns = %d, %v; want nothing left", n, err)
	}
}

// A fallback file whose record history already holds is removed without a
// second copy of the record.
func TestReplayPendingRunsSkipsWhatHistoryHolds(t *testing.T) {
	s := openTemp(t)
	r := finishedRun("r1", StatusSuccess)
	if err := s.AppendRun(r); err != nil {
		t.Fatal(err)
	}
	s.mu.Lock()
	s.pending = []Run{r}
	if err := s.markLocked(); err != nil {
		t.Fatal(err)
	}
	s.pending = nil
	s.mu.Unlock()

	n, err := s.ReplayPendingRuns()
	if err != nil || n != 0 {
		t.Fatalf("ReplayPendingRuns = %d, %v; want nothing written", n, err)
	}
	data, _ := os.ReadFile(s.path(historyFile))
	if c := strings.Count(string(data), `"run_id":"r1"`); c != 1 {
		t.Fatalf("history holds the record %d times, want once", c)
	}
	if _, err := os.Stat(s.pendingPath("r1")); !os.IsNotExist(err) {
		t.Fatalf("fallback file not removed: %v", err)
	}
}

// Retiring a one-shot whose queue write fails once still takes it out of the
// queue: the removal is tried again, not given up after the mark is written.
func TestRetireTaskRetriesTheQueueRemoval(t *testing.T) {
	s := openTemp(t)
	if err := s.SaveConfig(Config{Tasks: []task.Task{sampleTask("once")}}); err != nil {
		t.Fatal(err)
	}
	cfgPath := s.path(configFile)
	aside := filepath.Join(s.home, "config.aside")
	if err := os.Rename(cfgPath, aside); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(cfgPath, 0o755); err != nil {
		t.Fatal(err)
	}
	waits := 0
	s.sleep = func(time.Duration) {
		if waits++; waits == 1 {
			if err := os.Remove(cfgPath); err != nil {
				t.Error(err)
			}
			if err := os.Rename(aside, cfgPath); err != nil {
				t.Error(err)
			}
		}
	}

	if err := s.RetireTask("once", true); err != nil {
		t.Fatalf("RetireTask: %v", err)
	}
	cfg, err := s.LoadConfig()
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.Tasks) != 0 {
		t.Fatalf("completed one-shot still queued: %+v", cfg.Tasks)
	}
	st, _ := s.LoadState()
	if !st.IsCompletedOnce("once") {
		t.Fatal("one-shot not marked completed")
	}
}

// The repair at start removes exactly the one-shots that have run and are
// still listed: not a recurring task, not one that never ran, not one whose
// session waits to continue.
func TestRepairCompletedOneShots(t *testing.T) {
	s := openTemp(t)
	done, fresh, waiting, cron := sampleTask("done"), sampleTask("fresh"), sampleTask("waiting"), sampleTask("cron")
	cron.Trigger, cron.Cron = task.TriggerCron, "0 3 * * *"
	if err := s.SaveConfig(Config{Tasks: []task.Task{done, fresh, waiting, cron}}); err != nil {
		t.Fatal(err)
	}
	if err := s.UpdateState(func(st *State) error {
		for _, id := range []string{"done", "waiting", "cron"} {
			st.MarkCompletedOnce(id)
		}
		st.SetPendingResume("waiting", "sess", "claude")
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	removed, err := s.RepairCompletedOneShots()
	if err != nil {
		t.Fatalf("RepairCompletedOneShots: %v", err)
	}
	if len(removed) != 1 || removed[0].ID != "done" {
		t.Fatalf("removed = %+v, want only the completed one-shot", removed)
	}
	cfg, _ := s.LoadConfig()
	var ids []string
	for _, tk := range cfg.Tasks {
		ids = append(ids, tk.ID)
	}
	if strings.Join(ids, ",") != "fresh,waiting,cron" {
		t.Fatalf("queue = %v", ids)
	}
}
