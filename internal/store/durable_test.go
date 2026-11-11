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

// The repair at start removes exactly the one-shots whose removal a retire
// still owes: not one queued again by hand under the id of a task that ran,
// not one whose session waits to continue, not one with a run in flight.
func TestRepairCompletedOneShots(t *testing.T) {
	s := openTemp(t)
	ids := []string{"done", "fresh", "waiting", "readded", "running"}
	var tasks []task.Task
	for _, id := range ids {
		tasks = append(tasks, sampleTask(id))
	}
	if err := s.SaveConfig(Config{Tasks: tasks}); err != nil {
		t.Fatal(err)
	}
	if err := s.UpdateState(func(st *State) error {
		for _, id := range []string{"done", "waiting", "readded", "running"} {
			st.MarkCompletedOnce(id)
		}
		for _, id := range []string{"done", "waiting", "running"} {
			st.OweRemoval(id)
		}
		st.OweRemoval("gone") // its task already left the queue
		st.SetPendingResume("waiting", "sess", "claude")
		st.SetLiveRun(LiveRun{Run: Run{RunID: "r-running", TaskID: "running"}, PID: 4242, ProcessStart: "x"})
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	removed, err := s.RepairCompletedOneShots()
	if err != nil {
		t.Fatalf("RepairCompletedOneShots: %v", err)
	}
	var gone []string
	for _, tk := range removed {
		gone = append(gone, tk.ID)
	}
	if strings.Join(gone, ",") != "done" {
		t.Fatalf("removed = %v, want only the one-shot whose removal was owed", gone)
	}
	cfg, _ := s.LoadConfig()
	var queued []string
	for _, tk := range cfg.Tasks {
		queued = append(queued, tk.ID)
	}
	if strings.Join(queued, ",") != "fresh,waiting,readded,running" {
		t.Fatalf("queue = %v", queued)
	}
	st, _ := s.LoadState()
	for id, want := range map[string]bool{"done": false, "gone": false, "waiting": true, "running": true} {
		if st.IsRemovalOwed(id) != want {
			t.Fatalf("removal owed for %s = %v, want %v", id, !want, want)
		}
	}
}

// A retire whose removal went through owes nothing; one whose removal failed
// leaves that owed for the repair.
func TestRetireTaskRecordsAnOwedRemoval(t *testing.T) {
	s := openTemp(t)
	s.sleep = func(time.Duration) {}
	if err := s.SaveConfig(Config{Tasks: []task.Task{sampleTask("ok"), sampleTask("stuck")}}); err != nil {
		t.Fatal(err)
	}
	if err := s.RetireTask("ok", true); err != nil {
		t.Fatal(err)
	}
	if st, _ := s.LoadState(); st.IsRemovalOwed("ok") {
		t.Fatal("a completed removal is still owed")
	}

	cfgPath := s.path(configFile)
	aside := filepath.Join(s.home, "config.aside")
	if err := os.Rename(cfgPath, aside); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(cfgPath, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := s.RetireTask("stuck", true); err == nil {
		t.Fatal("RetireTask into an unreadable queue reported success")
	}
	if st, _ := s.LoadState(); !st.IsRemovalOwed("stuck") || !st.IsCompletedOnce("stuck") {
		t.Fatal("the failed removal is not recorded as owed")
	}
}

// The review finding: a limit on the continuation replaced its mark, so the
// "once" could repeat. A pause keeps the record that the one more go is spent,
// whatever session id the harness reports, but is an ordinary limit resume.
func TestALimitKeepsTheSpentContinuation(t *testing.T) {
	st := newState()
	st.SetInterruptedResume("a", "sess", "claude", "run-1")
	st.SetPendingResume("a", "sess-forked", "claude")
	p, _ := st.PendingResume("a")
	if p.InterruptedRun != "run-1" || p.AfterInterrupt || p.SessionID != "sess-forked" {
		t.Fatalf("pending after a limit = %+v, want a limit resume that keeps run-1 as spent", p)
	}
}
