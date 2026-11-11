//go:build unix

package engine

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/danielmaier42/claudeq/internal/clock"
	"github.com/danielmaier42/claudeq/internal/executor"
	"github.com/danielmaier42/claudeq/internal/provider"
	"github.com/danielmaier42/claudeq/internal/store"
	"github.com/danielmaier42/claudeq/internal/task"
)

// fakeProcs is a process table the test controls.
type fakeProcs struct {
	mu      sync.Mutex
	starts  map[int]string
	checks  int
	killed  []int
	orphans []Orphan
}

func newFakeProcs() *fakeProcs { return &fakeProcs{starts: map[int]string{}} }

func (f *fakeProcs) StartTime(pid int) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.checks++
	s, ok := f.starts[pid]
	if !ok {
		return "", errors.New("no such process")
	}
	return s, nil
}

func (f *fakeProcs) Kill(pid int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.killed = append(f.killed, pid)
	delete(f.starts, pid)
}

func (f *fakeProcs) Orphans() ([]Orphan, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.orphans), nil
}

func (f *fakeProcs) spawn(pid int, start string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.starts[pid] = start
}

func (f *fakeProcs) exit(pid int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.starts, pid)
}

// startedStub reports a process id like the executor does, then blocks until
// released: a run whose daemon dies under it.
type startedStub struct {
	pid   int
	procs *fakeProcs
	block chan struct{}
}

func (s *startedStub) Run(ctx context.Context, req executor.Request) (provider.Result, error) {
	s.procs.spawn(s.pid, "Mon Nov  9 20:00:04 2026")
	req.Started(s.pid)
	select {
	case <-s.block:
	case <-ctx.Done():
	}
	return provider.Result{Status: store.StatusFailed}, nil
}

func latestRun(t *testing.T, st *store.Store, runID string) store.Run {
	t.Helper()
	runs, err := st.Runs()
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range runs {
		if r.RunID == runID {
			return r
		}
	}
	t.Fatalf("run %s not in history", runID)
	return store.Run{}
}

func fixedTask(id string) task.Task {
	tk := asapTask(id, true)
	tk.Trigger = task.TriggerFixed
	tk.FixedAt = time.Date(2026, 11, 9, 19, 0, 0, 0, time.UTC)
	return tk
}

// daemonDies starts a one-shot run on a first daemon whose process outlives
// it, and returns a second daemon on the same store, the way an update's
// kill-and-restart leaves things.
func daemonDies(t *testing.T) (*Engine, *store.Store, *fakeProcs, *stub) {
	t.Helper()
	fc := clock.NewFake(time.Date(2026, 11, 9, 20, 0, 0, 0, time.UTC))
	procs := newFakeProcs()
	first := &startedStub{pid: 41315, procs: procs, block: make(chan struct{})}
	old, st := newTestEngine(t, first, fc)
	// Registered after the store's temp dir, so it runs first: the dead
	// daemon's goroutine ends before the directory goes.
	t.Cleanup(func() { close(first.block); old.WaitIdle() })
	old.procs = procs
	saveTasks(t, st, fixedTask("relock"))
	if err := old.Tick(context.Background()); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool {
		s, _ := st.LoadState()
		return len(s.LiveRuns) == 1
	})

	// The daemon is gone, so its run's process now has PID 1 as parent.
	procs.orphans = []Orphan{{PID: 41315, Command: "/bin/claude -p --output-format stream-json"}}

	second := &stub{}
	e, _ := newTestEngine(t, second, fc)
	e.store = st
	e.procs = procs
	e.adoptPoll = time.Millisecond
	return e, st, procs, second
}

// The incident: the daemon was killed by an update while a run went on under
// PID 1. The next daemon wrote the run off and started its one-shot task again
// four seconds later. It must take the run back instead.
func TestRecoverTakesBackRunThatOutlivedTheDaemon(t *testing.T) {
	e, st, procs, second := daemonDies(t)
	// Its transcript exists but cannot say how the turn ended.
	e.SetSessionReader(func(rec store.Run) (provider.Result, bool) {
		return provider.Result{SessionID: rec.SessionID, SessionConfirmed: true}, false
	})

	adopted, interrupted, err := e.Recover()
	if err != nil {
		t.Fatal(err)
	}
	if len(adopted) != 1 || interrupted != 0 {
		t.Fatalf("adopted %d, interrupted %d; want 1 taken back, none interrupted", len(adopted), interrupted)
	}
	run := latestRun(t, st, "run-1")
	if run.Status != store.StatusRunning || run.PID != 41315 {
		t.Fatalf("run after recovery = %s pid %d, want running as 41315", run.Status, run.PID)
	}
	if got := e.ActiveTaskIDs(); !slices.Equal(got, []string{"relock"}) {
		t.Fatalf("active tasks = %v, want the taken-back task", got)
	}

	if err := e.Tick(context.Background()); err != nil {
		t.Fatal(err)
	}
	if n := len(second.requests()); n != 0 {
		t.Fatalf("task started %d more time(s) while its first run still goes on", n)
	}

	procs.exit(41315)
	e.WaitIdle()
	run = latestRun(t, st, "run-1")
	if run.Status != store.StatusFailed || !strings.HasPrefix(run.Error, adoptedEndMessage) || run.FinishedAt == nil || run.PID != 0 {
		t.Fatalf("finished run = %+v, want failed with the unknown-outcome reason and no PID", run)
	}
	assertNoSlots(t, e)
	if s, _ := st.LoadState(); len(s.LiveRuns) != 0 {
		t.Fatalf("live runs after the end = %v, want none", s.LiveRuns)
	}
	// Nothing says how it ended, so it counts as cut off: the one-shot
	// continues its session once rather than starting over.
	if err := e.Tick(context.Background()); err != nil {
		t.Fatal(err)
	}
	e.WaitIdle()
	if reqs := second.requests(); len(reqs) != 1 || !reqs[0].Resume || reqs[0].SessionID != "sess-1" {
		t.Fatalf("runs after the taken-back run ended = %+v, want one continuing sess-1", reqs)
	}
	if err := e.Tick(context.Background()); err != nil {
		t.Fatal(err)
	}
	e.WaitIdle()
	if n := len(second.requests()); n != 1 {
		t.Fatalf("one-shot task ran again after its continuation ended (%d runs)", n)
	}
}

func TestCancelStopsTakenBackRun(t *testing.T) {
	e, st, procs, _ := daemonDies(t)
	if _, _, err := e.Recover(); err != nil {
		t.Fatal(err)
	}
	if err := e.CancelRun("run-1"); err != nil {
		t.Fatal(err)
	}
	e.WaitIdle()
	if !slices.Equal(procs.killed, []int{41315}) {
		t.Fatalf("killed %v, want the taken-back process", procs.killed)
	}
	if run := latestRun(t, st, "run-1"); run.Status != store.StatusCanceled {
		t.Fatalf("status = %s, want canceled", run.Status)
	}
	assertNoSlots(t, e)
}

// A process that ended between two polls may have handed its PID on; a cancel
// must not kill whatever runs under that number now.
func TestCancelDoesNotKillReusedPID(t *testing.T) {
	e, st, procs, _ := daemonDies(t)
	e.adoptPoll = time.Hour
	procs.mu.Lock()
	before := procs.checks
	procs.mu.Unlock()
	if _, _, err := e.Recover(); err != nil {
		t.Fatal(err)
	}
	// Recover checks the process once; the watcher's first check is the
	// next. After it the watcher waits for the next poll, an hour away.
	waitFor(t, func() bool {
		procs.mu.Lock()
		defer procs.mu.Unlock()
		return procs.checks >= before+2
	})
	procs.spawn(41315, "Tue Nov 10 08:00:00 2026")
	if err := e.CancelRun("run-1"); err != nil {
		t.Fatal(err)
	}
	e.WaitIdle()
	if len(procs.killed) != 0 {
		t.Fatalf("killed %v, a process that only reuses the PID", procs.killed)
	}
	if run := latestRun(t, st, "run-1"); run.Status != store.StatusCanceled {
		t.Fatalf("status = %s, want canceled", run.Status)
	}
}

// A run whose starter is still alive (a `dispatchq run-now`) records its own
// end; a restarting daemon neither takes it back nor writes it off.
func TestRecoverLeavesRunOfLiveOwnerAlone(t *testing.T) {
	e, st, _, _ := daemonDies(t)
	e.procs.(*fakeProcs).orphans = nil
	adopted, interrupted, err := e.Recover()
	if err != nil {
		t.Fatal(err)
	}
	if len(adopted) != 0 || interrupted != 0 {
		t.Fatalf("adopted %d, interrupted %d; want the owned run left alone", len(adopted), interrupted)
	}
	if run := latestRun(t, st, "run-1"); run.Status != store.StatusRunning {
		t.Fatalf("status = %s, want still running", run.Status)
	}
	if s, _ := st.LoadState(); len(s.LiveRuns) != 1 {
		t.Fatalf("live runs = %d, want kept for its owner", len(s.LiveRuns))
	}
}

func assertNoSlots(t *testing.T, e *Engine) {
	t.Helper()
	e.mu.Lock()
	defer e.mu.Unlock()
	if len(e.active) != 0 || e.parallelActive != 0 || e.nonParallelActive != 0 || len(e.runningOn) != 0 || len(e.cancels) != 0 {
		t.Fatalf("slots left: active %v parallel %d nonparallel %d on %v cancels %d",
			e.active, e.parallelActive, e.nonParallelActive, e.runningOn, len(e.cancels))
	}
}

func TestShutdownLetsGoOfTakenBackRun(t *testing.T) {
	e, st, procs, _ := daemonDies(t)
	if _, _, err := e.Recover(); err != nil {
		t.Fatal(err)
	}
	e.drain()
	if len(procs.killed) != 0 {
		t.Fatalf("shutdown killed %v; a taken-back run is not the daemon's to stop", procs.killed)
	}
	if run := latestRun(t, st, "run-1"); run.Status != store.StatusRunning {
		t.Fatalf("status = %s, want still running for the next daemon", run.Status)
	}
	if s, _ := st.LoadState(); len(s.LiveRuns) != 1 {
		t.Fatalf("live runs = %d, want the run kept for the next daemon", len(s.LiveRuns))
	}
	assertNoSlots(t, e)
}

func TestRecoverMarksGoneRunInterrupted(t *testing.T) {
	e, st, procs, _ := daemonDies(t)
	procs.exit(41315)
	adopted, interrupted, err := e.Recover()
	if err != nil {
		t.Fatal(err)
	}
	if len(adopted) != 0 || interrupted != 1 {
		t.Fatalf("adopted %d, interrupted %d; want 0 and 1", len(adopted), interrupted)
	}
	if run := latestRun(t, st, "run-1"); run.Status != store.StatusFailed || !strings.HasPrefix(run.Error, store.InterruptedError) {
		t.Fatalf("run = %+v, want interrupted", run)
	}
	if s, _ := st.LoadState(); len(s.LiveRuns) != 0 {
		t.Fatalf("live runs = %v, want the gone run forgotten", s.LiveRuns)
	}
}

// A PID reused by another process is not the run.
func TestRecoverIgnoresReusedPID(t *testing.T) {
	e, _, procs, _ := daemonDies(t)
	procs.spawn(41315, "Tue Nov 10 08:00:00 2026")
	adopted, interrupted, err := e.Recover()
	if err != nil {
		t.Fatal(err)
	}
	if len(adopted) != 0 || interrupted != 1 {
		t.Fatalf("adopted %d, interrupted %d; want the reused PID ignored", len(adopted), interrupted)
	}
}

// A daemon that did not record processes yet (v2.1.1, the incident) wrote the
// run off; the run is found again by its session on an orphaned headless
// harness, but not on an interactive takeover of the same session.
func TestRecoverFindsRunWrittenOffByOlderDaemon(t *testing.T) {
	fc := clock.NewFake(time.Date(2026, 11, 9, 21, 0, 0, 0, time.UTC))
	r := &stub{}
	e, st := newTestEngine(t, r, fc)
	procs := newFakeProcs()
	e.procs = procs
	e.adoptPoll = time.Millisecond
	tk := fixedTask("relock")
	saveTasks(t, st, tk)
	finished := fc.Now().Add(-4 * time.Minute)
	for _, run := range []store.Run{
		{RunID: "orphan", TaskID: tk.ID, TaskName: tk.Name, SessionID: "f2fae403", Task: &tk,
			StartedAt: finished.Add(-time.Hour), Status: store.StatusRunning},
		{RunID: "orphan", TaskID: tk.ID, TaskName: tk.Name, SessionID: "f2fae403", Task: &tk,
			StartedAt: finished.Add(-time.Hour), Status: store.StatusFailed, FinishedAt: &finished,
			Error: store.InterruptedError},
		{RunID: "rerun", TaskID: tk.ID, TaskName: tk.Name, SessionID: "b0b0", Task: &tk,
			StartedAt: finished, Status: store.StatusRunning},
		{RunID: "takenover", TaskID: "other", SessionID: "aaaa", Status: store.StatusFailed,
			StartedAt: finished.Add(-time.Hour), FinishedAt: &finished, Error: store.InterruptedError},
	} {
		if err := st.AppendRun(run); err != nil {
			t.Fatal(err)
		}
	}
	procs.spawn(41315, "Mon Nov  9 20:00:04 2026")
	procs.spawn(500, "Mon Nov  9 20:30:00 2026")
	procs.spawn(600, "Mon Nov  9 20:56:45 2026")
	procs.orphans = []Orphan{
		{PID: 600, Command: "/bin/claude -p --output-format stream-json --session-id b0b0"},
		{PID: 500, Command: "/bin/claude --resume aaaa"},
		{PID: 41315, Command: "/bin/claude -p --output-format stream-json --verbose --session-id f2fae403 --append-system-prompt x"},
	}

	adopted, _, err := e.Recover()
	if err != nil {
		t.Fatal(err)
	}
	// The older daemon wrote "orphan" off and started "rerun" of the same
	// task; both still run. The task has one slot, so the run still recorded
	// as running is taken back and the other is left written off.
	if len(adopted) != 1 || adopted[0].RunID != "rerun" || adopted[0].PID != 600 {
		t.Fatalf("adopted %+v, want only the run still recorded as running", adopted)
	}
	if run := latestRun(t, st, "orphan"); run.Status != store.StatusFailed {
		t.Fatalf("written-off run = %+v, want left as it was", run)
	}
	if err := e.Tick(context.Background()); err != nil {
		t.Fatal(err)
	}
	if n := len(r.requests()); n != 0 {
		t.Fatalf("task started again (%d) while its run still goes on", n)
	}
	procs.exit(600)
	e.WaitIdle()

	// Written off alone, the run is found by its session.
	e2, _ := newTestEngine(t, r, fc)
	e2.store, e2.procs, e2.adoptPoll = st, procs, time.Millisecond
	procs.orphans = procs.orphans[1:]
	adopted, _, err = e2.Recover()
	if err != nil {
		t.Fatal(err)
	}
	e = e2
	if len(adopted) != 1 || adopted[0].RunID != "orphan" || adopted[0].PID != 41315 {
		t.Fatalf("adopted %+v, want only the orphaned headless run", adopted)
	}
	if run := latestRun(t, st, "orphan"); run.Status != store.StatusRunning || run.FinishedAt != nil {
		t.Fatalf("run = %+v, want running again", run)
	}
	if err := e.Tick(context.Background()); err != nil {
		t.Fatal(err)
	}
	if n := len(r.requests()); n != 0 {
		t.Fatalf("task started again (%d) while its written-off run still goes on", n)
	}
	procs.exit(41315)
	e.WaitIdle()
	if run := latestRun(t, st, "orphan"); run.Status != store.StatusFailed || run.Error != adoptedEndMessage {
		t.Fatalf("run = %+v, want ended with the unknown-outcome reason", run)
	}
}
