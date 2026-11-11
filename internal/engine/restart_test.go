//go:build unix

package engine

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/danielmaier42/claudeq/internal/clock"
	"github.com/danielmaier42/claudeq/internal/provider"
	"github.com/danielmaier42/claudeq/internal/store"
	"github.com/danielmaier42/claudeq/internal/task"
)

// diedUnder starts tk on a first daemon whose run's process then ends while
// no daemon watches it, and returns a second daemon on the same store, the way
// a crash or an update's kill leaves things when the run did not survive.
func diedUnder(t *testing.T, tk task.Task) (*Engine, *store.Store, *stub) {
	t.Helper()
	fc := clock.NewFake(time.Date(2026, 11, 9, 9, 31, 27, 0, time.UTC))
	procs := newFakeProcs()
	first := &startedStub{pid: 50210, procs: procs, block: make(chan struct{})}
	old, st := newTestEngine(t, first, fc)
	t.Cleanup(func() { close(first.block); old.WaitIdle() })
	old.procs = procs
	saveTasks(t, st, tk)
	if err := old.Tick(context.Background()); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool {
		s, _ := st.LoadState()
		return len(s.LiveRuns) == 1
	})
	procs.exit(50210)

	second := &stub{}
	e, _ := newTestEngine(t, second, fc)
	e.store = st
	e.procs = procs
	return e, st, second
}

// The incident: nine runs cut off by a daemon restart in five days, written
// off at the next start with nobody told. The write-off notifies like any
// failed run.
func TestRecoverNotifiesARunCutOffByARestart(t *testing.T) {
	e, _, _ := diedUnder(t, asapTask("feature-watch", false))
	n := &capturingNotifier{}
	e.SetNotifier(n)

	if _, _, err := e.Recover(); err != nil {
		t.Fatal(err)
	}
	msgs := n.all()
	if len(msgs) != 1 {
		t.Fatalf("notifications = %+v, want one about the cut-off run", msgs)
	}
	if msgs[0].Kind != store.InboxKindFailure || msgs[0].RunID != "run-1" ||
		!strings.Contains(msgs[0].Message, store.InterruptedError) {
		t.Fatalf("notification = %+v, want a failure naming the interruption", msgs[0])
	}
}

// The incident: a quiet-history run cut off by a restart left an empty log and
// nothing in history. It is recorded as interrupted, so it can be seen.
func TestRecoverRecordsAQuietRunCutOffByARestart(t *testing.T) {
	tk := asapTask("watch", false)
	tk.QuietHistory = true
	e, st, _ := diedUnder(t, tk)
	if runs, _ := st.Runs(); len(runs) != 0 {
		t.Fatalf("history before the restart = %+v, want nothing (quiet task)", runs)
	}

	if _, interrupted, err := e.Recover(); err != nil || interrupted != 1 {
		t.Fatalf("Recover: interrupted %d, %v; want the quiet run written off", interrupted, err)
	}
	run := latestRun(t, st, "run-1")
	if run.Status != store.StatusFailed || !strings.HasPrefix(run.Error, store.InterruptedError) || run.FinishedAt == nil {
		t.Fatalf("quiet run = %+v, want recorded as interrupted", run)
	}
}

// The incident: a one-shot cut off by a restart either started over from
// scratch or was retired with its work half done. It continues its own
// session instead, once; cut off a second time, it is retired.
func TestRecoverContinuesAnInterruptedOneShotOnce(t *testing.T) {
	e, st, second := diedUnder(t, asapTask("relock", false))
	if _, _, err := e.Recover(); err != nil {
		t.Fatal(err)
	}
	if run := latestRun(t, st, "run-1"); !strings.Contains(run.Error, resumeAfterInterruptNote) {
		t.Fatalf("run reason = %q, want it to say the session continues", run.Error)
	}
	if err := e.Tick(context.Background()); err != nil {
		t.Fatal(err)
	}
	e.WaitIdle()
	reqs := second.requests()
	if len(reqs) != 1 || !reqs[0].Resume || reqs[0].SessionID != "sess-1" {
		t.Fatalf("runs after the restart = %+v, want one that continues session sess-1", reqs)
	}
	if reqs[0].Task.Prompt != interruptedContinuationPrompt {
		t.Fatalf("continuation prompt = %q, want the one asking for what is missing, not the whole task again", reqs[0].Task.Prompt)
	}

	// The continuation is cut off as well: the task is given up on.
	if err := st.UpdateState(func(s *store.State) error {
		s.SetInterruptedResume("relock", "sess-1", "claude", "run-0")
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	e.conclude(asapTask("relock", false), "claude", latestRun(t, st, "run-1"),
		provider.Result{Status: store.StatusFailed, SessionID: "sess-1", Message: store.InterruptedError},
		nil, outcome{interrupted: true})
	s, _ := st.LoadState()
	if _, waiting := s.PendingResume("relock"); waiting || !s.IsCompletedOnce("relock") {
		t.Fatal("a second interruption should retire the one-shot, not plan a third go")
	}
}

// The incident: the shutdown grace ran out on a Codex one-shot, its process
// was terminated, and the task was retired with the work unfinished. It
// continues its session at the next start instead.
func TestShutdownCutOffOneShotContinuesItsSession(t *testing.T) {
	fc := clock.NewFake(time.Date(2026, 11, 9, 9, 31, 27, 0, time.UTC))
	r := &ctxStub{}
	e, st := newTestEngine(t, r, fc)
	saveTasks(t, st, asapTask("q-e506ec", false))
	if err := e.Tick(context.Background()); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return r.activeStarted() == 1 })
	e.runCancel() // what drain does once ShutdownGrace has passed
	e.WaitIdle()

	run := latestRun(t, st, "run-1")
	if run.Status != store.StatusFailed || !strings.Contains(run.Error, resumeAfterInterruptNote) {
		t.Fatalf("run = %s %q, want failed with the session to continue", run.Status, run.Error)
	}
	cfg, _ := st.LoadConfig()
	if len(cfg.Tasks) != 1 {
		t.Fatal("the cut-off one-shot left the queue; its work is lost")
	}
	s, _ := st.LoadState()
	if p, ok := s.PendingResume("q-e506ec"); !ok || p.SessionID != "sess-1" || !p.AfterInterrupt {
		t.Fatalf("pending resume = %+v, %v; want session sess-1 after an interruption", p, ok)
	}
}

// A run stopped by the user during a shutdown is canceled, not continued.
func TestShutdownDoesNotContinueACanceledRun(t *testing.T) {
	fc := clock.NewFake(time.Date(2026, 11, 9, 9, 31, 27, 0, time.UTC))
	r := &ctxStub{}
	e, st := newTestEngine(t, r, fc)
	saveTasks(t, st, asapTask("a", false))
	if err := e.Tick(context.Background()); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return r.activeStarted() == 1 })
	if err := e.CancelRun("run-1"); err != nil {
		t.Fatal(err)
	}
	e.runCancel()
	e.WaitIdle()
	if run := latestRun(t, st, "run-1"); run.Status != store.StatusCanceled {
		t.Fatalf("status = %s, want canceled", run.Status)
	}
	if s, _ := st.LoadState(); len(s.PendingResumes) != 0 {
		t.Fatalf("pending resumes = %v, want none for a canceled run", s.PendingResumes)
	}
}

// The incident: a run taken back after a restart always ended failed with an
// unknown outcome, although its session had finished the work. The harness's
// transcript says how it ended, and that is what is recorded.
func TestTakenBackRunRecordsTheSessionsRealOutcome(t *testing.T) {
	e, st, procs, _ := daemonDies(t)
	var asked store.Run
	e.SetSessionReader(func(rec store.Run) (provider.Result, bool) {
		asked = rec
		return provider.Result{Status: store.StatusSuccess, SessionID: rec.SessionID, FinalOutput: "relocked both repos"}, true
	})
	if _, _, err := e.Recover(); err != nil {
		t.Fatal(err)
	}
	procs.exit(41315)
	e.WaitIdle()

	run := latestRun(t, st, "run-1")
	if run.Status != store.StatusSuccess || run.Error != "" || run.FinalOutput != "relocked both repos" {
		t.Fatalf("finished run = %s %q %q, want the session's success", run.Status, run.Error, run.FinalOutput)
	}
	if asked.SessionID != "sess-1" {
		t.Fatalf("transcript read for session %q, want sess-1", asked.SessionID)
	}
}

// A run whose process ended while no daemon watched it, and whose session had
// finished, is recorded as the success it was and does not run again.
func TestRecoverRecordsTheRealOutcomeOfARunThatEndedUnwatched(t *testing.T) {
	e, st, second := diedUnder(t, asapTask("relock", false))
	ended := time.Date(2026, 11, 9, 9, 40, 0, 0, time.UTC)
	e.SetSessionReader(func(rec store.Run) (provider.Result, bool) {
		return provider.Result{Status: store.StatusSuccess, SessionID: rec.SessionID, FinalOutput: "done", EndedAt: ended}, true
	})
	e.clock.(*clock.Fake).Advance(6 * time.Hour) // the restart comes much later
	if _, _, err := e.Recover(); err != nil {
		t.Fatal(err)
	}
	if run := latestRun(t, st, "run-1"); run.Status != store.StatusSuccess || run.FinishedAt == nil || !run.FinishedAt.Equal(ended) {
		t.Fatalf("run = %s finished %v, want success at the transcript's end %v", run.Status, run.FinishedAt, ended)
	}
	if err := e.Tick(context.Background()); err != nil {
		t.Fatal(err)
	}
	e.WaitIdle()
	if n := len(second.requests()); n != 0 {
		t.Fatalf("the finished one-shot ran again (%d runs)", n)
	}
}

// The review finding: a daemon that died after planning the continuation but
// before recording the run met the same run again at the next start, and
// counted it as a second interruption.
func TestTheSameInterruptedRunIsNotCountedTwice(t *testing.T) {
	e, st, _ := diedUnder(t, asapTask("relock", false))
	if _, _, err := e.Recover(); err != nil {
		t.Fatal(err)
	}
	run := latestRun(t, st, "run-1")
	run.Status, run.Error, run.FinishedAt = store.StatusRunning, "", nil
	e.conclude(asapTask("relock", false), "claude", run,
		provider.Result{Status: store.StatusFailed, SessionID: "sess-1", Message: store.InterruptedError},
		nil, outcome{interrupted: true})
	s, _ := st.LoadState()
	if p, ok := s.PendingResume("relock"); !ok || !p.AfterInterrupt || s.IsCompletedOnce("relock") {
		t.Fatalf("pending = %+v, %v; the same run met twice retired the task", p, ok)
	}
}

// The review finding: a task taken out of the queue while its run was cut off
// had a continuation planned for it anyway.
func TestNoContinuationForATaskNoLongerQueued(t *testing.T) {
	e, st, _ := diedUnder(t, asapTask("relock", false))
	saveTasks(t, st) // the task was deleted meanwhile
	if _, _, err := e.Recover(); err != nil {
		t.Fatal(err)
	}
	s, _ := st.LoadState()
	if _, ok := s.PendingResume("relock"); ok {
		t.Fatal("a continuation was planned for a task that is no longer queued")
	}
}

// The review finding: a run that completed and was retired, but whose removal
// failed and whose end was never recorded before the daemon died, was met at
// the next start as cut off and its finished one-shot continued again.
func TestNoContinuationForAOneShotAlreadyCompleted(t *testing.T) {
	e, st, second := diedUnder(t, asapTask("relock", false))
	if err := st.UpdateState(func(s *store.State) error {
		s.MarkCompletedOnce("relock")
		s.OweRemoval("relock")
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := e.Recover(); err != nil {
		t.Fatal(err)
	}
	if err := e.Tick(context.Background()); err != nil {
		t.Fatal(err)
	}
	e.WaitIdle()
	if n := len(second.requests()); n != 0 {
		t.Fatalf("the completed one-shot ran again (%d runs)", n)
	}
	if cfg, _ := st.LoadConfig(); len(cfg.Tasks) != 0 {
		t.Fatal("the completed one-shot is still queued")
	}
}
