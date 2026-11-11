package engine

import (
	"context"
	"fmt"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/danielmaier42/claudeq/internal/provider"
	"github.com/danielmaier42/claudeq/internal/store"
	"github.com/danielmaier42/claudeq/internal/task"
)

// A run's process lives in its own process group (see procown), so it is not
// stopped with the daemon that started it when that daemon dies without
// draining: an update that kills the old daemon, a crash, a SIGKILL. The run
// then goes on under PID 1. A daemon that wrote such a run off as interrupted
// would hide work that is still going and start its task a second time.
// Instead the next daemon takes the run back: it shows as running, keeps its
// task from starting again, can be stopped, and is finished when its process
// ends.

// adoptPoll is how often a taken-back run's process is checked. It is not the
// daemon's child, so there is no Wait to block on.
const adoptPoll = 5 * time.Second

// legacyAdoptWindow bounds how far back a run already written off as
// interrupted is looked at, for a run that a daemon without this recovery
// wrote off while its process went on.
const legacyAdoptWindow = 7 * 24 * time.Hour

// adoptedEndMessage is the reason a taken-back run ends with when its process
// ends on its own and its harness's transcript cannot tell how (see
// sessionOutcome). Its exit status went to the daemon that started it, and its
// output pipe closed with that daemon, so neither can be read any more.
const adoptedEndMessage = "the run's process ended after DispatchQ restarted and took it back; " +
	"its exit status and any output written after the restart could not be captured " +
	"(see the session for its result)"

// Orphan is a process whose parent has gone, as far as recovery needs to know it.
type Orphan struct {
	PID     int
	Command string
}

// processes is what recovery needs from the operating system.
type processes interface {
	// StartTime identifies one incarnation of pid; an error means it is gone.
	StartTime(pid int) (string, error)
	// Kill stops pid and every process descended from it.
	Kill(pid int)
	// Orphans lists the processes reparented to PID 1.
	Orphans() ([]Orphan, error)
}

// recordLive writes down the process of a run that has just started, so a
// daemon that did not start it can find it again.
func (e *Engine) recordLive(t task.Task, rec store.Run, pid int) {
	if !recoverRuns {
		return
	}
	start, err := e.procs.StartTime(pid)
	if err != nil {
		fmt.Fprintf(os.Stderr, "dispatchqd: run %s: read start of process %d: %v\n", rec.RunID, pid, err)
		return
	}
	rec.PID = pid
	if err := e.store.UpdateState(func(st *store.State) error {
		st.SetLiveRun(store.LiveRun{Run: rec, PID: pid, ProcessStart: start})
		return nil
	}); err != nil {
		fmt.Fprintf(os.Stderr, "dispatchqd: run %s: record process: %v\n", rec.RunID, err)
	}
	if !t.QuietHistory {
		if err := e.store.AppendRun(rec); err != nil {
			fmt.Fprintf(os.Stderr, "dispatchqd: run %s: record process: %v\n", rec.RunID, err)
		}
	}
}

// clearLive forgets the process of a run that has ended.
func (e *Engine) clearLive(runID string) {
	if err := e.store.UpdateState(func(st *store.State) error {
		st.ClearLiveRun(runID)
		return nil
	}); err != nil {
		fmt.Fprintf(os.Stderr, "dispatchqd: run %s: forget process: %v\n", runID, err)
	}
}

// alive reports whether pid is still the process that was recorded.
func (e *Engine) alive(pid int, start string) bool {
	got, err := e.procs.StartTime(pid)
	return err == nil && got == start
}

// Recover runs once at startup, before the first tick. It takes back every run
// whose process outlived the previous daemon, and records the end of every
// other run that was in flight: still recorded as running, or a quiet-history
// run known only by its process record. Such a run is recorded with the
// outcome its harness's transcript shows, or as interrupted, and notified like
// any run that ends (see settleEnded). It returns the runs it took back and how
// many ended runs it recorded.
//
// A run is found in two ways. The usual one is the process recorded when it
// started (State.LiveRuns), checked against its start time so a reused PID is
// never mistaken for it. The other covers a run started by a daemon that did
// not record its process yet: a run still recorded as running, or one such a
// daemon already wrote off as interrupted, is matched to an orphaned headless
// harness process that carries its session id on its command line.
//
// Before that it puts right what an earlier daemon could not finish writing:
// run records history did not take are written now, and one-shot tasks marked
// done but left in the queue are taken out of it. Both are logged.
func (e *Engine) Recover() (adopted []store.Run, interrupted int, err error) {
	e.repairRecords()
	st, err := e.store.LoadState()
	if err != nil {
		return nil, 0, fmt.Errorf("load state: %w", err)
	}
	runs, err := e.store.Runs()
	if err != nil {
		return nil, 0, fmt.Errorf("read history: %w", err)
	}
	now := e.clock.Now()

	type found struct {
		rec   store.Run
		pid   int
		start string
	}
	orphans, err := e.procs.Orphans()
	if err != nil {
		fmt.Fprintf(os.Stderr, "dispatchqd: list orphaned processes: %v\n", err)
	}
	orphaned := map[int]bool{}
	for _, o := range orphans {
		orphaned[o.PID] = true
	}

	var take []found
	keep := map[string]bool{}
	tasks := map[string]bool{} // one run per task: the task's active slot is a set
	var gone []string
	goneRuns := map[string]store.Run{}
	for id, l := range st.LiveRuns {
		switch {
		case !e.alive(l.PID, l.ProcessStart):
			gone = append(gone, id)
			goneRuns[id] = l.Run
		case !orphaned[l.PID] || tasks[l.Run.TaskID]:
			// Still owned by the process that started it (a `dispatchq
			// run-now` that records its own end), so neither taken back nor
			// written off.
			keep[id] = true
		default:
			rec := l.Run
			rec.PID = l.PID
			take = append(take, found{rec, l.PID, l.ProcessStart})
			keep[id] = true
			tasks[rec.TaskID] = true
		}
	}

	// A run still recorded as running is preferred to one already written
	// off: the older daemon wrote off one run of a task and started another.
	legacy := slices.Clone(runs)
	slices.SortStableFunc(legacy, func(a, b store.Run) int {
		return b2i(a.Status != store.StatusRunning) - b2i(b.Status != store.StatusRunning)
	})
	for _, r := range legacy {
		if keep[r.RunID] || tasks[r.TaskID] || !legacyCandidate(r, now) {
			continue
		}
		o, ok := orphanFor(orphans, r.SessionID)
		if !ok {
			continue
		}
		start, err := e.procs.StartTime(o.PID)
		if err != nil {
			continue
		}
		rec := r
		rec.PID = o.PID
		take = append(take, found{rec, o.PID, start})
		keep[r.RunID] = true
		tasks[r.TaskID] = true
	}

	if len(gone) > 0 {
		if err := e.store.UpdateState(func(st *store.State) error {
			for _, id := range gone {
				st.ClearLiveRun(id)
			}
			return nil
		}); err != nil {
			return nil, 0, fmt.Errorf("forget ended runs: %w", err)
		}
	}
	ended := endedRuns(runs, goneRuns, keep)
	latest := latestStarts(runs, ended)
	for _, rec := range ended {
		// Only a task's latest run speaks for its schedule. An older one, or
		// one whose task has a run going on that is being taken back, is
		// recorded and notified, but must not retire the task or plan a resume
		// under the run that came after it.
		current := !tasks[rec.TaskID] && !rec.StartedAt.Before(latest[rec.TaskID])
		e.settleEnded(rec, current)
		interrupted++
	}
	for _, f := range take {
		rec, err := e.adopt(f.rec, f.pid, f.start)
		if err != nil {
			return adopted, interrupted, err
		}
		adopted = append(adopted, rec)
	}
	return adopted, interrupted, nil
}

// endedRuns are the runs that ended while no daemon watched them: every run
// still recorded as running whose process is not being taken back (keep), and
// every run whose recorded process is gone. A quiet-history task's run has no
// history entry until it ends, so its process record is all there is of it.
// A run history already has an outcome for is left alone.
func endedRuns(history []store.Run, gone map[string]store.Run, keep map[string]bool) []store.Run {
	var out []store.Run
	seen := map[string]bool{}
	for _, r := range history {
		seen[r.RunID] = true
		if r.Status == store.StatusRunning && !keep[r.RunID] {
			out = append(out, r)
		}
	}
	ids := make([]string, 0, len(gone))
	for id := range gone {
		if !seen[id] && !keep[id] {
			ids = append(ids, id)
		}
	}
	slices.Sort(ids)
	for _, id := range ids {
		out = append(out, gone[id])
	}
	return out
}

// latestStarts is the start of each task's latest run.
func latestStarts(history, ended []store.Run) map[string]time.Time {
	out := map[string]time.Time{}
	for _, list := range [][]store.Run{history, ended} {
		for _, r := range list {
			if r.StartedAt.After(out[r.TaskID]) {
				out[r.TaskID] = r.StartedAt
			}
		}
	}
	return out
}

// settleEnded records the outcome of a run that ended while no daemon watched
// it. Its harness's transcript says how it ended when it can (see
// sessionOutcome); otherwise it is written off as interrupted. Either way it
// goes through the same recording as any run's end: history, the task's
// scheduling state, the notification.
//
// current says whether the run is its task's latest, the one whose end the
// task's schedule follows; for any other only the record is written.
func (e *Engine) settleEnded(rec store.Run, current bool) {
	t := task.Task{ID: rec.TaskID, Name: rec.TaskName}
	if rec.Task != nil {
		t = *rec.Task
	}
	rec.PID = 0
	how := outcome{keepSchedule: !current}
	res, ok := e.sessionOutcome(rec)
	if !ok {
		res = provider.Result{Status: store.StatusFailed, SessionID: rec.SessionID, Message: store.InterruptedError}
		how.interrupted = true
	}
	fmt.Fprintf(os.Stdout, "dispatchqd: run %s (%s) ended while no daemon watched it: %s\n",
		rec.RunID, rec.TaskName, endedText(res))
	e.conclude(t, rec.Provider.ID, rec, res, nil, how)
}

// endedText is how the daemon log names the outcome of a run that ended
// unwatched.
func endedText(res provider.Result) string {
	if res.Message != "" {
		return string(res.Status) + " (" + res.Message + ")"
	}
	return string(res.Status) + " (read from its session)"
}

// sessionOutcome reads a run's real outcome from its harness's transcript,
// for a run whose output DispatchQ could not read to its end. ok is false
// when there is no reader or the transcript cannot tell.
func (e *Engine) sessionOutcome(rec store.Run) (provider.Result, bool) {
	if e.readSession == nil {
		return provider.Result{}, false
	}
	return e.readSession(rec)
}

// repairRecords writes the run records an earlier process kept back because
// history did not take them, and removes the one-shot tasks a failed queue
// write left listed after they had run. Neither failing stops the start.
func (e *Engine) repairRecords() {
	if n, err := e.store.ReplayPendingRuns(); err != nil {
		fmt.Fprintf(os.Stderr, "dispatchqd: write kept run records to history: %v\n", err)
	} else if n > 0 {
		fmt.Fprintf(os.Stdout, "dispatchqd: wrote %d run record(s) to history that an earlier write could not\n", n)
	}
	removed, err := e.store.RepairCompletedOneShots()
	if err != nil {
		fmt.Fprintf(os.Stderr, "dispatchqd: %v\n", err)
	}
	for _, t := range removed {
		fmt.Fprintf(os.Stdout, "dispatchqd: removed task %s (%s) from the queue: it had already run, but was still listed\n", t.ID, t.Name)
	}
}

// legacyCandidate reports whether r may belong to a process that a daemon
// without process records left behind: still recorded as running, or written
// off as interrupted recently.
func legacyCandidate(r store.Run, now time.Time) bool {
	if r.SessionID == "" {
		return false
	}
	switch {
	case r.Status == store.StatusRunning:
		return true
	case r.Status == store.StatusFailed && strings.HasPrefix(r.Error, store.InterruptedError):
		return r.FinishedAt != nil && now.Sub(*r.FinishedAt) < legacyAdoptWindow
	}
	return false
}

// orphanFor finds the orphaned headless harness process running session. The
// session id alone is not enough: an operator who took a session over in a
// terminal runs the harness with the same id, but interactively and under a
// shell, not orphaned and not in the streaming output format a run uses.
func orphanFor(orphans []Orphan, session string) (Orphan, bool) {
	for _, o := range orphans {
		fields := strings.Fields(o.Command)
		if !hasArgPair(fields, "--session-id", session) && !hasArgPair(fields, "--resume", session) {
			continue
		}
		if !hasArgPair(fields, "--output-format", "stream-json") {
			continue
		}
		return o, true
	}
	return Orphan{}, false
}

func hasArgPair(fields []string, flag, value string) bool {
	for i := 0; i+1 < len(fields); i++ {
		if fields[i] == flag && fields[i+1] == value {
			return true
		}
	}
	return false
}

// adopt takes back a run whose process is still going, and returns the run as
// it is now recorded. It counts as running exactly like a run this daemon
// started: its task does not start again, it holds its concurrency slot, and
// it can be cancelled or taken over. The caller is Recover, before any tick.
func (e *Engine) adopt(rec store.Run, pid int, start string) (store.Run, error) {
	t := task.Task{ID: rec.TaskID, Name: rec.TaskName}
	if rec.Task != nil {
		t = *rec.Task
	}
	rec.Status = store.StatusRunning
	rec.FinishedAt = nil
	rec.Error = ""
	rec.PID = pid

	if err := e.store.UpdateState(func(st *store.State) error {
		st.SetLiveRun(store.LiveRun{Run: rec, PID: pid, ProcessStart: start})
		return nil
	}); err != nil {
		return rec, fmt.Errorf("record run %s: %w", rec.RunID, err)
	}
	if err := e.store.AppendRun(rec); err != nil {
		return rec, fmt.Errorf("record run %s: %w", rec.RunID, err)
	}
	if f, err := os.OpenFile(rec.LogPath, os.O_WRONLY|os.O_APPEND, 0); err == nil {
		writeNote(f, fmt.Sprintf("DispatchQ restarted at %s and took this run back (process %d). "+
			"Output written after the restart is not captured here.", e.clock.Now().Format(time.RFC3339), pid))
		_ = f.Close()
	}

	providerID := rec.Provider.ID
	runCtx, cancelRun := context.WithCancel(e.runCtx)
	e.mu.Lock()
	e.active[t.ID] = true
	e.inflight[rec.RunID] = rec
	if providerID != "" {
		e.runningOn[providerID]++
	}
	if t.Parallel {
		e.parallelActive++
	} else {
		e.nonParallelActive++
	}
	e.cancels[rec.RunID] = cancelRun
	e.mu.Unlock()
	e.awake.acquire()

	e.wg.Add(1)
	go func() {
		defer e.wg.Done()
		defer cancelRun()
		e.watchAdopted(runCtx, t, providerID, rec, pid, start)
	}()
	return rec, nil
}

// watchAdopted waits for a taken-back run's process to end and records the
// outcome. A cancel stops the process tree first. On daemon shutdown it lets
// go of the run without recording anything: the process is not the daemon's
// to stop, and the next daemon takes it back again.
func (e *Engine) watchAdopted(ctx context.Context, t task.Task, providerID string, rec store.Run, pid int, start string) {
	poll := e.adoptPoll
	if poll <= 0 {
		poll = adoptPoll
	}
	tick := time.NewTicker(poll)
	defer tick.Stop()
	for e.alive(pid, start) {
		select {
		case <-e.adoptStop.Done():
			e.letGo(t, providerID, rec.RunID)
			return
		case <-ctx.Done():
			e.mu.Lock()
			canceled := e.canceled[rec.RunID]
			e.mu.Unlock()
			if !canceled {
				e.letGo(t, providerID, rec.RunID)
				return
			}
			// Checked again right before the kill: the process may have ended
			// since the last poll and its PID gone to another program.
			if e.alive(pid, start) {
				e.procs.Kill(pid)
			}
			for e.alive(pid, start) {
				<-tick.C
			}
			e.finish(t, providerID, rec, provider.Result{Status: store.StatusFailed}, nil)
			return
		case <-tick.C:
		}
	}
	res := provider.Result{Status: store.StatusFailed, SessionID: rec.SessionID, Message: adoptedEndMessage}
	if read, ok := e.sessionOutcome(rec); ok {
		res = read
	}
	e.finish(t, providerID, rec, res, nil)
}

// letGo gives back a taken-back run's slot without recording an outcome, for a
// daemon that is shutting down while the run's process goes on.
func (e *Engine) letGo(t task.Task, providerID, runID string) {
	e.mu.Lock()
	delete(e.cancels, runID)
	delete(e.inflight, runID)
	delete(e.active, t.ID)
	if providerID != "" {
		if e.runningOn[providerID]--; e.runningOn[providerID] <= 0 {
			delete(e.runningOn, providerID)
		}
	}
	if t.Parallel {
		e.parallelActive--
	} else {
		e.nonParallelActive--
	}
	e.mu.Unlock()
	e.awake.release()
}
