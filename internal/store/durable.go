package store

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/danielmaier42/claudeq/internal/task"
)

// The daemon runs unattended, so a write that fails once (a full disk, a
// system out of file handles) must neither lose the run's record nor leave a
// task half-retired. Writes the run bookkeeping depends on are tried again a
// few times, and a run record that still cannot reach history is kept: in
// memory, written again before the next record, and in a small file next to
// the run's log that the next start replays.

// writeRetries are the pauses between the attempts of a write that failed:
// four attempts over a little more than four seconds, long enough for a burst
// of open files to drain, short enough not to hold up a finishing run.
var writeRetries = []time.Duration{200 * time.Millisecond, time.Second, 3 * time.Second}

// pendingSuffix names the fallback file of a run record history did not take.
const pendingSuffix = ".pending.json"

// retry runs fn until it succeeds, fails for a reason another attempt cannot
// change, or the attempts are used up, returning the last error. fn takes
// whatever locks it needs itself: the pauses are spent holding none, so the
// rest of the daemon is not held up while a write waits for the disk.
func (s *Store) retry(fn func() error) error {
	err := fn()
	for _, d := range writeRetries {
		if err == nil || !transient(err) {
			return err
		}
		if s.sleep != nil {
			s.sleep(d)
		} else {
			time.Sleep(d)
		}
		err = fn()
	}
	return err
}

// transient reports whether err came from the file system, which is what a
// full disk or a system out of file handles produces and what can pass. A
// config that does not parse or a task that does not validate fails the same
// way every time.
func transient(err error) bool {
	var pathErr *fs.PathError
	var linkErr *os.LinkError
	var sysErr *os.SyscallError
	return errors.As(err, &pathErr) || errors.As(err, &linkErr) || errors.As(err, &sysErr)
}

// flushPending is one attempt of AppendRun: it writes everything waiting.
// The cross-process lock keeps the append out of another process's rewrite of
// history (PruneHistory, PruneQuietRuns), which would otherwise replace the
// file with a copy that lacks it, and lets a failed write cut back only its
// own bytes.
func (s *Store) flushPending() error {
	return s.withWriteLock(func() error {
		s.mu.Lock()
		defer s.mu.Unlock()
		return s.flushLocked()
	})
}

// keepRun writes the fallback file of every record still waiting, after the
// attempts to write history ran out. It returns whether r is still waiting
// (another write may have taken it meanwhile) and the error keeping it.
func (s *Store) keepRun(r Run) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	waiting := false
	for _, p := range s.pending {
		waiting = waiting || p.RunID == r.RunID
	}
	if !waiting {
		return false, nil
	}
	return true, s.markLocked()
}

// pendingPath is the fallback file of a run record history did not take.
func (s *Store) pendingPath(runID string) string {
	return filepath.Join(s.home, runsDir, runID+pendingSuffix)
}

// queueLocked adds r to the records waiting for history. Only a run's latest
// record matters (see Runs), so a newer one replaces the older in place.
// The caller holds s.mu.
func (s *Store) queueLocked(r Run) {
	for i := range s.pending {
		if s.pending[i].RunID == r.RunID {
			s.pending[i] = r
			return
		}
	}
	s.pending = append(s.pending, r)
}

// flushLocked appends every waiting record to history in one write. A write
// cut short is truncated away again, so a half line never ends up in the file
// for every later read to trip over. The caller holds s.mu.
func (s *Store) flushLocked() error {
	if len(s.pending) == 0 {
		return nil
	}
	var buf bytes.Buffer
	for _, r := range s.pending {
		line, err := json.Marshal(r)
		if err != nil {
			return fmt.Errorf("encode run: %w", err)
		}
		buf.Write(line)
		buf.WriteByte('\n')
	}
	f, err := os.OpenFile(s.path(historyFile), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("open history: %w", err)
	}
	info, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return fmt.Errorf("stat history: %w", err)
	}
	if n, err := f.Write(buf.Bytes()); err != nil {
		// Cut back only what this write left. A file that grew by more holds
		// another process's record behind it, which must not go.
		if after, serr := f.Stat(); serr == nil && after.Size() == info.Size()+int64(n) {
			_ = f.Truncate(info.Size())
		}
		_ = f.Close()
		return fmt.Errorf("write history: %w", err)
	}
	// The records are written; what follows must not report them as not.
	// A close that fails after a complete write loses nothing that a later
	// write could fix, and calling them unwritten would write them twice.
	_ = f.Close()
	s.pending = nil
	// Every fallback file now stands for a record history holds, including
	// one an earlier write could not remove.
	for id := range s.marked {
		s.dropMarkerLocked(id)
	}
	return nil
}

// markLocked writes the fallback file of every record still waiting, so a
// daemon that stops before history takes them still has them at its next
// start. The caller holds s.mu.
func (s *Store) markLocked() error {
	var errs []error
	for _, r := range s.pending {
		data, err := json.Marshal(r)
		if err != nil {
			errs = append(errs, fmt.Errorf("encode run %s: %w", r.RunID, err))
			continue
		}
		if err := writeAtomic(s.pendingPath(r.RunID), data); err != nil {
			errs = append(errs, fmt.Errorf("keep run %s: %w", r.RunID, err))
			continue
		}
		if s.marked == nil {
			s.marked = map[string]bool{}
		}
		s.marked[r.RunID] = true
	}
	return errors.Join(errs...)
}

// dropMarkerLocked removes the fallback file of a record history now holds.
// One that cannot be removed is tried again after the next write; until then a
// replay finds history already saying the same and skips it. The caller holds
// s.mu.
func (s *Store) dropMarkerLocked(runID string) {
	if !s.marked[runID] {
		return
	}
	if err := os.Remove(s.pendingPath(runID)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return
	}
	delete(s.marked, runID)
}

// ReplayPendingRuns writes the run records an earlier process could not get
// into history (see AppendRun) and removes their fallback files. It is meant to
// run at daemon start, before anything else reads or writes history. A record
// history already holds unchanged is not written twice. Returns how many it
// wrote.
func (s *Store) ReplayPendingRuns() (int, error) {
	matches, err := filepath.Glob(filepath.Join(s.home, runsDir, "*"+pendingSuffix))
	if err != nil {
		return 0, fmt.Errorf("list kept runs: %w", err)
	}
	if len(matches) == 0 {
		return 0, nil
	}
	runs, err := s.Runs()
	if err != nil {
		return 0, err
	}
	latest := make(map[string]Run, len(runs))
	for _, r := range runs {
		latest[r.RunID] = r
	}
	n := 0
	var errs []error
	for _, path := range matches {
		data, err := os.ReadFile(path)
		if err != nil {
			errs = append(errs, fmt.Errorf("read kept run %s: %w", filepath.Base(path), err))
			continue
		}
		var r Run
		if err := json.Unmarshal(data, &r); err != nil || r.RunID == "" ||
			r.RunID != strings.TrimSuffix(filepath.Base(path), pendingSuffix) {
			errs = append(errs, fmt.Errorf("kept run %s is not a run record", filepath.Base(path)))
			continue
		}
		// History already saying the same, or saying the run ended where the
		// file says it was still going, has the newer word: the file is one a
		// write could not remove after history took a later record.
		if have, ok := latest[r.RunID]; ok && (sameRun(have, r) || have.Status.Terminal() && !r.Status.Terminal()) {
			if err := os.Remove(path); err != nil {
				errs = append(errs, fmt.Errorf("remove kept run %s: %w", r.RunID, err))
			}
			continue
		}
		s.mu.Lock()
		if s.marked == nil {
			s.marked = map[string]bool{}
		}
		s.marked[r.RunID] = true // the file is already there; remove it once written
		s.mu.Unlock()
		if err := s.AppendRun(r); err != nil {
			errs = append(errs, err)
			continue
		}
		n++
	}
	return n, errors.Join(errs...)
}

func sameRun(a, b Run) bool {
	ja, err := json.Marshal(a)
	if err != nil {
		return false
	}
	jb, err := json.Marshal(b)
	return err == nil && bytes.Equal(ja, jb)
}

// RetireTask ends a task's waiting session and, for a one-shot task, takes it
// out of the queue and marks it completed, in one locked update. The queue is
// written first: a task no longer listed cannot start again, whatever the
// state says. The state then records the task completed, and, only when the
// queue refused the removal, that the removal is still owed, so the next
// daemon start finishes it (see RepairCompletedOneShots). A failure the disk
// may get over is tried again (see writeRetries), with no lock held between
// the attempts.
func (s *Store) RetireTask(taskID string, oneShot bool) error {
	removed := !oneShot
	return s.retry(func() error {
		s.writeMu.Lock()
		defer s.writeMu.Unlock()
		return s.withWriteLock(func() error {
			var removeErr error
			if !removed {
				removeErr = s.dequeue(taskID)
				removed = removeErr == nil
			}
			st, err := s.LoadState()
			if err == nil {
				st.ClearPendingResume(taskID)
				if oneShot {
					st.MarkCompletedOnce(taskID)
					if removed {
						st.ClearRemovalOwed(taskID)
					} else {
						st.OweRemoval(taskID)
					}
				}
				err = s.SaveState(st)
			}
			if err != nil {
				return fmt.Errorf("mark task %q completed: %w", taskID, err)
			}
			if removeErr != nil {
				return fmt.Errorf("remove completed task %q from the queue: %w", taskID, removeErr)
			}
			return nil
		})
	})
}

// dequeue takes the task out of the queue; one not listed is already out.
func (s *Store) dequeue(taskID string) error {
	cfg, err := s.LoadConfig()
	if err != nil {
		return err
	}
	if !removeTask(&cfg, taskID) {
		return nil
	}
	return s.SaveConfig(cfg)
}

// RepairCompletedOneShots takes out of the queue every one-shot task whose
// removal RetireTask owes: it was marked completed, but the write that takes
// it out of the queue failed, so it stayed listed and is never due again. A
// task listed for another reason (queued again by hand under the id of one
// that ran) is left alone, and so is one with a session waiting to continue
// or a run in flight. It is meant to run at daemon start, before the first
// tick. Returns the tasks it removed.
func (s *Store) RepairCompletedOneShots() ([]task.Task, error) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	var removed []task.Task
	err := s.withWriteLock(func() error {
		st, err := s.LoadState()
		if err != nil {
			return err
		}
		if len(st.RemovalOwed) == 0 {
			return nil
		}
		cfg, err := s.LoadConfig()
		if err != nil {
			return err
		}
		queued := map[string]bool{}
		kept := cfg.Tasks[:0:0]
		for _, t := range cfg.Tasks {
			_, waiting := st.PendingResume(t.ID)
			if t.Trigger != task.TriggerCron && st.IsRemovalOwed(t.ID) && st.IsCompletedOnce(t.ID) &&
				!waiting && !st.hasLiveRun(t.ID) {
				removed = append(removed, t)
				continue
			}
			queued[t.ID] = true
			kept = append(kept, t)
		}
		if len(removed) > 0 {
			cfg.Tasks = kept
			if err := s.SaveConfig(cfg); err != nil {
				return err
			}
		}
		// What is owed is settled once the task is out of the queue, by this
		// repair or by anyone else.
		settled := false
		for id := range st.RemovalOwed {
			if !queued[id] {
				st.ClearRemovalOwed(id)
				settled = true
			}
		}
		if !settled {
			return nil
		}
		return s.SaveState(st)
	})
	if err != nil {
		return nil, fmt.Errorf("remove completed one-shot tasks: %w", err)
	}
	return removed, nil
}

// removeTask drops the task with id from cfg, reporting whether it was there.
func removeTask(cfg *Config, id string) bool {
	for i := range cfg.Tasks {
		if cfg.Tasks[i].ID == id {
			cfg.Tasks = append(cfg.Tasks[:i], cfg.Tasks[i+1:]...)
			return true
		}
	}
	return false
}
