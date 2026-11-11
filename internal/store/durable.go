package store

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
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

// retry runs fn until it succeeds or the attempts are used up, returning the
// last error.
func (s *Store) retry(fn func() error) error {
	err := fn()
	for _, d := range writeRetries {
		if err == nil {
			return nil
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
	if _, err := f.Write(buf.Bytes()); err != nil {
		_ = f.Truncate(info.Size())
		_ = f.Close()
		return fmt.Errorf("write history: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("close history: %w", err)
	}
	flushed := s.pending
	s.pending = nil
	for _, r := range flushed {
		s.dropMarkerLocked(r.RunID)
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
		if have, ok := latest[r.RunID]; ok && sameRun(have, r) {
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

// RetireTask ends a task's waiting session and, for a one-shot task, marks it
// completed and takes it out of the queue, in one locked update: another
// writer never sees the task marked done but still queued. Each half is tried
// again on failure (see writeRetries). The mark is written first, because it
// is what keeps the task from starting a second time; should the queue still
// refuse the removal, the task is completed but listed, and the next daemon
// start removes it (see RepairCompletedOneShots).
func (s *Store) RetireTask(taskID string, oneShot bool) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	return s.withWriteLock(func() error {
		err := s.retry(func() error {
			st, err := s.LoadState()
			if err != nil {
				return err
			}
			st.ClearPendingResume(taskID)
			if oneShot {
				st.MarkCompletedOnce(taskID)
			}
			return s.SaveState(st)
		})
		if err != nil {
			return fmt.Errorf("mark task %q completed: %w", taskID, err)
		}
		if !oneShot {
			return nil
		}
		err = s.retry(func() error {
			cfg, err := s.LoadConfig()
			if err != nil {
				return err
			}
			if !removeTask(&cfg, taskID) {
				return nil
			}
			return s.SaveConfig(cfg)
		})
		if err != nil {
			return fmt.Errorf("remove completed task %q from the queue: %w", taskID, err)
		}
		return nil
	})
}

// RepairCompletedOneShots takes every one-shot task out of the queue that has
// already run to completion but is still listed: a daemon whose removal failed
// after it had marked the task done left it there, where it is never due
// again. A task holding a waiting session has not completed and stays. It is
// meant to run at daemon start, before the first tick. Returns the tasks it
// removed.
func (s *Store) RepairCompletedOneShots() ([]task.Task, error) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	var removed []task.Task
	err := s.withWriteLock(func() error {
		st, err := s.LoadState()
		if err != nil {
			return err
		}
		cfg, err := s.LoadConfig()
		if err != nil {
			return err
		}
		kept := cfg.Tasks[:0:0]
		for _, t := range cfg.Tasks {
			_, waiting := st.PendingResume(t.ID)
			if t.Trigger != task.TriggerCron && st.IsCompletedOnce(t.ID) && !waiting {
				removed = append(removed, t)
				continue
			}
			kept = append(kept, t)
		}
		if len(removed) == 0 {
			return nil
		}
		cfg.Tasks = kept
		return s.SaveConfig(cfg)
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
