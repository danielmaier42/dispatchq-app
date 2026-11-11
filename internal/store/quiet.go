package store

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"
)

// Quiet runs are the routine successes of quiet-history tasks. They are kept
// so the Log can show them on request, but only for a short while: a watcher
// that ticks every five minutes would otherwise fill the history with hundreds
// of identical successes a day.
const (
	// QuietRunMaxAge is how long a quiet run is kept after it finished.
	QuietRunMaxAge = 48 * time.Hour
	// QuietRunsPerTask is how many quiet runs of one task are kept at most,
	// the newest ones. It is what bounds a task that ticks more often than
	// every fifteen minutes within QuietRunMaxAge.
	QuietRunsPerTask = 200
	// QuietPruneBatch and QuietPruneGrace keep a watcher past its retention
	// from rewriting the whole history on every tick to drop one run: history
	// is rewritten once more than QuietPruneBatch runs are due, or once a due
	// run is QuietPruneGrace past its retention. A due run therefore outlives
	// the limits by at most that batch or that grace, and the runs of a
	// watcher that stopped ticking still go.
	QuietPruneBatch = 20
	QuietPruneGrace = 6 * time.Hour
)

// PruneQuietRuns drops the quiet runs older than QuietRunMaxAge at now and,
// per task, all but the newest QuietRunsPerTask, deleting their logs with
// them, once a batch of them is due (see QuietPruneBatch). Every other run is
// left exactly as it is. History is first read without the cross-process
// write lock; only a due rewrite takes it, and reads again under it, so a run
// another process appends meanwhile is not lost and a tick with nothing to do
// holds nobody up.
func (s *Store) PruneQuietRuns(now time.Time) error {
	s.mu.Lock()
	due, err := s.pruneQuietRunsLocked(now, false)
	s.mu.Unlock()
	if err != nil || !due {
		return err
	}
	return s.withWriteLock(func() error {
		s.mu.Lock()
		defer s.mu.Unlock()
		_, err := s.pruneQuietRunsLocked(now, true)
		return err
	})
}

// pruneQuietRunsLocked reports whether a batch of quiet runs is due and, when
// rewrite is set, drops it. The caller holds s.mu, and for a rewrite the
// cross-process write lock as well.
func (s *Store) pruneQuietRunsLocked(now time.Time, rewrite bool) (bool, error) {
	latest, order, err := s.readHistoryLocked()
	if err != nil || len(order) == 0 {
		return false, err
	}
	cutoff := now.Add(-QuietRunMaxAge)
	overdue := false
	perTask := map[string]int{}
	dropped := map[string]bool{}
	// Newest first, so the per-task count keeps the latest runs.
	for i := len(order) - 1; i >= 0; i-- {
		r := latest[order[i]]
		if !r.Quiet {
			continue
		}
		ended := r.StartedAt
		if r.FinishedAt != nil {
			ended = *r.FinishedAt
		}
		perTask[r.TaskID]++
		if ended.Before(cutoff) || perTask[r.TaskID] > QuietRunsPerTask {
			dropped[r.RunID] = true
		}
		if ended.Before(cutoff.Add(-QuietPruneGrace)) {
			overdue = true
		}
	}
	if len(dropped) <= QuietPruneBatch && !overdue {
		return false, nil
	}
	if !rewrite {
		return true, nil
	}

	var buf bytes.Buffer
	for _, id := range order {
		if dropped[id] {
			continue
		}
		b, err := json.Marshal(latest[id])
		if err != nil {
			return true, fmt.Errorf("marshal run: %w", err)
		}
		buf.Write(b)
		buf.WriteByte('\n')
	}
	if err := writeAtomic(s.path(historyFile), buf.Bytes()); err != nil {
		return true, fmt.Errorf("rewrite history: %w", err)
	}
	var errs []error
	for id := range dropped {
		p := latest[id].LogPath
		if p == "" {
			p = s.LogPath(id)
		}
		if err := os.Remove(p); err != nil && !errors.Is(err, os.ErrNotExist) {
			errs = append(errs, fmt.Errorf("remove log of quiet run %s: %w", id, err))
		}
	}
	return true, errors.Join(errs...)
}

// readHistoryLocked reads history.jsonl collapsed to the latest event per run
// id, with the run ids in first-seen order; a nil map means there is no
// history file yet. The caller holds s.mu.
func (s *Store) readHistoryLocked() (map[string]Run, []string, error) {
	f, err := os.Open(s.path(historyFile))
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil, nil
	}
	if err != nil {
		return nil, nil, fmt.Errorf("open history: %w", err)
	}
	defer func() { _ = f.Close() }()

	latest := map[string]Run{}
	var order []string
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for sc.Scan() {
		line := sc.Bytes()
		if len(line) == 0 {
			continue
		}
		var r Run
		if err := json.Unmarshal(line, &r); err != nil {
			return nil, nil, fmt.Errorf("parse history line: %w", err)
		}
		if _, seen := latest[r.RunID]; !seen {
			order = append(order, r.RunID)
		}
		latest[r.RunID] = r
	}
	if err := sc.Err(); err != nil {
		return nil, nil, fmt.Errorf("read history: %w", err)
	}
	return latest, order, nil
}

// VisibleRuns returns runs without the quiet ones, keeping their order: what
// a list shows unless it is asked for hidden runs too.
func VisibleRuns(runs []Run) []Run {
	out := make([]Run, 0, len(runs))
	for _, r := range runs {
		if !r.Quiet {
			out = append(out, r)
		}
	}
	return out
}
