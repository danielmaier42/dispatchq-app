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
)

// PruneQuietRuns drops the quiet runs older than QuietRunMaxAge at now and,
// per task, all but the newest QuietRunsPerTask, deleting their logs with
// them. Every other run is left exactly as it is; history.jsonl is rewritten
// only when something is dropped.
func (s *Store) PruneQuietRuns(now time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	latest, order, err := s.readHistoryLocked()
	if err != nil || len(order) == 0 {
		return err
	}
	cutoff := now.Add(-QuietRunMaxAge)
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
	}
	if len(dropped) == 0 {
		return nil
	}

	var buf bytes.Buffer
	for _, id := range order {
		if dropped[id] {
			continue
		}
		b, err := json.Marshal(latest[id])
		if err != nil {
			return fmt.Errorf("marshal run: %w", err)
		}
		buf.Write(b)
		buf.WriteByte('\n')
	}
	if err := writeAtomic(s.path(historyFile), buf.Bytes()); err != nil {
		return fmt.Errorf("rewrite history: %w", err)
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
	return errors.Join(errs...)
}

// readHistoryLocked reads history.jsonl collapsed to the latest event per run
// id, with the run ids in first-seen order. The caller holds s.mu.
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
