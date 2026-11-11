package store

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"
)

func writeLog(t *testing.T, s *Store, id string) {
	t.Helper()
	if err := os.MkdirAll(s.path(runsDir), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(s.LogPath(id), []byte("log\n"), 0o644); err != nil {
		t.Fatal(err)
	}
}

// writeHistory writes runs as history.jsonl in one go, each with a log.
func writeHistory(t *testing.T, s *Store, runs ...Run) {
	t.Helper()
	var buf bytes.Buffer
	for _, r := range runs {
		if r.LogPath == "" {
			r.LogPath = s.LogPath(r.RunID)
		}
		writeLog(t, s, r.RunID)
		b, err := json.Marshal(r)
		if err != nil {
			t.Fatal(err)
		}
		buf.Write(append(b, '\n'))
	}
	if err := os.WriteFile(s.path(historyFile), buf.Bytes(), 0o644); err != nil {
		t.Fatal(err)
	}
}

func appendRun(t *testing.T, s *Store, r Run) {
	t.Helper()
	if r.LogPath == "" {
		r.LogPath = s.LogPath(r.RunID)
	}
	writeLog(t, s, r.RunID)
	if err := s.AppendRun(r); err != nil {
		t.Fatal(err)
	}
}

func runIDs(t *testing.T, s *Store) []string {
	t.Helper()
	runs, err := s.Runs()
	if err != nil {
		t.Fatal(err)
	}
	ids := make([]string, len(runs))
	for i, r := range runs {
		ids[i] = r.RunID
	}
	return ids
}

func TestPruneQuietRunsDropsExpiredAndSurplusQuietRunsOnly(t *testing.T) {
	s, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 11, 11, 12, 0, 0, 0, time.UTC)
	old := now.Add(-QuietRunMaxAge - time.Minute)
	fresh := now.Add(-time.Hour)
	runs := []Run{
		{RunID: "old-real", TaskID: "real", StartedAt: old, FinishedAt: &old, Status: StatusSuccess},
		{RunID: "old-quiet", TaskID: "watch", StartedAt: old, FinishedAt: &old, Status: StatusSuccess, Quiet: true},
	}
	// Enough expired ones to make a batch (see QuietPruneBatch).
	for i := range QuietPruneBatch {
		runs = append(runs, Run{RunID: fmt.Sprintf("old-%02d", i), TaskID: "watch", StartedAt: old, FinishedAt: &old, Status: StatusSuccess, Quiet: true})
	}
	// One more than a task may keep, all fresh: the oldest of them goes.
	for i := range QuietRunsPerTask + 1 {
		at := fresh.Add(time.Duration(i) * time.Second)
		runs = append(runs, Run{RunID: fmt.Sprintf("tick-%03d", i), TaskID: "busy", StartedAt: at, FinishedAt: &at, Status: StatusSuccess, Quiet: true})
	}
	runs = append(runs, Run{RunID: "fresh-quiet", TaskID: "watch", StartedAt: fresh, FinishedAt: &fresh, Status: StatusSuccess, Quiet: true})
	writeHistory(t, s, runs...)

	if err := s.PruneQuietRuns(now); err != nil {
		t.Fatal(err)
	}
	ids := runIDs(t, s)
	if len(ids) != QuietRunsPerTask+2 || ids[0] != "old-real" || ids[1] != "tick-001" || ids[len(ids)-1] != "fresh-quiet" {
		t.Fatalf("want old-real, tick-001…tick-%03d and fresh-quiet, got %d runs starting %v", QuietRunsPerTask, len(ids), ids[:3])
	}
	for _, gone := range []string{"old-quiet", "tick-000"} {
		if _, err := os.Stat(s.LogPath(gone)); !os.IsNotExist(err) {
			t.Fatalf("log of pruned quiet run %s must be deleted: %v", gone, err)
		}
	}
	if _, err := os.Stat(s.LogPath("old-real")); err != nil {
		t.Fatalf("a real run's log is not this function's business: %v", err)
	}
}

func TestPruneHistoryDoesNotCountQuietRuns(t *testing.T) {
	s, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	at := time.Now()
	appendRun(t, s, Run{RunID: "a", TaskID: "real", StartedAt: at, Status: StatusSuccess})
	appendRun(t, s, Run{RunID: "q1", TaskID: "watch", StartedAt: at, Status: StatusSuccess, Quiet: true})
	appendRun(t, s, Run{RunID: "b", TaskID: "real", StartedAt: at, Status: StatusSuccess})
	appendRun(t, s, Run{RunID: "q2", TaskID: "watch", StartedAt: at, Status: StatusSuccess, Quiet: true})
	appendRun(t, s, Run{RunID: "c", TaskID: "real", StartedAt: at, Status: StatusSuccess})

	if err := s.PruneHistory(2); err != nil {
		t.Fatal(err)
	}
	if got := fmt.Sprint(runIDs(t, s)); got != "[q1 b q2 c]" {
		t.Fatalf("the limit counts real runs only and leaves quiet ones alone: got %s", got)
	}
	if _, err := os.Stat(s.LogPath("a")); !os.IsNotExist(err) {
		t.Fatalf("the dropped run's log must go: %v", err)
	}
}

func TestVisibleRunsLeavesOutQuietRuns(t *testing.T) {
	runs := []Run{{RunID: "a"}, {RunID: "q", Quiet: true}, {RunID: "b", Status: StatusFailed}}
	got := VisibleRuns(runs)
	if len(got) != 2 || got[0].RunID != "a" || got[1].RunID != "b" {
		t.Fatalf("got %+v", got)
	}
}

// The daemon and a `dispatchq run-now` process write history through separate
// Store values. A rewrite by one must never drop a run the other appended
// meanwhile.
func TestHistoryRewritesDoNotLoseAnotherProcesssAppends(t *testing.T) {
	home := t.TempDir()
	daemon, err := Open(home)
	if err != nil {
		t.Fatal(err)
	}
	cli, err := Open(home)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	old := now.Add(-QuietRunMaxAge - time.Hour)
	const appends = 80
	done := make(chan error, 1)
	go func() {
		for i := range appends {
			if err := daemon.AppendRun(Run{RunID: fmt.Sprintf("real-%03d", i), TaskID: "real", StartedAt: now, Status: StatusFailed}); err != nil {
				done <- err
				return
			}
		}
		done <- nil
	}()
	// Meanwhile the other process keeps expiring a batch of hidden runs, and
	// with them rewriting the whole file.
	// Errors here are collected rather than fatal, so the appender is always
	// waited for before the test (and its directory) ends.
	var cliErr error
	var appendErr error
	for round := 0; ; round++ {
		select {
		case appendErr = <-done:
		default:
			if cliErr == nil {
				for i := range QuietPruneBatch + 1 {
					id := fmt.Sprintf("q-%d-%d", round, i)
					if cliErr = cli.AppendRun(Run{RunID: id, TaskID: "watch", StartedAt: old, FinishedAt: &old, Status: StatusSuccess, Quiet: true}); cliErr != nil {
						break
					}
				}
			}
			if cliErr == nil {
				cliErr = cli.PruneQuietRuns(now)
			}
			continue
		}
		break
	}
	if appendErr != nil || cliErr != nil {
		t.Fatalf("append: %v; other process: %v", appendErr, cliErr)
	}
	runs, err := daemon.Runs()
	if err != nil {
		t.Fatal(err)
	}
	if got := len(VisibleRuns(runs)); got != appends {
		t.Fatalf("the rewrites lost %d of %d appended runs", appends-got, appends)
	}
}

// Expired hidden runs are dropped in batches, so a watcher past its retention
// does not rewrite the whole history on every tick.
func TestPruneQuietRunsWaitsForABatch(t *testing.T) {
	s, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	old := now.Add(-QuietRunMaxAge - time.Hour)
	var runs []Run
	for i := range QuietPruneBatch {
		runs = append(runs, Run{RunID: fmt.Sprintf("q-%02d", i), TaskID: "watch", StartedAt: old, FinishedAt: &old, Status: StatusSuccess, Quiet: true})
	}
	writeHistory(t, s, runs...)
	if err := s.PruneQuietRuns(now); err != nil {
		t.Fatal(err)
	}
	if got := len(runIDs(t, s)); got != QuietPruneBatch {
		t.Fatalf("%d expired runs are not yet a batch: want them kept, got %d", QuietPruneBatch, got)
	}
	appendRun(t, s, Run{RunID: "q-last", TaskID: "watch", StartedAt: old, FinishedAt: &old, Status: StatusSuccess, Quiet: true})
	if err := s.PruneQuietRuns(now); err != nil {
		t.Fatal(err)
	}
	if got := runIDs(t, s); len(got) != 0 {
		t.Fatalf("one more makes a batch and all expired go: %v", got)
	}
}

// A watcher that stopped ticking never makes a batch; its expired runs still
// go once they are well past their retention.
func TestPruneQuietRunsDropsOverdueRunsWithoutABatch(t *testing.T) {
	s, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	due := now.Add(-QuietRunMaxAge - time.Hour)
	overdue := now.Add(-QuietRunMaxAge - QuietPruneGrace - time.Minute)
	writeHistory(t, s,
		Run{RunID: "overdue", TaskID: "stopped", StartedAt: overdue, FinishedAt: &overdue, Status: StatusSuccess, Quiet: true},
		Run{RunID: "due", TaskID: "stopped", StartedAt: due, FinishedAt: &due, Status: StatusSuccess, Quiet: true},
		Run{RunID: "real", TaskID: "real", StartedAt: overdue, FinishedAt: &overdue, Status: StatusSuccess},
	)
	if err := s.PruneQuietRuns(now); err != nil {
		t.Fatal(err)
	}
	if got := fmt.Sprint(runIDs(t, s)); got != "[real]" {
		t.Fatalf("an overdue run sends every due one, and only those: got %s", got)
	}
}

// At startup every expired hidden run goes, however few there are.
func TestPruneAllQuietRunsIgnoresTheBatch(t *testing.T) {
	s, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	due := now.Add(-QuietRunMaxAge - time.Hour)
	fresh := now.Add(-time.Hour)
	writeHistory(t, s,
		Run{RunID: "due", TaskID: "stopped", StartedAt: due, FinishedAt: &due, Status: StatusSuccess, Quiet: true},
		Run{RunID: "fresh", TaskID: "stopped", StartedAt: fresh, FinishedAt: &fresh, Status: StatusSuccess, Quiet: true},
	)
	if err := s.PruneQuietRuns(now); err != nil {
		t.Fatal(err)
	}
	if got := len(runIDs(t, s)); got != 2 {
		t.Fatalf("one due run is no batch after a finished run: got %d runs", got)
	}
	if err := s.PruneAllQuietRuns(now); err != nil {
		t.Fatal(err)
	}
	if got := fmt.Sprint(runIDs(t, s)); got != "[fresh]" {
		t.Fatalf("startup drops every expired run: got %s", got)
	}
}
