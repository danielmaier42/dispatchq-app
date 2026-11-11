package engine

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/danielmaier42/claudeq/internal/clock"
	"github.com/danielmaier42/claudeq/internal/store"
	"github.com/danielmaier42/claudeq/internal/task"
)

// fileJob is a job whose text lives in a file instead of the queue.
func fileJob(t *testing.T, id, kind, content string) (task.Task, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), id+".txt")
	if content != "" {
		if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
			t.Fatalf("write source file: %v", err)
		}
	}
	j := asapTask(id, false)
	j.Kind = task.Kind(kind)
	j.Prompt = ""
	j.SourceFile = path
	return j, path
}

func tickIdle(t *testing.T, e *Engine) {
	t.Helper()
	if err := e.Tick(context.Background()); err != nil {
		t.Fatalf("Tick: %v", err)
	}
	e.WaitIdle()
}

func TestSourceFileIsReadAtTheStartOfEveryRun(t *testing.T) {
	for _, kind := range []string{"", string(task.KindScript)} {
		t.Run("kind="+kind, func(t *testing.T) {
			fc := clock.NewFake(time.Date(2026, 11, 11, 2, 0, 0, 0, time.UTC))
			r := &stub{}
			e, st := newTestEngine(t, r, fc)
			job, path := fileJob(t, "from-file", kind, "first version\n")
			// A recurring job stays in the queue after it ran; run by hand, each
			// call is one run.
			job.Trigger, job.Cron, job.Enabled = task.TriggerCron, "0 0 1 1 *", false
			saveTasks(t, st, job)

			if err := e.RunTaskNow(context.Background(), job.ID); err != nil {
				t.Fatalf("RunTaskNow: %v", err)
			}
			e.WaitIdle()
			if err := os.WriteFile(path, []byte("second version\n"), 0o600); err != nil {
				t.Fatalf("rewrite source file: %v", err)
			}
			if err := e.RunTaskNow(context.Background(), job.ID); err != nil {
				t.Fatalf("RunTaskNow: %v", err)
			}
			e.WaitIdle()

			reqs := r.requests()
			if len(reqs) != 2 {
				t.Fatalf("expected 2 runs, got %d", len(reqs))
			}
			if got := reqs[0].Task.Prompt; got != "first version\n" {
				t.Fatalf("first run executed %q, want the file's first version", got)
			}
			if got := reqs[1].Task.Prompt; got != "second version\n" {
				t.Fatalf("second run executed %q, want the file as it is now", got)
			}

			// History keeps what each run read, with the path it came from.
			runs, err := st.Runs()
			if err != nil {
				t.Fatalf("Runs: %v", err)
			}
			if len(runs) != 2 {
				t.Fatalf("expected 2 runs in history, got %d", len(runs))
			}
			for i, want := range []string{"first version\n", "second version\n"} {
				snap := runs[i].Task
				if snap == nil || snap.Prompt != want || snap.SourceFile != path {
					t.Fatalf("run %d recorded task %+v, want prompt %q from %s", i, snap, want, path)
				}
			}

			// The stored job still points at the file and holds no copy of it.
			cfg, err := st.LoadConfig()
			if err != nil {
				t.Fatalf("LoadConfig: %v", err)
			}
			if got := cfg.Tasks[0]; got.Prompt != "" || got.SourceFile != path {
				t.Fatalf("stored job = prompt %q, file %q; want only the file", got.Prompt, got.SourceFile)
			}
		})
	}
}

func TestMissingSourceFileFailsTheRunWithTheReason(t *testing.T) {
	fc := clock.NewFake(time.Date(2026, 11, 11, 2, 0, 0, 0, time.UTC))
	r := &stub{}
	e, st := newTestEngine(t, r, fc)
	job, path := fileJob(t, "gone", string(task.KindScript), "")
	saveTasks(t, st, job)

	tickIdle(t, e)

	if n := len(r.requests()); n != 0 {
		t.Fatalf("the runner was started %d times for a job whose file is missing", n)
	}
	runs, err := st.Runs()
	if err != nil {
		t.Fatalf("Runs: %v", err)
	}
	if len(runs) != 1 {
		t.Fatalf("expected one failed run, got %d", len(runs))
	}
	run := runs[0]
	if run.Status != store.StatusFailed {
		t.Fatalf("status = %q, want failed", run.Status)
	}
	if want := "script file " + path + " does not exist"; run.Error != want {
		t.Fatalf("error = %q, want %q", run.Error, want)
	}
	logText, err := os.ReadFile(run.LogPath)
	if err != nil {
		t.Fatalf("read run log: %v", err)
	}
	if !strings.Contains(string(logText), "does not exist") {
		t.Fatalf("run log %q does not say why the run failed", logText)
	}
	if active := e.ActiveTaskIDs(); len(active) != 0 {
		t.Fatalf("the failed run left tasks marked active: %v", active)
	}
}

func TestSourceFileAgentJobGetsTheDependencyDigestInFront(t *testing.T) {
	fc := clock.NewFake(time.Date(2026, 11, 11, 2, 0, 0, 0, time.UTC))
	r := &stub{}
	e, st := newTestEngine(t, r, fc)
	join, _ := fileJob(t, "join", "", "Consolidate the findings.\n")
	join.DependsOn = []string{"first"}
	join.IncludeResults = true
	saveTasks(t, st, join)
	finished := fc.Now()
	if err := st.AppendRun(store.Run{
		RunID: "r1", TaskID: "first", TaskName: "first",
		Status: store.StatusSuccess, FinalOutput: "the answer from first",
		StartedAt: fc.Now(), FinishedAt: &finished,
	}); err != nil {
		t.Fatalf("AppendRun: %v", err)
	}

	tickIdle(t, e)

	reqs := r.requests()
	if len(reqs) != 1 {
		t.Fatalf("expected the join to run once, got %d", len(reqs))
	}
	got := reqs[0].Task.Prompt
	if !strings.Contains(got, "the answer from first") || !strings.HasSuffix(got, "\nConsolidate the findings.\n") {
		t.Fatalf("prompt = %q, want the digest followed by the file's text", got)
	}
}
