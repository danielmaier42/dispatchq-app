package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/danielmaier42/claudeq/internal/app"
	"github.com/danielmaier42/claudeq/internal/task"
)

func TestAddWithSourceFileStoresTheAbsolutePathOnly(t *testing.T) {
	st := newTestStore(t)
	dir := t.TempDir()
	t.Chdir(dir)
	if err := os.WriteFile("watch.sh", []byte("#!/bin/sh\necho hi\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := cmdAdd(st, []string{"--id", "watch", "--kind", "script", "--source-file", "watch.sh", "--dir", dir}); err != nil {
		t.Fatalf("cmdAdd: %v", err)
	}
	got, err := findTask(st, "watch")
	if err != nil {
		t.Fatalf("findTask: %v", err)
	}
	want, err := filepath.Abs("watch.sh")
	if err != nil {
		t.Fatal(err)
	}
	if got.SourceFile != want || got.Prompt != "" {
		t.Fatalf("stored source_file %q prompt %q, want %q and no copy of the script", got.SourceFile, got.Prompt, want)
	}

	if err := cmdAdd(st, []string{"--id", "both", "--prompt", "x", "--source-file", "watch.sh", "--dir", dir}); err == nil ||
		!strings.Contains(err.Error(), "not both") {
		t.Fatalf("--prompt with --source-file: err = %v, want a refusal", err)
	}
}

func TestEditSwitchesBetweenInlineAndFile(t *testing.T) {
	st := newTestStore(t)
	// Filed directly: adding an agent job through cmdAdd would check that a
	// provider is installed, which is not what this test is about.
	if err := app.AddTask(st, baseTask()); err != nil {
		t.Fatalf("AddTask: %v", err)
	}
	brief := filepath.Join(t.TempDir(), "brief.md")
	if err := cmdEdit(st, []string{"nightly", "--source-file", brief}); err != nil {
		t.Fatalf("cmdEdit --source-file: %v", err)
	}
	got, _ := findTask(st, "nightly")
	if got.SourceFile != brief || got.Prompt != "" {
		t.Fatalf("after --source-file: %+v", got)
	}
	if err := cmdEdit(st, []string{"nightly", "--prompt", "inline again"}); err != nil {
		t.Fatalf("cmdEdit --prompt: %v", err)
	}
	got, _ = findTask(st, "nightly")
	if got.SourceFile != "" || got.Prompt != "inline again" {
		t.Fatalf("after --prompt: %+v", got)
	}
}

func TestParseTaskPatchRefusesTwoSources(t *testing.T) {
	for _, args := range [][]string{
		{"--prompt", "x", "--source-file", "/a.md"},
		{"--prompt-file", "p.md", "--source-file", "/a.md"},
	} {
		if _, err := parseTaskPatch(args, failingRead); err == nil || !strings.Contains(err.Error(), "choose one of") {
			t.Fatalf("%v: err = %v, want a refusal", args, err)
		}
	}
}

func TestQueuedJobDoesNotInheritTheSourceFile(t *testing.T) {
	parent := task.Task{
		ID: "watch", Name: "watch", Kind: task.KindScript, SourceFile: "/scripts/watch.sh",
		// What a running job sees of itself: the text read for this run.
		Prompt:     "echo hi",
		WorkingDir: "/repo", Trigger: task.TriggerCron, Cron: "*/5 * * * *",
		Enabled: true, Permissions: task.PermissionsDefault,
	}
	now := time.Date(2026, 11, 11, 12, 0, 0, 0, time.UTC)

	o, err := parseQueueOpts([]string{"--prompt", "review the new item"})
	if err != nil {
		t.Fatal(err)
	}
	got, err := buildQueuedTask(parentJSON(t, parent), "q-1", o, now)
	if err != nil {
		t.Fatalf("buildQueuedTask: %v", err)
	}
	if got.SourceFile != "" || got.Prompt != "review the new item" {
		t.Fatalf("queued job = %+v, want its own inline prompt", got)
	}

	o, err = parseQueueOpts([]string{"--source-file", "/briefs/review.md"})
	if err != nil {
		t.Fatal(err)
	}
	got, err = buildQueuedTask(parentJSON(t, parent), "q-2", o, now)
	if err != nil {
		t.Fatalf("buildQueuedTask: %v", err)
	}
	wantFile, _ := filepath.Abs("/briefs/review.md")
	if got.SourceFile != wantFile || got.Prompt != "" || got.Name != "review.md" {
		t.Fatalf("queued job = %+v, want the file, no inline text, named after the file", got)
	}

	o, err = parseQueueOpts([]string{"--prompt", "x", "--source-file", "/briefs/review.md"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := buildQueuedTask(parentJSON(t, parent), "q-3", o, now); err == nil {
		t.Fatal("--prompt with --source-file must be refused")
	}
}

func TestTaskDocCarriesTheSourceFile(t *testing.T) {
	s := baseTask()
	s.Prompt = ""
	s.SourceFile = "/briefs/nightly.md"
	doc, err := encodeTaskDoc(s)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(doc), `source_file = '/briefs/nightly.md'`) {
		t.Fatalf("document does not show the source file:\n%s", doc)
	}
	got, err := decodeTaskDoc(doc, s)
	if err != nil {
		t.Fatal(err)
	}
	if got.SourceFile != s.SourceFile || got.Prompt != "" {
		t.Fatalf("round trip = %+v", got)
	}
}
