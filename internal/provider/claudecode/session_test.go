package claudecode

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/danielmaier42/claudeq/internal/provider"
	"github.com/danielmaier42/claudeq/internal/store"
)

// Lines as Claude Code writes them to a session transcript, cut to the fields
// the outcome depends on.
const (
	tPrompt    = `{"type":"user","isSidechain":false,"timestamp":"2026-11-10T08:00:00Z","message":{"role":"user","content":"relock storefront"}}`
	tToolUse   = `{"type":"assistant","isSidechain":false,"timestamp":"2026-11-10T08:01:00Z","message":{"id":"msg_1","model":"claude-opus-5-5","stop_reason":"tool_use","content":[{"type":"tool_use","name":"Bash"}]}}`
	tToolDone  = `{"type":"user","isSidechain":false,"timestamp":"2026-11-10T08:59:39Z","message":{"role":"user","content":[{"type":"tool_result","content":"Exit code 137","is_error":true}]}}`
	tThinking  = `{"type":"assistant","isSidechain":false,"timestamp":"2026-11-10T09:00:00Z","message":{"id":"msg_2","model":"claude-opus-5-5","stop_reason":null,"content":[{"type":"thinking","thinking":"done"}]}}`
	tAnswer    = `{"type":"assistant","isSidechain":false,"timestamp":"2026-11-10T09:00:01Z","message":{"id":"msg_2","model":"claude-opus-5-5","stop_reason":"end_turn","content":[{"type":"text","text":"Both locks are relocked."}]}}`
	tSidechain = `{"type":"assistant","isSidechain":true,"timestamp":"2026-11-10T09:00:02Z","message":{"id":"msg_3","model":"claude-haiku-4-5","stop_reason":"tool_use","content":[{"type":"tool_use","name":"Read"}]}}`
	tBookkeep  = `{"type":"last-prompt","lastPrompt":"relock storefront","sessionId":"s1"}`
	tAPIError  = `{"type":"assistant","isSidechain":false,"isApiErrorMessage":true,"timestamp":"2026-11-10T09:00:01Z","message":{"id":"e1","model":"<synthetic>","stop_reason":"stop_sequence","content":[{"type":"text","text":"API Error: 500 Internal server error"}]}}`
)

func writeTranscript(t *testing.T, dir, session string, lines ...string) {
	t.Helper()
	folder := filepath.Join(dir, "projects", "-Users-dm-Documents-Projekte-NextGen")
	if err := os.MkdirAll(folder, 0o755); err != nil {
		t.Fatal(err)
	}
	data := strings.Join(lines, "\n") + "\n"
	if err := os.WriteFile(filepath.Join(folder, session+".jsonl"), []byte(data), 0o600); err != nil {
		t.Fatal(err)
	}
}

func TestSessionOutcome(t *testing.T) {
	started := time.Date(2026, 11, 10, 7, 59, 0, 0, time.UTC)
	for _, tc := range []struct {
		name    string
		lines   []string
		since   time.Time
		ok      bool
		status  store.RunStatus
		output  string
		message string
	}{
		{name: "answered", lines: []string{tPrompt, tToolUse, tToolDone, tThinking, tAnswer, tSidechain, tBookkeep},
			since: started, ok: true, status: store.StatusSuccess, output: "Both locks are relocked."},
		{name: "cut off after a tool result", lines: []string{tPrompt, tToolUse, tToolDone, tBookkeep}, since: started},
		{name: "cut off during a tool call", lines: []string{tPrompt, tToolUse}, since: started},
		{name: "api error", lines: []string{tPrompt, tAPIError}, since: started,
			ok: true, status: store.StatusFailed, output: "API Error: 500 Internal server error",
			message: "API Error: 500 Internal server error"},
		{name: "answer belongs to an earlier run", lines: []string{tPrompt, tThinking, tAnswer},
			since: time.Date(2026, 11, 10, 10, 0, 0, 0, time.UTC)},
		{name: "no transcript", since: started},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			if tc.lines != nil {
				writeTranscript(t, dir, "s1", tc.lines...)
			}
			inst := provider.Instance{ID: "claude-1", Kind: provider.KindClaudeCode, ConfigDir: dir}
			res, ok, err := New().SessionOutcome(inst, "s1", tc.since)
			if err != nil {
				t.Fatal(err)
			}
			if ok != tc.ok {
				t.Fatalf("ok = %v, want %v (result %+v)", ok, tc.ok, res)
			}
			if !ok {
				return
			}
			if res.Status != tc.status || res.FinalOutput != tc.output || res.Message != tc.message {
				t.Fatalf("result = %+v, want %s %q %q", res, tc.status, tc.output, tc.message)
			}
			if res.SessionID != "s1" {
				t.Fatalf("session = %q", res.SessionID)
			}
			if tc.status == store.StatusSuccess && res.Model != "claude-opus-5-5" {
				t.Fatalf("model = %q, want the one that answered", res.Model)
			}
		})
	}
}

// The outcome lookup goes through the instance's own configuration and only
// for a provider that runs on this system.
func TestReadSessionOutcomeFindsTheInstance(t *testing.T) {
	dir := t.TempDir()
	writeTranscript(t, dir, "s1", tPrompt, tAnswer)
	reg := provider.NewRegistry(New())
	set, err := provider.FromConfig(store.Config{Providers: []store.Provider{
		{ID: "claude", Kind: string(provider.KindClaudeCode), Name: "Claude", Enabled: true},
		{ID: "claude-1", Kind: string(provider.KindClaudeCode), Name: "dm-claude1", ConfigDir: dir, Enabled: true},
	}})
	if err != nil {
		t.Fatal(err)
	}
	rec := store.Run{RunID: "r", SessionID: "s1", StartedAt: time.Date(2026, 11, 10, 7, 0, 0, 0, time.UTC),
		Provider: store.RunProvider{ID: "claude-1", Kind: string(provider.KindClaudeCode)}}
	res, ok, err := provider.ReadSessionOutcome(reg, set, rec)
	if err != nil || !ok || res.Status != store.StatusSuccess {
		t.Fatalf("ReadSessionOutcome = %+v, %v, %v; want the transcript's success", res, ok, err)
	}

	rec.Provider = store.RunProvider{Kind: store.ScriptRunKind}
	if _, ok, _ := provider.ReadSessionOutcome(reg, set, rec); ok {
		t.Fatal("a script run has no session to read")
	}
}
