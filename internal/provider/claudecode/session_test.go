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
	tReminder  = `{"type":"user","isSidechain":false,"isMeta":true,"timestamp":"2026-11-10T09:00:03Z","message":{"role":"user","content":[{"type":"text","text":"<system-reminder>a hook ran</system-reminder>"}]}}`
	// A real limit line, as Claude Code wrote it on 2026-11-09.
	tLimit    = `{"type":"assistant","isSidechain":false,"isApiErrorMessage":true,"timestamp":"2026-11-10T09:00:01Z","error":"rate_limit","apiError":"usage_limit_reached","apiErrorStatus":429,"apiErrorParams":{"rate_limit_info":{"status":"rejected","resetsAt":1794222000,"rateLimitType":"five_hour"}},"quotaLimits":{"status":"rejected","resetsAt":1794222000},"message":{"id":"e2","model":"<synthetic>","stop_reason":"stop_sequence","content":[{"type":"text","text":"You've hit your 5-hour limit · resets 12pm (Europe/Berlin)"}]}}`
	tAuth     = `{"type":"assistant","isSidechain":false,"isApiErrorMessage":true,"timestamp":"2026-11-10T09:00:01Z","error":"authentication_failed","apiErrorStatus":401,"message":{"id":"e3","model":"<synthetic>","stop_reason":"stop_sequence","content":[{"type":"text","text":"Invalid API key · Please run /login"}]}}`
	tOrgBlock = `{"type":"assistant","isSidechain":false,"isApiErrorMessage":true,"timestamp":"2026-11-10T09:00:01Z","error":"authentication_failed","apiError":"oauth_not_allowed_for_organization","apiErrorStatus":401,"message":{"id":"e4","model":"<synthetic>","stop_reason":"stop_sequence","content":[{"type":"text","text":"Your organization has disabled Claude subscription access for Claude Code"}]}}`
	tAPIError = `{"type":"assistant","isSidechain":false,"isApiErrorMessage":true,"timestamp":"2026-11-10T09:00:01Z","message":{"id":"e1","model":"<synthetic>","stop_reason":"stop_sequence","content":[{"type":"text","text":"API Error: 500 Internal server error"}]}}`
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
		{name: "answered, then a reminder", lines: []string{tPrompt, tThinking, tAnswer, tReminder},
			since: started, ok: true, status: store.StatusSuccess, output: "Both locks are relocked."},
		{name: "cut off after a tool result", lines: []string{tPrompt, tToolUse, tToolDone, tBookkeep}, since: started},
		{name: "cut off during a tool call", lines: []string{tPrompt, tToolUse}, since: started},
		{name: "api error", lines: []string{tPrompt, tAPIError}, since: started,
			ok: true, status: store.StatusFailed, output: "API Error: 500 Internal server error",
			message: "API Error: 500 Internal server error"},
		{name: "rate limit", lines: []string{tPrompt, tLimit}, since: started,
			ok: true, status: store.StatusRateLimited, output: "You've hit your 5-hour limit · resets 12pm (Europe/Berlin)",
			message: "rate limit hit; waiting for reset"},
		{name: "login problem", lines: []string{tPrompt, tAuth}, since: started,
			ok: true, status: store.StatusAuthError, output: "Invalid API key · Please run /login",
			message: "Invalid API key · Please run /login"},
		{name: "organisation block", lines: []string{tPrompt, tOrgBlock}, since: started,
			ok: true, status: store.StatusRateLimited, output: "Your organization has disabled Claude subscription access for Claude Code",
			message: orgBlockedDetail},
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
			// A transcript on disk names the session, whatever its turn says.
			if want := tc.lines != nil; res.SessionConfirmed != want {
				t.Fatalf("session confirmed = %v, want %v", res.SessionConfirmed, want)
			}
			if !ok {
				return
			}
			if res.Status != tc.status || res.FinalOutput != tc.output || res.Message != tc.message {
				t.Fatalf("result = %+v, want %s %q %q", res, tc.status, tc.output, tc.message)
			}
			if want := time.Date(2026, 11, 10, 9, 0, 1, 0, time.UTC); !res.EndedAt.Equal(want) {
				t.Fatalf("ended at %v, want the answer's time %v", res.EndedAt, want)
			}
			if tc.name == "rate limit" && !res.ResetAt.Equal(time.Unix(1794222000, 0)) {
				t.Fatalf("reset at %v, want the window the limit line names", res.ResetAt)
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

// A transcript that exists but cannot be read just now (a system out of file
// handles) still names the session, so a cut-off run can continue it.
func TestSessionOutcomeOfAnUnreadableTranscript(t *testing.T) {
	dir := t.TempDir()
	writeTranscript(t, dir, "s1", tPrompt, tToolUse)
	path := filepath.Join(dir, "projects", "-Users-dm-Documents-Projekte-NextGen", "s1.jsonl")
	if err := os.Chmod(path, 0); err != nil {
		t.Fatal(err)
	}
	inst := provider.Instance{ID: "claude-1", Kind: provider.KindClaudeCode, ConfigDir: dir}
	res, ok, err := New().SessionOutcome(inst, "s1", time.Time{})
	if err == nil || ok || !res.SessionConfirmed || res.SessionID != "s1" {
		t.Fatalf("SessionOutcome = %+v, %v, %v; want an error and the session confirmed", res, ok, err)
	}
}
