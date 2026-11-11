package claudecode

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/danielmaier42/claudeq/internal/provider"
)

// Claude Code writes every session to <config dir>/projects/<folder>/<session
// id>.jsonl, one line per message part, whether or not anyone reads its
// output. That transcript is what tells a run whose output stopped reaching
// DispatchQ (see provider.SessionReader) apart: a turn that ended with the
// model's answer, one that ended in an API error, or one cut off in the middle.

// transcriptLine is the part of a transcript line the outcome depends on.
type transcriptLine struct {
	Type              string    `json:"type"`
	IsSidechain       bool      `json:"isSidechain"`
	IsMeta            bool      `json:"isMeta"`
	IsAPIErrorMessage bool      `json:"isApiErrorMessage"`
	Timestamp         time.Time `json:"timestamp"`
	// An API error line names the error the way the stream does, plus the
	// rate-limit window when it was a limit.
	Error          string `json:"error"`
	APIErrorStatus *int   `json:"apiErrorStatus"`
	APIErrorParams struct {
		RateLimitInfo *rateLimitInfo `json:"rate_limit_info"`
	} `json:"apiErrorParams"`
	QuotaLimits *rateLimitInfo `json:"quotaLimits"`
	Message     struct {
		ID         string          `json:"id"`
		Model      string          `json:"model"`
		StopReason string          `json:"stop_reason"`
		Content    json.RawMessage `json:"content"`
	} `json:"message"`
}

// SessionOutcome implements provider.SessionReader.
func (a *Adapter) SessionOutcome(inst provider.Instance, sessionID string, since time.Time) (provider.Result, bool, error) {
	path, err := transcriptPath(configDir(inst), sessionID)
	if err != nil || path == "" {
		return provider.Result{}, false, err
	}
	f, err := os.Open(path)
	if err != nil {
		return provider.Result{}, false, fmt.Errorf("read session %s: %w", sessionID, err)
	}
	defer func() { _ = f.Close() }()

	// The turn's end is the last message of the main conversation: a subagent's
	// messages (sidechain) end on their own, and bookkeeping lines carry none.
	var tail []transcriptLine
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 64<<20)
	for sc.Scan() {
		var l transcriptLine
		// A meta line (a reminder or hook output the CLI adds) is not part of
		// the conversation's turns; a sidechain is a subagent's own.
		if json.Unmarshal(sc.Bytes(), &l) != nil || l.IsSidechain || l.IsMeta {
			continue
		}
		if l.Type != "assistant" && l.Type != "user" {
			continue
		}
		if len(tail) > 0 && l.Type == "assistant" && tail[len(tail)-1].Type == "assistant" &&
			l.Message.ID != "" && l.Message.ID == tail[len(tail)-1].Message.ID {
			tail = append(tail, l) // the next part of the same message
			continue
		}
		tail = append(tail[:0], l)
	}
	if err := sc.Err(); err != nil {
		return provider.Result{}, false, fmt.Errorf("read session %s: %w", sessionID, err)
	}
	if len(tail) == 0 || tail[0].Type != "assistant" {
		return provider.Result{}, false, nil // ended on a tool result or a prompt: cut off mid-turn
	}
	last := tail[len(tail)-1]
	if last.Timestamp.Before(since) {
		return provider.Result{}, false, nil // an earlier run's answer; this run left none
	}

	var text []string
	for _, l := range tail {
		text = append(text, textOf(l.Message.Content)...)
	}
	answer := strings.TrimSpace(strings.Join(text, "\n"))

	c := provider.NewCollector(sessionID, inst.Name)
	if m := last.Message.Model; m != "" && !strings.HasPrefix(m, "<") {
		c.Add(provider.Event{Type: provider.EventSessionStarted, SessionID: sessionID, Model: m})
	}
	switch {
	case last.IsAPIErrorMessage:
		// The turn ended on an error the API returned. It is classified by
		// the parser that reads a live run, as the error result the stream
		// would have ended with: a limit pauses the provider until its window
		// reopens, a login problem is one, an organisation or billing block
		// pauses the account.
		limit := last.APIErrorParams.RateLimitInfo
		if limit == nil {
			limit = last.QuotaLimits
		}
		c.AddAll((&parser{}).events(streamEvent{
			Type: "result", IsError: true, ResultText: answer,
			Error: last.Error, APIErrorStatus: last.APIErrorStatus, RateLimitInfo: limit,
		}))
		c.Add(provider.Event{Type: provider.EventFailed, Detail: answer})
	case last.Message.StopReason == "end_turn" || last.Message.StopReason == "stop_sequence":
		c.Add(provider.Event{Type: provider.EventCompleted, FinalOutput: answer})
	default:
		return provider.Result{}, false, nil // the model was still working (a tool call under way)
	}
	res := c.Result(0)
	res.EndedAt = last.Timestamp
	return res, true, nil
}

// textOf returns the text parts of a message's content.
func textOf(raw json.RawMessage) []string {
	var parts []struct {
		Type string `json:"type"`
		Text string `json:"text"`
	}
	if json.Unmarshal(raw, &parts) != nil {
		var s string
		if json.Unmarshal(raw, &s) == nil && s != "" {
			return []string{s}
		}
		return nil
	}
	var out []string
	for _, p := range parts {
		if p.Type == "text" && p.Text != "" {
			out = append(out, p.Text)
		}
	}
	return out
}

// configDir is where the instance's Claude Code keeps its sessions: its own
// configuration directory, else the one its environment or the daemon's
// names, else the CLI's default.
func configDir(inst provider.Instance) string {
	if inst.ConfigDir != "" {
		return provider.ExpandHome(inst.ConfigDir)
	}
	if d := inst.Env[ConfigDirEnv]; d != "" {
		return provider.ExpandHome(d)
	}
	if d := os.Getenv(ConfigDirEnv); d != "" {
		return provider.ExpandHome(d)
	}
	return provider.ExpandHome("~/.claude")
}

// transcriptPath finds the session's transcript under dir. The folder it sits
// in is derived from the working directory the CLI ran in, which a run's
// worktree or a symlink can change, so it is looked for under every folder
// rather than recomputed. "" means there is none.
func transcriptPath(dir, sessionID string) (string, error) {
	if sessionID == "" || strings.ContainsAny(sessionID, `/\*?[`) {
		return "", nil
	}
	matches, err := filepath.Glob(filepath.Join(dir, "projects", "*", sessionID+".jsonl"))
	if err != nil {
		return "", fmt.Errorf("find session %s: %w", sessionID, err)
	}
	var newest string
	var newestAt time.Time
	for _, m := range matches {
		info, err := os.Stat(m)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return "", fmt.Errorf("find session %s: %w", sessionID, err)
		}
		if newest == "" || info.ModTime().After(newestAt) {
			newest, newestAt = m, info.ModTime()
		}
	}
	return newest, nil
}
