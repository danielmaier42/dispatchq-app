package store

import (
	"encoding/json"
	"errors"
	"os"
	"time"
)

// State holds machine-managed bookkeeping kept separate from the
// human-editable config: run read-status (FA-26) and per-task scheduling
// history. It is not hand-edited.
type State struct {
	// ReadRuns maps a run id to true once it has been read (FA-23/24). A run
	// absent from this map is unread (FA-22).
	ReadRuns map[string]bool `json:"read_runs"`
	// ReadArtifacts maps an artifact id to true once it has been read. An
	// artifact absent from this map is unread (FA-A2).
	ReadArtifacts map[string]bool `json:"read_artifacts"`
	// NotifiedArtifacts maps an artifact id to true once its "new artifact"
	// notification has been sent, so the daemon notifies exactly once per
	// published artifact even though it re-reads the list on every tick.
	NotifiedArtifacts map[string]bool `json:"notified_artifacts"`
	// ArtifactNotifyPrimed records that the daemon has taken stock of the
	// artifacts that already existed when it first started watching. Without it
	// an upgrade (or a fresh state file) would notify for the whole backlog.
	ArtifactNotifyPrimed bool `json:"artifact_notify_primed"`
	// LastStarted maps a task id to the start time of its most recent run,
	// used to compute the next cron occurrence (FA-18). A freshly added cron
	// task is anchored here without having run, so this is not a run history.
	LastStarted map[string]time.Time `json:"last_started"`
	// LastRunAt maps a task id to the start time of its most recent actual run.
	// Unlike LastStarted it is only written when a run is really launched, so
	// the dashboard can show when a recurring task last executed.
	LastRunAt map[string]time.Time `json:"last_run_at"`
	// CompletedOnce marks one-shot tasks (asap/fixed) that have already run so
	// they are not re-enqueued (PLAN.md §7).
	CompletedOnce map[string]bool `json:"completed_once"`
	// RemovalOwed marks one-shot tasks that were marked completed but whose
	// removal from the queue has not been written yet (see Store.RetireTask).
	// Only these are taken out of the queue at the next start; a task listed
	// for any other reason is left alone.
	RemovalOwed map[string]bool `json:"removal_owed,omitempty"`
	// PendingResumes maps a task id to the session waiting to be picked up after
	// a rate-limit wait (PLAN.md D4/V1), and to the provider instance that owns
	// it. A session id means nothing outside the harness that issued it, so the
	// provider travels with it.
	PendingResumes map[string]PendingResume `json:"pending_resumes"`
	// DismissedUpdateVersion is the release version the user dismissed in the
	// update prompt. While the latest release equals it, no "update available"
	// prompt is shown; a newer release supersedes it and prompts again.
	DismissedUpdateVersion string `json:"dismissed_update_version"`
	// WhatsNewSeen is the edition of the "What's new" dialog the user has
	// closed. The dialog shows while it differs from the running edition.
	WhatsNewSeen string `json:"whats_new_seen,omitempty"`
	// RenameCheckDone records that the tasks were checked for ClaudeQ names
	// and nothing is left to migrate, or that the operator waved it off. Until
	// then a banner offers the check.
	RenameCheckDone bool `json:"rename_check_done,omitempty"`
	// CollapsedGroups maps a queue group name to true while its section is
	// folded shut in the dashboard. It lives in the state, not the config: it is
	// a view preference the app writes, not something to hand-edit — and it has
	// to outlive a reload, a restart and the window being closed.
	CollapsedGroups map[string]bool `json:"collapsed_groups,omitempty"`
	// NotifiedProviderHealth maps a provider id to the health state the operator
	// was last told about. The scheduler looks at provider health on every tick,
	// so this is what turns "not installed" into one alert instead of one per
	// tick — and it outlives a daemon restart, which an in-memory memo would not.
	NotifiedProviderHealth map[string]string `json:"notified_provider_health,omitempty"`
	// CreditChecks maps a provider id to what is known about the prepaid credit
	// of an account on an API key: whether the last ping or run got an answer or
	// was refused for credit. It outlives a restart, so an account known to be
	// out of credit does not get the next task handed to it again.
	CreditChecks map[string]CreditCheck `json:"credit_checks,omitempty"`
	// LiveRuns maps a run id to the process the daemon started for it, while it
	// is in flight. A run's process lives in its own process group, so it can
	// outlast the daemon that started it (an update that kills the daemon, a
	// crash). This is what lets the next daemon find it again and take it back
	// instead of writing it off and starting the task a second time.
	LiveRuns map[string]LiveRun `json:"live_runs,omitempty"`
}

// LiveRun identifies the process of a run in flight.
type LiveRun struct {
	// Run is the run as it was recorded when it started, so a run of a
	// quiet-history task, which has no history entry while running, can still be
	// shown and finished by a daemon that did not start it.
	Run Run `json:"run"`
	// PID is the run's top process.
	PID int `json:"pid"`
	// ProcessStart is the start time the operating system reports for PID. A
	// PID is reused once its process is gone; the start time tells this process
	// apart from a later one under the same number.
	ProcessStart string `json:"process_start"`
}

// SetLiveRun records the process of a run in flight.
func (s *State) SetLiveRun(l LiveRun) {
	if s.LiveRuns == nil {
		s.LiveRuns = map[string]LiveRun{}
	}
	s.LiveRuns[l.Run.RunID] = l
}

// ClearLiveRun forgets the process of a run that has ended.
func (s *State) ClearLiveRun(runID string) { delete(s.LiveRuns, runID) }

// CreditCheck is the latest proof of an API-key account's credit.
type CreditCheck struct {
	// Status is "ok" when the account answered, otherwise the provider.Block it
	// was refused with ("credit_exhausted", "billing_refused").
	Status string `json:"status"`
	// CheckedAt is when that answer or refusal came.
	CheckedAt time.Time `json:"checked_at"`
	// Source says what produced it: "ping" for dispatchq's own one-word test
	// request, "run" for a task's run on the account.
	Source string `json:"source"`
}

func newState() *State {
	s := &State{}
	s.ensureMaps()
	return s
}

func (s *State) ensureMaps() {
	if s.ReadRuns == nil {
		s.ReadRuns = map[string]bool{}
	}
	if s.ReadArtifacts == nil {
		s.ReadArtifacts = map[string]bool{}
	}
	if s.NotifiedArtifacts == nil {
		s.NotifiedArtifacts = map[string]bool{}
	}
	if s.LastStarted == nil {
		s.LastStarted = map[string]time.Time{}
	}
	if s.LastRunAt == nil {
		s.LastRunAt = map[string]time.Time{}
	}
	if s.CompletedOnce == nil {
		s.CompletedOnce = map[string]bool{}
	}
	if s.PendingResumes == nil {
		s.PendingResumes = map[string]PendingResume{}
	}
	if s.CollapsedGroups == nil {
		s.CollapsedGroups = map[string]bool{}
	}
	if s.NotifiedProviderHealth == nil {
		s.NotifiedProviderHealth = map[string]string{}
	}
	if s.CreditChecks == nil {
		s.CreditChecks = map[string]CreditCheck{}
	}
}

// NotifiedProviderState returns the provider health state the operator was last
// notified about, or "" when none has been reported yet.
func (s *State) NotifiedProviderState(providerID string) string {
	return s.NotifiedProviderHealth[providerID]
}

// SetNotifiedProviderState records the health state just reported for a
// provider, so the same unresolved condition is not announced again.
func (s *State) SetNotifiedProviderState(providerID, state string) {
	s.NotifiedProviderHealth[providerID] = state
}

// GroupCollapsed reports whether a group's section is folded shut.
func (s *State) GroupCollapsed(group string) bool { return s.CollapsedGroups[group] }

// SetGroupCollapsed records whether a group's section is folded shut. An open
// group is the default, so it is stored by absence rather than as false.
func (s *State) SetGroupCollapsed(group string, collapsed bool) {
	if collapsed {
		s.CollapsedGroups[group] = true
		return
	}
	delete(s.CollapsedGroups, group)
}

// KeepGroups drops what is remembered about every group not in keep. A group
// exists only as long as a task names it, so this is what stops the fold state
// of a long-gone group from coming back when its name is used again.
func (s *State) KeepGroups(keep map[string]bool) {
	for g := range s.CollapsedGroups {
		if !keep[g] {
			delete(s.CollapsedGroups, g)
		}
	}
}

// ForgetProvider drops what is remembered about a provider id, so an instance
// removed and later re-added does not inherit the old one's notification memo.
func (s *State) ForgetProvider(providerID string) {
	delete(s.NotifiedProviderHealth, providerID)
	delete(s.CreditChecks, providerID)
}

// PendingResume is an interrupted session waiting to continue.
type PendingResume struct {
	// SessionID is the harness's own id for the conversation.
	SessionID string `json:"session_id"`
	// ProviderID is the instance that issued it. A task moved to another
	// provider cannot continue a session the old one owns, and dispatchq does not
	// try — it starts fresh and says so.
	ProviderID string `json:"provider_id,omitempty"`
	// AfterInterrupt marks a session that continues because DispatchQ itself
	// cut its run off (a shutdown, a daemon that died), not because of a limit.
	// Such a session gets one more go: interrupted again, the task is retired.
	AfterInterrupt bool `json:"after_interrupt,omitempty"`
	// InterruptedRun is the run whose interruption planned a continuation of
	// this task, so the same run met again (a daemon that died before
	// recording it) does not count as a second interruption. It outlives the
	// continuation's own pauses, unlike AfterInterrupt: a limit that pauses
	// the continuation makes it an ordinary limit resume, but its one more go
	// is spent.
	InterruptedRun string `json:"interrupted_run,omitempty"`
}

// UnmarshalJSON accepts the shape written before the provider travelled with
// the session: a bare session id. Such an entry names no provider, which the
// engine reads as "the one the task runs on now" — the only provider there was
// when it was written.
func (p *PendingResume) UnmarshalJSON(data []byte) error {
	var sessionID string
	if err := json.Unmarshal(data, &sessionID); err == nil {
		p.SessionID, p.ProviderID = sessionID, ""
		return nil
	}
	type raw PendingResume // avoid recursing into this method
	var out raw
	if err := json.Unmarshal(data, &out); err != nil {
		return err
	}
	*p = PendingResume(out)
	return nil
}

// IsRead reports whether a run has been read.
func (s *State) IsRead(runID string) bool { return s.ReadRuns[runID] }

// MarkRead marks a single run as read.
func (s *State) MarkRead(runID string) { s.ReadRuns[runID] = true }

// MarkAllRead marks every given run id as read.
func (s *State) MarkAllRead(runIDs []string) {
	for _, id := range runIDs {
		s.ReadRuns[id] = true
	}
}

// IsArtifactRead reports whether an artifact has been read.
func (s *State) IsArtifactRead(id string) bool { return s.ReadArtifacts[id] }

// MarkArtifactRead marks a single artifact as read.
func (s *State) MarkArtifactRead(id string) { s.ReadArtifacts[id] = true }

// MarkAllArtifactsRead marks every given artifact id as read.
func (s *State) MarkAllArtifactsRead(ids []string) {
	for _, id := range ids {
		s.ReadArtifacts[id] = true
	}
}

// ForgetArtifact drops an artifact's read- and notified-status (used when it is
// deleted), so state.json does not accumulate entries for artifacts that are
// long gone.
func (s *State) ForgetArtifact(id string) {
	delete(s.ReadArtifacts, id)
	delete(s.NotifiedArtifacts, id)
}

// IsArtifactNotified reports whether the "new artifact" notification for an
// artifact has already been sent.
func (s *State) IsArtifactNotified(id string) bool { return s.NotifiedArtifacts[id] }

// MarkArtifactNotified records that an artifact has been notified about.
func (s *State) MarkArtifactNotified(id string) { s.NotifiedArtifacts[id] = true }

// PrimeArtifactNotify marks the given artifacts as already notified without
// sending anything and flips ArtifactNotifyPrimed. It is called once, the first
// time the daemon looks at the artifact list, so the artifacts published before
// this feature existed stay silent while every later one notifies.
func (s *State) PrimeArtifactNotify(ids []string) {
	for _, id := range ids {
		s.NotifiedArtifacts[id] = true
	}
	s.ArtifactNotifyPrimed = true
}

// IsArtifactNotifyPrimed reports whether the pre-existing artifacts have been
// taken stock of (see PrimeArtifactNotify).
func (s *State) IsArtifactNotifyPrimed() bool { return s.ArtifactNotifyPrimed }

// RecordStart records that a task started at t.
func (s *State) RecordStart(taskID string, t time.Time) {
	s.LastStarted[taskID] = t
}

// RecordRun records that a task actually began a run at t. Callers that only
// anchor a cron schedule use RecordStart alone.
func (s *State) RecordRun(taskID string, t time.Time) {
	if s.LastRunAt == nil {
		s.LastRunAt = map[string]time.Time{}
	}
	s.LastRunAt[taskID] = t
}

// LastRun returns the start time of a task's most recent actual run and
// whether one exists.
func (s *State) LastRun(taskID string) (time.Time, bool) {
	t, ok := s.LastRunAt[taskID]
	return t, ok
}

// LastStart returns the last start time for a task and whether one exists.
func (s *State) LastStart(taskID string) (time.Time, bool) {
	t, ok := s.LastStarted[taskID]
	return t, ok
}

// MarkCompletedOnce records that a one-shot task has run.
func (s *State) MarkCompletedOnce(taskID string) { s.CompletedOnce[taskID] = true }

// OweRemoval records that a completed one-shot task still has to leave the
// queue.
func (s *State) OweRemoval(taskID string) {
	if s.RemovalOwed == nil {
		s.RemovalOwed = map[string]bool{}
	}
	s.RemovalOwed[taskID] = true
}

// ClearRemovalOwed records that the task's removal is done or no longer due.
func (s *State) ClearRemovalOwed(taskID string) { delete(s.RemovalOwed, taskID) }

// IsRemovalOwed reports whether a task's removal from the queue is still owed.
func (s *State) IsRemovalOwed(taskID string) bool { return s.RemovalOwed[taskID] }

// hasLiveRun reports whether a run of the task is recorded as in flight.
func (s *State) hasLiveRun(taskID string) bool {
	for _, l := range s.LiveRuns {
		if l.Run.TaskID == taskID {
			return true
		}
	}
	return false
}

// ClearCompletedOnce puts a one-shot task back in the queue.
func (s *State) ClearCompletedOnce(taskID string) { delete(s.CompletedOnce, taskID) }

// IsCompletedOnce reports whether a one-shot task has already run.
func (s *State) IsCompletedOnce(taskID string) bool { return s.CompletedOnce[taskID] }

// PendingResume returns the session a task should continue, and whether there
// is one.
func (s *State) PendingResume(taskID string) (PendingResume, bool) {
	p, ok := s.PendingResumes[taskID]
	return p, ok && p.SessionID != ""
}

// SetPendingResume records that a task should continue the given session on the
// provider that issued it.
//
// A task already continuing after an interruption keeps the record of that
// (InterruptedRun) when a limit pauses it, whatever session id the harness
// reports: it was given its one more go, and a pause on the way does not give
// it another.
func (s *State) SetPendingResume(taskID, sessionID, providerID string) {
	next := PendingResume{SessionID: sessionID, ProviderID: providerID}
	if prev, ok := s.PendingResumes[taskID]; ok {
		next.InterruptedRun = prev.InterruptedRun
	}
	s.PendingResumes[taskID] = next
}

// SetInterruptedResume records that a task should continue the session a
// DispatchQ shutdown or crash cut off (see PendingResume.AfterInterrupt).
func (s *State) SetInterruptedResume(taskID, sessionID, providerID, runID string) {
	s.PendingResumes[taskID] = PendingResume{
		SessionID: sessionID, ProviderID: providerID, AfterInterrupt: true, InterruptedRun: runID,
	}
}

// ClearPendingResume clears any pending resume for a task.
func (s *State) ClearPendingResume(taskID string) { delete(s.PendingResumes, taskID) }

// ForgetTask drops everything recorded about a task id — last start,
// completed-once flag, pending resume — so a task created later under the
// same id starts from a clean slate instead of inheriting a dead task's
// scheduling history.
func (s *State) ForgetTask(taskID string) {
	delete(s.RemovalOwed, taskID)
	delete(s.LastStarted, taskID)
	delete(s.LastRunAt, taskID)
	delete(s.CompletedOnce, taskID)
	delete(s.PendingResumes, taskID)
}

// DismissUpdate records that the user dismissed the update prompt for a version.
func (s *State) DismissUpdate(version string) { s.DismissedUpdateVersion = version }

// DismissedUpdate returns the last dismissed update version, or "" if none.
func (s *State) DismissedUpdate() string { return s.DismissedUpdateVersion }

// PrimeWhatsNew marks the "What's new" edition as seen, and the rename check as
// done, on a fresh installation, one with neither a config nor a state yet: the
// dialog introduces changes to people who used the app before, not to someone
// opening it for the first time.
func (s *Store) PrimeWhatsNew(edition string) error {
	for _, name := range []string{configFile, stateFile} {
		if _, err := os.Stat(s.path(name)); err == nil || !errors.Is(err, os.ErrNotExist) {
			return nil
		}
	}
	return s.UpdateState(func(st *State) error {
		st.WhatsNewSeen = edition
		st.RenameCheckDone = true
		return nil
	})
}
