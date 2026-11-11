package engine

import (
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/danielmaier42/claudeq/internal/notify"
	"github.com/danielmaier42/claudeq/internal/store"
	"github.com/danielmaier42/claudeq/internal/task"
)

// When the daemon speaks up about something that is not a single run's
// outcome. Each is announced once per episode: a task that keeps failing,
// a provider that stays paused, a task that keeps waiting.
const (
	// FailureReminderAfter is how long a task may keep failing before the
	// streak is pushed again. In between, every further failure only updates
	// the streak's entry in the Notifications view.
	FailureReminderAfter = 12 * time.Hour
	// PauseNotifyAfter is how long a provider's rate-limit pause must last
	// before it is announced. Shorter ones resume by themselves while nobody
	// is looking, which is the point of the queue.
	PauseNotifyAfter = 2 * time.Hour
	// pauseChainGap is how soon after a pause ended a new one still counts as
	// the same: a resumed session that hits the limit again at once.
	pauseChainGap = 30 * time.Minute
	// HoldNotifyAfter is how long a due task may wait without starting before
	// the operator is told, for a reason nothing else announces (it runs alone
	// and the parallel work keeps going, its pool has no member, ...).
	HoldNotifyAfter = 30 * time.Minute
	// BackfillHoldNotifyAfter is the same for a backfill task, whose job is
	// to wait for spare allowance: only a wait of a whole day is unusual.
	BackfillHoldNotifyAfter = 24 * time.Hour
)

// alerts is the memory of the episodes that are not kept on disk.
type alerts struct {
	mu sync.Mutex
	// pauses is the rate-limit pause in progress per provider instance.
	pauses map[string]pauseEpisode
}

// pauseEpisode is one provider pause: since when the provider has been
// limited, until when it is now expected to stay so, and whether that was
// announced already.
type pauseEpisode struct {
	since, until time.Time
	notified     bool
}

// notifyOutcome announces a finished run. Failures and auth problems notify
// (FA-35/FA-38) with high priority, once per failing streak: the first failure
// is pushed, further ones only update that notification's inbox entry, a
// reminder goes out after FailureReminderAfter, and the first success after
// the streak is pushed as a recovery. Successes notify otherwise only when the
// task opted in via NotifyOnResult, with the last result message. A
// rate-limit pause is announced once it lasts PauseNotifyAfter. A cancellation
// is not announced at all: the operator asked for it, and the Log records it.
func (e *Engine) notifyOutcome(t task.Task, rec store.Run, resultText string) {
	if e.notifier == nil {
		return
	}
	at := e.clock.Now()
	if rec.FinishedAt != nil {
		at = *rec.FinishedAt
	}
	if rec.Status == store.StatusRateLimited {
		e.notePause(rec, at)
		return
	}
	if rec.Status != store.StatusCanceled {
		e.endPause(rec.Provider.ID, rec.StartedAt)
	}
	msg := truncateRunes(strings.TrimSpace(resultText), 300)
	switch rec.Status {
	case store.StatusSuccess:
		e.notifySuccess(t, rec, msg, at)
	case store.StatusFailed, store.StatusAuthError:
		e.notifyFailure(failureNotification(t, rec, msg), at)
	}
}

// failureNotification is what a failed run says on its own.
func failureNotification(t task.Task, rec store.Run, msg string) notify.Notification {
	n := notify.Notification{
		Kind: store.InboxKindFailure, Priority: notify.PriorityHigh,
		RunID: rec.RunID, TaskID: rec.TaskID, TaskName: rec.TaskName,
	}
	if rec.Status == store.StatusAuthError {
		n.Title = "DispatchQ: login problem"
		n.Message = rec.TaskName + ": Claude Code authentication problem — please re-login."
	} else {
		n.Title = "DispatchQ ✗ " + rec.TaskName + " failed"
		switch {
		case t.NotifyOnResult && msg != "":
			n.Message = msg
		case rec.Error != "":
			n.Message = rec.Error
		default:
			n.Message = "Task failed."
		}
	}
	// The phone cannot open the run, but the id finds it in the Log.
	n.Message += "\nRun " + rec.RunID
	return n
}

// notifyFailure pushes the first failure of a streak and a reminder after
// FailureReminderAfter; any other failure in the streak updates its inbox
// entry instead, so the Notifications view says how long and how often.
func (e *Engine) notifyFailure(n notify.Notification, at time.Time) {
	var streak store.FailingTask
	push := false
	err := e.store.UpdateFailingTask(n.TaskID, func(cur *store.FailingTask) *store.FailingTask {
		if cur == nil {
			streak = store.FailingTask{TaskID: n.TaskID, TaskName: n.TaskName, Since: at, Count: 1, LastFailure: at, Pushed: at}
			push = true
			return &streak
		}
		streak = *cur
		streak.TaskName, streak.LastFailure = n.TaskName, at
		streak.Count++
		if at.Sub(streak.Pushed) >= FailureReminderAfter {
			streak.Pushed, push = at, true
		}
		return &streak
	})
	if err != nil {
		// Without the streak there is no telling a repeat from a first
		// failure; better one push too many than a failure nobody hears of.
		fmt.Fprintf(e.deliveryErrLog, "dispatchqd: record failure streak of %q: %v\n", n.TaskID, err)
		e.send(n)
		return
	}
	if streak.Count > 1 {
		n.Title = fmt.Sprintf("DispatchQ ✗ %s failing since %s (%d times)", n.TaskName, reopenText(streak.Since, at), streak.Count)
	}
	var id string
	switch {
	case push && streak.Count == 1:
		id = e.send(n)
	case push:
		// A reminder: the streak keeps its one row, and the channels get it
		// once more.
		id = e.refreshStreakEntry(streak, n, at)
		n.ID = id
		e.deliver(n, at)
	default:
		id = e.refreshStreakEntry(streak, n, at)
	}
	if id == "" || id == streak.InboxID {
		return
	}
	if err := e.store.UpdateFailingTask(n.TaskID, func(cur *store.FailingTask) *store.FailingTask {
		if cur != nil {
			cur.InboxID = id
		}
		return cur
	}); err != nil {
		fmt.Fprintf(e.deliveryErrLog, "dispatchqd: record failure streak of %q: %v\n", n.TaskID, err)
	}
}

// refreshStreakEntry makes the streak's inbox entry say n, unread, as the
// newest notification, without sending it anywhere. It returns the entry's id:
// a new one when the old entry is gone (dropped from a full inbox).
func (e *Engine) refreshStreakEntry(streak store.FailingTask, n notify.Notification, at time.Time) string {
	n.ID = streak.InboxID
	err := e.store.UpdateInbox(func(list *[]store.InboxEntry) error {
		i := slices.IndexFunc(*list, func(q store.InboxEntry) bool { return q.ID == n.ID })
		if i < 0 || n.ID == "" {
			n.ID = newInboxID(at)
		} else {
			*list = slices.Delete(*list, i, i+1)
		}
		*list = append(*list, inboxEntry(n, at))
		if extra := len(*list) - store.InboxLimit; extra > 0 {
			*list = (*list)[extra:]
		}
		return nil
	})
	if err != nil {
		fmt.Fprintf(e.deliveryErrLog, "dispatchqd: update notification of failing %q: %v\n", n.TaskID, err)
		return ""
	}
	return n.ID
}

// notifySuccess announces the end of a failing streak, once, or else a success
// the task opted in to.
func (e *Engine) notifySuccess(t task.Task, rec store.Run, msg string, at time.Time) {
	var ended *store.FailingTask
	// Read first and changed only when there is a streak: a success is the
	// common case, and taking the store's cross-process write lock for every
	// one of them would be for nothing.
	if open, err := e.store.FailingTasks(); err != nil {
		fmt.Fprintf(e.deliveryErrLog, "dispatchqd: read failure streaks: %v\n", err)
	} else if slices.ContainsFunc(open, func(f store.FailingTask) bool { return f.TaskID == rec.TaskID }) {
		if err := e.store.UpdateFailingTask(rec.TaskID, func(cur *store.FailingTask) *store.FailingTask {
			ended = cur
			return nil
		}); err != nil {
			fmt.Fprintf(e.deliveryErrLog, "dispatchqd: end failure streak of %q: %v\n", rec.TaskID, err)
		}
	}
	n := notify.Notification{Kind: store.InboxKindSuccess, RunID: rec.RunID, TaskID: rec.TaskID, TaskName: rec.TaskName}
	switch {
	case ended != nil:
		n.Title = "DispatchQ ✓ " + rec.TaskName + " works again"
		n.Message = fmt.Sprintf("Failed %s since %s; this run succeeded.", times(ended.Count), reopenText(ended.Since, at))
		if t.NotifyOnResult && msg != "" {
			n.Message += "\n" + msg
		}
	case t.NotifyOnResult:
		n.Title = "DispatchQ ✓ " + rec.TaskName
		n.Message = msg
		if n.Message == "" {
			n.Message = "Completed successfully."
		}
	default:
		return
	}
	e.send(n)
}

func times(n int) string {
	if n == 1 {
		return "once"
	}
	return fmt.Sprintf("%d times", n)
}

// notePause keeps track of a provider's rate-limit pause and announces it once
// it lasts PauseNotifyAfter: measured from the first limit, so a session that
// resumes and hits the limit again at once (a blind 15-minute backoff, over
// and over) counts as one long pause.
func (e *Engine) notePause(rec store.Run, at time.Time) {
	providerID := rec.Provider.ID
	if providerID == "" || rec.ResumeAt == nil {
		return
	}
	until := *rec.ResumeAt
	e.alerts.mu.Lock()
	if e.alerts.pauses == nil {
		e.alerts.pauses = map[string]pauseEpisode{}
	}
	ep, ok := e.alerts.pauses[providerID]
	if !ok || rec.StartedAt.Sub(ep.until) > pauseChainGap {
		ep = pauseEpisode{since: at}
	}
	ep.until = until
	announce := !ep.notified && until.Sub(ep.since) >= PauseNotifyAfter
	ep.notified = ep.notified || announce
	e.alerts.pauses[providerID] = ep
	e.alerts.mu.Unlock()
	if !announce {
		return
	}
	name := rec.Provider.Name
	if name == "" {
		name = providerID
	}
	e.send(notify.Notification{
		Kind:     store.InboxKindProvider,
		Title:    "DispatchQ: " + name + " paused until " + reopenText(until, at),
		Message:  rec.TaskName + " reached the allowance limit. Tasks on this provider wait until then, and the paused session continues by itself.",
		RunID:    rec.RunID,
		TaskID:   rec.TaskID,
		TaskName: rec.TaskName,
	})
}

// endPause forgets a provider's pause: a run on it that started after the
// pause began ended for another reason, so the provider is working again. A
// run that was already under way when the limit hit proves nothing.
func (e *Engine) endPause(providerID string, started time.Time) {
	e.alerts.mu.Lock()
	defer e.alerts.mu.Unlock()
	if ep, ok := e.alerts.pauses[providerID]; ok && !started.Before(ep.since) {
		delete(e.alerts.pauses, providerID)
	}
}

// holdLimits says how long each task may wait before its wait is announced:
// never while held for a reason that is announced elsewhere or is the
// operator's own doing (explained), a day for backfill work, half an hour
// for anything else.
func holdLimits(cfg store.Config, explained map[string]bool) func(id string) time.Duration {
	backfill := map[string]bool{}
	for _, t := range cfg.Tasks {
		backfill[t.ID] = t.Backfill
	}
	return func(id string) time.Duration {
		switch {
		case explained[id]:
			return 0
		case backfill[id]:
			return BackfillHoldNotifyAfter
		default:
			return HoldNotifyAfter
		}
	}
}

// taskNames maps task ids to their names, for the hold announcements.
func taskNames(cfg store.Config) map[string]string {
	names := make(map[string]string, len(cfg.Tasks))
	for _, t := range cfg.Tasks {
		names[t.ID] = t.Name
	}
	return names
}

// announceHolds tells the operator about each task that waited too long, off
// the scheduler's lock.
func (e *Engine) announceHolds(long []longHold, names map[string]string) {
	if e.notifier == nil || len(long) == 0 {
		return
	}
	e.wg.Add(1)
	go func() {
		defer e.wg.Done()
		for _, h := range long {
			name := names[h.taskID]
			if name == "" {
				name = h.taskID
			}
			e.send(notify.Notification{
				Kind:     store.InboxKindWaiting,
				Title:    "DispatchQ: " + name + " is waiting",
				Message:  fmt.Sprintf("Waiting for %s: %s", h.waited.Round(time.Minute), h.reason),
				TaskID:   h.taskID,
				TaskName: name,
			})
		}
	}()
}
