package engine

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/danielmaier42/claudeq/internal/clock"
	"github.com/danielmaier42/claudeq/internal/executor"
	"github.com/danielmaier42/claudeq/internal/notify"
	"github.com/danielmaier42/claudeq/internal/provider"
	"github.com/danielmaier42/claudeq/internal/store"
	"github.com/danielmaier42/claudeq/internal/task"
)

// outcomes is a runner whose next results the test sets one by one.
type outcomes struct {
	mu   sync.Mutex
	next []store.RunStatus
}

func (o *outcomes) Run(_ context.Context, req executor.Request) (provider.Result, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	status := store.StatusSuccess
	if len(o.next) > 0 {
		status, o.next = o.next[0], o.next[1:]
	}
	res := provider.Result{Status: status, SessionID: req.SessionID}
	if status == store.StatusFailed {
		res.Message = "watcher could not reach the board"
	}
	return res, nil
}

func (o *outcomes) push(s ...store.RunStatus) {
	o.mu.Lock()
	o.next = append(o.next, s...)
	o.mu.Unlock()
}

// watchTask is a recurring job, run by hand in these tests.
func watchTask() task.Task {
	w := asapTask("watch", false)
	w.Trigger, w.Cron = task.TriggerCron, "0 0 1 1 *"
	return w
}

func runNow(t *testing.T, e *Engine, id string) {
	t.Helper()
	if err := e.RunTaskNow(context.Background(), id); err != nil {
		t.Fatalf("RunTaskNow: %v", err)
	}
	e.WaitIdle()
}

func inbox(t *testing.T, st *store.Store) []store.InboxEntry {
	t.Helper()
	list, err := st.Inbox()
	if err != nil {
		t.Fatalf("Inbox: %v", err)
	}
	return list
}

// A watcher that fails every few minutes used to push every single failure.
// Now the first failure pushes, the others only update its inbox entry, and
// the first success afterwards pushes once.
func TestRepeatedFailuresPushOnceAndTheRecoveryOnce(t *testing.T) {
	fc := clock.NewFake(time.Date(2026, 11, 10, 8, 0, 0, 0, time.Local))
	r := &outcomes{}
	e, st := newTestEngine(t, r, fc)
	n := &capturingNotifier{}
	e.SetNotifier(n)
	e.deliveryLog, e.deliveryErrLog = &bytes.Buffer{}, &bytes.Buffer{}
	saveTasks(t, st, watchTask())

	r.push(store.StatusFailed, store.StatusFailed, store.StatusFailed)
	for range 3 {
		runNow(t, e, "watch")
		fc.Advance(5 * time.Minute)
	}

	msgs := n.all()
	if len(msgs) != 1 {
		t.Fatalf("three failures pushed %d notifications, want 1: %+v", len(msgs), msgs)
	}
	first := msgs[0]
	if first.Priority != notify.PriorityHigh || !strings.Contains(first.Message, "Run run-1") {
		t.Fatalf("failure push = %+v, want high priority naming the run", first)
	}
	entries := inbox(t, st)
	if len(entries) != 1 || entries[0].Title != "DispatchQ ✗ watch failing since 08:00 (3 times)" || entries[0].RunID != "run-3" {
		t.Fatalf("inbox = %+v, want one entry counting the streak", entries)
	}

	runNow(t, e, "watch") // succeeds
	runNow(t, e, "watch") // and again: nothing more to say
	msgs = n.all()
	if len(msgs) != 2 {
		t.Fatalf("after the recovery %d notifications, want 2: %+v", len(msgs), msgs)
	}
	if rec := msgs[1]; rec.Title != "DispatchQ ✓ watch works again" || rec.Kind != store.InboxKindSuccess || rec.Priority != notify.PriorityNormal {
		t.Fatalf("recovery push = %+v", rec)
	}
	if open, _ := st.FailingTasks(); len(open) != 0 {
		t.Fatalf("streak still open after the recovery: %+v", open)
	}
}

func TestFailureStreakSurvivesARestart(t *testing.T) {
	fc := clock.NewFake(time.Date(2026, 11, 10, 8, 0, 0, 0, time.Local))
	r := &outcomes{}
	e, st := newTestEngine(t, r, fc)
	n := &capturingNotifier{}
	e.SetNotifier(n)
	saveTasks(t, st, watchTask())
	r.push(store.StatusFailed)
	runNow(t, e, "watch")

	next, _ := newTestEngine(t, r, fc)
	next.store = st
	next.newRunID = func() string { return "run-after-restart" }
	n2 := &capturingNotifier{}
	next.SetNotifier(n2)
	r.push(store.StatusFailed)
	runNow(t, next, "watch")
	runNow(t, next, "watch")

	msgs := n2.all()
	if len(msgs) != 1 || msgs[0].Title != "DispatchQ ✓ watch works again" {
		t.Fatalf("after the restart = %+v, want only the recovery", msgs)
	}
}

func TestLongFailureStreakIsPushedAgainAsAReminder(t *testing.T) {
	fc := clock.NewFake(time.Date(2026, 11, 10, 8, 0, 0, 0, time.Local))
	r := &outcomes{}
	e, st := newTestEngine(t, r, fc)
	n := &capturingNotifier{}
	e.SetNotifier(n)
	saveTasks(t, st, watchTask())
	r.push(store.StatusFailed, store.StatusFailed, store.StatusFailed)

	runNow(t, e, "watch")
	fc.Advance(FailureReminderAfter - time.Minute)
	runNow(t, e, "watch")
	fc.Advance(time.Minute)
	runNow(t, e, "watch")

	msgs := n.all()
	if len(msgs) != 2 || !strings.Contains(msgs[1].Title, "failing since") || !strings.Contains(msgs[1].Title, "(3 times)") {
		t.Fatalf("pushes = %+v, want the first failure and one reminder", msgs)
	}
}

// A provider pause that lasts hours used to be silent: the queue just stopped.
func TestLongRateLimitPauseIsAnnouncedOnce(t *testing.T) {
	start := time.Date(2026, 11, 10, 9, 0, 0, 0, time.Local)
	fc := clock.NewFake(start)
	r := &stub{result: func(req executor.Request, _ int) provider.Result {
		return provider.Result{Status: store.StatusRateLimited, SessionID: req.SessionID, ResetAt: start.Add(5 * time.Hour)}
	}}
	e, st := newTestEngine(t, r, fc)
	n := &capturingNotifier{}
	e.SetNotifier(n)
	saveTasks(t, st, asapTask("a", true), asapTask("b", true))
	tick(t, e)
	e.WaitIdle()

	msgs := n.all()
	if len(msgs) != 1 || msgs[0].Kind != store.InboxKindProvider || !strings.Contains(msgs[0].Title, "paused until 14:01") {
		t.Fatalf("pushes = %+v, want one pause announcement", msgs)
	}
}

func TestShortRateLimitPauseIsNotAnnounced(t *testing.T) {
	fc := clock.NewFake(time.Date(2026, 11, 10, 9, 0, 0, 0, time.Local))
	r := &stub{result: func(req executor.Request, _ int) provider.Result {
		return provider.Result{Status: store.StatusRateLimited, SessionID: req.SessionID, RetryAfter: time.Hour}
	}}
	e, st := newTestEngine(t, r, fc)
	n := &capturingNotifier{}
	e.SetNotifier(n)
	saveTasks(t, st, asapTask("a", false))
	tick(t, e)
	e.WaitIdle()
	if msgs := n.all(); len(msgs) != 0 {
		t.Fatalf("a one-hour pause pushed %+v", msgs)
	}
}

// Short pauses back to back (no reset time reported, so the blind backoff)
// add up to one long pause.
func TestChainedShortPausesAreAnnouncedOnceTheyAddUp(t *testing.T) {
	at := time.Date(2026, 11, 10, 9, 0, 0, 0, time.Local)
	e, _ := newTestEngine(t, &stub{}, clock.NewFake(at))
	n := &capturingNotifier{}
	e.SetNotifier(n)
	for i := range 10 {
		started := at.Add(time.Duration(i) * 16 * time.Minute)
		resume := started.Add(15 * time.Minute)
		e.notePause(store.Run{RunID: "r", TaskName: "a", StartedAt: started, ResumeAt: &resume,
			Provider: store.RunProvider{ID: "claude", Name: "Claude"}}, started)
	}
	if msgs := n.all(); len(msgs) != 1 {
		t.Fatalf("chained pauses pushed %d notifications, want 1", len(msgs))
	}
}

func TestCancellationIsNotAnnounced(t *testing.T) {
	e, _ := newTestEngine(t, &stub{}, clock.NewFake(time.Now()))
	n := &capturingNotifier{}
	e.SetNotifier(n)
	e.notifyOutcome(asapTask("a", false), store.Run{RunID: "r", TaskID: "a", Status: store.StatusCanceled}, "")
	if msgs := n.all(); len(msgs) != 0 {
		t.Fatalf("a cancellation pushed %+v", msgs)
	}
}

// A due task that sits in the queue for long used to show up only in the log.
func TestLongWaitIsAnnouncedOncePerWait(t *testing.T) {
	fc := clock.NewFake(time.Date(2026, 10, 5, 15, 40, 0, 0, time.UTC))
	r := &stub{block: make(chan struct{})}
	e, st := newTestEngine(t, r, fc)
	e.holdLog = &bytes.Buffer{}
	n := &capturingNotifier{}
	e.SetNotifier(n)
	saveTasks(t, st, asapTask("first", false), asapTask("second", true))

	tick(t, e)
	waitFor(t, func() bool { r.mu.Lock(); defer r.mu.Unlock(); return r.active == 1 })
	fc.Advance(HoldNotifyAfter - time.Minute)
	tick(t, e)
	fc.Advance(time.Minute)
	tick(t, e)
	fc.Advance(time.Hour)
	tick(t, e)
	waitFor(t, func() bool { return len(n.all()) > 0 })
	close(r.block)
	e.WaitIdle()

	msgs := n.all()
	if len(msgs) != 1 || msgs[0].Kind != store.InboxKindWaiting || msgs[0].Title != "DispatchQ: second is waiting" ||
		!strings.Contains(msgs[0].Message, "Waiting for 30m0s") {
		t.Fatalf("pushes = %+v, want one waiting notice", msgs)
	}
}

func TestExpectedWaitsAreNotAnnounced(t *testing.T) {
	fc := clock.NewFake(time.Date(2026, 10, 5, 15, 40, 0, 0, time.UTC))
	e, st := newTestEngine(t, &stub{}, fc)
	e.holdLog = &bytes.Buffer{}
	n := &capturingNotifier{}
	e.SetNotifier(n)
	saveTasks(t, st, asapTask("a", true))
	// Waiting out the allowance is the pause notification's business.
	e.gates.For(store.DefaultProviderID).BlockFor(10 * time.Hour)
	for range 3 {
		tick(t, e)
		fc.Advance(time.Hour)
	}
	// So is a pause the operator switched on.
	savePaused(t, st, asapTask("a", true))
	for range 3 {
		tick(t, e)
		fc.Advance(time.Hour)
	}
	e.WaitIdle()
	if msgs := n.all(); len(msgs) != 0 {
		t.Fatalf("expected waits pushed %+v", msgs)
	}
}

func TestBackfillWaitIsAnnouncedOnlyAfterADay(t *testing.T) {
	limits := holdLimits(store.Config{Tasks: []task.Task{{ID: "b", Backfill: true}, {ID: "n"}}}, map[string]bool{"x": true})
	if got := limits("b"); got != BackfillHoldNotifyAfter {
		t.Errorf("backfill limit = %s", got)
	}
	if got := limits("n"); got != HoldNotifyAfter {
		t.Errorf("normal limit = %s", got)
	}
	if got := limits("x"); got != 0 {
		t.Errorf("explained limit = %s, want never", got)
	}
}

// A run started in a wake without network failed at once, and so did its
// push. It now waits for the network instead.
func TestAgentTaskWaitsForTheNetwork(t *testing.T) {
	fc := clock.NewFake(time.Date(2026, 11, 9, 3, 15, 0, 0, time.UTC))
	r := &stub{}
	e, st := newTestEngine(t, r, fc)
	e.holdLog = &bytes.Buffer{}
	e.deliveryLog, e.deliveryErrLog = &bytes.Buffer{}, &bytes.Buffer{}
	var mu sync.Mutex
	offline := true
	var asked []string
	e.SetNetworkCheck(func(_ context.Context, host string) error {
		mu.Lock()
		defer mu.Unlock()
		asked = append(asked, host)
		if offline {
			return errors.New("lookup " + host + ": no such host")
		}
		return nil
	})
	saveTasks(t, st, asapTask("a", false))

	tick(t, e)
	e.WaitIdle()
	if got := len(r.requests()); got != 0 {
		t.Fatalf("started %d run(s) without network", got)
	}
	if h := e.Holds()["a"]; !strings.Contains(h.Reason, "No network connection") {
		t.Fatalf("hold = %+v", h)
	}
	if len(asked) != 1 || asked[0] != "api.anthropic.com" {
		t.Fatalf("looked up %v", asked)
	}

	mu.Lock()
	offline = false
	mu.Unlock()
	tick(t, e)
	e.WaitIdle()
	if got := len(r.requests()); got != 1 {
		t.Fatalf("started %d run(s) once online, want 1", got)
	}
}

func TestScriptTaskDoesNotWaitForTheNetwork(t *testing.T) {
	fc := clock.NewFake(time.Date(2026, 11, 9, 3, 15, 0, 0, time.UTC))
	r := &stub{}
	e, st := newTestEngine(t, r, fc)
	called := false
	e.SetNetworkCheck(func(context.Context, string) error { called = true; return errors.New("offline") })
	tk := asapTask("s", false)
	tk.Kind = task.KindScript
	saveTasks(t, st, tk)
	tick(t, e)
	e.WaitIdle()
	if called || len(r.requests()) != 1 {
		t.Fatalf("script: network asked %v, runs %d", called, len(r.requests()))
	}
}

// A reminder keeps the streak's one row in the Notifications view.
func TestFailureReminderKeepsOneInboxRow(t *testing.T) {
	fc := clock.NewFake(time.Date(2026, 11, 10, 8, 0, 0, 0, time.Local))
	r := &outcomes{}
	e, st := newTestEngine(t, r, fc)
	e.SetNotifier(&capturingNotifier{})
	saveTasks(t, st, watchTask())
	r.push(store.StatusFailed, store.StatusFailed)
	runNow(t, e, "watch")
	fc.Advance(FailureReminderAfter)
	runNow(t, e, "watch")
	if entries := inbox(t, st); len(entries) != 1 || !strings.Contains(entries[0].Title, "(2 times)") {
		t.Fatalf("inbox = %+v, want the one streak row", entries)
	}
}

// A run that was under way when the limit hit and then finished proves
// nothing about the pause: the chain keeps counting.
func TestRunStartedBeforeThePauseDoesNotEndIt(t *testing.T) {
	at := time.Date(2026, 11, 10, 9, 0, 0, 0, time.Local)
	e, _ := newTestEngine(t, &stub{}, clock.NewFake(at))
	n := &capturingNotifier{}
	e.SetNotifier(n)
	prov := store.RunProvider{ID: "claude", Name: "Claude"}
	for i := range 10 {
		started := at.Add(time.Duration(i) * 16 * time.Minute)
		resume := started.Add(15 * time.Minute)
		e.notePause(store.Run{RunID: "r", TaskName: "a", StartedAt: started, ResumeAt: &resume, Provider: prov}, started)
		e.notifyOutcome(asapTask("b", true), store.Run{RunID: "p", TaskID: "b", Status: store.StatusFailed,
			StartedAt: at.Add(-time.Minute), Provider: prov}, "")
	}
	pauses := 0
	for _, m := range n.all() {
		if m.Kind == store.InboxKindProvider {
			pauses++
		}
	}
	if pauses != 1 {
		t.Fatalf("pause announcements = %d, want 1", pauses)
	}
}

// Time held for a reason that is not announced does not count towards the
// waiting notice once the reason changes.
func TestExplainedWaitDoesNotCountTowardsTheNotice(t *testing.T) {
	now := time.Date(2026, 11, 10, 9, 0, 0, 0, time.UTC)
	var h holds
	log := &bytes.Buffer{}
	explained := func(string) time.Duration { return 0 }
	unexplained := func(string) time.Duration { return HoldNotifyAfter }
	h.update(map[string]string{"a": "No network connection"}, now, log, explained)
	now = now.Add(3 * time.Hour)
	if long := h.update(map[string]string{"a": "runs alone"}, now, log, unexplained); len(long) != 0 {
		t.Fatalf("announced at once after an explained wait: %+v", long)
	}
	now = now.Add(HoldNotifyAfter)
	if long := h.update(map[string]string{"a": "runs alone"}, now, log, unexplained); len(long) != 1 || long[0].waited != HoldNotifyAfter {
		t.Fatalf("long = %+v, want one notice counting 30 minutes", long)
	}
}

func TestAPIHostFollowsTheInstanceEnvironment(t *testing.T) {
	cases := []struct {
		kind provider.Kind
		env  map[string]string
		want string
	}{
		{provider.KindClaudeCode, nil, "api.anthropic.com"},
		{provider.KindCodex, nil, "api.openai.com"},
		{provider.KindOpencode, nil, ""},
		{provider.KindClaudeCode, map[string]string{"ANTHROPIC_BASE_URL": "https://llm.corp.example/anthropic"}, "llm.corp.example"},
		{provider.KindClaudeCode, map[string]string{"ANTHROPIC_BASE_URL": "http://127.0.0.1:4000"}, ""},
		{provider.KindClaudeCode, map[string]string{"CLAUDE_CODE_USE_BEDROCK": "1"}, ""},
		{provider.KindCodex, map[string]string{"HTTPS_PROXY": "http://proxy:3128"}, ""},
	}
	for _, c := range cases {
		if got := apiHost(provider.Instance{Kind: c.kind, Env: c.env}); got != c.want {
			t.Errorf("%s %v: host = %q, want %q", c.kind, c.env, got, c.want)
		}
	}
}
