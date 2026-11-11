package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/danielmaier42/claudeq/internal/notify"
	"github.com/danielmaier42/claudeq/internal/randid"
	"github.com/danielmaier42/claudeq/internal/store"
)

// How a notification a channel did not take is tried again. A Mac that sleeps
// with the lid closed wakes for a minute every quarter of an hour, often
// without network; a push raised in such a wake used to be tried once and
// lost. Now it waits in the delivery outbox (deliveries.json) and is tried
// again with growing pauses, at once after the machine wakes, and for a day
// before the daemon gives up on it.
const (
	// DeliveryTimeout bounds one try on one channel.
	DeliveryTimeout = 15 * time.Second
	// DeliveryFirstRetry is the pause after the first failed try; it doubles
	// with every further one, up to DeliveryMaxPause.
	DeliveryFirstRetry = 30 * time.Second
	DeliveryMaxPause   = 5 * time.Minute
	// RemoteRetryWindow is how long Pushover, ntfy and the webhook are tried:
	// a failure push is worth getting the next morning.
	RemoteRetryWindow = 24 * time.Hour
	// DesktopRetryWindow is how long the desktop banner is tried. Later than
	// that it is news nobody needs on screen, and the Notifications view
	// lists it anyway.
	DesktopRetryWindow = time.Hour
	// WakeGrace is how long after a wake a try that fails on the network
	// (not a refusal: a rate limit or server error keeps its backoff) does
	// not count towards the pause. Wi-Fi often comes up a little after the
	// machine does; until then the delivery is tried every DeliveryFirstRetry
	// instead of being pushed minutes away.
	WakeGrace = 2 * time.Minute
)

// deliveries tracks which outbox entries a goroutine is trying right now, so
// the retry sweep and a fresh send never deliver the same entry twice, and
// whether the next sweep should try everything at once instead of waiting for
// each entry's pause (after a wake, or once a channel works again).
type deliveries struct {
	mu      sync.Mutex
	claimed map[string]bool
	// forceOnce makes the next sweep try everything; graceUntil is the end of
	// the grace after a wake (see WakeGrace).
	forceOnce  bool
	graceUntil time.Time
	sweeping   bool
	// remote is the remote channels last known, so a send while the settings
	// cannot be read still queues them instead of losing the push.
	remote []string
}

// claim marks ids as being tried and returns the ones nobody else holds.
func (d *deliveries) claim(ids []string) []string {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.claimed == nil {
		d.claimed = map[string]bool{}
	}
	out := make([]string, 0, len(ids))
	for _, id := range ids {
		if !d.claimed[id] {
			d.claimed[id] = true
			out = append(out, id)
		}
	}
	return out
}

func (d *deliveries) release(ids []string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, id := range ids {
		delete(d.claimed, id)
	}
}

// retrySoon makes the next sweep try every waiting delivery at once.
func (d *deliveries) retrySoon() {
	d.mu.Lock()
	d.forceOnce = true
	d.mu.Unlock()
}

// woke makes the next sweep try everything and starts the grace after a
// wake.
func (d *deliveries) woke(now time.Time) {
	d.mu.Lock()
	d.forceOnce = true
	d.graceUntil = now.Add(WakeGrace)
	d.mu.Unlock()
}

// inGrace reports whether now is within the grace after a wake.
func (d *deliveries) inGrace(now time.Time) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	return now.Before(d.graceUntil)
}

// isClaimed reports whether a goroutine is trying id right now.
func (d *deliveries) isClaimed(id string) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.claimed[id]
}

// startSweep reports whether a sweep may start (none is running) and whether
// it is to try everything at once, clearing that request.
func (d *deliveries) startSweep() (ok, force bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.sweeping {
		return false, false
	}
	d.sweeping = true
	force, d.forceOnce = d.forceOnce, false
	return true, force
}

// rememberRemote keeps the remote channels of a successful lookup.
func (d *deliveries) rememberRemote(chans []notify.Channel) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.remote = d.remote[:0]
	for _, c := range chans {
		if c.Remote {
			d.remote = append(d.remote, c.Name)
		}
	}
}

// knownRemote is the remote channels of the last successful lookup.
func (d *deliveries) knownRemote() []string {
	d.mu.Lock()
	defer d.mu.Unlock()
	return slices.Clone(d.remote)
}

func (d *deliveries) endSweep() {
	d.mu.Lock()
	d.sweeping = false
	d.mu.Unlock()
}

// channels lists where notifications go right now, by name.
func (e *Engine) channels() ([]notify.Channel, error) {
	if c, ok := e.notifier.(notify.Channels); ok {
		chans, err := c.Channels()
		if err == nil {
			e.outbox.rememberRemote(chans)
		}
		return chans, err
	}
	return []notify.Channel{{Name: "notify", Remote: true, Notifier: e.notifier}}, nil
}

// send records a notification in the inbox and delivers it to every channel.
// It returns the inbox id, empty when the inbox could not be written.
//
// Before anything goes out, the notification is written to the inbox the
// Notifications view lists, under the id the macOS notification then carries:
// a click marks that entry read, and a channel that swallows the message still
// leaves it where the operator can find it. Deliberately not bound to the
// loop's context: a notification raised moments before shutdown would
// otherwise be lost.
func (e *Engine) send(n notify.Notification) string {
	now := e.clock.Now()
	n.ID = newInboxID(now)
	if err := e.store.AddInboxEntry(inboxEntry(n, now)); err != nil {
		fmt.Fprintf(e.deliveryErrLog, "dispatchqd: notification %q not recorded: %v\n", n.Title, err)
		n.ID = "" // nothing to mark read; a click falls back to the target alone
	}
	e.deliver(n, now)
	return n.ID
}

func newInboxID(now time.Time) string {
	return "i-" + now.UTC().Format("20060102T150405") + "-" + randid.Hex(3)
}

// deliver hands n to every channel at once. Each channel's delivery is first
// written to the outbox, so one that fails, or a daemon stopped mid-send,
// leaves it there to be tried again; a channel that takes it removes it.
func (e *Engine) deliver(n notify.Notification, now time.Time) {
	chans, err := e.channels()
	// Remote channels that were there before the settings stopped loading
	// are queued without a try: the sweep sends them once the settings load
	// again, or drops them if they were switched off meanwhile.
	var later []string
	if err != nil {
		fmt.Fprintf(e.deliveryErrLog, "dispatchqd: notify: %v (sending to the channels that are known, queueing the others)\n", err)
		for _, name := range e.outbox.knownRemote() {
			if !slices.ContainsFunc(chans, func(c notify.Channel) bool { return c.Name == name }) {
				later = append(later, name)
			}
		}
	}
	if len(chans)+len(later) == 0 {
		return
	}
	body, err := json.Marshal(n)
	if err != nil {
		fmt.Fprintf(e.deliveryErrLog, "dispatchqd: notification %q not encoded: %v\n", n.Title, err)
		return
	}
	// Every send gets its own key, also when it repeats an inbox id (a
	// failure streak's reminder), so it never collides with a delivery of the
	// same notification that is being tried right now.
	key := "d-" + now.UTC().Format("20060102T150405") + "-" + randid.Hex(4)
	pending := make([]store.PendingDelivery, len(chans))
	ids := make([]string, len(chans))
	for i, c := range chans {
		pending[i] = store.PendingDelivery{ID: key + "/" + c.Name, Channel: c.Name, Notification: body, Created: now, NextTry: now}
		ids[i] = pending[i].ID
	}
	queued := slices.Clone(pending)
	for _, name := range later {
		queued = append(queued, store.PendingDelivery{ID: key + "/" + name, Channel: name, Notification: body, Created: now, NextTry: now})
	}
	// Claimed before they are written, so a retry sweep reading the outbox
	// in between finds them taken and leaves them to this send. Only what was
	// claimed is tried and released: releasing someone else's claim would let
	// a third goroutine take the same delivery.
	mine := e.outbox.claim(ids)
	defer e.outbox.release(mine)
	durable := true
	if err := e.store.UpdatePendingDeliveries(func(list *[]store.PendingDelivery) error {
		// A newer send of the same inbox notification (a streak's reminder)
		// supersedes what still waits of the older one: the phone gets the
		// latest count once, not a stale alert followed by the reminder.
		if n.ID != "" {
			*list = slices.DeleteFunc(*list, func(d store.PendingDelivery) bool {
				return deliveryInboxID(d) == n.ID && !e.outbox.isClaimed(d.ID)
			})
		}
		*list = append(*list, queued...)
		return nil
	}); err != nil {
		// Still try once: an unwritable outbox must not cost the alert itself.
		fmt.Fprintf(e.deliveryErrLog, "dispatchqd: notification %q not queued for retry: %v\n", n.Title, err)
		durable = false
	}

	var mu sync.Mutex
	var tries []deliveryTry
	var wg sync.WaitGroup
	for i, c := range chans {
		if !slices.Contains(mine, ids[i]) {
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			t := e.try(context.Background(), c, pending[i], n)
			mu.Lock()
			tries = append(tries, t)
			mu.Unlock()
		}()
	}
	wg.Wait()
	e.settle(tries, durable)
}

// deliveryTry is the outcome of one try on one channel.
type deliveryTry struct {
	channel notify.Channel
	entry   store.PendingDelivery
	title   string
	receipt string
	err     error
	at      time.Time
	// retry is a try out of the outbox, as opposed to a fresh send; grace a
	// try within the grace after a wake (see WakeGrace).
	retry, grace bool
}

// failedTry is when a failed delivery is tried next and how many tries count
// towards its pause: a network failure within the grace after a wake does not
// count and comes back after the shortest pause.
func failedTry(t deliveryTry) (next time.Time, attempts int) {
	if t.grace && !notify.Refused(t.err) {
		return t.at.Add(DeliveryFirstRetry), t.entry.Attempts
	}
	return t.at.Add(retryPause(t.entry.Attempts + 1)), t.entry.Attempts + 1
}

// try delivers n on one channel. parent is what may cut the try short: nothing
// for a fresh send, which is to go out even moments before shutdown, and the
// daemon's stop for a retry, whose delivery is safe in the outbox.
func (e *Engine) try(parent context.Context, c notify.Channel, entry store.PendingDelivery, n notify.Notification) deliveryTry {
	ctx, cancel := context.WithTimeout(parent, DeliveryTimeout)
	defer cancel()
	receipt, err := notify.Deliver(ctx, c.Notifier, n)
	return deliveryTry{channel: c, entry: entry, title: n.Title, receipt: receipt, err: err, at: e.clock.Now()}
}

// retryWindow is how long a channel keeps being tried.
func retryWindow(c notify.Channel) time.Duration {
	if c.Remote {
		return RemoteRetryWindow
	}
	return DesktopRetryWindow
}

// retryPause is the wait after the given number of failed tries.
func retryPause(attempts int) time.Duration {
	pause := DeliveryFirstRetry
	for i := 1; i < attempts && pause < DeliveryMaxPause; i++ {
		pause *= 2
	}
	return min(pause, DeliveryMaxPause)
}

// settle writes what the tries came to: an accepted delivery leaves the
// outbox and is logged with its receipt, a refused one is dropped loudly, and
// a failed one waits for its next try or, past its channel's window, is given
// up loudly. The log says it once per delivery, not once per try: a channel
// down for a night would otherwise write a line every five minutes.
func (e *Engine) settle(tries []deliveryTry, durable bool) {
	done := map[string]bool{}
	retry := map[string]store.PendingDelivery{}
	for _, t := range tries {
		switch {
		case t.err == nil:
			done[t.entry.ID] = true
			e.logAccepted(t)
		case notify.Permanent(t.err):
			done[t.entry.ID] = true
			fmt.Fprintf(e.deliveryErrLog, "dispatchqd: notification %q refused by %s, not retried: %v\n", t.title, t.channel.Name, t.err)
		case !durable:
			fmt.Fprintf(e.deliveryErrLog, "dispatchqd: notification %q not delivered by %s: %v\n", t.title, t.channel.Name, t.err)
		case t.at.Sub(t.entry.Created) >= retryWindow(t.channel):
			done[t.entry.ID] = true
			e.logLost(t.title, t.channel, t.err.Error())
		default:
			next := t.entry
			next.NextTry, next.Attempts = failedTry(t)
			next.LastError = t.err.Error()
			retry[next.ID] = next
			if t.entry.LastError == "" {
				fmt.Fprintf(e.deliveryErrLog, "dispatchqd: notification %q not delivered by %s, trying again for up to %s: %v\n",
					t.title, t.channel.Name, retryWindow(t.channel), t.err)
			}
		}
	}
	if !durable || len(done)+len(retry) == 0 {
		return
	}
	if err := e.store.UpdatePendingDeliveries(func(list *[]store.PendingDelivery) error {
		*list = slices.DeleteFunc(*list, func(d store.PendingDelivery) bool { return done[d.ID] })
		for i, d := range *list {
			if r, ok := retry[d.ID]; ok {
				(*list)[i] = r
			}
		}
		return nil
	}); err != nil {
		fmt.Fprintf(e.deliveryErrLog, "dispatchqd: update the notification outbox: %v\n", err)
	}
}

// logLost says, loudly, that a notification will never reach a channel.
func (e *Engine) logLost(title string, c notify.Channel, lastErr string) {
	fmt.Fprintf(e.deliveryErrLog, "dispatchqd: NOTIFICATION LOST: %q could not be delivered by %s within %s; last error: %s\n",
		title, c.Name, retryWindow(c), lastErr)
}

func (e *Engine) logAccepted(t deliveryTry) {
	line := fmt.Sprintf("dispatchqd: notification %q accepted by %s", t.title, t.channel.Name)
	if t.receipt != "" {
		line += " (receipt " + t.receipt + ")"
	}
	if t.retry {
		line += fmt.Sprintf(" on a retry, %s after it was raised", t.at.Sub(t.entry.Created).Round(time.Second))
	}
	fmt.Fprintln(e.deliveryLog, line)
	// A channel that just worked again is reachable: whatever else waits for
	// it need not sit out its pause.
	if t.retry {
		e.outbox.retrySoon()
	}
}

// retryDeliveries tries again every outbox entry whose pause is over, or every
// entry when force is set. Entries of a channel that is switched off, or past
// their channel's retry window, are dropped without a try: a banner from three
// hours ago is not shown as if it were new. The channels are tried side by
// side, the entries of one channel one after another, and the first of those
// that fails for a reason other than the message itself postpones the rest of
// that channel's, so a dead network costs one timeout, not one per message.
// The daemon's stop cuts the tries short; the entries stay in the outbox.
func (e *Engine) retryDeliveries(force bool) {
	if e.notifier == nil {
		return
	}
	pending, err := e.store.PendingDeliveries()
	noteErr(&e.lastDeliveryErr, "read the notification outbox", err)
	if err != nil {
		return
	}
	// Only what is due is worth the rest; most sweeps find nothing.
	now := e.clock.Now()
	ids := make([]string, 0, len(pending))
	for _, d := range pending {
		if force || !d.NextTry.After(now) {
			ids = append(ids, d.ID)
		}
	}
	if len(ids) == 0 {
		return
	}
	chans, err := e.channels()
	if err != nil {
		// Which channels exist is unknown, so nothing may be dropped as
		// switched off; the next sweep tries again.
		noteErr(&e.lastChannelsErr, "notify", err)
		return
	}
	e.lastChannelsErr = ""
	byName := make(map[string]notify.Channel, len(chans))
	for _, c := range chans {
		byName[c.Name] = c
	}
	if e.afterOutboxRead != nil {
		e.afterOutboxRead()
	}
	mine := e.outbox.claim(ids)
	defer e.outbox.release(mine)
	held := make(map[string]bool, len(mine))
	for _, id := range mine {
		held[id] = true
	}
	// What was read before the claim may be stale: a send that held an entry
	// meanwhile may have delivered and removed it, or counted a try. Only
	// what is still in the outbox, as it is now, is tried.
	pending, err = e.store.PendingDeliveries()
	noteErr(&e.lastDeliveryErr, "read the notification outbox", err)
	if err != nil {
		return
	}

	gone := map[string]bool{}
	queues := map[string][]store.PendingDelivery{}
	for _, d := range pending {
		if !held[d.ID] || (!force && d.NextTry.After(now)) {
			continue
		}
		c, ok := byName[d.Channel]
		switch {
		case !ok:
			gone[d.ID] = true
		case now.Sub(d.Created) >= retryWindow(c):
			gone[d.ID] = true
			e.logLost(deliveryTitle(d), c, d.LastError)
		default:
			queues[d.Channel] = append(queues[d.Channel], d)
		}
	}

	grace := e.outbox.inGrace(now)
	var mu sync.Mutex
	var tries []deliveryTry
	postponed := map[string]time.Time{}
	var wg sync.WaitGroup
	for name, queue := range queues {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c := byName[name]
			for i, d := range queue {
				var n notify.Notification
				if err := json.Unmarshal(d.Notification, &n); err != nil {
					fmt.Fprintf(e.deliveryErrLog, "dispatchqd: drop unreadable notification %s from the outbox: %v\n", d.ID, err)
					mu.Lock()
					gone[d.ID] = true
					mu.Unlock()
					continue
				}
				t := e.try(e.runCtx, c, d, n)
				t.retry, t.grace = true, grace
				mu.Lock()
				tries = append(tries, t)
				if t.err != nil && !notify.Permanent(t.err) {
					next, _ := failedTry(t)
					for _, rest := range queue[i+1:] {
						postponed[rest.ID] = next
					}
					mu.Unlock()
					return
				}
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	e.settle(tries, true)
	if len(gone)+len(postponed) == 0 {
		return
	}
	if err := e.store.UpdatePendingDeliveries(func(list *[]store.PendingDelivery) error {
		*list = slices.DeleteFunc(*list, func(d store.PendingDelivery) bool { return gone[d.ID] })
		for i, d := range *list {
			if next, ok := postponed[d.ID]; ok {
				(*list)[i].NextTry = next
			}
		}
		return nil
	}); err != nil {
		fmt.Fprintf(e.deliveryErrLog, "dispatchqd: update the notification outbox: %v\n", err)
	}
}

// deliveryInboxID is the inbox id of the notification a delivery carries.
func deliveryInboxID(d store.PendingDelivery) string {
	var n notify.Notification
	if json.Unmarshal(d.Notification, &n) != nil {
		return ""
	}
	return n.ID
}

// deliveryTitle is the title of a waiting delivery, for the log.
func deliveryTitle(d store.PendingDelivery) string {
	var n notify.Notification
	if json.Unmarshal(d.Notification, &n) != nil || n.Title == "" {
		return d.ID
	}
	return n.Title
}

// sweepDeliveries runs one retry sweep in the background unless one is
// already running: a channel that times out must not hold up the next tick.
func (e *Engine) sweepDeliveries(woke bool) {
	if e.notifier == nil {
		return
	}
	if woke {
		e.outbox.woke(e.clock.Now())
	}
	ok, force := e.outbox.startSweep()
	if !ok {
		return
	}
	e.wg.Add(1)
	go func() {
		defer e.wg.Done()
		defer e.outbox.endSweep()
		e.retryDeliveries(force)
	}()
}

// inboxEntry is the inbox record of a notification about to be sent.
func inboxEntry(n notify.Notification, sentAt time.Time) store.InboxEntry {
	return store.InboxEntry{
		ID:         n.ID,
		Kind:       n.Kind,
		Title:      n.Title,
		Message:    n.Message,
		ArtifactID: n.ArtifactID,
		RunID:      n.RunID,
		URL:        n.URL,
		TaskID:     n.TaskID,
		TaskName:   n.TaskName,
		SentAt:     sentAt,
	}
}
