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
	"github.com/danielmaier42/claudeq/internal/notify"
	"github.com/danielmaier42/claudeq/internal/store"
)

// fakeChannel is one channel whose answer the test sets.
type fakeChannel struct {
	mu      sync.Mutex
	err     error
	receipt string
	got     []notify.Notification
}

func (f *fakeChannel) Deliver(_ context.Context, n notify.Notification) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.got = append(f.got, n)
	if f.err != nil {
		return "", f.err
	}
	return f.receipt, nil
}

func (f *fakeChannel) Notify(ctx context.Context, n notify.Notification) error {
	_, err := f.Deliver(ctx, n)
	return err
}

func (f *fakeChannel) fail(err error) {
	f.mu.Lock()
	f.err = err
	f.mu.Unlock()
}

func (f *fakeChannel) received() []notify.Notification {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]notify.Notification(nil), f.got...)
}

// fakeChannels is the daemon's fan-out: a desktop and a phone channel.
type fakeChannels struct {
	desktop, phone *fakeChannel
	noPhone        bool
}

func (f *fakeChannels) Notify(context.Context, notify.Notification) error {
	return errors.New("the engine delivers per channel")
}

func (f *fakeChannels) Channels() ([]notify.Channel, error) {
	chans := []notify.Channel{{Name: "desktop", Notifier: f.desktop}}
	if !f.noPhone {
		chans = append(chans, notify.Channel{Name: "pushover", Remote: true, Notifier: f.phone})
	}
	return chans, nil
}

func newDeliveryEngine(t *testing.T, fc *clock.Fake) (*Engine, *store.Store, *fakeChannels, *bytes.Buffer, *bytes.Buffer) {
	t.Helper()
	e, st := newTestEngine(t, &stub{}, fc)
	chans := &fakeChannels{desktop: &fakeChannel{}, phone: &fakeChannel{receipt: "req-1"}}
	e.SetNotifier(chans)
	var out, errs bytes.Buffer
	e.deliveryLog, e.deliveryErrLog = &out, &errs
	return e, st, chans, &out, &errs
}

func pending(t *testing.T, st *store.Store) []store.PendingDelivery {
	t.Helper()
	list, err := st.PendingDeliveries()
	if err != nil {
		t.Fatalf("PendingDeliveries: %v", err)
	}
	return list
}

var errNoDNS = errors.New(`pushover request: Post "https://api.pushover.net/1/messages.json": dial tcp: lookup api.pushover.net: no such host`)

// The failure push raised in a wake without network used to be tried once and
// lost. It must wait in the outbox and reach the phone once the network is
// back, without the desktop showing the banner a second time.
func TestUndeliveredPushIsRetriedUntilTheNetworkIsBack(t *testing.T) {
	fc := clock.NewFake(time.Date(2026, 11, 9, 3, 15, 0, 0, time.UTC))
	e, st, chans, out, errs := newDeliveryEngine(t, fc)
	chans.phone.fail(errNoDNS)

	e.send(notify.Notification{Title: "DispatchQ ✗ watch failed", Message: "boom"})

	if list := pending(t, st); len(list) != 1 || list[0].Channel != "pushover" || list[0].Attempts != 1 {
		t.Fatalf("outbox after a failed push = %+v, want the pushover delivery waiting", list)
	}
	if !strings.Contains(errs.String(), "not delivered by pushover, trying again") {
		t.Fatalf("error log = %q", errs.String())
	}

	// Before the pause is over nothing is tried.
	fc.Advance(DeliveryFirstRetry / 2)
	e.retryDeliveries(false)
	if got := len(chans.phone.received()); got != 1 {
		t.Fatalf("pushover tried %d times before the pause ended", got)
	}
	// Still offline: tried again, still waiting, with a longer pause.
	fc.Advance(DeliveryFirstRetry)
	e.retryDeliveries(false)
	if list := pending(t, st); len(list) != 1 || list[0].Attempts != 2 || !list[0].NextTry.Equal(fc.Now().Add(2*DeliveryFirstRetry)) {
		t.Fatalf("outbox after the second try = %+v", list)
	}

	chans.phone.fail(nil)
	fc.Advance(time.Hour)
	e.retryDeliveries(false)
	if list := pending(t, st); len(list) != 0 {
		t.Fatalf("outbox after the network came back = %+v, want empty", list)
	}
	if got := len(chans.desktop.received()); got != 1 {
		t.Fatalf("desktop shown %d times, want once", got)
	}
	if !strings.Contains(out.String(), `notification "DispatchQ ✗ watch failed" accepted by pushover (receipt req-1) on try 3`) {
		t.Fatalf("delivery log = %q", out.String())
	}
}

func TestAcceptedSendIsLoggedWithItsReceipt(t *testing.T) {
	fc := clock.NewFake(time.Date(2026, 11, 9, 9, 0, 0, 0, time.UTC))
	e, st, _, out, _ := newDeliveryEngine(t, fc)

	e.send(notify.Notification{Title: "hello"})

	for _, want := range []string{`notification "hello" accepted by desktop`, `notification "hello" accepted by pushover (receipt req-1)`} {
		if !strings.Contains(out.String(), want) {
			t.Fatalf("delivery log = %q, want %q", out.String(), want)
		}
	}
	if list := pending(t, st); len(list) != 0 {
		t.Fatalf("outbox after a clean send = %+v", list)
	}
}

func TestPendingDeliverySurvivesARestart(t *testing.T) {
	fc := clock.NewFake(time.Date(2026, 11, 9, 3, 15, 0, 0, time.UTC))
	e, st, chans, _, _ := newDeliveryEngine(t, fc)
	chans.phone.fail(errNoDNS)
	e.send(notify.Notification{Title: "raised before the restart", Priority: notify.PriorityHigh})

	// The next daemon on the same data directory takes it over.
	next, _ := newTestEngine(t, &stub{}, fc)
	next.store = st
	phone := &fakeChannel{}
	next.SetNotifier(&fakeChannels{desktop: &fakeChannel{}, phone: phone})
	next.deliveryLog, next.deliveryErrLog = &bytes.Buffer{}, &bytes.Buffer{}
	fc.Advance(time.Minute)
	next.retryDeliveries(false)

	got := phone.received()
	if len(got) != 1 || got[0].Title != "raised before the restart" || got[0].Priority != notify.PriorityHigh {
		t.Fatalf("after the restart pushover received %+v", got)
	}
	if list := pending(t, st); len(list) != 0 {
		t.Fatalf("outbox = %+v", list)
	}
}

func TestDeliveryIsGivenUpLoudlyAfterTheRetryWindow(t *testing.T) {
	fc := clock.NewFake(time.Date(2026, 11, 9, 3, 15, 0, 0, time.UTC))
	e, st, chans, _, errs := newDeliveryEngine(t, fc)
	chans.phone.fail(errNoDNS)
	e.send(notify.Notification{Title: "never arrives"})

	for range 400 {
		fc.Advance(DeliveryMaxPause)
		e.retryDeliveries(false)
		if len(pending(t, st)) == 0 {
			break
		}
	}
	if list := pending(t, st); len(list) != 0 {
		t.Fatalf("still waiting after the window: %+v", list)
	}
	if !strings.Contains(errs.String(), `NOTIFICATION LOST: "never arrives" could not be delivered by pushover within 24h0m0s`) {
		t.Fatalf("error log = %q", errs.String())
	}
	// Once per delivery, not once per try.
	if got := strings.Count(errs.String(), "\n"); got != 2 {
		t.Fatalf("error log has %d lines, want the first failure and the give-up:\n%s", got, errs.String())
	}
}

func TestRefusedDeliveryIsNotRetried(t *testing.T) {
	fc := clock.NewFake(time.Date(2026, 11, 9, 3, 15, 0, 0, time.UTC))
	e, st, chans, _, errs := newDeliveryEngine(t, fc)
	chans.phone.fail(&notify.StatusError{Channel: "pushover", Code: 400, Reason: " (application token is invalid)"})
	e.send(notify.Notification{Title: "x"})
	if list := pending(t, st); len(list) != 0 {
		t.Fatalf("a refused push was queued: %+v", list)
	}
	if !strings.Contains(errs.String(), "refused by pushover, not retried") {
		t.Fatalf("error log = %q", errs.String())
	}
}

func TestDeliveryToAChannelSwitchedOffIsDropped(t *testing.T) {
	fc := clock.NewFake(time.Date(2026, 11, 9, 3, 15, 0, 0, time.UTC))
	e, st, chans, _, _ := newDeliveryEngine(t, fc)
	chans.phone.fail(errNoDNS)
	e.send(notify.Notification{Title: "x"})
	chans.noPhone = true
	fc.Advance(time.Hour)
	e.retryDeliveries(false)
	if list := pending(t, st); len(list) != 0 {
		t.Fatalf("outbox = %+v, want the switched-off channel's delivery gone", list)
	}
}

// After a wake, or once the network is back, the outbox is tried at once
// instead of waiting for each entry's pause.
func TestNetworkBackTriesTheOutboxAtOnce(t *testing.T) {
	fc := clock.NewFake(time.Date(2026, 11, 9, 3, 15, 0, 0, time.UTC))
	e, st, chans, _, _ := newDeliveryEngine(t, fc)
	chans.phone.fail(errNoDNS)
	e.send(notify.Notification{Title: "x"})
	chans.phone.fail(nil)

	e.retryDeliveries(false)
	if len(pending(t, st)) != 1 {
		t.Fatal("tried before its pause without being asked to")
	}
	e.outbox.retrySoon()
	e.sweepDeliveries(false)
	e.WaitIdle()
	if list := pending(t, st); len(list) != 0 {
		t.Fatalf("outbox after the network came back = %+v", list)
	}
}

func TestRetryPauseGrowsToItsCap(t *testing.T) {
	want := []time.Duration{30 * time.Second, time.Minute, 2 * time.Minute, 4 * time.Minute, 5 * time.Minute, 5 * time.Minute}
	for i, w := range want {
		if got := retryPause(i + 1); got != w {
			t.Errorf("pause after %d tries = %s, want %s", i+1, got, w)
		}
	}
}
