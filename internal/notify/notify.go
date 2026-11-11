// Package notify delivers dispatchq notifications to native macOS Notification
// Center (via osascript) and to Pushover (FA-39/40/41). Delivery is best-effort:
// a failing channel never blocks the caller's flow.
package notify

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"

	"github.com/danielmaier42/claudeq/internal/system"
)

// Notification is a single message to deliver. It is kept on disk while a
// channel still has to deliver it (the daemon's delivery outbox), hence the
// JSON names.
type Notification struct {
	Title   string `json:"title"`
	Message string `json:"message"`
	// ID names the notification in the daemon's inbox (store.InboxEntry). The
	// macOS notification carries it so a click can mark that entry read (see
	// cmd/dispatchqapp, notifyclick_cocoa.m); other channels ignore it.
	ID string `json:"id,omitempty"`
	// Kind says what the notification announces (a store.InboxKind constant).
	// It is bookkeeping for the inbox, not something a channel shows.
	Kind string `json:"kind,omitempty"`
	// ArtifactID, when set, makes the macOS notification open that artifact in
	// the DispatchQ window on click. Channels that cannot carry a click target
	// ignore it.
	ArtifactID string `json:"artifact_id,omitempty"`
	// RunID, when set, makes the macOS notification open that run's log in the
	// DispatchQ window on click. Channels that cannot carry a click target ignore
	// it.
	RunID string `json:"run_id,omitempty"`
	// URL, when set, is the notification's link: Pushover shows it as the
	// message's supplementary URL, and clicking the macOS notification opens it
	// (again via cmd/dispatchqapp). Channels that cannot carry a link ignore it.
	URL string `json:"url,omitempty"`
	// TaskID and TaskName attribute the notification to the task it is about
	// or that sent it, for the inbox. Channels ignore them.
	TaskID   string `json:"task_id,omitempty"`
	TaskName string `json:"task_name,omitempty"`
	// Priority asks the phone channels to make more noise: Pushover sends it
	// as priority 1 (bypasses quiet hours), ntfy as "high". Channels without
	// priorities ignore it.
	Priority Priority `json:"priority,omitempty"`
}

// Priority is how urgently a notification asks to be noticed.
type Priority int

const (
	// PriorityNormal is every notification that is not about something broken.
	PriorityNormal Priority = 0
	// PriorityHigh is for a failure: something stopped working unattended.
	PriorityHigh Priority = 1
)

// IsWebURL reports whether s is an absolute http or https URL — the only kind
// of link a notification carries, because it is opened on click without
// further ado.
func IsWebURL(s string) bool {
	u, err := url.Parse(s)
	if err != nil {
		return false
	}
	return (u.Scheme == "http" || u.Scheme == "https") && u.Host != ""
}

// Notifier delivers a notification.
type Notifier interface {
	Notify(ctx context.Context, n Notification) error
}

// Mac posts native macOS notifications via `osascript`. It works from a user
// LaunchAgent session (PLAN.md V3).
type Mac struct {
	Runner system.Runner
}

// Notify displays a native notification. When dispatchq runs from its app bundle
// it posts through UNUserNotificationCenter so the notification carries the app
// icon; otherwise (or if that fails) it falls back to osascript, whose
// notifications show a generic script icon but always work from a LaunchAgent.
func (m Mac) Notify(ctx context.Context, n Notification) error {
	if nativeNotifyAvailable() {
		if err := postNativeNotification(n); err == nil {
			return nil
		}
		// fall through to osascript on failure
	}
	script := fmt.Sprintf("display notification %s with title %s",
		asString(n.Message), asString(n.Title))
	if out, err := m.Runner.Run(ctx, "osascript", "-e", script); err != nil {
		return fmt.Errorf("osascript: %w (%s)", err, string(out))
	}
	return nil
}

// RequestMacAuthorization asks for notification permission up front (the one-time
// system prompt), so later notifications can be delivered with the app icon. A
// no-op when not running from the app bundle.
func RequestMacAuthorization() {
	if nativeNotifyAvailable() {
		requestNativeAuth()
	}
}

// Authorization states reported by [MacAuthorization]. Only AuthorizationAllowed
// (and AuthorizationProvisional) actually put a notification on screen: macOS
// accepts and stores the others, then presents them as nothing.
const (
	AuthorizationAllowed       = "authorized"
	AuthorizationDenied        = "denied"
	AuthorizationNotDetermined = "not_determined"
	AuthorizationProvisional   = "provisional"
	AuthorizationUnknown       = "unknown"
	// AuthorizationUnsupported means the question cannot be asked: not macOS, or
	// not running from the app bundle (a bare dev binary uses osascript, which
	// has no authorization of its own).
	AuthorizationUnsupported = "unsupported"
)

// MacAuthorization reports whether macOS will show this app's notifications, so
// the UI can point out that alerts are being swallowed instead of leaving the
// operator to wonder why nothing arrives.
func MacAuthorization() string {
	if !nativeNotifyAvailable() {
		return AuthorizationUnsupported
	}
	return nativeAuthorizationStatus()
}

// asString renders a Go string as an AppleScript double-quoted string literal.
func asString(s string) string {
	r := strings.NewReplacer("\\", "\\\\", "\"", "\\\"")
	return "\"" + r.Replace(s) + "\""
}

// DefaultPushoverURL is the Pushover messages endpoint.
const DefaultPushoverURL = "https://api.pushover.net/1/messages.json"

// Pushover delivers notifications to the Pushover mobile service.
type Pushover struct {
	Token   string
	UserKey string
	// URL overrides the endpoint (for tests). Empty uses DefaultPushoverURL.
	URL string
	// Client overrides the HTTP client (for tests). Empty uses a 10s client.
	Client *http.Client
}

// Configured reports whether credentials are present.
func (p Pushover) Configured() bool { return p.Token != "" && p.UserKey != "" }

// ValidatePushover checks credentials before they are saved, so an enabled
// channel with a missing key is refused instead of quietly delivering nothing.
func ValidatePushover(token, userKey string) error {
	if strings.TrimSpace(token) == "" {
		return fmt.Errorf("pushover: the API token is required")
	}
	if strings.TrimSpace(userKey) == "" {
		return fmt.Errorf("pushover: the user key is required")
	}
	return nil
}

// Notify posts the message to Pushover.
func (p Pushover) Notify(ctx context.Context, n Notification) error {
	_, err := p.Deliver(ctx, n)
	return err
}

// Deliver posts the message to Pushover and returns the request id Pushover
// answered with, which is what its support asks for when a message went
// missing on the way to the phone.
func (p Pushover) Deliver(ctx context.Context, n Notification) (string, error) {
	if !p.Configured() {
		return "", fmt.Errorf("pushover not configured")
	}
	endpoint := p.URL
	if endpoint == "" {
		endpoint = DefaultPushoverURL
	}
	form := url.Values{
		"token":   {p.Token},
		"user":    {p.UserKey},
		"title":   {n.Title},
		"message": {n.Message},
	}
	if n.URL != "" {
		form.Set("url", n.URL)
	}
	if n.Priority == PriorityHigh {
		form.Set("priority", "1")
	}
	reply, err := post(ctx, p.Client, "pushover", endpoint, "application/x-www-form-urlencoded", form.Encode(), nil)
	return receiptField(reply, "request"), err
}

// Deliverer is a notifier that reports what the channel answered: the id the
// service gave the message, empty when it gives none. The daemon logs it for
// every accepted notification, so "it never arrived" can be told apart from
// "it was accepted and lost on the way".
type Deliverer interface {
	Deliver(ctx context.Context, n Notification) (string, error)
}

// Deliver sends n through notifier and returns its receipt when the notifier
// reports one.
func Deliver(ctx context.Context, notifier Notifier, n Notification) (string, error) {
	if d, ok := notifier.(Deliverer); ok {
		return d.Deliver(ctx, n)
	}
	return "", notifier.Notify(ctx, n)
}

// Channel is one named way out: the desktop, Pushover, ntfy or the webhook.
// The daemon delivers to and retries each channel on its own, so a phone that
// was offline gets the message later without the desktop showing it twice.
type Channel struct {
	// Name identifies the channel across a daemon restart ("desktop",
	// "pushover", "ntfy", "webhook").
	Name string
	// Remote is set for a channel that needs the network. It is retried for
	// much longer than the desktop, whose banner is stale after an hour.
	Remote   bool
	Notifier Notifier
}

// Channels names each channel a notifier fans out to right now. A notifier
// that does not implement it counts as a single channel. An error means the
// channels cannot be told (the settings did not load); the channels that are
// known still come back, and nothing waiting for another channel may be
// dropped because of it.
type Channels interface {
	Channels() ([]Channel, error)
}

// Multi fans a notification out to several notifiers, best-effort: it attempts
// all of them concurrently and joins any errors. Concurrent because the caller
// bounds the whole call with one shared context (engine.send's 15s budget) —
// run sequentially, a slow channel would eat into, or exhaust, the time left
// for every channel behind it.
type Multi struct {
	Notifiers []Notifier
}

// Notify delivers to every configured notifier.
func (m Multi) Notify(ctx context.Context, n Notification) error {
	var wg sync.WaitGroup
	errs := make([]string, len(m.Notifiers))
	for i, notifier := range m.Notifiers {
		if notifier == nil {
			continue
		}
		wg.Add(1)
		go func(i int, notifier Notifier) {
			defer wg.Done()
			if err := notifier.Notify(ctx, n); err != nil {
				errs[i] = err.Error()
			}
		}(i, notifier)
	}
	wg.Wait()
	var joined []string
	for _, e := range errs {
		if e != "" {
			joined = append(joined, e)
		}
	}
	if len(joined) > 0 {
		return fmt.Errorf("notify: %s", strings.Join(joined, "; "))
	}
	return nil
}
