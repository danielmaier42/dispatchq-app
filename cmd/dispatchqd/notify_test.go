//go:build !windows

package main

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/danielmaier42/claudeq/internal/notify"
	"github.com/danielmaier42/claudeq/internal/store"
)

// desktopStub stands in for the platform's on-screen channel.
type desktopStub struct{}

func (desktopStub) Notify(context.Context, notify.Notification) error { return nil }

func TestNotifyChannelsAlwaysIncludesTheDesktop(t *testing.T) {
	channels := notifyChannels(desktopStub{}, store.Settings{})
	if len(channels) != 1 {
		t.Fatalf("channels = %+v, want just the desktop", channels)
	}
	if _, ok := channels[0].Notifier.(desktopStub); !ok || channels[0].Name != "desktop" || channels[0].Remote {
		t.Fatalf("channels[0] = %+v, want the local desktop channel", channels[0])
	}
}

func TestNotifyChannelsWithoutADesktopKeepsTheRemoteChannels(t *testing.T) {
	if channels := notifyChannels(nil, store.Settings{}); len(channels) != 0 {
		t.Fatalf("channels = %+v, want none", channels)
	}
	s := store.Settings{Ntfy: store.Ntfy{Enabled: true, Topic: "dispatchq"}}
	channels := notifyChannels(nil, s)
	if len(channels) != 1 {
		t.Fatalf("channels = %+v, want only ntfy", channels)
	}
	if _, ok := channels[0].Notifier.(notify.Ntfy); !ok || channels[0].Name != "ntfy" || !channels[0].Remote {
		t.Fatalf("channels[0] = %+v, want the remote ntfy channel", channels[0])
	}
}

func TestNotifyChannelsSkipsEnabledButUnconfigured(t *testing.T) {
	// Each channel is switched on but missing what it needs to deliver: a
	// half-filled-in channel must not turn into a failed request on every send.
	s := store.Settings{
		Pushover: store.Pushover{Enabled: true},
		Ntfy:     store.Ntfy{Enabled: true},
		Webhook:  store.Webhook{Enabled: true},
	}
	channels := notifyChannels(desktopStub{}, s)
	if len(channels) != 1 {
		t.Fatalf("channels = %+v, want only the desktop", channels)
	}
}

func TestNotifyChannelsIncludesEveryConfiguredEnabledChannel(t *testing.T) {
	s := store.Settings{
		Pushover: store.Pushover{Enabled: true, Token: "tok", UserKey: "usr"},
		Ntfy:     store.Ntfy{Enabled: true, Topic: "dispatchq"},
		Webhook:  store.Webhook{Enabled: true, URL: "https://hooks.example.com/x"},
	}
	channels := notifyChannels(desktopStub{}, s)
	if len(channels) != 4 {
		t.Fatalf("channels = %+v, want the desktop + 3 remote channels", channels)
	}
	var sawPushover, sawNtfy, sawWebhook bool
	for _, c := range channels {
		switch c.Notifier.(type) {
		case notify.Pushover:
			sawPushover = true
		case notify.Ntfy:
			sawNtfy = true
		case notify.Webhook:
			sawWebhook = true
		}
	}
	if !sawPushover || !sawNtfy || !sawWebhook {
		t.Fatalf("channels = %+v, missing an enabled+configured channel", channels)
	}
}

func TestNotifyChannelsSkipsDisabledEvenWhenConfigured(t *testing.T) {
	s := store.Settings{
		Ntfy:    store.Ntfy{Topic: "dispatchq"},
		Webhook: store.Webhook{URL: "https://hooks.example.com/x"},
	}
	channels := notifyChannels(desktopStub{}, s)
	if len(channels) != 1 {
		t.Fatalf("channels = %+v, want only the desktop (channels are configured but not enabled)", channels)
	}
}

// The incident: config.toml could not be read while the system was out of
// file handles, and every notification went to the desktop alone, so nothing
// reached the phone. A failed read keeps the channels of the settings read
// last, and still reports the failure, so the engine's outbox neither loses
// the remote channels nor drops their queued deliveries as switched off.
func TestLiveNotifierKeepsTheLastChannelsWhenTheConfigCannotBeRead(t *testing.T) {
	st, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	cfg := store.Config{Settings: store.Settings{Ntfy: store.Ntfy{Enabled: true, Topic: "dispatchq"}}}
	if err := st.SaveConfig(cfg); err != nil {
		t.Fatal(err)
	}
	l := &liveNotifier{st: st, desktop: desktopStub{}}
	if got, err := l.Channels(); err != nil || len(got) != 2 {
		t.Fatalf("channels = %+v, %v; want the desktop and ntfy", got, err)
	}

	path := filepath.Join(st.Home(), "config.toml")
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(path, 0o755); err != nil {
		t.Fatal(err)
	}
	got, err := l.Channels()
	if err == nil {
		t.Fatal("a config that cannot be read was not reported")
	}
	var names []string
	for _, c := range got {
		names = append(names, c.Name)
	}
	if !slices.Contains(names, "ntfy") || !slices.Contains(names, "desktop") {
		t.Fatalf("channels after a failed read = %v, want the desktop and ntfy still", names)
	}
}
