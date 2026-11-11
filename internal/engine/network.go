package engine

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"os"
	"time"

	"github.com/danielmaier42/claudeq/internal/provider"
)

// A Mac on battery with the lid closed wakes for a minute every quarter of an
// hour (macOS's DarkWake), usually without network. A task started in such a
// wake used to fail at once, because its harness could not reach the
// provider's API. Before an agent run starts, the daemon now checks that the
// API's host name resolves; while it does not, the task stays due and waits
// with the reason "no network", and starts on the first tick that has it.
//
// apiHost is the host a provider instance's harness needs, or nothing when
// the daemon cannot tell, in which case the run is not held back: better a run
// that fails than one that never starts. Claude Code talks to
// api.anthropic.com and Codex to api.openai.com, unless the instance's (or
// the daemon's) environment points them elsewhere: a base URL is checked
// instead (a loopback or IP address needs no look-up), and Bedrock, Vertex or
// a proxy are not checked. opencode is not checked: it is as often pointed at
// a model on this machine or the local network as at a cloud API.
func apiHost(inst provider.Instance) string {
	env := func(key string) string {
		if v := inst.Env[key]; v != "" {
			return v
		}
		return os.Getenv(key)
	}
	for _, key := range []string{"HTTPS_PROXY", "https_proxy", "ALL_PROXY", "all_proxy"} {
		if env(key) != "" {
			return ""
		}
	}
	switch inst.Kind {
	case provider.KindClaudeCode:
		if env("CLAUDE_CODE_USE_BEDROCK") != "" || env("CLAUDE_CODE_USE_VERTEX") != "" {
			return ""
		}
		return hostOf(env("ANTHROPIC_BASE_URL"), "api.anthropic.com")
	case provider.KindCodex:
		return hostOf(env("OPENAI_BASE_URL"), "api.openai.com")
	default:
		return ""
	}
}

// hostOf is the host name in a base URL, or fallback when there is none; a
// URL that names no host, or names an address or localhost, needs no look-up.
func hostOf(baseURL, fallback string) string {
	if baseURL == "" {
		return fallback
	}
	u, err := url.Parse(baseURL)
	if err != nil {
		return ""
	}
	h := u.Hostname()
	if h == "" || h == "localhost" || net.ParseIP(h) != nil {
		return ""
	}
	return h
}

// NetworkCheckTimeout bounds one look-up. Offline, the resolver answers at
// once; a resolver that hangs counts as no network.
const NetworkCheckTimeout = 3 * time.Second

// SetNetworkCheck sets how the engine tells whether a provider's API host can
// be reached. Nil (the default) treats the network as always there.
func (e *Engine) SetNetworkCheck(f func(ctx context.Context, host string) error) { e.netCheck = f }

// LookupHost is the production network check: the host name resolves.
func LookupHost(ctx context.Context, host string) error {
	ctx, cancel := context.WithTimeout(ctx, NetworkCheckTimeout)
	defer cancel()
	if _, err := net.DefaultResolver.LookupHost(ctx, host); err != nil {
		return fmt.Errorf("look up %s: %w", host, err)
	}
	return nil
}

// network is the verdict of one tick's look-ups, by host.
type network map[string]error

// reachable reports whether a resolved provider's API can be reached; a
// harness without a known host always can.
func (n network) reachable(r provider.Resolved) bool {
	return n[apiHost(r.Instance)] == nil
}

// probeNetwork looks up the API host of every agent task about to start, once
// per host. It runs outside the scheduler's lock. When the network comes back
// after a tick without it, whatever waits in the notification outbox is tried
// at once.
func (e *Engine) probeNetwork(ctx context.Context, due []assignment) network {
	if e.netCheck == nil {
		return nil
	}
	out := network{}
	for _, a := range due {
		if a.err != nil || a.task.IsScript() {
			continue
		}
		host := apiHost(a.resolved.Instance)
		if _, done := out[host]; done || host == "" {
			continue
		}
		out[host] = e.netCheck(ctx, host)
	}
	if len(out) == 0 || ctx.Err() != nil {
		// Stopping cuts the look-ups short; that is not the network going.
		return out
	}
	var down error
	for _, err := range out {
		if err != nil {
			down = err
			break
		}
	}
	switch wasDown := e.offline.Load(); {
	case down != nil && !wasDown:
		fmt.Fprintf(e.deliveryErrLog, "dispatchqd: no network (%v); agent tasks wait until it is back\n", down)
		e.offline.Store(true)
	case down == nil && wasDown:
		fmt.Fprintln(e.deliveryLog, "dispatchqd: the network is back")
		e.offline.Store(false)
		e.outbox.retrySoon()
	}
	return out
}
