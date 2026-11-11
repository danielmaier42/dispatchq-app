package engine

import (
	"context"
	"fmt"
	"net"
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
// apiHosts is the host each harness needs. opencode is not checked: it is as
// often pointed at a model on this machine or the local network as at a
// cloud API, and holding it back offline would stop work that can run.
var apiHosts = map[provider.Kind]string{
	provider.KindClaudeCode: "api.anthropic.com",
	provider.KindCodex:      "api.openai.com",
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
	return n[apiHosts[r.Instance.Kind]] == nil
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
		host := apiHosts[a.resolved.Instance.Kind]
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
