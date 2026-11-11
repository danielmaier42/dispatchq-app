package provider

import (
	"time"

	"github.com/danielmaier42/claudeq/internal/store"
)

// SessionReader is implemented by an adapter whose harness keeps every
// session's transcript on disk. It is how a run DispatchQ could not watch to
// its end (its process outlived the daemon that started it, so its output and
// exit status went nowhere) still gets its real outcome.
type SessionReader interface {
	// SessionOutcome reads how the session's latest turn ended, ignoring
	// anything written before since (the run's start), so an earlier run of
	// the same session is never taken for this one. ok is false when the
	// transcript is not there or that turn had not ended when it stopped.
	SessionOutcome(inst Instance, sessionID string, since time.Time) (res Result, ok bool, err error)
}

// ReadSessionOutcome reads a run's outcome from its harness's own transcript,
// for a run whose output DispatchQ could not read to the end. ok is false when
// it cannot be told: a script, a run without a session, a provider no longer
// configured or running outside this system (a WSL instance's transcript lives
// in the distribution), or a harness whose adapter keeps no transcript.
func ReadSessionOutcome(reg *Registry, set Set, rec store.Run) (Result, bool, error) {
	if rec.Provider.IsScript() || rec.SessionID == "" {
		return Result{}, false, nil
	}
	id := rec.Provider.ID
	if id == "" {
		id = set.DefaultID() // recorded before runs named their provider
	}
	inst, ok := set.Lookup(id)
	if !ok || !inst.Execution.IsNative() {
		return Result{}, false, nil
	}
	a, err := reg.Lookup(inst.Kind)
	if err != nil {
		return Result{}, false, nil
	}
	sr, ok := a.(SessionReader)
	if !ok {
		return Result{}, false, nil
	}
	return sr.SessionOutcome(inst, rec.SessionID, rec.StartedAt)
}
