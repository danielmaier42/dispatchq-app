//go:build windows

package engine

import "errors"

// osProcesses has nothing to recover on Windows: every run belongs to a Job
// Object that closes with the daemon, so no run outlives it.
type osProcesses struct{}

var errNoRecovery = errors.New("runs do not outlive the daemon on Windows")

func (osProcesses) StartTime(int) (string, error) { return "", errNoRecovery }

func (osProcesses) Kill(int) {}

func (osProcesses) Orphans() ([]Orphan, error) { return nil, nil }
