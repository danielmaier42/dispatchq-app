package store

import (
	"fmt"
	"os"
)

// lockFile is an advisory lock held for the duration of a read-modify-write
// update, so separate processes serialize their config/state changes.
const lockFile = ".lock"

// withWriteLock runs fn while holding an exclusive, cross-process advisory lock
// on the data directory. The in-process writeMu serializes updates within one
// process; this file lock closes the cross-process gap so the daemon and a dispatchq
// CLI process (for example a running task queueing follow-up work) never
// interleave load → modify → save and silently lose each other's changes.
// AppendRun, PruneHistory and PruneQuietRuns take it too, so a rewrite of
// history never drops another process's append. It is not reentrant: fn must
// not call anything that takes it again (UpdateState, UpdateConfig, AppendRun,
// …), or it waits on itself for good.
func (s *Store) withWriteLock(fn func() error) error {
	f, err := os.OpenFile(s.path(lockFile), os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return fmt.Errorf("open lock file: %w", err)
	}
	defer func() { _ = f.Close() }()

	if err := lockExclusive(f); err != nil {
		return fmt.Errorf("acquire lock: %w", err)
	}
	defer func() { _ = unlockFile(f) }()

	return fn()
}
