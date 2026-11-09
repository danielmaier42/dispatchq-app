//go:build unix

package engine

import "github.com/danielmaier42/claudeq/internal/proctree"

// osProcesses is recovery's view of the process table on Unix.
type osProcesses struct{}

func (osProcesses) StartTime(pid int) (string, error) { return proctree.StartTime(pid) }

func (osProcesses) Kill(pid int) { proctree.KillTree(pid) }

func (osProcesses) Orphans() ([]Orphan, error) {
	list, err := proctree.Orphans()
	out := make([]Orphan, 0, len(list))
	for _, o := range list {
		out = append(out, Orphan{PID: o.PID, Command: o.Command})
	}
	return out, err
}
