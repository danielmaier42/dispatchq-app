//go:build unix

package proctree

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
)

// Orphan is a process of the current user whose parent has gone, so it was
// reparented to PID 1.
type Orphan struct {
	PID     int
	Command string
}

// Orphans lists the current user's processes reparented to PID 1, with their
// full command line.
func Orphans() ([]Orphan, error) {
	ctx, cancel := context.WithTimeout(context.Background(), killTreeTimeout)
	defer cancel()
	out, err := exec.CommandContext(ctx, "ps", "-axww", "-o", "pid=,ppid=,uid=,command=").Output()
	if err != nil {
		return nil, fmt.Errorf("list processes: %w", err)
	}
	return parseOrphans(string(out), os.Getuid()), nil
}

// parseOrphans reads `ps -o pid=,ppid=,uid=,command=` output.
func parseOrphans(out string, uid int) []Orphan {
	var found []Orphan
	for _, line := range strings.Split(out, "\n") {
		fields := strings.Fields(line)
		if len(fields) < 4 {
			continue
		}
		pid, err1 := strconv.Atoi(fields[0])
		ppid, err2 := strconv.Atoi(fields[1])
		owner, err3 := strconv.Atoi(fields[2])
		if err1 != nil || err2 != nil || err3 != nil || ppid != 1 || owner != uid {
			continue
		}
		found = append(found, Orphan{PID: pid, Command: strings.Join(fields[3:], " ")})
	}
	return found
}
