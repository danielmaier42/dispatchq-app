//go:build unix

package proctree

import (
	"os"
	"testing"
)

func TestParseOrphansKeepsOwnReparentedProcesses(t *testing.T) {
	out := "  41315     1   501 /Users/dm/.local/bin/claude -p --output-format stream-json --session-id f2fae403\n" +
		"  42000   900   501 /bin/sleep 5\n" +
		"  43000     1     0 /usr/sbin/somedaemon\n" +
		"  garbage\n"
	got := parseOrphans(out, 501)
	if len(got) != 1 || got[0].PID != 41315 ||
		got[0].Command != "/Users/dm/.local/bin/claude -p --output-format stream-json --session-id f2fae403" {
		t.Fatalf("parseOrphans = %+v", got)
	}
}

func TestOrphansReadsTheProcessTable(t *testing.T) {
	if _, err := Orphans(); err != nil {
		t.Fatalf("Orphans: %v (uid %d)", err, os.Getuid())
	}
}
