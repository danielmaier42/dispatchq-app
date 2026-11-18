package executor

import (
	"fmt"
	"os"
	"path/filepath"
)

const powerShellJobFile = "job.ps1"

// PowerShell -File normally reports 0 when a script ends without an explicit
// exit, even if its last native command failed. Invoke the job as a separate
// script so PowerShell records explicit exits in LASTEXITCODE too.
const powerShellScriptLauncher = `& (Join-Path $PSScriptRoot '` + powerShellJobFile + `')
exit $LASTEXITCODE
`

func powerShellScriptArgs(jobPath string) ([]string, error) {
	launcher := filepath.Join(filepath.Dir(jobPath), "launch.ps1")
	if err := os.WriteFile(launcher, []byte(powerShellScriptLauncher), 0o600); err != nil {
		return nil, fmt.Errorf("write PowerShell script launcher: %w", err)
	}
	return []string{"-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass", "-File", launcher}, nil
}
