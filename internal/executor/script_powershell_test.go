package executor

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestPowerShellScriptExitCode(t *testing.T) {
	shell := "pwsh"
	nativeFailure := "/bin/sh -c 'exit 3'"
	if runtime.GOOS == "windows" {
		shell = "powershell.exe"
		nativeFailure = "cmd /c exit 3"
	}
	binary, err := exec.LookPath(shell)
	if err != nil {
		t.Skipf("%s unavailable: %v", shell, err)
	}

	for _, tc := range []struct {
		name, source, output string
		code                 int
	}{
		{name: "native exit", source: nativeFailure, code: 3},
		{name: "explicit exit", source: nativeFailure + "\nexit 7", code: 7},
		{name: "handled failure", source: nativeFailure + "\nexit 0"},
		{name: "terminating error", source: "throw 'failure'", code: 1},
		{name: "success", source: "[Console]::In.ReadToEnd()", output: "payload"},
		{name: "named block", source: "end { " + nativeFailure + " }", code: 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "space ' ü")
			if err := os.Mkdir(dir, 0o700); err != nil {
				t.Fatal(err)
			}
			jobPath := filepath.Join(dir, powerShellJobFile)
			if err := os.WriteFile(jobPath, withUTF8BOM(tc.source), 0o600); err != nil {
				t.Fatal(err)
			}
			args, err := powerShellScriptArgs(jobPath)
			if err != nil {
				t.Fatal(err)
			}
			cmd := exec.CommandContext(t.Context(), binary, args...)
			cmd.Stdin = strings.NewReader("payload")
			out, err := cmd.CombinedOutput()
			var exit *exec.ExitError
			code := 0
			switch {
			case errors.As(err, &exit):
				code = exit.ExitCode()
			case err != nil:
				t.Fatal(err)
			}
			if code != tc.code {
				t.Fatalf("got exit %d, want %d\n%s", code, tc.code, out)
			}
			t.Logf("exit %d", code)
			if tc.output != "" && strings.TrimSpace(string(out)) != tc.output {
				t.Fatalf("got output %q, want %q", out, tc.output)
			}
		})
	}
}
