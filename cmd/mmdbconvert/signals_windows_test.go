package main

import (
	"io"
	"log/slog"
	"os"
	"os/exec"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCLIWindowsSignalExit(t *testing.T) {
	if os.Getenv("MMDBCONVERT_WINDOWS_SIGNAL_HELPER") == "1" {
		logger := newLogger(io.Discard, logFormatJSON, slog.LevelWarn)
		//revive:disable-next-line:deep-exit Subprocess helper exposes the native console exit status.
		os.Exit(exitAfterSignal(os.Interrupt, logger))
	}
	binary, err := os.Executable()
	require.NoError(t, err)
	// #nosec G204 -- run only this test binary's exit-status helper.
	cmd := exec.CommandContext(t.Context(), binary, "-test.run=^TestCLIWindowsSignalExit$")
	cmd.Env = append(os.Environ(), "MMDBCONVERT_WINDOWS_SIGNAL_HELPER=1")
	err = cmd.Run()
	var exitErr *exec.ExitError
	require.ErrorAs(t, err, &exitErr)
	status := exitErr.Sys().(syscall.WaitStatus)
	require.Equal(t, uint32(0xC000013A), status.ExitCode)
}
