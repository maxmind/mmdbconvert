//go:build unix

package main

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"os/signal"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestCLISignals(t *testing.T) {
	for _, sig := range []os.Signal{os.Interrupt, syscall.SIGTERM, syscall.SIGHUP} {
		t.Run(sig.String(), func(t *testing.T) {
			skipIgnoredSignal(t, sig)
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			binary, err := os.Executable()
			require.NoError(t, err)
			// #nosec G204 -- run only this test binary's synchronized signal helper.
			cmd := exec.CommandContext(ctx, binary, "-test.run=^TestCLISignalHelper$")
			cmd.Env = append(os.Environ(), "MMDBCONVERT_SIGNAL_TEST_HELPER=1")
			var stderr bytes.Buffer
			cmd.Stderr = &stderr
			stdout, err := cmd.StdoutPipe()
			require.NoError(t, err)
			require.NoError(t, cmd.Start())
			// The child announces readiness only after signal handling is installed.
			line, err := bufio.NewReader(stdout).ReadString('\n')
			require.NoError(t, err)
			require.Equal(t, "ready\n", line)
			require.NoError(t, cmd.Process.Signal(sig))
			err = cmd.Wait()
			var exitErr *exec.ExitError
			require.ErrorAs(t, err, &exitErr, stderr.String())
			status := exitErr.Sys().(syscall.WaitStatus)
			require.True(t, status.Signaled(), stderr.String())
			require.Equal(t, sig, status.Signal())
			require.Contains(t, stderr.String(), sig.String()+" signal received")
			require.NoError(t, ctx.Err(), "child should exit through cancellation, not timeout")
			require.Contains(t, stderr.String(), "cleanup complete")
		})
	}
}

func TestCLISignalHelper(t *testing.T) {
	if os.Getenv("MMDBCONVERT_SIGNAL_TEST_HELPER") != "1" {
		return
	}
	var ignored []os.Signal
	switch os.Getenv("MMDBCONVERT_SIGNAL_TEST_IGNORED") {
	case "INT":
		ignored = []os.Signal{os.Interrupt}
	case "HUP":
		ignored = []os.Signal{syscall.SIGHUP}
	case "ALL":
		ignored = []os.Signal{os.Interrupt, syscall.SIGTERM, syscall.SIGHUP}
		// Unlike INT/HUP, Go may install a handler over inherited ignored TERM.
		// Ignore it explicitly to exercise having no signals to subscribe to.
		signal.Ignore(ignored...)
	}
	logger := newLogger(os.Stderr, logFormatJSON, slog.LevelWarn)
	code := runWithSignals(t.Context(), logger, func(ctx context.Context) int {
		if len(ignored) != 0 {
			for _, sig := range ignored {
				require.True(t, signal.Ignored(sig), "%s must remain ignored", sig)
			}
			return 0
		}
		defer fmt.Fprintln(os.Stderr, "cleanup complete")
		_, err := fmt.Fprintln(os.Stdout, "ready")
		require.NoError(t, err)
		<-ctx.Done()
		require.ErrorIs(t, ctx.Err(), context.Canceled)
		if os.Getenv("MMDBCONVERT_SIGNAL_TEST_BLOCK") == "1" {
			_, err = fmt.Fprintln(os.Stdout, "canceled")
			require.NoError(t, err)
			select {}
		}
		// Publication may complete successfully even after a signal arrives.
		return 0
	})
	//revive:disable-next-line:deep-exit Subprocess helper must expose the CLI's exit status.
	os.Exit(code)
}

func TestCLIPreservesIgnoredSignals(t *testing.T) {
	bash := requireBash(t)
	binary, err := os.Executable()
	require.NoError(t, err)
	for _, ignored := range []string{"INT", "HUP", "ALL"} {
		t.Run(ignored, func(t *testing.T) {
			trap := ignored
			if ignored == "ALL" {
				trap = "INT"
			}
			// #nosec G204 -- inherit ignored signals before executing this test binary.
			cmd := exec.CommandContext(t.Context(), bash, "-c", `trap '' "$1"; shift; exec "$@"`,
				"signal-test", trap, binary, "-test.run=^TestCLISignalHelper$")
			cmd.Env = append(
				os.Environ(),
				"MMDBCONVERT_SIGNAL_TEST_HELPER=1",
				"MMDBCONVERT_SIGNAL_TEST_IGNORED="+ignored,
			)
			output, err := cmd.CombinedOutput()
			require.NoError(t, err, string(output))
		})
	}
}

func TestCLISecondSignal(t *testing.T) {
	for _, tt := range []struct {
		name   string
		first  os.Signal
		second os.Signal
	}{
		{name: "interrupt", first: os.Interrupt, second: os.Interrupt},
		{name: "terminated", first: syscall.SIGTERM, second: syscall.SIGTERM},
		{name: "hangup", first: syscall.SIGHUP, second: syscall.SIGHUP},
		{name: "interrupt then terminate", first: os.Interrupt, second: syscall.SIGTERM},
		{name: "terminate then interrupt", first: syscall.SIGTERM, second: os.Interrupt},
	} {
		t.Run(tt.name, func(t *testing.T) {
			skipIgnoredSignal(t, tt.first)
			skipIgnoredSignal(t, tt.second)
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			binary, err := os.Executable()
			require.NoError(t, err)
			// #nosec G204 -- run only this test binary's synchronized signal helper.
			cmd := exec.CommandContext(ctx, binary, "-test.run=^TestCLISignalHelper$")
			cmd.Env = append(
				os.Environ(),
				"MMDBCONVERT_SIGNAL_TEST_HELPER=1",
				"MMDBCONVERT_SIGNAL_TEST_BLOCK=1",
			)
			var stderr bytes.Buffer
			cmd.Stderr = &stderr
			stdout, err := cmd.StdoutPipe()
			require.NoError(t, err)
			require.NoError(t, cmd.Start())
			reader := bufio.NewReader(stdout)
			line, err := reader.ReadString('\n')
			require.NoError(t, err)
			require.Equal(t, "ready\n", line)
			require.NoError(t, cmd.Process.Signal(tt.first))
			line, err = reader.ReadString('\n')
			require.NoError(t, err)
			require.Equal(t, "canceled\n", line)
			require.NoError(t, cmd.Process.Signal(tt.second))
			var exitErr *exec.ExitError
			require.ErrorAs(t, cmd.Wait(), &exitErr, stderr.String())
			status := exitErr.Sys().(syscall.WaitStatus)
			require.True(t, status.Signaled(), stderr.String())
			require.Equal(t, tt.second, status.Signal())
			require.NoError(t, ctx.Err(), "second signal must terminate a blocked conversion")
			require.NotContains(t, stderr.String(), "cleanup complete")
		})
	}
}

func TestCLIInterruptStopsShellLoop(t *testing.T) {
	skipIgnoredSignal(t, os.Interrupt)
	bash := requireBash(t)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	binary, err := os.Executable()
	require.NoError(t, err)
	// #nosec G204 -- run the fixed loop with this test binary as a positional argument.
	cmd := exec.CommandContext(ctx, bash, "-c", `for n in 1 2; do "$@"; done`,
		"signal-test", binary, "-test.run=^TestCLISignalHelper$")
	cmd.Env = append(os.Environ(), "MMDBCONVERT_SIGNAL_TEST_HELPER=1")
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error { return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL) }
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	stdout, err := cmd.StdoutPipe()
	require.NoError(t, err)
	require.NoError(t, cmd.Start())
	t.Cleanup(func() {
		// Also reap a second child if a regression lets the loop continue.
		if err := syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL); err != nil {
			require.ErrorIs(t, err, syscall.ESRCH)
		}
	})
	line, err := bufio.NewReader(stdout).ReadString('\n')
	require.NoError(t, err)
	require.Equal(t, "ready\n", line)
	require.NoError(t, syscall.Kill(-cmd.Process.Pid, syscall.SIGINT))
	var exitErr *exec.ExitError
	require.ErrorAs(t, cmd.Wait(), &exitErr, stderr.String())
	require.NoError(t, ctx.Err(), "interrupt must stop the loop before a second conversion")
	require.Contains(t, stderr.String(), "cleanup complete")
	status := exitErr.Sys().(syscall.WaitStatus)
	require.True(t, status.Signaled(), stderr.String())
	require.Equal(t, syscall.SIGINT, status.Signal())
}

func skipIgnoredSignal(t *testing.T, sig os.Signal) {
	t.Helper()
	if signal.Ignored(sig) {
		t.Skipf("%s is inherited as ignored", sig)
	}
}

func requireBash(t *testing.T) string {
	t.Helper()
	path, err := exec.LookPath("bash")
	if err != nil {
		t.Skipf("bash is unavailable: %v", err)
	}
	return path
}
