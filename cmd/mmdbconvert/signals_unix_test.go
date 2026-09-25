//go:build !windows

package main

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
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
	ignoreHangup := os.Getenv("MMDBCONVERT_SIGNAL_TEST_NOHUP") == "1"
	if ignoreHangup {
		signal.Ignore(syscall.SIGHUP)
	}
	code := runWithSignals(func(ctx context.Context) int {
		if ignoreHangup {
			require.True(t, signal.Ignored(syscall.SIGHUP))
			return 0
		}
		defer fmt.Fprintln(os.Stderr, "cleanup complete")
		_, err := fmt.Fprintln(os.Stdout, "ready")
		require.NoError(t, err)
		<-ctx.Done()
		require.ErrorIs(t, ctx.Err(), context.Canceled)
		_, err = fmt.Fprintln(os.Stderr, context.Cause(ctx))
		require.NoError(t, err)
		if os.Getenv("MMDBCONVERT_SIGNAL_TEST_BLOCK") == "1" {
			_, err = fmt.Fprintln(os.Stdout, "canceled")
			require.NoError(t, err)
			select {}
		}
		return 1
	})
	//revive:disable-next-line:deep-exit Subprocess helper must expose the CLI's exit status.
	os.Exit(code)
}

func TestCLIPreservesIgnoredHangup(t *testing.T) {
	binary, err := os.Executable()
	require.NoError(t, err)
	// #nosec G204 -- run only this test binary's signal helper.
	cmd := exec.CommandContext(t.Context(), binary, "-test.run=^TestCLISignalHelper$")
	cmd.Env = append(
		os.Environ(),
		"MMDBCONVERT_SIGNAL_TEST_HELPER=1",
		"MMDBCONVERT_SIGNAL_TEST_NOHUP=1",
	)
	output, err := cmd.CombinedOutput()
	require.NoError(t, err, string(output))
}

func TestCLISecondSignal(t *testing.T) {
	for _, sig := range []os.Signal{os.Interrupt, syscall.SIGTERM, syscall.SIGHUP} {
		t.Run(sig.String(), func(t *testing.T) {
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
			require.NoError(t, cmd.Process.Signal(sig))
			line, err = reader.ReadString('\n')
			require.NoError(t, err)
			require.Equal(t, "canceled\n", line)
			require.NoError(t, cmd.Process.Signal(sig))
			var exitErr *exec.ExitError
			require.ErrorAs(t, cmd.Wait(), &exitErr, stderr.String())
			status := exitErr.Sys().(syscall.WaitStatus)
			require.True(t, status.Signaled(), stderr.String())
			require.Equal(t, sig, status.Signal())
			require.NoError(t, ctx.Err(), "second signal must terminate a blocked conversion")
			require.NotContains(t, stderr.String(), "cleanup complete")
		})
	}
}

func TestCLIInterruptStopsShellLoop(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	binary, err := os.Executable()
	require.NoError(t, err)
	// #nosec G204 -- run the fixed loop with this test binary as a positional argument.
	cmd := exec.CommandContext(ctx, "bash", "-c", `for n in 1 2; do "$@"; done`,
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
