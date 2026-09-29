//go:build unix

package main

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"os/signal"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCLISignals(t *testing.T) {
	for _, tt := range []struct {
		name      string
		first     os.Signal
		second    os.Signal
		completed bool
	}{
		{name: "interrupt", first: os.Interrupt},
		{name: "terminated", first: syscall.SIGTERM},
		{name: "hangup", first: syscall.SIGHUP},
		{name: "interrupt with publication", first: os.Interrupt, completed: true},
		{name: "terminated with publication", first: syscall.SIGTERM, completed: true},
		{name: "hangup with publication", first: syscall.SIGHUP, completed: true},
		{name: "interrupt twice", first: os.Interrupt, second: os.Interrupt},
		{name: "terminated twice", first: syscall.SIGTERM, second: syscall.SIGTERM},
		{name: "hangup twice", first: syscall.SIGHUP, second: syscall.SIGHUP},
		{name: "interrupt then terminate", first: os.Interrupt, second: syscall.SIGTERM},
		{name: "terminate then interrupt", first: syscall.SIGTERM, second: os.Interrupt},
	} {
		t.Run(tt.name, func(t *testing.T) {
			skipIgnoredSignal(t, tt.first)
			if tt.second != nil {
				skipIgnoredSignal(t, tt.second)
			}
			child := startSignalChild(t, signalChildOptions{
				blockCleanup: tt.second != nil,
				completed:    tt.completed,
			})
			require.NoError(t, child.cmd.Process.Signal(tt.first))
			last := tt.first
			if tt.second != nil {
				child.requireStage(t, "canceled")
				require.NoError(t, child.cmd.Process.Signal(tt.second))
				last = tt.second
			}
			status, stderr := child.wait(t)
			require.Equal(t, last, status.Signal())
			if tt.second != nil {
				require.NotContains(t, stderr, "cleanup complete")
				return
			}
			require.Contains(t, stderr, "cleanup complete")
			records := decodeLogRecords(t, stderr)
			require.Len(t, records, 2)
			warning := records[1]
			message := "Signal received"
			if tt.completed {
				message = "Signal received; conversion completed and outputs were published"
			}
			require.Equal(t, message, warning["message"])
			require.Contains(t, warning["error"], tt.first.String()+" signal received")
			require.NotContains(t, warning, "cause")
		})
	}
}

type signalChildOptions struct {
	blockCleanup bool
	shellLoop    bool
	completed    bool
}

type signalChild struct {
	//nolint:containedctx // This test fixture owns the subprocess's bounded lifetime.
	ctx         context.Context
	cmd         *exec.Cmd
	stdout      *bufio.Reader
	stderr      bytes.Buffer
	waited      bool
	stopHelpers func()
}

func startSignalChild(t *testing.T, opts signalChildOptions) *signalChild {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	t.Cleanup(cancel)
	binary, err := os.Executable()
	require.NoError(t, err)
	// #nosec G204 -- run only this test binary's synchronized signal helper.
	cmd := exec.CommandContext(ctx, binary, "-test.run=^TestCLISignalHelper$")
	if opts.shellLoop {
		// #nosec G204 -- run the fixed loop with this test binary as a positional argument.
		cmd = exec.CommandContext(ctx, requireBash(t), "-c", `for n in 1 2; do "$@"; done`,
			"signal-test", binary, "-test.run=^TestCLISignalHelper$")
		cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	}
	cmd.Env = append(os.Environ(), "MMDBCONVERT_SIGNAL_TEST_HELPER=1")
	if opts.blockCleanup {
		cmd.Env = append(cmd.Env, "MMDBCONVERT_SIGNAL_TEST_BLOCK=1")
	}
	if !opts.completed {
		cmd.Env = append(cmd.Env, "MMDBCONVERT_SIGNAL_TEST_FAILED=1")
	}
	child := &signalChild{ctx: ctx, cmd: cmd, stopHelpers: func() {}}
	closeInput := func() {}
	if opts.shellLoop {
		input, lifetime, err := os.Pipe()
		require.NoError(t, err)
		closeInput = sync.OnceFunc(func() { assert.NoError(t, input.Close()) })
		t.Cleanup(closeInput)
		child.stopHelpers = sync.OnceFunc(func() { assert.NoError(t, lifetime.Close()) })
		t.Cleanup(child.stopHelpers)
		// Cmd.Wait may reap Bash before a helper releases its inherited stderr.
		// Keep this independent of os/exec's context watcher, which stops at reap.
		stop := context.AfterFunc(ctx, child.stopHelpers)
		t.Cleanup(func() { stop() })
		cmd.Stdin = input
		cmd.Env = append(cmd.Env, "MMDBCONVERT_SIGNAL_TEST_LIFETIME=1")
	}
	cmd.Stderr = &child.stderr
	stdout, err := cmd.StdoutPipe()
	require.NoError(t, err)
	child.stdout = bufio.NewReader(stdout)
	require.NoError(t, cmd.Start())
	t.Cleanup(func() {
		child.stopHelpers()
		cancel()
		if !child.waited {
			child.waited = true
			// Early assertion failures may leave a child to kill and reap.
			var exitErr *exec.ExitError
			if err := cmd.Wait(); err != nil && !errors.As(err, &exitErr) {
				assert.ErrorIs(t, err, context.Canceled)
			}
		}
	})
	closeInput()
	// The child announces readiness only after signal handling is installed.
	child.requireStage(t, "ready")
	return child
}

func (child *signalChild) requireStage(t *testing.T, stage string) {
	t.Helper()
	line, err := child.stdout.ReadString('\n')
	require.NoError(t, err)
	require.Equal(t, stage+"\n", line)
}

func (child *signalChild) wait(t *testing.T) (syscall.WaitStatus, string) {
	t.Helper()
	require.False(t, child.waited, "child must be reaped exactly once")
	child.waited = true
	err := child.cmd.Wait()
	// Wait completes the stderr copy before the buffer is read.
	stderr := child.stderr.String()
	var exitErr *exec.ExitError
	require.ErrorAs(t, err, &exitErr, stderr)
	require.NoError(t, child.ctx.Err(), "child must exit through a signal, not timeout")
	status := exitErr.Sys().(syscall.WaitStatus)
	require.True(t, status.Signaled(), stderr)
	return status, stderr
}

func TestCLISignalHelper(t *testing.T) {
	if os.Getenv("MMDBCONVERT_SIGNAL_TEST_HELPER") != "1" {
		return
	}
	if os.Getenv("MMDBCONVERT_SIGNAL_TEST_LIFETIME") == "1" {
		go func() {
			// EOF ends every helper, including a second loop iteration orphaned
			// after Bash exits. A read failure must also stop the helper.
			_, err := io.Copy(io.Discard, os.Stdin)
			assert.NoError(t, err)
			//revive:disable-next-line:deep-exit The parent is tearing down the subprocess fixture.
			os.Exit(1)
		}()
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
		defer logger.Warn("cleanup complete")
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
		if os.Getenv("MMDBCONVERT_SIGNAL_TEST_FAILED") == "1" {
			return 1
		}
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

func TestCLIInterruptStopsShellLoop(t *testing.T) {
	skipIgnoredSignal(t, os.Interrupt)
	child := startSignalChild(t, signalChildOptions{shellLoop: true, completed: true})
	require.NoError(t, syscall.Kill(-child.cmd.Process.Pid, syscall.SIGINT))
	status, stderr := child.wait(t)
	require.Contains(t, stderr, "cleanup complete")
	require.Equal(t, syscall.SIGINT, status.Signal())
}

func TestCLIShellLoopCleanup(t *testing.T) {
	child := startSignalChild(t, signalChildOptions{shellLoop: true})
	// Killing only Bash leaves its helper holding stderr open. Closing the
	// inherited input must stop that helper so Wait can finish before timeout.
	require.NoError(t, child.cmd.Process.Kill())
	child.stopHelpers()
	status, _ := child.wait(t)
	require.Equal(t, syscall.SIGKILL, status.Signal())
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
