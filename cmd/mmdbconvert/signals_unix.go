//go:build !windows

package main

import (
	"os"
	"os/signal"
	"syscall"
	"time"
)

func terminationSignals() []os.Signal {
	signals := []os.Signal{os.Interrupt, syscall.SIGTERM}
	// Preserve inherited SIGHUP ignoring, as set by nohup.
	if !signal.Ignored(syscall.SIGHUP) {
		signals = append(signals, syscall.SIGHUP)
	}
	return signals
}

func exitAfterSignal(sig os.Signal) int {
	// Shells distinguish signal termination from a normal exit with code 130.
	// Re-raise after cleanup so an interrupt also stops a waiting shell loop.
	signal.Reset(sig)
	if !signal.Ignored(sig) {
		if err := syscall.Kill(os.Getpid(), sig.(syscall.Signal)); err == nil {
			// Allow asynchronous delivery before falling back to a numeric status.
			time.Sleep(time.Second)
		}
	}
	return 128 + int(sig.(syscall.Signal))
}
