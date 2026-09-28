//go:build !windows

package main

import (
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func terminationSignals() []os.Signal {
	var signals []os.Signal
	// Honor the parent's dispositions, including nohup and background jobs.
	for _, sig := range []os.Signal{os.Interrupt, syscall.SIGTERM, syscall.SIGHUP} {
		if !signal.Ignored(sig) {
			signals = append(signals, sig)
		}
	}
	return signals
}

func exitAfterSignal(sig os.Signal, logger *slog.Logger) int {
	// Shells distinguish signal termination from a normal exit with code 130.
	// Re-raise after cleanup so an interrupt also stops a waiting shell loop.
	signal.Reset(sig)
	if err := syscall.Kill(os.Getpid(), sig.(syscall.Signal)); err != nil {
		logger.Error("Re-raising signal", "signal", sig, "error", err)
	} else {
		// Delivery is asynchronous. Normally the signal terminates us immediately;
		// retain a bounded fallback if the process unexpectedly survives it.
		time.Sleep(time.Second)
		logger.Warn("Signal did not terminate process", "signal", sig)
	}
	return 128 + int(sig.(syscall.Signal))
}
