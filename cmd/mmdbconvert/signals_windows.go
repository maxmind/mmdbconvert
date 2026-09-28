package main

import (
	"log/slog"
	"os"
	"syscall"
)

func terminationSignals() []os.Signal {
	return []os.Signal{os.Interrupt, syscall.SIGTERM}
}

func exitAfterSignal(_ os.Signal, _ *slog.Logger) int {
	// Preserve the native console termination status, STATUS_CONTROL_C_EXIT
	// (0xC000013A), expressed as a signed exit code for os.Exit.
	return -1073741510
}
