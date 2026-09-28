//go:build !unix && !windows

package main

import (
	"log/slog"
	"os"
)

// Other platforms retain their native signal handling. Callers can still cancel
// conversions through their context.
func terminationSignals() []os.Signal {
	return nil
}

func exitAfterSignal(_ os.Signal, _ *slog.Logger) int {
	return 1
}
