package main

import (
	"os"
	"syscall"
)

func terminationSignals() []os.Signal {
	return []os.Signal{os.Interrupt, syscall.SIGTERM}
}

func exitAfterSignal(sig os.Signal) int {
	return 128 + int(sig.(syscall.Signal))
}
