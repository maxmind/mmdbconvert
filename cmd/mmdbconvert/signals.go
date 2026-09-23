package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
)

// runWithSignals owns signal handling only for the lifetime of the CLI.
func runWithSignals(run func(context.Context) int) int {
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, terminationSignals()...)
	defer signal.Stop(signals)
	done := make(chan struct{})
	go func() {
		defer close(done)
		select {
		case sig := <-signals:
			// Restore default handling before cancellation becomes visible, so a
			// second interrupt can terminate even a blocked conversion.
			signal.Stop(signals)
			cancel(&signalError{signal: sig})
		case <-ctx.Done():
		}
	}()
	code := run(ctx)
	signal.Stop(signals)
	cancel(nil)
	<-done
	if cause, ok := errors.AsType[*signalError](context.Cause(ctx)); ok {
		return exitAfterSignal(cause.signal)
	}
	return code
}

type signalError struct {
	signal os.Signal
}

func (e *signalError) Error() string {
	return fmt.Sprintf("%s signal received", e.signal)
}
