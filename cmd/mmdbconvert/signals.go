package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
)

// runWithSignals owns signal handling only for the lifetime of the CLI.
func runWithSignals(ctx context.Context, logger *slog.Logger, run func(context.Context) int) int {
	signals := make(chan os.Signal, 1)
	if watched := terminationSignals(); len(watched) != 0 {
		signal.Notify(signals, watched...)
	}
	code, cause := runWithSignalChannel(ctx, run, signals, logger)
	if cause != nil {
		logger.Warn("Conversion interrupted", "cause", cause)
	}
	if interruption, ok := errors.AsType[*signalError](cause); ok {
		return exitAfterSignal(interruption.signal, logger)
	}
	return code
}

// runWithSignalChannel consumes the first signal and owns stopping the channel.
func runWithSignalChannel(
	parent context.Context,
	run func(context.Context) int,
	signals chan os.Signal,
	logger *slog.Logger,
) (int, error) {
	ctx, cancel := context.WithCancelCause(parent)
	defer cancel(nil)
	finished := make(chan struct{})
	done := make(chan struct{})
	go func(finished <-chan struct{}) {
		defer close(done)
		interrupted := false
		for {
			select {
			case sig, ok := <-signals:
				if !ok {
					return
				}
				if interrupted {
					//revive:disable-next-line:deep-exit A second signal must bypass blocked cleanup.
					os.Exit(exitAfterSignal(sig, logger))
				}
				// Restore default handling before cancellation becomes visible,
				// so a second signal can terminate blocked cleanup.
				signal.Stop(signals)
				interrupted = true
				cancel(&signalError{signal: sig})
			case <-finished:
				// Stop guarantees no more sends. Drain queued signals before
				// normal completion can cancel the context without a cause.
				signal.Stop(signals)
				close(signals)
				finished = nil
			}
		}
	}(finished)
	code := run(ctx)
	close(finished)
	<-done
	if cause := context.Cause(ctx); cause != nil {
		return code, fmt.Errorf("running conversion: %w", cause)
	}
	return code, nil
}

type signalError struct {
	signal os.Signal
}

func (e *signalError) Error() string {
	return fmt.Sprintf("%s signal received", e.signal)
}
