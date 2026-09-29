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
	if interruption, ok := errors.AsType[*signalError](cause); ok {
		message := "Signal received"
		if code == 0 {
			message = "Signal received; conversion completed and outputs were published"
		}
		logger.Warn(message, "error", cause)
		return exitAfterSignal(interruption.signal, logger)
	}
	if cause != nil && code != 0 {
		logger.Warn("Conversion context canceled", "error", cause)
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
	var interruption *signalError
	go func(finished <-chan struct{}) {
		defer close(done)
		for {
			select {
			case sig, ok := <-signals:
				if !ok {
					return
				}
				if interruption != nil {
					//revive:disable-next-line:deep-exit A second signal must bypass blocked cleanup.
					os.Exit(exitAfterSignal(sig, logger))
				}
				// Restore default handling before cancellation becomes visible,
				// so a second signal can terminate blocked cleanup.
				signal.Stop(signals)
				interruption = &signalError{signal: sig}
				cancel(interruption)
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
	// Parent cancellation may have won the context's cause. A received signal
	// still determines process termination, independently of the callback's cause.
	if interruption != nil {
		return code, fmt.Errorf("running conversion: %w", interruption)
	}
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
