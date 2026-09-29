package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRunWithSignalChannelCompletion(t *testing.T) {
	for _, interrupted := range []bool{false, true} {
		name := "normal"
		if interrupted {
			name = "queued interrupt"
		}
		t.Run(name, func(t *testing.T) {
			signals := make(chan os.Signal, 1)
			code, cause := runWithSignalChannel(t.Context(), func(context.Context) int {
				if interrupted {
					signals <- os.Interrupt
				}
				return 0
			}, signals, newLogger(io.Discard, logFormatJSON, slog.LevelWarn))
			require.Zero(t, code)
			if interrupted {
				var interruption *signalError
				require.ErrorAs(t, cause, &interruption)
				require.Equal(t, os.Interrupt, interruption.signal)
			} else {
				require.NoError(t, cause)
			}
		})
	}
}

func TestRunWithSignalChannelCanceledParent(t *testing.T) {
	for _, code := range []int{0, 1} {
		t.Run(fmt.Sprintf("code=%d", code), func(t *testing.T) {
			ctx, cancel := context.WithCancelCause(t.Context())
			defer cancel(nil)
			parentCause := errors.New("parent stopped conversion")
			cancel(parentCause)
			signals := make(chan os.Signal, 1)
			signals <- os.Interrupt
			got, cause := runWithSignalChannel(ctx, func(ctx context.Context) int {
				require.ErrorIs(t, context.Cause(ctx), parentCause)
				return code
			}, signals, newLogger(io.Discard, logFormatJSON, slog.LevelWarn))
			require.Equal(t, code, got)
			var interruption *signalError
			require.ErrorAs(t, cause, &interruption)
			require.Equal(t, os.Interrupt, interruption.signal)
			require.ErrorIs(t, context.Cause(ctx), parentCause)
		})
	}
}

func TestRunWithSignalsParentCancellation(t *testing.T) {
	for _, code := range []int{0, 1} {
		t.Run(fmt.Sprintf("code=%d", code), func(t *testing.T) {
			ctx, cancel := context.WithCancelCause(t.Context())
			defer cancel(nil)
			parentCause := errors.New("parent stopped conversion")
			var stderr bytes.Buffer
			logger := newLogger(&stderr, logFormatJSON, slog.LevelWarn)
			got := runWithSignals(ctx, logger, func(context.Context) int {
				cancel(parentCause)
				return code
			})
			require.Equal(t, code, got)
			if code == 0 {
				require.Empty(t, stderr.String())
				return
			}
			records := decodeLogRecords(t, stderr.String())
			require.Len(t, records, 1)
			require.Equal(t, "WARN", records[0]["level"])
			require.Equal(t, "Conversion context canceled", records[0]["message"])
			require.Contains(t, records[0]["error"], parentCause.Error())
		})
	}
}
