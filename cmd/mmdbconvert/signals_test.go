package main

import (
	"context"
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
