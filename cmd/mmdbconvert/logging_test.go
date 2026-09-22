package main

import (
	"bytes"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestJSONLogEscaping(t *testing.T) {
	var output bytes.Buffer
	logger := newLogger(&output, logFormatJSON, slog.LevelInfo)
	message := "Diagnostic with a \"quote\"\nand a new line"
	err := errors.New("wrapped error:\n\tinvalid \"value\"")
	logger.Error(message, "error", err)

	records := decodeLogRecords(t, output.String())
	require.Len(t, records, 1)
	assert.Equal(t, message, records[0]["message"])
	assert.Equal(t, err.Error(), records[0]["error"])
}

func TestQuietKeepsWarningsAndErrors(t *testing.T) {
	var output bytes.Buffer
	logger := newLogger(&output, logFormatJSON, slog.LevelWarn)
	logger.Info("Progress")
	logger.Warn("Warning")
	logger.Error("Error")

	records := decodeLogRecords(t, output.String())
	require.Len(t, records, 2)
	assert.Equal(t, "WARN", records[0]["level"])
	assert.Equal(t, "ERROR", records[1]["level"])
}

func TestLogDuration(t *testing.T) {
	for _, format := range []string{logFormatJSON, logFormatText} {
		t.Run(format, func(t *testing.T) {
			var output bytes.Buffer
			logger := newLogger(&output, format, slog.LevelInfo)
			logger.Info("Successfully completed", slog.Duration("elapsed", 2*time.Hour))
			if format == logFormatJSON {
				records := decodeLogRecords(t, output.String())
				require.Len(t, records, 1)
				assert.InDelta(t, 7200000, records[0]["elapsed_ms"], 0)
				assert.NotContains(t, records[0], "elapsed")
			} else {
				assert.Contains(t, output.String(), "elapsed=2h0m0s")
				assert.NotContains(t, output.String(), "elapsed_ms")
			}
		})
	}
}
