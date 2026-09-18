package main

import (
	"bytes"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestJSONLogEscaping(t *testing.T) {
	var output bytes.Buffer
	logger := newLogger(&output, true, false)
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
	logger := newLogger(&output, true, true)
	logger.Info("Progress")
	logger.Warn("Warning")
	logger.Error("Error")

	records := decodeLogRecords(t, output.String())
	require.Len(t, records, 2)
	assert.Equal(t, "WARN", records[0]["level"])
	assert.Equal(t, "ERROR", records[1]["level"])
}
