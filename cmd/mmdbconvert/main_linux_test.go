package main

import (
	"bytes"
	"errors"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRunCLIMemoryProfileWriteError(t *testing.T) {
	info, err := os.Stat("/dev/full")
	if errors.Is(err, os.ErrNotExist) {
		t.Skip("/dev/full is unavailable")
	}
	require.NoError(t, err)
	if info.Mode()&os.ModeCharDevice == 0 {
		t.Skip("/dev/full is not a character device")
	}
	configPath, _ := writeTestConfig(t, testDatabasePath(t))
	var stdout, stderr bytes.Buffer
	args := []string{"--quiet", "--memprofile", "/dev/full", configPath}
	assert.Equal(t, 1, runCLI(args, &stdout, &stderr, false))
	assert.Empty(t, stdout.String())
	records := decodeLogRecords(t, stderr.String())
	require.Len(t, records, 1)
	assert.Equal(t, "ERROR", records[0]["level"])
	assert.Equal(t, "Writing memory profile", records[0]["message"])
	assert.Contains(t, records[0]["error"], "no space left on device")
}
