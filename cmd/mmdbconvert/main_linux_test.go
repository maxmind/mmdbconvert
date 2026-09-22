package main

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
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
	for _, failConversion := range []bool{false, true} {
		t.Run(fmt.Sprintf("conversion_failure=%t", failConversion), func(t *testing.T) {
			path := configPath
			if failConversion {
				path = filepath.Join(t.TempDir(), "missing.toml")
			}
			var stdout, stderr bytes.Buffer
			args := []string{"--quiet", "--memprofile", "/dev/full", path}
			assert.Equal(t, 1, runCLI(args, &stdout, &stderr, false))
			assert.Empty(t, stdout.String())

			records := decodeLogRecords(t, stderr.String())
			if failConversion {
				require.Len(t, records, 2)
				assert.Equal(t, "Converting databases", records[0]["message"])
				assert.Contains(t, records[0]["error"], path)
			} else {
				require.Len(t, records, 1)
			}
			for _, record := range records {
				assert.Equal(t, "ERROR", record["level"])
			}
			record := records[len(records)-1]
			assert.Equal(t, "Writing memory profile", record["message"])
			assert.Contains(t, record["error"], "no space left on device")
		})
	}
}
