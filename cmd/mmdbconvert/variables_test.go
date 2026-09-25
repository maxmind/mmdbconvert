package main

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRunCLIPathVariables(t *testing.T) {
	for _, positional := range []bool{false, true} {
		name := "config flag"
		if positional {
			name = "positional config"
		}
		t.Run(name, func(t *testing.T) {
			configPath := writePathVariableConfig(t)
			outputDir := t.TempDir()
			ipv4 := filepath.Join(outputDir, "IPv4 = ${literal}.csv")
			ipv6 := filepath.Join(outputDir, "IPv6.csv")
			args := []string{
				"--quiet",
				"--var", "input=wrong.mmdb",
				"--var=input=" + testDatabasePath(t),
				"--var", "ipv4=" + ipv4,
				"--var", "ipv6=" + ipv6,
				"--var", "prefix=",
			}
			if positional {
				args = append(args, configPath)
			} else {
				args = append([]string{"--config", configPath}, args...)
			}
			var stdout, stderr bytes.Buffer
			require.Equal(t, 0, runCLI(args, &stdout, &stderr, false), stderr.String())
			assert.Empty(t, stdout.String())
			assert.Empty(t, stderr.String())
			for _, path := range []string{ipv4, ipv6} {
				data, err := os.ReadFile(filepath.Clean(path))
				require.NoError(t, err)
				assert.True(t, bytes.HasPrefix(data, []byte("network,country_code\n")))
				assert.Greater(t, bytes.Count(data, []byte("\n")), 1)
			}
		})
	}
}

func TestRunCLIPathVariableErrors(t *testing.T) {
	configPath := writePathVariableConfig(t)
	ipv4 := filepath.Join(t.TempDir(), "existing.csv")
	ipv6 := filepath.Join(t.TempDir(), "new.csv")
	require.NoError(t, os.WriteFile(ipv4, []byte("keep this output"), 0o600))
	baseArgs := []string{
		"--quiet", "--config", configPath,
		"--var", "input=" + testDatabasePath(t),
		"--var", "ipv4=" + ipv4,
		"--var", "ipv6=" + ipv6,
		"--var", "prefix=",
	}
	tests := []struct {
		name    string
		args    []string
		code    int
		errText string
	}{
		{
			name:    "missing value",
			args:    []string{"--var"},
			code:    2,
			errText: "flag needs an argument",
		},
		{
			name:    "missing equals",
			args:    []string{"--var", "input"},
			code:    2,
			errText: "expected NAME=VALUE",
		},
		{
			name:    "empty name",
			args:    []string{"--var", "=value"},
			code:    2,
			errText: `invalid variable name ""`,
		},
		{
			name:    "invalid name",
			args:    []string{"--var", "bad-name=value"},
			code:    2,
			errText: "invalid variable name",
		},
		{
			name:    "unused",
			args:    []string{"--var", "unused=value"},
			code:    1,
			errText: `unused variable "unused"`,
		},
		{
			name:    "empty input",
			args:    []string{"--var", "input="},
			code:    1,
			errText: "database path is required",
		},
	}
	for _, format := range []string{"json", "text"} {
		for _, tt := range tests {
			t.Run(format+"/"+tt.name, func(t *testing.T) {
				args := append([]string{"--log-format=" + format}, baseArgs...)
				args = append(args, tt.args...)
				var stdout, stderr bytes.Buffer
				assert.Equal(t, tt.code, runCLI(args, &stdout, &stderr, false))
				assert.Empty(t, stdout.String())
				if format == "json" {
					records := decodeLogRecords(t, stderr.String())
					require.Len(t, records, 1)
					assert.Contains(t, records[0]["error"], tt.errText)
					assert.NotContains(t, stderr.String(), "USAGE:")
				} else {
					assert.Contains(t, stderr.String(), "level=ERROR")
					if tt.code == 2 {
						assert.Contains(t, stderr.String(), "USAGE:")
					} else {
						assert.NotContains(t, stderr.String(), "USAGE:")
					}
				}
				data, err := os.ReadFile(filepath.Clean(ipv4))
				require.NoError(t, err)
				assert.Equal(t, "keep this output", string(data))
				assert.NoFileExists(t, ipv6)
			})
		}
	}
}

func TestRunCLIPathVariablesRequired(t *testing.T) {
	var stdout, stderr bytes.Buffer
	args := []string{"--quiet", "--log-format=json", "--config", writePathVariableConfig(t)}
	assert.Equal(t, 1, runCLI(args, &stdout, &stderr, false))
	records := decodeLogRecords(t, stderr.String())
	require.Len(t, records, 1)
	assert.Contains(t, records[0]["error"], `output.ipv4_file: undefined variable "prefix"`)
}

func writePathVariableConfig(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.toml")
	content := `
[output]
format = "csv"
ipv4_file = "${prefix}${ipv4}"
ipv6_file = "${prefix}${ipv6}"

[[databases]]
name = "city"
path = "${input}"

[[columns]]
name = "country_code"
database = "city"
path = ["country", "iso_code"]
`
	require.NoError(t, os.WriteFile(path, []byte(content), 0o600))
	return path
}
