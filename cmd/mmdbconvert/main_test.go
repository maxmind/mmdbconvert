package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime/pprof"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRunCLIFormats(t *testing.T) {
	tests := []struct {
		name     string
		args     []string
		terminal bool
		json     bool
		code     int
	}{
		{name: "auto pipe", json: true, code: 1},
		{name: "auto terminal", terminal: true, code: 1},
		{name: "explicit auto pipe", args: []string{"--log-format=auto"}, json: true, code: 1},
		{
			name:     "explicit auto terminal",
			args:     []string{"--log-format=auto"},
			terminal: true,
			code:     1,
		},
		{
			name:     "force JSON at terminal",
			args:     []string{"--log-format", "json"},
			terminal: true,
			json:     true,
			code:     1,
		},
		{name: "force text in pipe", args: []string{"--log-format=text"}, code: 1},
		{
			name: "parsed override on error", args: []string{"--log-format=json", "--unknown"},
			terminal: true, json: true, code: 2,
		},
		{
			name: "parsed text override on error", args: []string{"--log-format=text", "--unknown"},
			code: 2,
		},
		{
			name: "unparsed override in pipe", args: []string{"--unknown", "--log-format=text"},
			json: true, code: 2,
		},
		{
			name: "unparsed override at terminal", args: []string{"--unknown", "--log-format=json"},
			terminal: true, code: 2,
		},
		{
			name: "invalid override in pipe",
			args: []string{"--log-format=text", "--log-format=invalid"},
			json: true,
			code: 2,
		},
		{
			name:     "invalid override at terminal",
			args:     []string{"--log-format=json", "--log-format=invalid"},
			terminal: true,
			code:     2,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			assert.Equal(t, tt.code, runCLI(tt.args, &stdout, &stderr, tt.terminal))
			assert.Empty(t, stdout.String())
			if tt.json {
				records := decodeLogRecords(t, stderr.String())
				require.Len(t, records, 1)
				assert.Equal(t, "ERROR", records[0]["level"])
			} else {
				assert.Contains(t, stderr.String(), "level=ERROR message=")
				assert.Contains(t, stderr.String(), "USAGE:")
				assert.NotContains(t, stderr.String(), "msg=")
			}
		})
	}
}

func TestRunCLISuccess(t *testing.T) {
	for _, format := range []string{"json", "text"} {
		for _, quiet := range []bool{false, true} {
			for _, disableCache := range []bool{false, true} {
				name := fmt.Sprintf("%s/quiet=%t/disable_cache=%t", format, quiet, disableCache)
				t.Run(name, func(t *testing.T) {
					configPath, outputPath := writeTestConfig(t, testDatabasePath(t))
					args := []string{
						"--log-format=" + format,
						fmt.Sprintf("--quiet=%t", quiet),
						fmt.Sprintf("--disable-cache=%t", disableCache),
						configPath,
					}
					var stdout, stderr bytes.Buffer
					assert.Equal(t, 0, runCLI(args, &stdout, &stderr, false))
					assert.Empty(t, stdout.String())
					output, err := os.ReadFile(filepath.Clean(outputPath))
					require.NoError(t, err)
					assert.True(t, bytes.HasPrefix(output, []byte("network,country_code\n")))
					assert.Greater(t, bytes.Count(output, []byte("\n")), 1)

					if quiet {
						assert.Empty(t, stderr.String())
						return
					}
					if format == "text" {
						assert.Contains(
							t,
							stderr.String(),
							"level=INFO message=\"Successfully completed\"",
						)
						assert.Contains(t, stderr.String(), "elapsed_ms=")
						assert.NotContains(t, stderr.String(), "msg=")
						return
					}
					records := decodeLogRecords(t, stderr.String())
					var foundVersion, foundConfig, foundCache, foundCompletion bool
					for _, record := range records {
						assert.Equal(t, "INFO", record["level"])
						if v, ok := record["version"]; ok {
							assert.Equal(t, version, v)
							foundVersion = true
						}
						if v, ok := record["config_path"]; ok {
							assert.Equal(t, configPath, v)
							foundConfig = true
						}
						if v, ok := record["disable_cache"]; ok {
							assert.Equal(t, disableCache, v)
							foundCache = true
						}
						if v, ok := record["elapsed_ms"]; ok {
							assert.GreaterOrEqual(t, v, float64(0))
							assert.Equal(t, "Successfully completed", record["message"])
							foundCompletion = true
						}
					}
					assert.True(t, foundVersion)
					assert.True(t, foundConfig)
					assert.Equal(t, disableCache, foundCache)
					assert.True(t, foundCompletion)
				})
			}
		}
	}
}

func TestRunCLIArgumentErrors(t *testing.T) {
	tests := []struct {
		name    string
		args    []string
		code    int
		message string
		errText string
	}{
		{name: "missing config", code: 1, message: "Config file path required"},
		{
			name:    "unknown flag",
			args:    []string{"--unknown"},
			errText: "flag provided but not defined",
		},
		{name: "bad syntax", args: []string{"---config"}, errText: "bad flag syntax"},
		{
			name:    "invalid boolean",
			args:    []string{"--quiet=maybe"},
			errText: "invalid boolean value",
		},
		{
			name:    "missing config value",
			args:    []string{"--config"},
			errText: "flag needs an argument",
		},
		{
			name:    "missing format value",
			args:    []string{"--log-format"},
			errText: "flag needs an argument",
		},
		{name: "invalid format", args: []string{"--log-format=xml"}, errText: "invalid log format"},
		{name: "empty format", args: []string{"--log-format="}, errText: "invalid log format"},
		{
			name:    "uppercase format",
			args:    []string{"--log-format=JSON"},
			errText: "invalid log format",
		},
		{
			name:    "invalid format with help",
			args:    []string{"--log-format=xml", "--help"},
			errText: "invalid log format",
		},
		{
			name:    "invalid format with short help",
			args:    []string{"--log-format=xml", "-h"},
			errText: "invalid log format",
		},
	}
	for _, format := range []string{"json", "text"} {
		for _, tt := range tests {
			t.Run(format+"/"+tt.name, func(t *testing.T) {
				args := append([]string{"--quiet", "--log-format=" + format}, tt.args...)
				var stdout, stderr bytes.Buffer
				code := tt.code
				if code == 0 {
					code = 2
				}
				assert.Equal(t, code, runCLI(args, &stdout, &stderr, format == "text"))
				assert.Empty(t, stdout.String())
				message := tt.message
				if message == "" {
					message = "Parsing command-line flags"
				}
				if format == "text" {
					assert.Contains(t, stderr.String(), "level=ERROR")
					assert.Contains(t, stderr.String(), message)
					assert.Contains(t, stderr.String(), tt.errText)
					assert.Contains(t, stderr.String(), "USAGE:")
					return
				}
				records := decodeLogRecords(t, stderr.String())
				require.Len(t, records, 1)
				assert.Equal(t, "ERROR", records[0]["level"])
				assert.Equal(t, message, records[0]["message"])
				if tt.errText != "" {
					assert.Contains(t, records[0]["error"], tt.errText)
				}
			})
		}
	}
}

func TestRunCLIOperationalErrors(t *testing.T) {
	configPath, _ := writeTestConfig(t, testDatabasePath(t))
	missingConfig := filepath.Join(t.TempDir(), "missing.toml")
	missingDatabaseConfig, _ := writeTestConfig(t, filepath.Join(t.TempDir(), "missing.mmdb"))
	invalidConfig := filepath.Join(t.TempDir(), "invalid.toml")
	require.NoError(t, os.WriteFile(invalidConfig, []byte("invalid toml [[["), 0o600))
	directory := t.TempDir()
	tests := []struct {
		name    string
		args    []string
		message string
		errText string
	}{
		{
			name: "missing config", args: []string{"--config", missingConfig},
			message: "Converting databases", errText: "loading config: reading config file:",
		},
		{
			name: "unreadable config", args: []string{"--config", directory},
			message: "Converting databases", errText: "loading config: reading config file:",
		},
		{
			name: "invalid config", args: []string{"--config", invalidConfig},
			message: "Converting databases", errText: "loading config: parsing TOML:",
		},
		{
			name: "missing database", args: []string{"--config", missingDatabaseConfig},
			message: "Converting databases", errText: "opening databases:",
		},
		{
			name: "CPU profile creation", args: []string{"--cpuprofile", directory, configPath},
			message: "Creating CPU profile", errText: directory,
		},
		{
			name: "memory profile creation", args: []string{"--memprofile", directory, configPath},
			message: "Creating memory profile", errText: directory,
		},
	}
	for _, format := range []string{"json", "text"} {
		for _, quiet := range []bool{false, true} {
			for _, tt := range tests {
				t.Run(fmt.Sprintf("%s/quiet=%t/%s", format, quiet, tt.name), func(t *testing.T) {
					args := append(
						[]string{"--log-format=" + format, fmt.Sprintf("--quiet=%t", quiet)},
						tt.args...)
					var stdout, stderr bytes.Buffer
					assert.Equal(t, 1, runCLI(args, &stdout, &stderr, false))
					assert.Empty(t, stdout.String())
					assert.NotContains(t, stderr.String(), "USAGE:")
					if format == "text" {
						assert.Contains(t, stderr.String(), "level=ERROR")
						assert.Contains(t, stderr.String(), tt.message)
						if quiet {
							assert.NotContains(t, stderr.String(), "level=INFO")
						}
						return
					}
					records := decodeLogRecords(t, stderr.String())
					if quiet {
						require.Len(t, records, 1)
					}
					record := records[len(records)-1]
					assert.Equal(t, "ERROR", record["level"])
					assert.Equal(t, tt.message, record["message"])
					assert.Contains(t, record["error"], tt.errText)
				})
			}
		}
	}
}

func TestRunCLIHelpAndVersion(t *testing.T) {
	for _, format := range []string{"json", "text"} {
		for _, flagName := range []string{"--help", "-h", "--version"} {
			t.Run(format+"/"+flagName, func(t *testing.T) {
				var stdout, stderr bytes.Buffer
				args := []string{"--quiet", "--log-format=" + format, flagName}
				assert.Equal(t, 0, runCLI(args, &stdout, &stderr, false))
				if flagName == "--version" {
					assert.Equal(t, "mmdbconvert version "+version+"\n", stdout.String())
					assert.Empty(t, stderr.String())
				} else {
					assert.Empty(t, stdout.String())
					assert.Contains(t, stderr.String(), "USAGE:")
					assert.Contains(t, stderr.String(), "--log-format")
					assert.NotContains(t, stderr.String(), "level=")
				}
			})
		}
	}
}

func TestRunCLIVersionWriteError(t *testing.T) {
	stdout, err := os.Create(filepath.Join(t.TempDir(), "closed"))
	require.NoError(t, err)
	require.NoError(t, stdout.Close())
	var stderr bytes.Buffer
	assert.Equal(t, 1, runCLI([]string{"--version"}, stdout, &stderr, false))
	records := decodeLogRecords(t, stderr.String())
	require.Len(t, records, 1)
	assert.Equal(t, "ERROR", records[0]["level"])
	assert.Equal(t, "Writing version", records[0]["message"])
	assert.Contains(t, records[0]["error"], os.ErrClosed.Error())
}

func TestRunCLIProfiling(t *testing.T) {
	configPath, _ := writeTestConfig(t, testDatabasePath(t))
	for _, failConversion := range []bool{false, true} {
		t.Run(fmt.Sprintf("conversion_failure=%t", failConversion), func(t *testing.T) {
			cpuPath := filepath.Join(t.TempDir(), "cpu.prof")
			memoryPath := filepath.Join(t.TempDir(), "memory.prof")
			path := configPath
			wantCode := 0
			if failConversion {
				path = filepath.Join(t.TempDir(), "missing.toml")
				wantCode = 1
			}
			var stdout, stderr bytes.Buffer
			args := []string{"--quiet", "--cpuprofile", cpuPath, "--memprofile", memoryPath, path}
			assert.Equal(t, wantCode, runCLI(args, &stdout, &stderr, false))
			assert.Empty(t, stdout.String())
			if failConversion {
				records := decodeLogRecords(t, stderr.String())
				require.Len(t, records, 1)
				assert.Equal(t, "Converting databases", records[0]["message"])
			} else {
				assert.Empty(t, stderr.String())
			}
			for _, profilePath := range []string{cpuPath, memoryPath} {
				info, err := os.Stat(profilePath)
				require.NoError(t, err)
				assert.Positive(t, info.Size())
				// Removing the files also checks that the CLI closed them on Windows.
				require.NoError(t, os.Remove(profilePath))
			}
			// A second profiler can start only if the CLI stopped its profiler.
			require.NoError(t, pprof.StartCPUProfile(io.Discard))
			pprof.StopCPUProfile()
		})
	}
}

func TestRunCLICPUProfileAlreadyStarted(t *testing.T) {
	require.NoError(t, pprof.StartCPUProfile(io.Discard))
	t.Cleanup(pprof.StopCPUProfile)
	cpuPath := filepath.Join(t.TempDir(), "cpu.prof")
	var stdout, stderr bytes.Buffer
	args := []string{"--quiet", "--cpuprofile", cpuPath, "config.toml"}
	assert.Equal(t, 1, runCLI(args, &stdout, &stderr, false))
	assert.Empty(t, stdout.String())
	records := decodeLogRecords(t, stderr.String())
	require.Len(t, records, 1)
	assert.Equal(t, "ERROR", records[0]["level"])
	assert.Equal(t, "Starting CPU profile", records[0]["message"])
	assert.Contains(t, records[0]["error"], "cpu profiling already in use")
	require.NoError(t, os.Remove(cpuPath))
}

func decodeLogRecords(t *testing.T, output string) []map[string]any {
	t.Helper()
	require.NotEmpty(t, output)
	require.True(t, strings.HasSuffix(output, "\n"), "log output must end with a newline")
	var records []map[string]any
	for line := range strings.SplitSeq(strings.TrimSuffix(output, "\n"), "\n") {
		var record map[string]any
		require.NoError(t, json.Unmarshal([]byte(line), &record), "log line: %s", line)
		assert.NotContains(t, record, "msg")
		assert.NotEmpty(t, record["message"])
		assert.NotEmpty(t, record["level"])
		stamp, ok := record["time"].(string)
		require.True(t, ok, "time must be a string")
		_, err := time.Parse(time.RFC3339Nano, stamp)
		require.NoError(t, err)
		records = append(records, record)
	}
	return records
}

func testDatabasePath(t *testing.T) string {
	t.Helper()
	path, err := filepath.Abs("../../testdata/MaxMind-DB/test-data/GeoIP2-City-Test.mmdb")
	require.NoError(t, err)
	return path
}

func writeTestConfig(t *testing.T, databasePath string) (configPath, outputPath string) {
	t.Helper()
	dir := t.TempDir()
	configPath = filepath.Join(dir, "config.toml")
	outputPath = filepath.Join(dir, "output.csv")
	config := `
[output]
format = "csv"
file = "` + filepath.ToSlash(outputPath) + `"

[[databases]]
name = "city"
path = "` + filepath.ToSlash(databasePath) + `"

[[columns]]
name = "country_code"
database = "city"
path = ["country", "iso_code"]
`
	require.NoError(t, os.WriteFile(configPath, []byte(config), 0o600))
	return configPath, outputPath
}
