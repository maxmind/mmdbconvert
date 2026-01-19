package mmdbconvert

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/maxmind/mmdbconvert/internal/config"
	"github.com/maxmind/mmdbconvert/internal/mmdb"
)

const testDataDir = "testdata/MaxMind-DB/test-data"

func TestRun_ValidConfig(t *testing.T) {
	outputFile := filepath.Join(t.TempDir(), "output.csv")
	configFile := filepath.Join(t.TempDir(), "config.toml")

	absTestDataDir, err := filepath.Abs(testDataDir)
	require.NoError(t, err)

	configContent := `
[output]
format = "csv"
file = "` + outputFile + `"

[[databases]]
name = "city"
path = "` + filepath.Join(absTestDataDir, "GeoIP2-City-Test.mmdb") + `"

[[columns]]
name = "country_code"
database = "city"
path = ["country", "iso_code"]
`

	err = os.WriteFile(configFile, []byte(configContent), 0o600)
	require.NoError(t, err)

	err = Run(Options{ConfigPath: configFile})
	require.NoError(t, err)

	// Verify output file was created
	info, err := os.Stat(outputFile)
	require.NoError(t, err)
	assert.Positive(t, info.Size())

	// Verify output contains expected content
	content, err := os.ReadFile(filepath.Clean(outputFile))
	require.NoError(t, err)
	lines := strings.Split(strings.TrimSpace(string(content)), "\n")
	assert.Greater(t, len(lines), 1)
	assert.Equal(t, "network,country_code", lines[0])
}

func TestRun_DisableCache(t *testing.T) {
	outputFile := filepath.Join(t.TempDir(), "output.csv")
	configFile := filepath.Join(t.TempDir(), "config.toml")

	absTestDataDir, err := filepath.Abs(testDataDir)
	require.NoError(t, err)

	configContent := `
[output]
format = "csv"
file = "` + outputFile + `"

[[databases]]
name = "city"
path = "` + filepath.Join(absTestDataDir, "GeoIP2-City-Test.mmdb") + `"

[[columns]]
name = "country_code"
database = "city"
path = ["country", "iso_code"]
`

	err = os.WriteFile(configFile, []byte(configContent), 0o600)
	require.NoError(t, err)

	// Run with DisableCache option
	err = Run(Options{
		ConfigPath:   configFile,
		DisableCache: true,
	})
	require.NoError(t, err)

	// Verify output file was created
	info, err := os.Stat(outputFile)
	require.NoError(t, err)
	assert.Positive(t, info.Size())
}

func TestRun_MissingConfigPath(t *testing.T) {
	err := Run(Options{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "config path is required")
}

func TestRun_NonexistentConfigFile(t *testing.T) {
	err := Run(Options{ConfigPath: "/nonexistent/config.toml"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "loading config")
}

func TestRun_InvalidConfig(t *testing.T) {
	configFile := filepath.Join(t.TempDir(), "config.toml")

	// Write invalid TOML
	err := os.WriteFile(configFile, []byte("invalid toml [[["), 0o600)
	require.NoError(t, err)

	err = Run(Options{ConfigPath: configFile})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "loading config")
}

func TestRun_NonexistentDatabase(t *testing.T) {
	outputFile := filepath.Join(t.TempDir(), "output.csv")
	configFile := filepath.Join(t.TempDir(), "config.toml")

	configContent := `
[output]
format = "csv"
file = "` + outputFile + `"

[[databases]]
name = "city"
path = "/nonexistent/database.mmdb"

[[columns]]
name = "country_code"
database = "city"
path = ["country", "iso_code"]
`

	err := os.WriteFile(configFile, []byte(configContent), 0o600)
	require.NoError(t, err)

	err = Run(Options{ConfigPath: configFile})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "opening databases")
}

func TestRun_ParquetOutput(t *testing.T) {
	outputFile := filepath.Join(t.TempDir(), "output.parquet")
	configFile := filepath.Join(t.TempDir(), "config.toml")

	absTestDataDir, err := filepath.Abs(testDataDir)
	require.NoError(t, err)

	configContent := `
[output]
format = "parquet"
file = "` + outputFile + `"

[[databases]]
name = "city"
path = "` + filepath.Join(absTestDataDir, "GeoIP2-City-Test.mmdb") + `"

[network]
columns = [
	{ name = "network", type = "cidr" },
]

[[columns]]
name = "country_code"
database = "city"
path = ["country", "iso_code"]
`

	err = os.WriteFile(configFile, []byte(configContent), 0o600)
	require.NoError(t, err)

	err = Run(Options{ConfigPath: configFile})
	require.NoError(t, err)

	// Verify output file was created
	info, err := os.Stat(outputFile)
	require.NoError(t, err)
	assert.Positive(t, info.Size())
}

func TestRun_SplitIPv4IPv6Output(t *testing.T) {
	tmpDir := t.TempDir()
	ipv4File := filepath.Join(tmpDir, "ipv4.csv")
	ipv6File := filepath.Join(tmpDir, "ipv6.csv")
	configFile := filepath.Join(tmpDir, "config.toml")

	absTestDataDir, err := filepath.Abs(testDataDir)
	require.NoError(t, err)

	configContent := `
[output]
format = "csv"
ipv4_file = "` + ipv4File + `"
ipv6_file = "` + ipv6File + `"

[[databases]]
name = "city"
path = "` + filepath.Join(absTestDataDir, "GeoIP2-City-Test.mmdb") + `"

[[columns]]
name = "country_code"
database = "city"
path = ["country", "iso_code"]
`

	err = os.WriteFile(configFile, []byte(configContent), 0o600)
	require.NoError(t, err)

	err = Run(Options{ConfigPath: configFile})
	require.NoError(t, err)

	// Verify both output files were created
	info, err := os.Stat(ipv4File)
	require.NoError(t, err)
	assert.Positive(t, info.Size())

	info, err = os.Stat(ipv6File)
	require.NoError(t, err)
	assert.Positive(t, info.Size())
}

// Tests for internal validation functions

func TestValidateParquetNetworkColumns_IPv6SingleFileError(t *testing.T) {
	cfg := &config.Config{
		Output: config.OutputConfig{
			Format: "parquet",
			File:   "out.parquet",
		},
		Network: config.NetworkConfig{
			Columns: []config.NetworkColumn{
				{Name: "start_int", Type: "start_int"},
				{Name: "end_int", Type: "end_int"},
			},
		},
		Databases: []config.Database{
			{
				Name: "city",
				Path: filepath.Join(testDataDir, "GeoIP2-City-Test.mmdb"),
			},
		},
		Columns: []config.Column{
			{
				Name:     "country",
				Database: "city",
				Path:     config.Path{"country"},
			},
		},
	}

	readers := openTestReaders(t, cfg)
	err := validateParquetNetworkColumns(cfg, readers)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "start_int")
}

func TestValidateParquetNetworkColumns_SplitOutputsAllowed(t *testing.T) {
	cfg := &config.Config{
		Output: config.OutputConfig{
			Format:   "parquet",
			IPv4File: "ipv4.parquet",
			IPv6File: "ipv6.parquet",
		},
		Network: config.NetworkConfig{
			Columns: []config.NetworkColumn{
				{Name: "start_int", Type: "start_int"},
				{Name: "end_int", Type: "end_int"},
			},
		},
		Databases: []config.Database{
			{
				Name: "city",
				Path: filepath.Join(testDataDir, "GeoIP2-City-Test.mmdb"),
			},
		},
		Columns: []config.Column{
			{
				Name:     "country",
				Database: "city",
				Path:     config.Path{"country"},
			},
		},
	}

	readers := openTestReaders(t, cfg)
	require.NoError(t, validateParquetNetworkColumns(cfg, readers))
}

func TestValidateParquetNetworkColumns_IPv4SingleFileAllowed(t *testing.T) {
	cfg := &config.Config{
		Output: config.OutputConfig{
			Format: "parquet",
			File:   "out.parquet",
		},
		Network: config.NetworkConfig{
			Columns: []config.NetworkColumn{
				{Name: "start_int", Type: "start_int"},
			},
		},
		Databases: []config.Database{
			{
				Name: "ipv4",
				Path: filepath.Join(testDataDir, "MaxMind-DB-test-ipv4-24.mmdb"),
			},
		},
		Columns: []config.Column{
			{
				Name:     "test",
				Database: "ipv4",
				Path:     config.Path{"data"},
			},
		},
	}

	readers := openTestReaders(t, cfg)
	require.NoError(t, validateParquetNetworkColumns(cfg, readers))
}

func openTestReaders(t *testing.T, cfg *config.Config) *mmdb.Readers {
	paths := make(map[string]string, len(cfg.Databases))
	for _, db := range cfg.Databases {
		paths[db.Name] = db.Path
	}
	readers, err := mmdb.OpenDatabases(paths)
	require.NoError(t, err)
	t.Cleanup(func() { _ = readers.Close() })
	return readers
}
