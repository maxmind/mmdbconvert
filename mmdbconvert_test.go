package mmdbconvert

import (
	"fmt"
	"net/netip"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/maxmind/mmdbwriter"
	"github.com/maxmind/mmdbwriter/mmdbtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go4.org/netipx"

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
file = "` + tomlPath(outputFile) + `"

[[databases]]
name = "city"
path = "` + tomlPath(filepath.Join(absTestDataDir, "GeoIP2-City-Test.mmdb")) + `"

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
file = "` + tomlPath(outputFile) + `"

[[databases]]
name = "city"
path = "` + tomlPath(filepath.Join(absTestDataDir, "GeoIP2-City-Test.mmdb")) + `"

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
file = "` + tomlPath(outputFile) + `"

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
file = "` + tomlPath(outputFile) + `"

[[databases]]
name = "city"
path = "` + tomlPath(filepath.Join(absTestDataDir, "GeoIP2-City-Test.mmdb")) + `"

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
ipv4_file = "` + tomlPath(ipv4File) + `"
ipv6_file = "` + tomlPath(ipv6File) + `"

[[databases]]
name = "city"
path = "` + tomlPath(filepath.Join(absTestDataDir, "GeoIP2-City-Test.mmdb")) + `"

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

func TestRun_CSVEmptyOutput(t *testing.T) {
	tests := []struct {
		name          string
		ipVersion     int
		networks      []string
		field         string
		disableHeader bool
		wantIPv4      string
		wantIPv6      string
	}{
		{
			name:      "IPv4 database",
			ipVersion: 4,
			networks:  []string{"1.2.3.0/24"},
			field:     "value",
			wantIPv4:  "1.2.3.0/24,record\n",
		},
		{
			name:      "IPv6 database with only IPv4 records",
			ipVersion: 6,
			networks:  []string{"1.2.3.0/24"},
			field:     "value",
			wantIPv4:  "1.2.3.0/24,record\n",
		},
		{
			name:      "IPv6 records only",
			ipVersion: 6,
			networks:  []string{"2001:db8::/32"},
			field:     "value",
			wantIPv6:  "2001:db8::/32,record\n",
		},
		{
			name:      "empty database",
			ipVersion: 6,
			field:     "value",
		},
		{
			name:      "missing field",
			ipVersion: 6,
			networks:  []string{"1.2.3.0/24", "2001:db8::/32"},
			field:     "missing",
		},
		{
			name:          "empty database without header",
			ipVersion:     6,
			field:         "value",
			disableHeader: true,
		},
		{
			name:          "missing field without header",
			ipVersion:     6,
			networks:      []string{"1.2.3.0/24", "2001:db8::/32"},
			field:         "missing",
			disableHeader: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			databasePath := createCSVTestDatabase(t, tt.ipVersion, tt.networks)
			header := "network,value\n"
			csvConfig := ""
			if tt.disableHeader {
				header = ""
				csvConfig = "[output.csv]\ninclude_header = false"
			}
			for _, mode := range []string{"split", "combined"} {
				t.Run(mode, func(t *testing.T) {
					tmpDir := t.TempDir()
					outputPaths := fmt.Sprintf(
						"file = %q",
						tomlPath(filepath.Join(tmpDir, "output.csv")),
					)
					wantFiles := map[string]string{"output.csv": header + tt.wantIPv4 + tt.wantIPv6}
					if mode == "split" {
						outputPaths = fmt.Sprintf("ipv4_file = %q\nipv6_file = %q",
							tomlPath(filepath.Join(tmpDir, "ipv4.csv")),
							tomlPath(filepath.Join(tmpDir, "ipv6.csv")),
						)
						wantFiles = map[string]string{
							"ipv4.csv": header + tt.wantIPv4,
							"ipv6.csv": header + tt.wantIPv6,
						}
					}
					configContent := fmt.Sprintf(`
[output]
format = "csv"
%s

%s

[[databases]]
name = "test"
path = %q

[[columns]]
name = "value"
database = "test"
path = [%q]
`, outputPaths, csvConfig, tomlPath(databasePath), tt.field)
					configFile := filepath.Join(tmpDir, "config.toml")
					require.NoError(t, os.WriteFile(configFile, []byte(configContent), 0o600))
					require.NoError(t, Run(Options{ConfigPath: configFile}))

					for name, want := range wantFiles {
						content, err := os.ReadFile(filepath.Clean(filepath.Join(tmpDir, name)))
						require.NoError(t, err)
						assert.Equal(t, want, string(content), name)
					}
				})
			}
		})
	}
}

func createCSVTestDatabase(t *testing.T, ipVersion int, networks []string) string {
	t.Helper()

	tree, err := mmdbwriter.New(mmdbwriter.Options{
		DatabaseType:            "test",
		IPVersion:               ipVersion,
		IncludeReservedNetworks: true,
		DisableIPv4Aliasing:     true,
	})
	require.NoError(t, err)
	for _, network := range networks {
		prefix := netipx.PrefixIPNet(netip.MustParsePrefix(network))
		require.NoError(t, tree.Insert(prefix, mmdbtype.Map{"value": mmdbtype.String("record")}))
	}

	path := filepath.Join(t.TempDir(), "input.mmdb")
	file, err := os.Create(filepath.Clean(path))
	require.NoError(t, err)
	defer file.Close()
	_, err = tree.WriteTo(file)
	require.NoError(t, err)
	require.NoError(t, file.Close())
	return path
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

// tomlPath converts a file path to use forward slashes for embedding in TOML.
// This is necessary because backslashes in TOML strings are escape sequences,
// and Windows paths like "D:\path\to\file" would be misinterpreted.
// Forward slashes work as path separators on all platforms including Windows.
func tomlPath(path string) string {
	return filepath.ToSlash(path)
}
