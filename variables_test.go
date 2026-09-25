package mmdbconvert

import (
	"fmt"
	"maps"
	"net/netip"
	"os"
	"path/filepath"
	"testing"

	"github.com/oschwald/maxminddb-golang/v2"
	"github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRun_PathVariables(t *testing.T) {
	databasePath := createCSVTestDatabase(t, 6, []string{"1.2.3.0/24", "2001:db8::/32"})
	for _, format := range []string{"csv", "parquet", "mmdb"} {
		t.Run(format, func(t *testing.T) {
			outputPath := filepath.Join(t.TempDir(), "output = ${literal}."+format)
			configPath := filepath.Join(t.TempDir(), "config.toml")
			content := fmt.Sprintf(`
[output]
format = %q
file = "${output}"

[output.mmdb]
database_type = "Path-Parameters-Test"
include_reserved_networks = true

[[network.columns]]
name = "network"
type = "cidr"

[[databases]]
name = "source"
path = "${input}"

[[columns]]
name = "value"
database = "source"
path = ["value"]
`, format)
			require.NoError(t, os.WriteFile(configPath, []byte(content), 0o600))
			variables := map[string]string{"input": databasePath, "output": outputPath}
			original := maps.Clone(variables)
			require.NoError(t, Run(Options{ConfigPath: configPath, Variables: variables}))
			assert.Equal(t, original, variables)

			switch format {
			case "csv":
				data, err := os.ReadFile(filepath.Clean(outputPath))
				require.NoError(t, err)
				assert.Equal(
					t,
					"network,value\n1.2.3.0/24,record\n2001:db8::/32,record\n",
					string(data),
				)
			case "parquet":
				type row struct {
					Network string `parquet:"network"`
					Value   string `parquet:"value"`
				}
				rows, err := parquet.ReadFile[row](outputPath)
				require.NoError(t, err)
				assert.Equal(t, []row{{"1.2.3.0/24", "record"}, {"2001:db8::/32", "record"}}, rows)
			case "mmdb":
				reader, err := maxminddb.Open(outputPath)
				require.NoError(t, err)
				defer reader.Close()
				assert.Equal(t, "Path-Parameters-Test", reader.Metadata.DatabaseType)
				for _, ip := range []string{"1.2.3.1", "2001:db8::1"} {
					var record map[string]string
					require.NoError(t, reader.Lookup(netip.MustParseAddr(ip)).Decode(&record))
					assert.Equal(t, map[string]string{"value": "record"}, record)
				}
			}
		})
	}
}

func TestRun_PathVariablesRelativeToWorkingDirectory(t *testing.T) {
	databasePath := createCSVTestDatabase(t, 6, []string{"1.2.3.0/24"})
	// The config, input, and working directory are deliberately different.
	workingDir := t.TempDir()
	t.Chdir(workingDir)
	relativeInput, err := filepath.Rel(workingDir, databasePath)
	require.NoError(t, err)
	configPath := filepath.Join(t.TempDir(), "config.toml")
	content := `
[output]
format = "csv"
ipv4_file = "${prefix}v4.csv"
ipv6_file = "${prefix}v6.csv"

[[databases]]
name = "source"
path = "${input}"

[[columns]]
name = "value"
database = "source"
path = ["value"]
`
	require.NoError(t, os.WriteFile(configPath, []byte(content), 0o600))
	require.NoError(t, Run(Options{
		ConfigPath: configPath,
		Variables:  map[string]string{"input": relativeInput, "prefix": ""},
	}))
	ipv4, err := os.ReadFile("v4.csv")
	require.NoError(t, err)
	assert.Equal(t, "network,value\n1.2.3.0/24,record\n", string(ipv4))
	ipv6, err := os.ReadFile("v6.csv")
	require.NoError(t, err)
	assert.Equal(t, "network,value\n", string(ipv6))
	assert.NoFileExists(t, filepath.Join(filepath.Dir(configPath), "v4.csv"))
}

func TestRun_PathVariableErrorsPreserveOutputs(t *testing.T) {
	for _, badInput := range []string{"${missing}", "${unterminated", "${bad-name}"} {
		t.Run(badInput, func(t *testing.T) {
			outputPath := filepath.Join(t.TempDir(), "existing.csv")
			require.NoError(t, os.WriteFile(outputPath, []byte("keep this output"), 0o600))
			configPath := filepath.Join(t.TempDir(), "config.toml")
			content := fmt.Sprintf(`
[output]
format = "csv"
file = "${output}"

[[databases]]
name = "first"
path = "nonexistent.mmdb"

[[databases]]
name = "second"
path = %q
`, badInput)
			require.NoError(t, os.WriteFile(configPath, []byte(content), 0o600))
			err := Run(
				Options{ConfigPath: configPath, Variables: map[string]string{"output": outputPath}},
			)
			require.ErrorContains(
				t,
				err,
				`resolving path parameters: databases[1].path (name "second")`,
			)
			data, err := os.ReadFile(filepath.Clean(outputPath))
			require.NoError(t, err)
			assert.Equal(t, "keep this output", string(data))
		})
	}
}
