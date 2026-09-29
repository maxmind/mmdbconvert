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
			require.NoError(
				t,
				Run(t.Context(), Options{ConfigPath: configPath, Variables: variables}),
			)
			assert.Equal(t, original, variables)

			switch format {
			case "csv":
				assertFileContent(
					t,
					outputPath,
					"network,value\n1.2.3.0/24,record\n2001:db8::/32,record\n",
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
ipv4_file = "${output_dir}/v4.csv"
ipv6_file = "${output_dir}/v6.csv"

[[databases]]
name = "source"
path = "${input}"

[[columns]]
name = "value"
database = "source"
path = ["value"]
`
	require.NoError(t, os.WriteFile(configPath, []byte(content), 0o600))
	require.NoError(t, Run(t.Context(), Options{
		ConfigPath: configPath,
		Variables:  map[string]string{"input": relativeInput, "output_dir": "."},
	}))
	assertFileContent(t, "v4.csv", "network,value\n1.2.3.0/24,record\n")
	assertFileContent(t, "v6.csv", "network,value\n")
	assert.NoFileExists(t, filepath.Join(filepath.Dir(configPath), "v4.csv"))
}

func TestRun_PathVariableErrorsPreserveOutputs(t *testing.T) {
	tests := []struct {
		input     string
		variables map[string]string
		errText   string
	}{
		{
			input:   "${missing}",
			errText: `databases[1].path (name "second"): undefined variable "missing"`,
		},
		{
			input:   "${unterminated",
			errText: `databases[1].path (name "second"): unterminated variable placeholder`,
		},
		{
			input:   "${bad-name}",
			errText: `databases[1].path (name "second"): invalid variable name "bad-name"`,
		},
		{
			input:     "${empty}input.mmdb",
			variables: map[string]string{"empty": ""},
			errText:   `variables: empty value for variable "empty"`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
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
`, tt.input)
			require.NoError(t, os.WriteFile(configPath, []byte(content), 0o600))
			variables := map[string]string{"output": outputPath}
			maps.Copy(variables, tt.variables)
			err := Run(
				t.Context(),
				Options{ConfigPath: configPath, Variables: variables},
			)
			require.ErrorContains(
				t,
				err,
				"resolving path parameters: "+tt.errText,
			)
			assertFileContent(t, outputPath, "keep this output")
		})
	}
}
