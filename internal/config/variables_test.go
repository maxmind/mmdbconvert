package config

import (
	"maps"
	"os"
	"path/filepath"
	"testing"

	"github.com/maxmind/mmdbwriter/v2/mmdbtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExpandPath(t *testing.T) {
	tests := []struct {
		name      string
		path      string
		variables map[string]string
		want      string
		errText   string
	}{
		{name: "empty"},
		{name: "relative", path: "../data/geo.mmdb", want: "../data/geo.mmdb"},
		{name: "dollars", path: "$HOME/$$/geo$.mmdb", want: "$HOME/$$/geo$.mmdb"},
		{name: "unicode", path: "données/地理.mmdb", want: "données/地理.mmdb"},
		{
			name: "complete path", path: "${input}",
			variables: map[string]string{"input": "/data/geo.mmdb"}, want: "/data/geo.mmdb",
		},
		{
			name: "reuse and adjacent references",
			path: "${dir}/${name}${name}.csv",
			variables: map[string]string{
				"dir":  "relative",
				"name": "blocks",
			},
			want: "relative/blocksblocks.csv",
		},
		{
			name: "case sensitive identifiers", path: "${name}/${NAME}/${_name2}",
			variables: map[string]string{"name": "a", "NAME": "b", "_name2": "c"}, want: "a/b/c",
		},
		{
			name: "empty value", path: "${prefix}blocks.csv",
			variables: map[string]string{"prefix": ""}, want: "blocks.csv",
		},
		{
			name: "literal value",
			path: "${input}",
			variables: map[string]string{
				"input": " C:\\some dir\\\"file\"=地理\n${other}$${escaped}.mmdb ",
			},
			want: " C:\\some dir\\\"file\"=地理\n${other}$${escaped}.mmdb ",
		},
		{
			name: "self reference is literal", path: "${input}",
			variables: map[string]string{"input": "${input}"}, want: "${input}",
		},
		{name: "escaped", path: "$${input}.csv", want: "${input}.csv"},
		{name: "escaped opener", path: "$${", want: "${"},
		{
			name: "dollar before escaped reference", path: "$$${input}",
			variables: map[string]string{"input": "file"}, want: "$${input}",
		},
		{
			name: "escaped and expanded", path: "$${input}/${input}",
			variables: map[string]string{"input": "file"}, want: "${input}/file",
		},
		{
			name: "adjacent escaped and expanded", path: "$${input}${input}",
			variables: map[string]string{"input": "file"}, want: "${input}file",
		},
		{
			name: "supplied dollar prefix", path: "${prefix}${input}",
			variables: map[string]string{"prefix": "$", "input": "file"}, want: "$file",
		},
		{
			name: "closing brace after reference", path: "${input}}",
			variables: map[string]string{"input": "file"}, want: "file}",
		},
		{name: "literal closing brace", path: "file}.csv", want: "file}.csv"},
		{name: "missing", path: "${input}", errText: `undefined variable "input"`},
		{name: "unterminated", path: "dir/${input", errText: "unterminated variable placeholder"},
		{name: "empty name", path: "${}", errText: `invalid variable name ""`},
		{name: "numeric name", path: "${1input}", errText: `invalid variable name "1input"`},
		{name: "spaces in name", path: "${ input }", errText: `invalid variable name " input "`},
		{
			name:    "dots in name",
			path:    "${input.path}",
			errText: `invalid variable name "input.path"`,
		},
		{name: "non ASCII name", path: "${entrée}", errText: `invalid variable name "entrée"`},
		{
			name:    "expression",
			path:    "${input:-default}",
			errText: `invalid variable name "input:-default"`,
		},
		{name: "nested", path: "${${input}}", errText: `invalid variable name "${input"`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := expandPath(tt.path, tt.variables, map[string]bool{})
			if tt.errText != "" {
				require.ErrorContains(t, err, tt.errText)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestLoadConfig_PathVariables(t *testing.T) {
	path := writeVariableConfig(t, `
[output]
format = "csv"
ipv4_file = "${output_dir}/${edition}-${date}-IPv4.csv"
ipv6_file = "${output_dir}/${edition}-${date}-IPv6.csv"

[[databases]]
name = "proxy.with.dots"
path = "${input}"

[[databases]]
name = "city"
path = "${input_dir}/city.mmdb"
`)
	variables := map[string]string{
		"input":      `C:\data\input "quoted"=${literal}.mmdb`,
		"input_dir":  "../data",
		"output_dir": "relative output",
		"edition":    "Proxy",
		"date":       "20260902",
	}
	original := maps.Clone(variables)
	cfg, err := LoadConfig(path, variables)
	require.NoError(t, err)
	assert.Equal(t, variables["input"], cfg.Databases[0].Path)
	assert.Equal(t, "../data/city.mmdb", cfg.Databases[1].Path)
	assert.Equal(t, "relative output/Proxy-20260902-IPv4.csv", cfg.Output.IPv4File)
	assert.Equal(t, "relative output/Proxy-20260902-IPv6.csv", cfg.Output.IPv6File)
	assert.Equal(t, ",", cfg.Output.CSV.Delimiter)
	assert.Equal(t, original, variables)
	data, err := os.ReadFile(filepath.Clean(path))
	require.NoError(t, err)
	assert.Contains(t, string(data), `path = "${input}"`)
}

func TestLoadConfig_OnlyFilesystemPathsExpand(t *testing.T) {
	path := writeVariableConfig(t, `
[output]
format = "mmdb"
file = "${output}"

[output.mmdb]
database_type = "${type}"
description = { "${locale}" = "${description}" }

[[network.columns]]
name = "${network}"
type = "cidr"

[[databases]]
name = "${source}"
path = "${input}"

[[columns]]
name = "${column}"
database = "${source}"
path = ["${lookup}", 0]
output_path = ["${destination}"]
`)
	cfg, err := LoadConfig(path, map[string]string{"input": "input.mmdb", "output": "out.mmdb"})
	require.NoError(t, err)
	assert.Equal(t, "out.mmdb", cfg.Output.File)
	assert.Equal(t, "input.mmdb", cfg.Databases[0].Path)
	assert.Equal(t, "${type}", cfg.Output.MMDB.DatabaseType)
	assert.Equal(t, map[string]string{"${locale}": "${description}"}, cfg.Output.MMDB.Description)
	assert.Equal(t, []string{"${locale}"}, cfg.Output.MMDB.Languages)
	assert.Equal(t, mmdbtype.String("${network}"), cfg.Network.Columns[0].Name)
	assert.Equal(t, "${source}", cfg.Databases[0].Name)
	assert.Equal(t, mmdbtype.String("${column}"), cfg.Columns[0].Name)
	assert.Equal(t, "${source}", cfg.Columns[0].Database)
	assert.Equal(t, Path{"${lookup}", int64(0)}, cfg.Columns[0].Path)
	assert.Equal(t, &Path{"${destination}"}, cfg.Columns[0].OutputPath)
}

func TestLoadConfig_PathVariableErrors(t *testing.T) {
	tests := []struct {
		name      string
		config    string
		variables map[string]string
		errText   string
	}{
		{
			name: "file", config: "[output]\nfile = '${missing}'",
			errText: `output.file: undefined variable "missing"`,
		},
		{
			name: "ipv4", config: "[output]\nipv4_file = '${missing}'",
			errText: `output.ipv4_file: undefined variable "missing"`,
		},
		{
			name: "ipv6", config: "[output]\nipv6_file = '${missing}'",
			errText: `output.ipv6_file: undefined variable "missing"`,
		},
		{
			name: "database", config: "[[databases]]\nname = 'proxy'\npath = '${missing}'",
			errText: `databases[0].path (name "proxy"): undefined variable "missing"`,
		},
		{
			name: "malformed", config: "[output]\nfile = '${missing'",
			errText: "output.file: unterminated variable placeholder",
		},
		{
			name: "unused", variables: map[string]string{"unused": "value"},
			errText: `unused variable "unused"`,
		},
		{
			name: "escaped reference is unused", config: "[output]\nfile = '$${unused}'",
			variables: map[string]string{"unused": "value"}, errText: `unused variable "unused"`,
		},
		{
			name:      "dollar before escaped reference is unused",
			config:    "[output]\nfile = '$$${unused}'",
			variables: map[string]string{"unused": "value"},
			errText:   `unused variable "unused"`,
		},
		{
			name:      "non filesystem reference is unused",
			config:    "[output.mmdb]\ndatabase_type = '${unused}'",
			variables: map[string]string{"unused": "value"},
			errText:   `unused variable "unused"`,
		},
		{
			name: "invalid API name", variables: map[string]string{"bad-name": "value"},
			errText: `invalid variable name "bad-name"`,
		},
		{
			name: "empty API name", variables: map[string]string{"": "value"},
			errText: `invalid variable name ""`,
		},
		{
			name: "deterministic error", variables: map[string]string{"z": "value", "a": "value"},
			errText: `unused variable "a"`,
		},
		{
			name:   "empty output is validated",
			config: "[output]\nformat = 'csv'\nfile = '${output}'",
			variables: map[string]string{
				"output": "",
			},
			errText: "invalid configuration: either output.file",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := writeVariableConfig(t, tt.config)
			_, err := LoadConfig(path, tt.variables)
			require.ErrorContains(t, err, tt.errText)
		})
	}
}

func TestLoadConfig_PathVariablesIgnoreEnvironment(t *testing.T) {
	t.Setenv("input", "from-environment.mmdb")
	path := writeVariableConfig(t, `
[output]
format = "csv"
file = "$HOME/$${literal}.csv"

[[databases]]
name = "geo"
path = "${input}"
`)
	_, err := LoadConfig(path, nil)
	require.ErrorContains(t, err, `undefined variable "input"`)
	cfg, err := LoadConfig(path, map[string]string{"input": "explicit.mmdb"})
	require.NoError(t, err)
	assert.Equal(t, "explicit.mmdb", cfg.Databases[0].Path)
	assert.Equal(t, "$HOME/${literal}.csv", cfg.Output.File)
}

func writeVariableConfig(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.toml")
	require.NoError(t, os.WriteFile(path, []byte(content), 0o600))
	return path
}
