package config

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLoadConfig_ColumnFormat(t *testing.T) {
	tests := []struct {
		name   string
		output string
		format string
		err    string
	}{
		{"default", "csv", "", ""},
		{"float", "csv", `format = { precision = 4 }`, ""},
		{"zero precision", "csv", `format = { precision = 0 }`, ""},
		{"maximum precision", "csv", `format = { precision = 1000 }`, ""},
		{
			"excessive precision",
			"csv",
			`format = { precision = 1001 }`,
			"precision from 0 through 1000",
		},
		{
			"enormous precision",
			"csv",
			`format = { precision = 1000000000 }`,
			"precision from 0 through 1000",
		},
		{"empty false", "csv", `format = { true = "1", false = "" }`, ""},
		{"empty labels", "csv", `format = { true = "", false = "" }`, ""},
		{"table", "csv", "[columns.format]\nprecision = 4", ""},
		{
			"negative precision",
			"csv",
			`format = { precision = -1 }`,
			"precision from 0 through 1000",
		},
		{"missing true", "csv", `format = { false = "0" }`, "both true and false"},
		{"missing false", "csv", `format = { true = "1" }`, "both true and false"},
		{
			"precision with true label",
			"csv",
			`format = { precision = 4, true = "1" }`,
			"cannot be combined",
		},
		{
			"precision with false label",
			"csv",
			`format = { precision = 4, false = "0" }`,
			"cannot be combined",
		},
		{
			"precision with both labels",
			"csv",
			`format = { precision = 4, true = "1", false = "0" }`,
			"cannot be combined",
		},
		{
			"type selector",
			"csv",
			`format = { type = "float", precision = 4 }`,
			`unknown format option "type"`,
		},
		{"empty table", "csv", `format = {}`, "requires precision or both true and false"},
		{
			"unknown option",
			"csv",
			`format = { precision = 4, width = 8 }`,
			"unknown format option",
		},
		{
			"capitalized labels",
			"csv",
			`format = { True = "1", False = "0" }`,
			"unknown format option",
		},
		{
			"parquet",
			"parquet",
			`format = { precision = 4 }`,
			"supported only for CSV",
		},
		{
			"mmdb",
			"mmdb",
			`format = { true = "t", false = "f" }`,
			"supported only for CSV",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "config.toml")
			content := fmt.Sprintf(`
[output]
format = %q
file = "output"
[output.mmdb]
database_type = "Test"
[[databases]]
name = "geo"
path = "input.mmdb"
[[columns]]
name = "value"
database = "geo"
path = ["value"]
%s
`, tt.output, tt.format)
			require.NoError(t, os.WriteFile(path, []byte(content), 0o600))
			cfg, err := LoadConfig(path)
			if tt.err != "" {
				require.ErrorContains(t, err, tt.err)
				require.ErrorContains(t, err, "invalid configuration: column 'value'")
				return
			}
			require.NoError(t, err)
			if tt.format == "" {
				require.Nil(t, cfg.Columns[0].Format)
			} else {
				require.NotNil(t, cfg.Columns[0].Format)
			}
		})
	}
}

func TestLoadConfig_ColumnNameBeforeFormat(t *testing.T) {
	for _, name := range []string{"", `name = ""`} {
		for _, format := range []string{
			`{}`,
			`{ type = "x" }`,
			`{ True = "1", False = "0" }`,
			`{ precision = 4, width = 8 }`,
		} {
			t.Run(name+"/"+format, func(t *testing.T) {
				path := filepath.Join(t.TempDir(), "config.toml")
				content := fmt.Sprintf(`
[output]
format = "csv"
file = "output"
[[databases]]
name = "geo"
path = "input.mmdb"
[[columns]]
%s
database = "geo"
path = ["value"]
format = %s
[[columns]]
name = "second"
database = "geo"
path = ["value"]
format = { width = 8 }
`, name, format)
				require.NoError(t, os.WriteFile(path, []byte(content), 0o600))
				_, err := LoadConfig(path)
				require.EqualError(t, err, "invalid configuration: column name is required")
			})
		}
	}
}

func TestValidate_ColumnNameRequired(t *testing.T) {
	err := validate(&Config{Columns: []Column{{}}}, nil)
	require.EqualError(t, err, "column name is required")
}
