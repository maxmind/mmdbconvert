package mmdbconvert

import (
	"bytes"
	"net/netip"
	"os"
	"path/filepath"
	"testing"

	"github.com/maxmind/mmdbwriter/v2"
	"github.com/maxmind/mmdbwriter/v2/mmdbtype"
	"github.com/pelletier/go-toml/v2"
	"github.com/stretchr/testify/require"

	"github.com/maxmind/mmdbconvert/internal/config"
)

func TestRun_FormatsAfterMergingAndFiltering(t *testing.T) {
	precision := 4
	trueLabel, falseLabel := "1", ""
	for _, tt := range []struct {
		name   string
		values []mmdbtype.DataType
		format *config.ColumnFormat
		want   string
	}{
		{
			name:   "float64 values round to identical text",
			values: []mmdbtype.DataType{mmdbtype.Float64(37.75101), mmdbtype.Float64(37.75102)},
			format: &config.ColumnFormat{Precision: &precision},
			want:   "network,value\n2.0.0.0/25,37.7510\n2.0.0.128/25,37.7510\n",
		},
		{
			name:   "float32 values round to identical text",
			values: []mmdbtype.DataType{mmdbtype.Float32(37.75101), mmdbtype.Float32(37.75102)},
			format: &config.ColumnFormat{Precision: &precision},
			want:   "network,value\n2.0.0.0/25,37.7510\n2.0.0.128/25,37.7510\n",
		},
		{
			name:   "false with empty label survives while nil is skipped",
			values: []mmdbtype.DataType{mmdbtype.Bool(false), nil},
			format: &config.ColumnFormat{True: &trueLabel, False: &falseLabel},
			want:   "network,value\n2.0.0.0/25,\n",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			dbPath := filepath.Join(dir, "input.mmdb")
			outputPath := filepath.Join(dir, "output.csv")
			tree, err := mmdbwriter.New(mmdbwriter.Options{IPVersion: 4, DatabaseType: "Test"})
			require.NoError(t, err)
			for i, prefix := range []string{"2.0.0.0/25", "2.0.0.128/25"} {
				// Keep a record present even when the projected field is missing.
				record := mmdbtype.Map{"omitted": mmdbtype.Bool(true)}
				if tt.values[i] != nil {
					record["value"] = tt.values[i]
				}
				require.NoError(t, tree.Insert(netip.MustParsePrefix(prefix), record))
			}
			var database bytes.Buffer
			_, err = tree.WriteTo(&database)
			require.NoError(t, err)
			require.NoError(t, os.WriteFile(dbPath, database.Bytes(), 0o600))
			cfg := config.Config{
				Output:    config.OutputConfig{Format: config.OutputFormatCSV, File: outputPath},
				Databases: []config.Database{{Name: "test", Path: dbPath}},
				Columns: []config.Column{{
					Name: "value", Database: "test", Path: config.Path{"value"}, Format: tt.format,
				}},
			}
			data, err := toml.Marshal(cfg)
			require.NoError(t, err)
			configPath := filepath.Join(dir, "config.toml")
			require.NoError(t, os.WriteFile(configPath, data, 0o600))
			require.NoError(t, Run(Options{ConfigPath: configPath}))
			assertFileContent(t, outputPath, tt.want)
		})
	}
}

func TestRun_FormatTypeMismatchPreservesOutput(t *testing.T) {
	precision := 4
	trueLabel, falseLabel := "1", "0"
	for _, tt := range []struct {
		name   string
		path   string
		format *config.ColumnFormat
		want   string
	}{
		{
			name:   "precision on a string",
			path:   `["country", "iso_code"]`,
			format: &config.ColumnFormat{Precision: &precision},
			want:   "precision requires a floating-point value",
		},
		{
			name:   "labels on a map",
			path:   `["country"]`,
			format: &config.ColumnFormat{True: &trueLabel, False: &falseLabel},
			want:   "true/false labels require a boolean value",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			configPath, cfg, paths := outputTestConfig(t, "csv", true, tt.path)
			cfg.Columns[0].Format = tt.format
			data, err := toml.Marshal(cfg)
			require.NoError(t, err)
			require.NoError(t, os.WriteFile(configPath, data, 0o600))
			for _, path := range paths {
				require.NoError(t, os.WriteFile(path, []byte("previous output"), 0o600))
			}
			err = Run(Options{ConfigPath: configPath})
			require.ErrorContains(t, err, "column 'country_code'")
			require.ErrorContains(t, err, tt.want)
			for _, path := range paths {
				assertFileContent(t, path, "previous output")
			}
			assertOutputDirectory(t, paths)
		})
	}
}
