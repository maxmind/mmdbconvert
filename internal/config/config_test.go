package config

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

//nolint:gocyclo // Table-driven tests with validation callbacks have inherent complexity
func TestLoadConfig_Valid(t *testing.T) {
	tests := []struct {
		name     string
		toml     string
		validate func(t *testing.T, cfg *Config)
	}{
		{
			name: "minimal CSV config",
			toml: `
[output]
format = "csv"
file = "output.csv"

[[databases]]
name = "geo"
path = "/path/to/geo.mmdb"

[[columns]]
name = "country"
database = "geo"
path = ["country", "iso_code"]
`,
			validate: func(t *testing.T, cfg *Config) {
				if cfg.Output.Format != "csv" {
					t.Errorf("expected format=csv, got %s", cfg.Output.Format)
				}
				if cfg.Output.File != "output.csv" {
					t.Errorf("expected file=output.csv, got %s", cfg.Output.File)
				}
				if len(cfg.Databases) != 1 {
					t.Errorf("expected 1 database, got %d", len(cfg.Databases))
				}
				if len(cfg.Columns) != 1 {
					t.Errorf("expected 1 column, got %d", len(cfg.Columns))
				}
				if cfg.Output.CSV.IncludeHeader == nil || !*cfg.Output.CSV.IncludeHeader {
					t.Error("expected include_header default true")
				}
				// Check CSV defaults
				if cfg.Output.CSV.Delimiter != "," {
					t.Errorf("expected default delimiter=',', got %s", cfg.Output.CSV.Delimiter)
				}
				// Check network column defaults (CSV should get "network" CIDR column)
				if len(cfg.Network.Columns) != 1 {
					t.Errorf(
						"expected 1 network column (default), got %d",
						len(cfg.Network.Columns),
					)
				}
				if len(cfg.Network.Columns) > 0 && cfg.Network.Columns[0].Type != "cidr" {
					t.Errorf(
						"expected default network column type=cidr, got %s",
						cfg.Network.Columns[0].Type,
					)
				}
				assertPathEquals(t, cfg.Columns[0].Path, "country", "iso_code")
			},
		},
		{
			name: "per-IP version files",
			toml: `
[output]
format = "csv"
ipv4_file = "v4.csv"
ipv6_file = "v6.csv"

[[databases]]
name = "geo"
path = "/path/to/geo.mmdb"

[[columns]]
name = "country"
database = "geo"
path = ["country", "iso_code"]
`,
			validate: func(t *testing.T, cfg *Config) {
				if cfg.Output.File != "" {
					t.Error("expected output.file empty when splitting")
				}
				if cfg.Output.IPv4File != "v4.csv" || cfg.Output.IPv6File != "v6.csv" {
					t.Error("missing per-version filenames")
				}
				assertPathEquals(t, cfg.Columns[0].Path, "country", "iso_code")
			},
		},
		{
			name: "parquet config with custom network columns",
			toml: `
[output]
format = "parquet"
file = "output.parquet"

[output.parquet]
compression = "zstd"
row_group_size = 100000

[[network.columns]]
name = "start_int"
type = "start_int"

[[network.columns]]
name = "end_int"
type = "end_int"

[[databases]]
name = "db1"
path = "/path/to/db1.mmdb"

[[columns]]
name = "field1"
database = "db1"
path = ["field1"]
type = "int64"
`,
			validate: func(t *testing.T, cfg *Config) {
				if cfg.Output.Format != "parquet" {
					t.Errorf("expected format=parquet, got %s", cfg.Output.Format)
				}
				if cfg.Output.Parquet.Compression != "zstd" {
					t.Errorf("expected compression=zstd, got %s", cfg.Output.Parquet.Compression)
				}
				if cfg.Output.Parquet.RowGroupSize != 100000 {
					t.Errorf(
						"expected row_group_size=100000, got %d",
						cfg.Output.Parquet.RowGroupSize,
					)
				}
				if len(cfg.Network.Columns) != 2 {
					t.Errorf("expected 2 network columns, got %d", len(cfg.Network.Columns))
				}
				if cfg.Columns[0].Type != "int64" {
					t.Errorf("expected column type=int64, got %s", cfg.Columns[0].Type)
				}
				assertPathEquals(t, cfg.Columns[0].Path, "field1")
			},
		},
		{
			name: "multiple databases",
			toml: `
[output]
format = "csv"
file = "output.csv"

[[databases]]
name = "geo"
path = "/path/to/geo.mmdb"

[[databases]]
name = "anon"
path = "/path/to/anon.mmdb"

[[columns]]
name = "country"
database = "geo"
path = ["country", "iso_code"]

[[columns]]
name = "is_anon"
database = "anon"
path = ["is_anonymous"]
`,
			validate: func(t *testing.T, cfg *Config) {
				if len(cfg.Databases) != 2 {
					t.Errorf("expected 2 databases, got %d", len(cfg.Databases))
				}
				if len(cfg.Columns) != 2 {
					t.Errorf("expected 2 columns, got %d", len(cfg.Columns))
				}
				assertPathEquals(t, cfg.Columns[0].Path, "country", "iso_code")
				assertPathEquals(t, cfg.Columns[1].Path, "is_anonymous")
			},
		},
		{
			name: "parquet with default network columns",
			toml: `
[output]
format = "parquet"
file = "output.parquet"

[[databases]]
name = "geo"
path = "/path/to/geo.mmdb"

[[columns]]
name = "country"
database = "geo"
path = ["country", "iso_code"]
`,
			validate: func(t *testing.T, cfg *Config) {
				// Parquet should default to start_int and end_int columns
				if len(cfg.Network.Columns) != 2 {
					t.Errorf(
						"expected 2 default network columns for parquet, got %d",
						len(cfg.Network.Columns),
					)
				}
				if len(cfg.Network.Columns) >= 2 {
					if cfg.Network.Columns[0].Type != "start_int" {
						t.Errorf(
							"expected first network column type=start_int, got %s",
							cfg.Network.Columns[0].Type,
						)
					}
					if cfg.Network.Columns[1].Type != "end_int" {
						t.Errorf(
							"expected second network column type=end_int, got %s",
							cfg.Network.Columns[1].Type,
						)
					}
				}
				assertPathEquals(t, cfg.Columns[0].Path, "country", "iso_code")
			},
		},
		{
			name: "parquet with network_bucket column and split files",
			toml: `
[output]
format = "parquet"
ipv4_file = "output-v4.parquet"
ipv6_file = "output-v6.parquet"

[[network.columns]]
name = "start_int"
type = "start_int"

[[network.columns]]
name = "end_int"
type = "end_int"

[[network.columns]]
name = "network_bucket"
type = "network_bucket"

[[databases]]
name = "geo"
path = "/path/to/geo.mmdb"

[[columns]]
name = "country"
database = "geo"
path = ["country", "iso_code"]
`,
			validate: func(t *testing.T, cfg *Config) {
				if len(cfg.Network.Columns) != 3 {
					t.Errorf("expected 3 network columns, got %d", len(cfg.Network.Columns))
				}
				if cfg.Network.Columns[2].Type != "network_bucket" {
					t.Errorf(
						"expected third network column type=network_bucket, got %s",
						cfg.Network.Columns[2].Type,
					)
				}
				// Bucket sizes should default to 16
				if cfg.Output.Parquet.IPv4BucketSize != 16 {
					t.Errorf(
						"expected IPv4BucketSize=16, got %d",
						cfg.Output.Parquet.IPv4BucketSize,
					)
				}
				if cfg.Output.Parquet.IPv6BucketSize != 16 {
					t.Errorf(
						"expected IPv6BucketSize=16, got %d",
						cfg.Output.Parquet.IPv6BucketSize,
					)
				}
			},
		},
		{
			name: "parquet with custom bucket sizes",
			toml: `
[output]
format = "parquet"
ipv4_file = "output-v4.parquet"
ipv6_file = "output-v6.parquet"

[output.parquet]
ipv4_bucket_size = 24
ipv6_bucket_size = 32

[[network.columns]]
name = "start_int"
type = "start_int"

[[network.columns]]
name = "network_bucket"
type = "network_bucket"

[[databases]]
name = "geo"
path = "/path/to/geo.mmdb"

[[columns]]
name = "country"
database = "geo"
path = ["country", "iso_code"]
`,
			validate: func(t *testing.T, cfg *Config) {
				if cfg.Output.Parquet.IPv4BucketSize != 24 {
					t.Errorf(
						"expected IPv4BucketSize=24, got %d",
						cfg.Output.Parquet.IPv4BucketSize,
					)
				}
				if cfg.Output.Parquet.IPv6BucketSize != 32 {
					t.Errorf(
						"expected IPv6BucketSize=32, got %d",
						cfg.Output.Parquet.IPv6BucketSize,
					)
				}
			},
		},
		{
			name: "parquet with ipv6_bucket_type int",
			toml: `
[output]
format = "parquet"
ipv4_file = "output-v4.parquet"
ipv6_file = "output-v6.parquet"

[output.parquet]
ipv6_bucket_type = "int"
ipv6_bucket_size = 48

[[network.columns]]
name = "start_int"
type = "start_int"

[[network.columns]]
name = "network_bucket"
type = "network_bucket"

[[databases]]
name = "geo"
path = "/path/to/geo.mmdb"

[[columns]]
name = "country"
database = "geo"
path = ["country", "iso_code"]
`,
			validate: func(t *testing.T, cfg *Config) {
				if cfg.Output.Parquet.IPv6BucketType != IPv6BucketTypeInt {
					t.Errorf(
						"expected IPv6BucketType=int, got %s",
						cfg.Output.Parquet.IPv6BucketType,
					)
				}
				if cfg.Output.Parquet.IPv6BucketSize != 48 {
					t.Errorf(
						"expected IPv6BucketSize=48, got %d",
						cfg.Output.Parquet.IPv6BucketSize,
					)
				}
			},
		},
		{
			name: "csv with network_bucket column and split files",
			toml: `
[output]
format = "csv"
ipv4_file = "output-v4.csv"
ipv6_file = "output-v6.csv"

[[network.columns]]
name = "start_int"
type = "start_int"

[[network.columns]]
name = "end_int"
type = "end_int"

[[network.columns]]
name = "network_bucket"
type = "network_bucket"

[[databases]]
name = "geo"
path = "/path/to/geo.mmdb"

[[columns]]
name = "country"
database = "geo"
path = ["country", "iso_code"]
`,
			validate: func(t *testing.T, cfg *Config) {
				if len(cfg.Network.Columns) != 3 {
					t.Errorf("expected 3 network columns, got %d", len(cfg.Network.Columns))
				}
				if cfg.Network.Columns[2].Type != "network_bucket" {
					t.Errorf(
						"expected third network column type=network_bucket, got %s",
						cfg.Network.Columns[2].Type,
					)
				}
				// Bucket sizes should default to 16
				if cfg.Output.CSV.IPv4BucketSize != 16 {
					t.Errorf(
						"expected IPv4BucketSize=16, got %d",
						cfg.Output.CSV.IPv4BucketSize,
					)
				}
				if cfg.Output.CSV.IPv6BucketSize != 16 {
					t.Errorf(
						"expected IPv6BucketSize=16, got %d",
						cfg.Output.CSV.IPv6BucketSize,
					)
				}
			},
		},
		{
			name: "csv with custom bucket sizes",
			toml: `
[output]
format = "csv"
ipv4_file = "output-v4.csv"
ipv6_file = "output-v6.csv"

[output.csv]
ipv4_bucket_size = 24
ipv6_bucket_size = 32

[[network.columns]]
name = "start_int"
type = "start_int"

[[network.columns]]
name = "network_bucket"
type = "network_bucket"

[[databases]]
name = "geo"
path = "/path/to/geo.mmdb"

[[columns]]
name = "country"
database = "geo"
path = ["country", "iso_code"]
`,
			validate: func(t *testing.T, cfg *Config) {
				if cfg.Output.CSV.IPv4BucketSize != 24 {
					t.Errorf(
						"expected IPv4BucketSize=24, got %d",
						cfg.Output.CSV.IPv4BucketSize,
					)
				}
				if cfg.Output.CSV.IPv6BucketSize != 32 {
					t.Errorf(
						"expected IPv6BucketSize=32, got %d",
						cfg.Output.CSV.IPv6BucketSize,
					)
				}
			},
		},
		{
			name: "csv with ipv6_bucket_type int",
			toml: `
[output]
format = "csv"
ipv4_file = "output-v4.csv"
ipv6_file = "output-v6.csv"

[output.csv]
ipv6_bucket_type = "int"
ipv6_bucket_size = 48

[[network.columns]]
name = "start_int"
type = "start_int"

[[network.columns]]
name = "network_bucket"
type = "network_bucket"

[[databases]]
name = "geo"
path = "/path/to/geo.mmdb"

[[columns]]
name = "country"
database = "geo"
path = ["country", "iso_code"]
`,
			validate: func(t *testing.T, cfg *Config) {
				if cfg.Output.CSV.IPv6BucketType != IPv6BucketTypeInt {
					t.Errorf(
						"expected IPv6BucketType='int', got '%s'",
						cfg.Output.CSV.IPv6BucketType,
					)
				}
				if cfg.Output.CSV.IPv6BucketSize != 48 {
					t.Errorf(
						"expected IPv6BucketSize=48, got %d",
						cfg.Output.CSV.IPv6BucketSize,
					)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create temporary config file
			tmpDir := t.TempDir()
			configPath := filepath.Join(tmpDir, "config.toml")
			if err := os.WriteFile(configPath, []byte(tt.toml), 0o644); err != nil {
				t.Fatalf("failed to write test config: %v", err)
			}

			// Load and validate config
			cfg, err := LoadConfig(configPath)
			if err != nil {
				t.Fatalf("LoadConfig failed: %v", err)
			}

			// Run custom validation
			tt.validate(t, cfg)
		})
	}
}

func TestLoadConfig_InvalidMixedOutputs(t *testing.T) {
	const toml = `
[output]
format = "csv"
file = "combined.csv"
ipv4_file = "v4.csv"

[[databases]]
name = "geo"
path = "/path/to/geo.mmdb"

[[columns]]
name = "country"
database = "geo"
path = ["country", "iso_code"]
`

	tmp := t.TempDir()
	path := filepath.Join(tmp, "config.toml")
	require.NoError(t, os.WriteFile(path, []byte(toml), 0o644))

	_, err := LoadConfig(path)
	if err == nil || !strings.Contains(err.Error(), "cannot be used together") {
		t.Fatalf("expected error about mutually exclusive files, got %v", err)
	}
}

func TestLoadConfig_InvalidPartialSplit(t *testing.T) {
	const toml = `
[output]
format = "csv"
ipv4_file = "v4.csv"

[[databases]]
name = "geo"
path = "/path/to/geo.mmdb"

[[columns]]
name = "country"
database = "geo"
path = ["country", "iso_code"]
`

	tmp := t.TempDir()
	path := filepath.Join(tmp, "config.toml")
	require.NoError(t, os.WriteFile(path, []byte(toml), 0o644))

	_, err := LoadConfig(path)
	if err == nil || !strings.Contains(err.Error(), "either output.file must be set") {
		t.Fatalf("expected error about providing both ipv4 and ipv6 files, got %v", err)
	}
}

func TestLoadConfig_Invalid(t *testing.T) {
	tests := []struct {
		name        string
		toml        string
		expectError string
	}{
		{
			name: "missing output format",
			toml: `
[output]
file = "output.csv"

[[databases]]
name = "geo"
path = "/path/to/geo.mmdb"

[[columns]]
name = "country"
database = "geo"
path = ["country", "iso_code"]
`,
			expectError: "output.format is required",
		},
		{
			name: "invalid output format",
			toml: `
[output]
format = "json"
file = "output.json"

[[databases]]
name = "geo"
path = "/path/to/geo.mmdb"

[[columns]]
name = "country"
database = "geo"
path = ["country", "iso_code"]
`,
			expectError: "output.format must be 'csv', 'parquet', or 'mmdb'",
		},
		{
			name: "missing output file",
			toml: `
[output]
format = "csv"

[[databases]]
name = "geo"
path = "/path/to/geo.mmdb"

[[columns]]
name = "country"
database = "geo"
path = ["country", "iso_code"]
`,
			expectError: "either output.file must be set or both output.ipv4_file and output.ipv6_file must be provided",
		},
		{
			name: "no databases",
			toml: `
[output]
format = "csv"
file = "output.csv"

[[columns]]
name = "country"
database = "geo"
path = ["country", "iso_code"]
`,
			expectError: "at least one database is required",
		},
		{
			name: "duplicate database names",
			toml: `
[output]
format = "csv"
file = "output.csv"

[[databases]]
name = "geo"
path = "/path/to/geo1.mmdb"

[[databases]]
name = "geo"
path = "/path/to/geo2.mmdb"

[[columns]]
name = "country"
database = "geo"
path = ["country", "iso_code"]
`,
			expectError: "duplicate database name 'geo'",
		},
		{
			name: "missing database name",
			toml: `
[output]
format = "csv"
file = "output.csv"

[[databases]]
path = "/path/to/geo.mmdb"

[[columns]]
name = "country"
database = "geo"
path = ["country", "iso_code"]
`,
			expectError: "database name is required",
		},
		{
			name: "missing database path",
			toml: `
[output]
format = "csv"
file = "output.csv"

[[databases]]
name = "geo"

[[columns]]
name = "country"
database = "geo"
path = ["country", "iso_code"]
`,
			expectError: "database path is required",
		},
		{
			name: "unknown database reference",
			toml: `
[output]
format = "csv"
file = "output.csv"

[[databases]]
name = "geo"
path = "/path/to/geo.mmdb"

[[columns]]
name = "country"
database = "unknown"
path = ["country", "iso_code"]
`,
			expectError: "references unknown database 'unknown'",
		},
		{
			name: "duplicate column names",
			toml: `
[output]
format = "csv"
file = "output.csv"

[[databases]]
name = "geo"
path = "/path/to/geo.mmdb"

[[columns]]
name = "country"
database = "geo"
path = ["country", "iso_code"]

[[columns]]
name = "country"
database = "geo"
path = ["country", "name"]
`,
			expectError: "duplicate column name 'country'",
		},
		{
			name: "missing column name",
			toml: `
[output]
format = "csv"
file = "output.csv"

[[databases]]
name = "geo"
path = "/path/to/geo.mmdb"

[[columns]]
database = "geo"
path = ["country", "iso_code"]
`,
			expectError: "column name is required",
		},
		{
			name: "missing column database",
			toml: `
[output]
format = "csv"
file = "output.csv"

[[databases]]
name = "geo"
path = "/path/to/geo.mmdb"

[[columns]]
name = "country"
path = ["country", "iso_code"]
`,
			expectError: "column database is required",
		},
		// Note: missing column path is now valid - path = [] means "copy entire record"
		{
			name: "invalid network column type",
			toml: `
[output]
format = "csv"
file = "output.csv"

[[network.columns]]
name = "network"
type = "invalid"

[[databases]]
name = "geo"
path = "/path/to/geo.mmdb"

[[columns]]
name = "country"
database = "geo"
path = ["country", "iso_code"]
`,
			expectError: "invalid network column type 'invalid'",
		},
		{
			name: "invalid data column type",
			toml: `
[output]
format = "csv"
file = "output.csv"

[[databases]]
name = "geo"
path = "/path/to/geo.mmdb"

[[columns]]
name = "country"
database = "geo"
path = ["country", "iso_code"]
type = "invalid"
`,
			expectError: "type hints not supported for csv output",
		},
		{
			name: "invalid parquet compression",
			toml: `
[output]
format = "parquet"
file = "output.parquet"

[output.parquet]
compression = "invalid"

[[databases]]
name = "geo"
path = "/path/to/geo.mmdb"

[[columns]]
name = "country"
database = "geo"
path = ["country", "iso_code"]
`,
			expectError: "invalid parquet compression 'invalid'",
		},
		{
			name: "network_bucket without split files",
			toml: `
[output]
format = "parquet"
file = "output.parquet"

[[network.columns]]
name = "network_bucket"
type = "network_bucket"

[[databases]]
name = "geo"
path = "/path/to/geo.mmdb"

[[columns]]
name = "country"
database = "geo"
path = ["country", "iso_code"]
`,
			expectError: "network_bucket column requires split files",
		},
		{
			name: "network_bucket with MMDB format",
			toml: `
[output]
format = "mmdb"
file = "output.mmdb"

[output.mmdb]
database_type = "GeoIP2-City"
description = { en = "Test" }

[[network.columns]]
name = "network_bucket"
type = "network_bucket"

[[databases]]
name = "geo"
path = "/path/to/geo.mmdb"

[[columns]]
name = "country"
database = "geo"
path = ["country", "iso_code"]
`,
			expectError: "network_bucket column type is only supported for CSV and Parquet output",
		},
		{
			name: "duplicate network column names",
			toml: `
[output]
format = "csv"
file = "output.csv"

[[network.columns]]
name = "network"
type = "cidr"

[[network.columns]]
name = "network"
type = "start_ip"

[[databases]]
name = "geo"
path = "/path/to/geo.mmdb"

[[columns]]
name = "country"
database = "geo"
path = ["country", "iso_code"]
`,
			expectError: "duplicate network column name 'network'",
		},
		{
			name: "column name conflicts with network column",
			toml: `
[output]
format = "csv"
file = "output.csv"

[[network.columns]]
name = "network"
type = "cidr"

[[databases]]
name = "geo"
path = "/path/to/geo.mmdb"

[[columns]]
name = "network"
database = "geo"
path = ["country", "iso_code"]
`,
			expectError: "duplicate column name 'network' (already used as network column)",
		},
		{
			name: "ipv4_bucket_size too large",
			toml: `
[output]
format = "parquet"
ipv4_file = "output-v4.parquet"
ipv6_file = "output-v6.parquet"

[output.parquet]
ipv4_bucket_size = 33

[[network.columns]]
name = "network_bucket"
type = "network_bucket"

[[databases]]
name = "geo"
path = "/path/to/geo.mmdb"

[[columns]]
name = "country"
database = "geo"
path = ["country", "iso_code"]
`,
			expectError: "ipv4_bucket_size must be between 1 and 32",
		},
		{
			name: "ipv4_bucket_size negative",
			toml: `
[output]
format = "parquet"
ipv4_file = "output-v4.parquet"
ipv6_file = "output-v6.parquet"

[output.parquet]
ipv4_bucket_size = -1

[[network.columns]]
name = "network_bucket"
type = "network_bucket"

[[databases]]
name = "geo"
path = "/path/to/geo.mmdb"

[[columns]]
name = "country"
database = "geo"
path = ["country", "iso_code"]
`,
			expectError: "ipv4_bucket_size must be between 1 and 32",
		},
		{
			name: "ipv6_bucket_size too large",
			toml: `
[output]
format = "parquet"
ipv4_file = "output-v4.parquet"
ipv6_file = "output-v6.parquet"

[output.parquet]
ipv6_bucket_size = 61

[[network.columns]]
name = "network_bucket"
type = "network_bucket"

[[databases]]
name = "geo"
path = "/path/to/geo.mmdb"

[[columns]]
name = "country"
database = "geo"
path = ["country", "iso_code"]
`,
			expectError: "ipv6_bucket_size must be between 1 and 60",
		},
		{
			name: "invalid ipv6_bucket_type",
			toml: `
[output]
format = "parquet"
ipv4_file = "output-v4.parquet"
ipv6_file = "output-v6.parquet"

[output.parquet]
ipv6_bucket_type = "invalid"

[[network.columns]]
name = "network_bucket"
type = "network_bucket"

[[databases]]
name = "geo"
path = "/path/to/geo.mmdb"

[[columns]]
name = "country"
database = "geo"
path = ["country", "iso_code"]
`,
			expectError: "ipv6_bucket_type must be 'string' or 'int'",
		},
		{
			name: "csv ipv4_bucket_size too large",
			toml: `
[output]
format = "csv"
ipv4_file = "output-v4.csv"
ipv6_file = "output-v6.csv"

[output.csv]
ipv4_bucket_size = 33

[[network.columns]]
name = "network_bucket"
type = "network_bucket"

[[databases]]
name = "geo"
path = "/path/to/geo.mmdb"

[[columns]]
name = "country"
database = "geo"
path = ["country", "iso_code"]
`,
			expectError: "ipv4_bucket_size must be between 1 and 32",
		},
		{
			name: "csv ipv6_bucket_size too large",
			toml: `
[output]
format = "csv"
ipv4_file = "output-v4.csv"
ipv6_file = "output-v6.csv"

[output.csv]
ipv6_bucket_size = 61

[[network.columns]]
name = "network_bucket"
type = "network_bucket"

[[databases]]
name = "geo"
path = "/path/to/geo.mmdb"

[[columns]]
name = "country"
database = "geo"
path = ["country", "iso_code"]
`,
			expectError: "ipv6_bucket_size must be between 1 and 60",
		},
		{
			name: "csv invalid ipv6_bucket_type",
			toml: `
[output]
format = "csv"
ipv4_file = "output-v4.csv"
ipv6_file = "output-v6.csv"

[output.csv]
ipv6_bucket_type = "invalid"

[[network.columns]]
name = "network_bucket"
type = "network_bucket"

[[databases]]
name = "geo"
path = "/path/to/geo.mmdb"

[[columns]]
name = "country"
database = "geo"
path = ["country", "iso_code"]
`,
			expectError: "ipv6_bucket_type must be 'string' or 'int'",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create temporary config file
			tmpDir := t.TempDir()
			configPath := filepath.Join(tmpDir, "config.toml")
			if err := os.WriteFile(configPath, []byte(tt.toml), 0o644); err != nil {
				t.Fatalf("failed to write test config: %v", err)
			}

			// Load config should fail
			_, err := LoadConfig(configPath)
			if err == nil {
				t.Fatal("expected error, got nil")
			}

			// Check error message contains expected substring
			if !strings.Contains(err.Error(), tt.expectError) {
				t.Errorf("expected error containing '%s', got '%s'", tt.expectError, err.Error())
			}
		})
	}
}

func TestApplyDefaults(t *testing.T) {
	tests := []struct {
		name     string
		input    Config
		validate func(t *testing.T, cfg *Config)
	}{
		{
			name: "CSV delimiter default",
			input: Config{
				Output: OutputConfig{Format: "csv"},
			},
			validate: func(t *testing.T, cfg *Config) {
				if cfg.Output.CSV.Delimiter != "," {
					t.Errorf("expected default delimiter=',', got %s", cfg.Output.CSV.Delimiter)
				}
			},
		},
		{
			name: "Parquet compression default",
			input: Config{
				Output: OutputConfig{Format: "parquet"},
			},
			validate: func(t *testing.T, cfg *Config) {
				if cfg.Output.Parquet.Compression != "snappy" {
					t.Errorf(
						"expected default compression='snappy', got %s",
						cfg.Output.Parquet.Compression,
					)
				}
			},
		},
		{
			name: "Parquet row group size default",
			input: Config{
				Output: OutputConfig{Format: "parquet"},
			},
			validate: func(t *testing.T, cfg *Config) {
				if cfg.Output.Parquet.RowGroupSize != 500000 {
					t.Errorf(
						"expected default row_group_size=500000, got %d",
						cfg.Output.Parquet.RowGroupSize,
					)
				}
			},
		},
		{
			name: "CSV network columns default",
			input: Config{
				Output: OutputConfig{Format: "csv"},
			},
			validate: func(t *testing.T, cfg *Config) {
				if len(cfg.Network.Columns) != 1 {
					t.Errorf("expected 1 default network column, got %d", len(cfg.Network.Columns))
				}
				if len(cfg.Network.Columns) > 0 {
					if cfg.Network.Columns[0].Name != "network" {
						t.Errorf(
							"expected default network column name='network', got %s",
							cfg.Network.Columns[0].Name,
						)
					}
					if cfg.Network.Columns[0].Type != "cidr" {
						t.Errorf(
							"expected default network column type='cidr', got %s",
							cfg.Network.Columns[0].Type,
						)
					}
				}
			},
		},
		{
			name: "Parquet network columns default",
			input: Config{
				Output: OutputConfig{Format: "parquet"},
			},
			validate: func(t *testing.T, cfg *Config) {
				if len(cfg.Network.Columns) != 2 {
					t.Errorf("expected 2 default network columns, got %d", len(cfg.Network.Columns))
				}
				if len(cfg.Network.Columns) >= 2 {
					if cfg.Network.Columns[0].Type != "start_int" {
						t.Errorf(
							"expected first network column type='start_int', got %s",
							cfg.Network.Columns[0].Type,
						)
					}
					if cfg.Network.Columns[1].Type != "end_int" {
						t.Errorf(
							"expected second network column type='end_int', got %s",
							cfg.Network.Columns[1].Type,
						)
					}
				}
			},
		},
		{
			name: "Parquet bucket size defaults",
			input: Config{
				Output: OutputConfig{Format: "parquet"},
			},
			validate: func(t *testing.T, cfg *Config) {
				if cfg.Output.Parquet.IPv4BucketSize != 16 {
					t.Errorf(
						"expected default IPv4BucketSize=16, got %d",
						cfg.Output.Parquet.IPv4BucketSize,
					)
				}
				if cfg.Output.Parquet.IPv6BucketSize != 16 {
					t.Errorf(
						"expected default IPv6BucketSize=16, got %d",
						cfg.Output.Parquet.IPv6BucketSize,
					)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := tt.input
			applyDefaults(&cfg)
			tt.validate(t, &cfg)
		})
	}
}

func assertPathEquals(t *testing.T, path Path, expected ...any) {
	t.Helper()
	if !reflect.DeepEqual(path.Segments(), expected) {
		t.Fatalf("expected path %v, got %v", expected, path.Segments())
	}
}
