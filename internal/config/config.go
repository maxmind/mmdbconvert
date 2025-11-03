// Package config provides TOML configuration parsing and validation for mmdbconvert.
package config

import (
	"errors"
	"fmt"
	"math"
	"os"
	"slices"

	"github.com/pelletier/go-toml/v2"
)

const (
	formatCSV     = "csv"
	formatParquet = "parquet"
	formatMMDB    = "mmdb"
)

// Config represents the complete configuration file structure.
type Config struct {
	Output    OutputConfig  `toml:"output"`
	Network   NetworkConfig `toml:"network"`
	Databases []Database    `toml:"databases"`
	Columns   []Column      `toml:"columns"`
}

// OutputConfig defines output file settings.
type OutputConfig struct {
	Format           string        `toml:"format"`  // "csv", "parquet", or "mmdb"
	File             string        `toml:"file"`    // Output file path
	CSV              CSVConfig     `toml:"csv"`     // CSV-specific options
	Parquet          ParquetConfig `toml:"parquet"` // Parquet-specific options
	MMDB             MMDBConfig    `toml:"mmdb"`    // MMDB-specific options
	IPv4File         string        `toml:"ipv4_file"`
	IPv6File         string        `toml:"ipv6_file"`
	IncludeEmptyRows *bool         `toml:"include_empty_rows"` // Include rows with no MMDB data (default: false)
}

// CSVConfig defines CSV output options.
type CSVConfig struct {
	Delimiter     string `toml:"delimiter"`      // Field delimiter (default: ",")
	IncludeHeader *bool  `toml:"include_header"` // Include column headers (default: true)
}

// ParquetConfig defines Parquet output options.
type ParquetConfig struct {
	Compression  string `toml:"compression"`    // "none", "snappy", "gzip", "lz4", "zstd" (default: "snappy")
	RowGroupSize int    `toml:"row_group_size"` // Rows per row group (default: 500000)
}

// MMDBConfig defines MMDB output options.
type MMDBConfig struct {
	DatabaseType            string            `toml:"database_type"`             // Database type (e.g., "GeoIP2-City")
	Description             map[string]string `toml:"description"`               // Descriptions by language
	Languages               []string          `toml:"languages"`                 // List of languages (auto-populated from description if empty)
	RecordSize              *int              `toml:"record_size"`               // 24, 28, or 32 (default: 28)
	IncludeReservedNetworks *bool             `toml:"include_reserved_networks"` // Include reserved networks (default: false)
}

// NetworkConfig defines network column configuration.
type NetworkConfig struct {
	Columns []NetworkColumn `toml:"columns"`
}

// NetworkColumn defines a network column in the output.
type NetworkColumn struct {
	Name string `toml:"name"` // Column name
	Type string `toml:"type"` // "cidr", "start_ip", "end_ip", "start_int", "end_int"
}

// Database defines an MMDB database source.
type Database struct {
	Name string `toml:"name"` // Identifier for referencing in columns
	Path string `toml:"path"` // Path to MMDB file
}

// Column defines a data column mapping from MMDB to output.
type Column struct {
	Name       string `toml:"name"`        // Output column name
	Database   string `toml:"database"`    // Database to read from (references Database.Name)
	Path       Path   `toml:"path"`        // Path segments to the field
	OutputPath *Path  `toml:"output_path"` // Path segments for MMDB output (defaults to [name])
	Type       string `toml:"type"`        // Optional type hint: "string", "int64", "float64", "bool", "binary" (Parquet only)
}

// Path represents the decoded path segments for MMDB lookup.
type Path []any

// UnmarshalTOML implements toml.Unmarshaler allowing mixed string/int arrays.
// Empty arrays are allowed - path = [] means "copy entire record".
func (p *Path) UnmarshalTOML(v any) error {
	arr, ok := v.([]any)
	if !ok {
		return errors.New("path must be an array")
	}

	segments := make([]any, len(arr))
	for i, item := range arr {
		switch val := item.(type) {
		case string:
			segments[i] = val
		case int64:
			if val > int64(math.MaxInt) || val < int64(math.MinInt) {
				return fmt.Errorf("path index %d out of range", val)
			}
			segments[i] = int(val)
		default:
			return fmt.Errorf("path elements must be strings or integers, got %T", item)
		}
	}

	*p = Path(segments)
	return nil
}

// Segments returns a copy of the path segments suitable for DecodePath.
func (p *Path) Segments() []any {
	if p == nil || len(*p) == 0 {
		return nil
	}
	segments := make([]any, len(*p))
	copy(segments, *p)
	return segments
}

// LoadConfig loads and parses a TOML configuration file.
func LoadConfig(path string) (*Config, error) {
	// #nosec G304 -- path is a user-provided config file path, which is intentional
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading config file: %w", err)
	}

	var config Config
	if err := toml.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("parsing TOML: %w", err)
	}

	// Apply defaults
	applyDefaults(&config)

	// Validate configuration
	if err := validate(&config); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}

	return &config, nil
}

// applyDefaults applies default values to configuration.
func applyDefaults(config *Config) {
	// Output defaults
	if config.Output.IncludeEmptyRows == nil {
		config.Output.IncludeEmptyRows = boolPtr(false)
	}

	// CSV defaults
	if config.Output.CSV.Delimiter == "" {
		config.Output.CSV.Delimiter = ","
	}
	if config.Output.CSV.IncludeHeader == nil {
		config.Output.CSV.IncludeHeader = boolPtr(true)
	}

	// Parquet defaults
	if config.Output.Parquet.Compression == "" {
		config.Output.Parquet.Compression = "snappy"
	}
	if config.Output.Parquet.RowGroupSize == 0 {
		config.Output.Parquet.RowGroupSize = 500000
	}

	// MMDB defaults
	if config.Output.Format == formatMMDB {
		if config.Output.MMDB.RecordSize == nil {
			config.Output.MMDB.RecordSize = intPtr(28)
		}
		if config.Output.MMDB.IncludeReservedNetworks == nil {
			config.Output.MMDB.IncludeReservedNetworks = boolPtr(false)
		}
		// Auto-populate languages from description keys if not specified
		if len(config.Output.MMDB.Languages) == 0 {
			for lang := range config.Output.MMDB.Description {
				config.Output.MMDB.Languages = append(config.Output.MMDB.Languages, lang)
			}
			// Sort for deterministic output
			slices.Sort(config.Output.MMDB.Languages)
		}
	}

	// Network column defaults - apply format-specific defaults if no columns specified
	if len(config.Network.Columns) == 0 {
		switch config.Output.Format {
		case formatParquet:
			// Parquet default: integer columns for query performance
			config.Network.Columns = []NetworkColumn{
				{Name: "start_int", Type: "start_int"},
				{Name: "end_int", Type: "end_int"},
			}
		case formatMMDB:
			// MMDB default: no network columns (data written by prefix)
			config.Network.Columns = []NetworkColumn{}
		default:
			// CSV default: human-readable CIDR
			config.Network.Columns = []NetworkColumn{
				{Name: "network", Type: "cidr"},
			}
		}
	}
}

func boolPtr(v bool) *bool {
	return &v
}

func intPtr(v int) *int {
	return &v
}

// validate performs comprehensive validation of the configuration.
//
//nolint:gocyclo // Configuration validation is inherently complex
func validate(config *Config) error {
	// Validate output settings
	if config.Output.Format == "" {
		return errors.New("output.format is required")
	}
	if config.Output.Format != formatCSV && config.Output.Format != formatParquet &&
		config.Output.Format != formatMMDB {
		return fmt.Errorf(
			"output.format must be 'csv', 'parquet', or 'mmdb', got '%s'",
			config.Output.Format,
		)
	}
	if config.Output.File == "" && (config.Output.IPv4File == "" || config.Output.IPv6File == "") {
		return errors.New(
			"either output.file must be set or both output.ipv4_file and output.ipv6_file must be provided",
		)
	}
	if config.Output.File != "" && (config.Output.IPv4File != "" || config.Output.IPv6File != "") {
		return errors.New(
			"output.ipv4_file and output.ipv6_file cannot be used together with output.file",
		)
	}

	// Validate Parquet compression
	if config.Output.Format == formatParquet {
		validCompressions := map[string]bool{
			"none": true, "snappy": true, "gzip": true, "lz4": true, "zstd": true,
		}
		if !validCompressions[config.Output.Parquet.Compression] {
			return fmt.Errorf(
				"invalid parquet compression '%s', must be one of: none, snappy, gzip, lz4, zstd",
				config.Output.Parquet.Compression,
			)
		}
	}

	// Validate MMDB configuration
	if config.Output.Format == formatMMDB {
		if config.Output.MMDB.DatabaseType == "" {
			return errors.New("output.mmdb.database_type is required for MMDB output")
		}

		if config.Output.MMDB.RecordSize != nil {
			rs := *config.Output.MMDB.RecordSize
			if rs != 24 && rs != 28 && rs != 32 {
				return fmt.Errorf("output.mmdb.record_size must be 24, 28, or 32, got %d", rs)
			}
		}

		// Reject split files for MMDB
		if config.Output.IPv4File != "" || config.Output.IPv6File != "" {
			return errors.New("split IPv4/IPv6 files not supported for MMDB output")
		}
	}

	// Validate type hints only allowed for Parquet
	if config.Output.Format == formatCSV || config.Output.Format == formatMMDB {
		for _, col := range config.Columns {
			if col.Type != "" {
				return fmt.Errorf(
					"column '%s': type hints not supported for %s output (only for parquet)",
					col.Name, config.Output.Format,
				)
			}
		}
	}

	// Validate databases
	if len(config.Databases) == 0 {
		return errors.New("at least one database is required")
	}

	// Check for duplicate database names
	dbNames := map[string]bool{}
	for _, db := range config.Databases {
		if db.Name == "" {
			return errors.New("database name is required")
		}
		if db.Path == "" {
			return fmt.Errorf("database path is required for database '%s'", db.Name)
		}
		if dbNames[db.Name] {
			return fmt.Errorf("duplicate database name '%s'", db.Name)
		}
		dbNames[db.Name] = true
	}

	// Validate network columns
	validNetworkTypes := map[string]bool{
		"cidr": true, "start_ip": true, "end_ip": true, "start_int": true, "end_int": true,
	}
	networkColNames := map[string]bool{}
	for _, col := range config.Network.Columns {
		if col.Name == "" {
			return errors.New("network column name is required")
		}
		if col.Type == "" {
			return fmt.Errorf("network column type is required for column '%s'", col.Name)
		}
		if !validNetworkTypes[col.Type] {
			return fmt.Errorf(
				"invalid network column type '%s' for column '%s', must be one of: cidr, start_ip, end_ip, start_int, end_int",
				col.Type,
				col.Name,
			)
		}
		if networkColNames[col.Name] {
			return fmt.Errorf("duplicate network column name '%s'", col.Name)
		}
		networkColNames[col.Name] = true
	}

	// Validate data columns
	validDataTypes := map[string]bool{
		"": true, "string": true, "int64": true, "float64": true, "bool": true, "binary": true,
	}
	dataColNames := map[string]bool{}
	for _, col := range config.Columns {
		if col.Name == "" {
			return errors.New("column name is required")
		}
		if col.Database == "" {
			return fmt.Errorf("column database is required for column '%s'", col.Name)
		}
		// Empty path is allowed - path = [] means "copy entire record"

		// Validate database reference
		if !dbNames[col.Database] {
			return fmt.Errorf(
				"column '%s' references unknown database '%s'",
				col.Name,
				col.Database,
			)
		}

		// Validate type hint
		if !validDataTypes[col.Type] {
			return fmt.Errorf(
				"invalid type '%s' for column '%s', must be one of: string, int64, float64, bool, binary",
				col.Type,
				col.Name,
			)
		}

		// Check for duplicate column names (including network columns)
		if networkColNames[col.Name] {
			return fmt.Errorf(
				"duplicate column name '%s' (already used as network column)",
				col.Name,
			)
		}
		if dataColNames[col.Name] {
			return fmt.Errorf("duplicate column name '%s'", col.Name)
		}
		dataColNames[col.Name] = true

		// Empty output_path is allowed - it means merge into root for MMDB output
	}

	return nil
}
