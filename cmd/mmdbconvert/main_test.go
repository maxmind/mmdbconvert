package main

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/maxmind/mmdbconvert/internal/config"
	"github.com/maxmind/mmdbconvert/internal/mmdb"
)

const testDataDir = "../../testdata/MaxMind-DB/test-data"

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
