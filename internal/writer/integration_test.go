package writer

import (
	"bytes"
	"iter"
	"net/netip"
	"strings"
	"testing"

	"github.com/maxmind/mmdbwriter/v2/mmdbtype"
	"github.com/oschwald/maxminddb-golang/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go4.org/netipx"

	"github.com/maxmind/mmdbconvert/internal/config"
	"github.com/maxmind/mmdbconvert/internal/merger"
	"github.com/maxmind/mmdbconvert/internal/mmdb"
)

const (
	testDataDir = "../../testdata/MaxMind-DB/test-data"
	cityTestDB  = testDataDir + "/GeoIP2-City-Test.mmdb"
)

func TestEndToEnd_MMDBExport(t *testing.T) {
	for _, tt := range []struct {
		name         string
		disableCache bool
	}{
		{"with cache", false},
		{"without cache", true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			readers, err := mmdb.OpenDatabases(map[string]string{"city": cityTestDB})
			require.NoError(t, err)
			defer readers.Close()
			source, ok := readers.Get("city")
			require.True(t, ok)

			recordSize := 28
			includeReserved := true
			cfg := &config.Config{
				DisableCache: tt.disableCache,
				Output: config.OutputConfig{
					Format: config.OutputFormatMMDB,
					MMDB: config.MMDBConfig{
						DatabaseType:            source.Metadata().DatabaseType,
						RecordSize:              &recordSize,
						IncludeReservedNetworks: &includeReserved,
					},
				},
				Columns: []config.Column{
					{
						Name:       "record",
						Database:   "city",
						Path:       config.Path{},
						OutputPath: &config.Path{},
					},
				},
			}
			var buf bytes.Buffer
			writer, err := NewMMDBWriter(&buf, cfg, int(source.Metadata().IPVersion))
			require.NoError(t, err)
			m, err := merger.NewMerger(readers, cfg, writer)
			require.NoError(t, err)
			require.NoError(t, m.Merge(t.Context()))
			require.NoError(t, writer.Flush())

			output, err := maxminddb.OpenBytes(buf.Bytes())
			require.NoError(t, err)
			defer output.Close()

			expected := collectMMDBRanges(t, source.Networks())
			actual := collectMMDBRanges(t, output.Networks())
			require.NotEmpty(t, expected)
			require.Len(t, actual, len(expected))
			for i := range expected {
				require.Equal(t, expected[i], actual[i], "range %d", i)
			}
		})
	}
}

type mmdbRange struct {
	start netip.Addr
	end   netip.Addr
	data  mmdbtype.DataType
}

// collectMMDBRanges coalesces adjacent equal records so prefix compaction is allowed.
func collectMMDBRanges(t *testing.T, networks iter.Seq[maxminddb.Result]) []mmdbRange {
	t.Helper()
	var ranges []mmdbRange
	for result := range networks {
		require.NoError(t, result.Err())
		var decoded mmdbtype.Unmarshaler
		require.NoError(t, result.Decode(&decoded))
		current := mmdbRange{
			start: result.Prefix().Addr(),
			end:   netipx.PrefixLastIP(result.Prefix()),
			data:  decoded.Result(),
		}
		if len(ranges) > 0 {
			previous := &ranges[len(ranges)-1]
			if previous.end.Next() == current.start && previous.data.Equal(current.data) {
				previous.end = current.end
				continue
			}
		}
		ranges = append(ranges, current)
	}
	return ranges
}

// TestEndToEnd_CSVExport tests the complete flow from MMDB to CSV output.
func TestEndToEnd_CSVExport(t *testing.T) {
	// Open test database
	databases := map[string]string{
		"city": cityTestDB,
	}

	readers, err := mmdb.OpenDatabases(databases)
	require.NoError(t, err)
	defer readers.Close()

	// Create config
	cfg := &config.Config{
		Output: config.OutputConfig{
			Format: "csv",
			CSV: config.CSVConfig{
				Delimiter: ",",
			},
		},
		Network: config.NetworkConfig{
			Columns: []config.NetworkColumn{
				{Name: "network", Type: "cidr"},
			},
		},
		Columns: []config.Column{
			{
				Name:     "country_code",
				Database: "city",
				Path:     config.Path{"country", "iso_code"},
				Type:     "string",
			},
		},
	}

	// Create CSV writer
	buf := &bytes.Buffer{}
	csvWriter := NewCSVWriter(buf, cfg)

	// Create merger and run
	m, err := merger.NewMerger(readers, cfg, csvWriter)
	require.NoError(t, err)
	err = m.Merge(t.Context())
	require.NoError(t, err)

	// Flush CSV
	err = csvWriter.Flush()
	require.NoError(t, err)

	// Verify output
	output := buf.String()
	lines := strings.Split(strings.TrimSpace(output), "\n")

	// Should have header + data rows
	assert.Greater(t, len(lines), 1, "should have at least header and one data row")

	// Check header
	assert.Equal(t, "network,country_code", lines[0])

	// Check that we have data rows with proper format
	for i := 1; i < len(lines) && i < 5; i++ {
		parts := strings.Split(lines[i], ",")
		assert.Len(t, parts, 2, "each row should have 2 columns")

		// First column should be a valid CIDR
		assert.Contains(t, parts[0], "/", "network column should contain CIDR notation")

		// Second column is country_code (may be empty for some ranges)
		t.Logf("Row %d: %s -> %s", i, parts[0], parts[1])
	}
}

// TestEndToEnd_CSVExport_MultipleNetworkColumns tests CSV with multiple network column types.
func TestEndToEnd_CSVExport_MultipleNetworkColumns(t *testing.T) {
	databases := map[string]string{
		"city": cityTestDB,
	}

	readers, err := mmdb.OpenDatabases(databases)
	require.NoError(t, err)
	defer readers.Close()

	cfg := &config.Config{
		Output: config.OutputConfig{
			Format: "csv",
			CSV: config.CSVConfig{
				Delimiter: ",",
			},
		},
		Network: config.NetworkConfig{
			Columns: []config.NetworkColumn{
				{Name: "network", Type: "cidr"},
				{Name: "start_ip", Type: "start_ip"},
				{Name: "end_ip", Type: "end_ip"},
			},
		},
		Columns: []config.Column{
			{
				Name:     "country_code",
				Database: "city",
				Path:     config.Path{"country", "iso_code"},
				Type:     "string",
			},
		},
	}

	buf := &bytes.Buffer{}
	csvWriter := NewCSVWriter(buf, cfg)

	m, err := merger.NewMerger(readers, cfg, csvWriter)
	require.NoError(t, err)
	err = m.Merge(t.Context())
	require.NoError(t, err)

	err = csvWriter.Flush()
	require.NoError(t, err)

	output := buf.String()
	lines := strings.Split(strings.TrimSpace(output), "\n")

	assert.Greater(t, len(lines), 1)
	assert.Equal(t, "network,start_ip,end_ip,country_code", lines[0])

	// Check a sample data row
	if len(lines) > 1 {
		parts := strings.Split(lines[1], ",")
		assert.Len(t, parts, 4, "should have 4 columns")
		t.Logf("Sample row: network=%s, start=%s, end=%s, country=%s",
			parts[0], parts[1], parts[2], parts[3])
	}
}

// TestEndToEnd_CSVExport_MultipleColumns tests CSV with multiple data columns.
func TestEndToEnd_CSVExport_MultipleColumns(t *testing.T) {
	databases := map[string]string{
		"city": cityTestDB,
	}

	readers, err := mmdb.OpenDatabases(databases)
	require.NoError(t, err)
	defer readers.Close()

	cfg := &config.Config{
		Output: config.OutputConfig{
			Format: "csv",
			CSV: config.CSVConfig{
				Delimiter: ",",
			},
		},
		Network: config.NetworkConfig{
			Columns: []config.NetworkColumn{
				{Name: "network", Type: "cidr"},
			},
		},
		Columns: []config.Column{
			{
				Name:     "country_code",
				Database: "city",
				Path:     config.Path{"country", "iso_code"},
				Type:     "string",
			},
			{
				Name:     "city_name",
				Database: "city",
				Path:     config.Path{"city", "names", "en"},
				Type:     "string",
			},
		},
	}

	buf := &bytes.Buffer{}
	csvWriter := NewCSVWriter(buf, cfg)

	m, err := merger.NewMerger(readers, cfg, csvWriter)
	require.NoError(t, err)
	err = m.Merge(t.Context())
	require.NoError(t, err)

	err = csvWriter.Flush()
	require.NoError(t, err)

	output := buf.String()
	lines := strings.Split(strings.TrimSpace(output), "\n")

	assert.Greater(t, len(lines), 1)
	assert.Equal(t, "network,country_code,city_name", lines[0])

	// Verify we have rows with the correct column count
	foundNonEmpty := false
	for i := 1; i < len(lines) && i < 10; i++ {
		parts := strings.Split(lines[i], ",")
		// Note: Some values might be quoted if they contain special characters
		// so we check that we have at least 3 parts
		assert.GreaterOrEqual(t, len(parts), 3, "should have at least 3 columns")

		if len(parts) >= 3 && parts[2] != "" {
			foundNonEmpty = true
			t.Logf("Found city: %s in %s", parts[2], parts[1])
		}
	}

	// The test database should have at least some cities
	assert.True(t, foundNonEmpty, "should have found at least one city name")
}

// TestEndToEnd_CSVExport_NilValues tests that nil values are handled correctly.
func TestEndToEnd_CSVExport_NilValues(t *testing.T) {
	databases := map[string]string{
		"city": cityTestDB,
	}

	includeEmpty := true

	readers, err := mmdb.OpenDatabases(databases)
	require.NoError(t, err)
	defer readers.Close()

	// Request a field that might not exist in all records
	cfg := &config.Config{
		Output: config.OutputConfig{
			Format: "csv",
			CSV: config.CSVConfig{
				Delimiter: ",",
			},
		},
		Network: config.NetworkConfig{
			Columns: []config.NetworkColumn{
				{Name: "network", Type: "cidr"},
			},
		},
		Columns: []config.Column{
			{
				Name:     "postal_code",
				Database: "city",
				Path:     config.Path{"postal", "code"},
				Type:     "string",
			},
		},
	}
	cfg.Output.IncludeEmptyRows = &includeEmpty

	buf := &bytes.Buffer{}
	csvWriter := NewCSVWriter(buf, cfg)

	m, err := merger.NewMerger(readers, cfg, csvWriter)
	require.NoError(t, err)
	err = m.Merge(t.Context())
	require.NoError(t, err)

	err = csvWriter.Flush()
	require.NoError(t, err)

	output := buf.String()
	lines := strings.Split(strings.TrimSpace(output), "\n")

	assert.Greater(t, len(lines), 1)

	// Some rows should have empty postal codes (nil values)
	foundEmpty := false
	foundNonEmpty := false
	for i := 1; i < len(lines); i++ {
		parts := strings.Split(lines[i], ",")
		if len(parts) >= 2 {
			if parts[1] == "" {
				foundEmpty = true
			} else {
				foundNonEmpty = true
			}
		}
	}

	// We should have some rows with and without postal codes
	t.Logf("Found empty: %v, found non-empty: %v", foundEmpty, foundNonEmpty)
	assert.True(t, foundEmpty, "expected at least one row without a postal code")
	assert.True(t, foundNonEmpty, "expected at least one row with a postal code")
}
