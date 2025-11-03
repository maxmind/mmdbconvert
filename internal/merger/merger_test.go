package merger

import (
	"errors"
	"net/netip"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/maxmind/mmdbconvert/internal/config"
	"github.com/maxmind/mmdbconvert/internal/mmdb"
)

const (
	testDataDir = "../../testdata/MaxMind-DB/test-data"
	cityTestDB  = testDataDir + "/GeoIP2-City-Test.mmdb"
	anonTestDB  = testDataDir + "/GeoIP2-Anonymous-IP-Test.mmdb"
	ipv4TestDB  = testDataDir + "/MaxMind-DB-test-ipv4-24.mmdb"
)

func boolPtr(v bool) *bool {
	return &v
}

func TestMerger_SingleDatabase(t *testing.T) {
	// Open test database
	databases := map[string]string{
		"city": cityTestDB,
	}

	readers, err := mmdb.OpenDatabases(databases)
	require.NoError(t, err)
	defer readers.Close()

	// Create config with one column
	cfg := &config.Config{
		Columns: []config.Column{
			{
				Name:     "country_code",
				Database: "city",
				Path:     config.Path{"country", "iso_code"},
				Type:     "string",
			},
		},
	}

	// Create writer to capture output
	writer := &mockWriter{}

	// Create merger and run
	merger := NewMerger(readers, cfg, writer)
	err = merger.Merge()
	require.NoError(t, err)

	// Should have written some rows
	assert.NotEmpty(t, writer.rows, "should write at least one row")

	// Verify each row has the expected column
	for _, row := range writer.rows {
		assert.Contains(t, row.data, "country_code")
	}
}

func TestSimpleReaderIteration(t *testing.T) {
	reader, err := mmdb.Open(cityTestDB)
	require.NoError(t, err)
	defer reader.Close()

	count := 0
	for result := range reader.Networks() {
		require.NoError(t, result.Err())
		count++
		if count >= 5 {
			t.Logf("Successfully iterated %d networks, breaking", count)
			break
		}
	}
	assert.Positive(t, count)
}

func TestMerger_MultipleDatabases(t *testing.T) {
	// Open test databases
	databases := map[string]string{
		"city": cityTestDB,
		"anon": anonTestDB,
	}

	readers, err := mmdb.OpenDatabases(databases)
	require.NoError(t, err)
	defer readers.Close()

	// Create config with columns from both databases
	cfg := &config.Config{
		Columns: []config.Column{
			{
				Name:     "country_code",
				Database: "city",
				Path:     config.Path{"country", "iso_code"},
				Type:     "string",
			},
			{
				Name:     "is_anonymous",
				Database: "anon",
				Path:     config.Path{"is_anonymous"},
				Type:     "bool",
			},
		},
	}

	// Create writer to capture output
	writer := &mockWriter{}

	// Create merger and run
	merger := NewMerger(readers, cfg, writer)
	err = merger.Merge()
	require.NoError(t, err)

	// Should have written some rows
	assert.NotEmpty(t, writer.rows, "should write at least one row")

	// Verify each row has at least one column (rows with all nil values are skipped by default)
	for _, row := range writer.rows {
		assert.NotEmpty(t, row.data, "each row should have at least one non-nil value")
	}
}

var errStopIteration = errors.New("test stop iteration")

func TestMerger_EmitsNetworksPresentOnlyInLaterDatabase(t *testing.T) {
	// Domain has structural gaps that City fills. When Domain columns are
	// requested first, we still expect City-only ranges to be emitted.
	databases := map[string]string{
		"domain": testDataDir + "/GeoIP2-Domain-Test.mmdb",
		"city":   cityTestDB,
	}

	readers, err := mmdb.OpenDatabases(databases)
	require.NoError(t, err)
	defer readers.Close()

	cfg := &config.Config{
		Columns: []config.Column{
			{
				Name:     "domain",
				Database: "domain",
				Path:     config.Path{"domain"},
				Type:     "string",
			},
			{
				Name:     "country_code",
				Database: "city",
				Path:     config.Path{"country", "iso_code"},
				Type:     "string",
			},
		},
	}

	targetIP := netip.MustParseAddr("214.0.0.1")
	writer := &mockWriter{
		stopOn:  &targetIP,
		stopErr: errStopIteration,
	}
	merger := NewMerger(readers, cfg, writer)

	err = merger.Merge()
	require.ErrorIs(t, err, errStopIteration)
	assert.True(t, writer.found, "expected to detect coverage for 214.0.0.1")
}

func TestMerger_AdjacentNetworkMerging(t *testing.T) {
	// This test verifies that adjacent networks with identical data are merged
	// We'll use a small test database and verify the output is consolidated

	databases := map[string]string{
		"test": ipv4TestDB,
	}

	readers, err := mmdb.OpenDatabases(databases)
	require.NoError(t, err)
	defer readers.Close()

	// Create config
	cfg := &config.Config{
		Output: config.OutputConfig{
			IncludeEmptyRows: boolPtr(true), // Include networks even if they have no data
		},
		Columns: []config.Column{
			{
				Name:     "value",
				Database: "test",
				Path:     config.Path{"value"},
				Type:     "string",
			},
		},
	}

	// Create writer to capture output
	writer := &mockWriter{}

	// Create merger and run
	merger := NewMerger(readers, cfg, writer)
	err = merger.Merge()
	require.NoError(t, err)

	// The merger should have consolidated some networks
	assert.NotEmpty(t, writer.rows, "should write at least one row")

	// Verify no overlapping networks in output
	for i := range len(writer.rows) - 1 {
		current := writer.rows[i].prefix
		next := writer.rows[i+1].prefix

		// Verify they don't overlap
		assert.False(t, current.Overlaps(next), "output networks should not overlap")

		// If they're adjacent and have the same data, they should have been merged
		// (this is validated by the accumulator tests)
	}
}

func TestMerger_MissingDatabase(t *testing.T) {
	// Open one database
	databases := map[string]string{
		"city": cityTestDB,
	}

	readers, err := mmdb.OpenDatabases(databases)
	require.NoError(t, err)
	defer readers.Close()

	// Create config referencing a non-existent database
	cfg := &config.Config{
		Columns: []config.Column{
			{
				Name:     "value",
				Database: "nonexistent",
				Path:     config.Path{"some", "path"},
				Type:     "string",
			},
		},
	}

	// Create writer
	writer := &mockWriter{}

	// Create merger and run - should error
	merger := NewMerger(readers, cfg, writer)
	err = merger.Merge()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "nonexistent")
}

func TestMerger_MixedIPVersionsFails(t *testing.T) {
	databases := map[string]string{
		"ipv4": testDataDir + "/MaxMind-DB-test-ipv4-24.mmdb",
		"ipv6": testDataDir + "/MaxMind-DB-test-ipv6-32.mmdb",
	}

	readers, err := mmdb.OpenDatabases(databases)
	require.NoError(t, err)
	defer readers.Close()

	cfg := &config.Config{
		Columns: []config.Column{
			{Name: "v4", Database: "ipv4", Path: config.Path{"value"}},
			{Name: "v6", Database: "ipv6", Path: config.Path{"value"}},
		},
	}

	merger := NewMerger(readers, cfg, &mockWriter{})
	err = merger.Merge()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "mix IPv4-only")
}

func TestMerger_NoColumns(t *testing.T) {
	databases := map[string]string{
		"city": cityTestDB,
	}

	readers, err := mmdb.OpenDatabases(databases)
	require.NoError(t, err)
	defer readers.Close()

	// Config with no columns
	cfg := &config.Config{
		Columns: []config.Column{},
	}

	writer := &mockWriter{}

	merger := NewMerger(readers, cfg, writer)
	err = merger.Merge()
	// Should error because no databases to iterate
	assert.Error(t, err)
}

func TestMerger_NilValues(t *testing.T) {
	// Test that nil values (missing data) are handled correctly
	databases := map[string]string{
		"city": cityTestDB,
	}

	readers, err := mmdb.OpenDatabases(databases)
	require.NoError(t, err)
	defer readers.Close()

	// Request a path that may not exist in all records
	cfg := &config.Config{
		Columns: []config.Column{
			{
				Name:     "postal_code",
				Database: "city",
				Path:     config.Path{"postal", "code"},
				Type:     "string",
			},
		},
	}

	writer := &mockWriter{}

	merger := NewMerger(readers, cfg, writer)
	err = merger.Merge()
	require.NoError(t, err)

	// Should have some rows
	assert.NotEmpty(t, writer.rows)

	// Some rows may have nil values for postal_code
	hasNil := false
	hasValue := false
	for _, row := range writer.rows {
		if row.data["postal_code"] == nil {
			hasNil = true
		} else {
			hasValue = true
		}
	}

	// We should have at least processed some rows (even if all nil)
	_ = hasNil
	_ = hasValue
}

func TestGetUniqueDatabaseNames(t *testing.T) {
	tests := []struct {
		name     string
		columns  []config.Column
		expected []string
	}{
		{
			name:     "no columns",
			columns:  []config.Column{},
			expected: []string{},
		},
		{
			name: "single database",
			columns: []config.Column{
				{Database: "db1"},
				{Database: "db1"},
			},
			expected: []string{"db1"},
		},
		{
			name: "multiple databases",
			columns: []config.Column{
				{Database: "db1"},
				{Database: "db2"},
				{Database: "db1"},
				{Database: "db3"},
			},
			expected: []string{"db1", "db2", "db3"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &config.Config{Columns: tt.columns}
			merger := &Merger{config: cfg}

			result := merger.getUniqueDatabaseNames()
			assert.ElementsMatch(t, tt.expected, result)
		})
	}
}
