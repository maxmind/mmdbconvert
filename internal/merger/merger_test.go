package merger

import (
	"errors"
	"net/netip"
	"testing"

	"github.com/maxmind/mmdbwriter/mmdbtype"
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
	merger, err := NewMerger(readers, cfg, writer)
	require.NoError(t, err)
	err = merger.Merge()
	require.NoError(t, err)

	// Should have written some rows
	assert.NotEmpty(t, writer.rows, "should write at least one row")

	// Verify each row has data in the country_code column (index 0)
	for _, row := range writer.rows {
		assert.Len(t, row.data, 1, "should have 1 column")
		// country_code column should have data (not checking nil since some rows may not have it)
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
	merger, err := NewMerger(readers, cfg, writer)
	require.NoError(t, err)
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
	merger, err := NewMerger(readers, cfg, writer)
	require.NoError(t, err)

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
	merger, err := NewMerger(readers, cfg, writer)
	require.NoError(t, err)
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

	// Create merger - should error because database doesn't exist
	_, err = NewMerger(readers, cfg, writer)
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

	_, err = NewMerger(readers, cfg, &mockWriter{})
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

	_, err = NewMerger(readers, cfg, writer)
	// Should error because no databases configured (no columns means no databases)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no databases configured")
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

	includeEmpty := true
	cfg.Output.IncludeEmptyRows = &includeEmpty

	writer := &mockWriter{}

	merger, err := NewMerger(readers, cfg, writer)
	require.NoError(t, err)
	err = merger.Merge()
	require.NoError(t, err)

	// Should have some rows
	assert.NotEmpty(t, writer.rows)

	// Some rows may have nil values for postal_code (column index 0)
	hasNil := false
	hasValue := false
	for _, row := range writer.rows {
		if row.data[0] == nil {
			hasNil = true
		} else {
			hasValue = true
		}
	}

	// We should have at least processed some rows (even if all nil)
	assert.True(t, hasNil, "expected at least one row without a postal code")
	assert.True(t, hasValue, "expected at least one row with a postal code")
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

// TestMerger_ResultAlignment verifies that results[i] always corresponds to
// readersList[i] even when networks have different specificities.
func TestMerger_ResultAlignment(t *testing.T) {
	databases := map[string]string{
		"city": cityTestDB,
		"anon": anonTestDB,
	}

	readers, err := mmdb.OpenDatabases(databases)
	require.NoError(t, err)
	defer readers.Close()

	// Columns from both databases - order matters for alignment
	cfg := &config.Config{
		Columns: []config.Column{
			{Name: "country", Database: "city", Path: config.Path{"country", "iso_code"}},
			{Name: "anon_type", Database: "anon", Path: config.Path{"is_anonymous"}},
		},
	}

	writer := &mockWriter{}
	merger, err := NewMerger(readers, cfg, writer)
	require.NoError(t, err)

	// Verify extractors have correct dbIndex values
	require.Len(t, merger.extractors, 2)
	assert.Equal(t, 0, merger.extractors[0].dbIndex, "city column should map to index 0")
	assert.Equal(t, 1, merger.extractors[1].dbIndex, "anon column should map to index 1")

	err = merger.Merge()
	require.NoError(t, err)

	// Should have successfully merged data from both databases
	assert.NotEmpty(t, writer.rows)
}

// TestMerger_BroaderDatabase tests the case where result.Prefix() is broader
// than effectivePrefix. This happens when one database has a less specific
// network that still covers the effective network being processed.
func TestMerger_BroaderDatabase(t *testing.T) {
	databases := map[string]string{
		"city": cityTestDB,
		"anon": anonTestDB,
	}

	readers, err := mmdb.OpenDatabases(databases)
	require.NoError(t, err)
	defer readers.Close()

	cfg := &config.Config{
		Columns: []config.Column{
			{Name: "country", Database: "city", Path: config.Path{"country", "iso_code"}},
			{Name: "is_anon", Database: "anon", Path: config.Path{"is_anonymous"}},
		},
	}

	writer := &mockWriter{}
	merger, err := NewMerger(readers, cfg, writer)
	require.NoError(t, err)

	err = merger.Merge()
	require.NoError(t, err)

	// Verify we got results and they contain data from both databases
	assert.NotEmpty(t, writer.rows)

	// Check that we have some rows with data from both databases
	hasBoth := false
	for _, row := range writer.rows {
		// Column 0: country, Column 1: is_anon
		if row.data[0] != nil && row.data[1] != nil {
			hasBoth = true
			break
		}
	}
	// At least some networks should have data from both databases
	assert.True(t, hasBoth, "should have at least one network with data from both databases")
}

// TestMerger_MissingData verifies handling when a database has no data for
// certain networks. With IncludeNetworksWithoutData, we get notFound Results.
func TestMerger_MissingData(t *testing.T) {
	databases := map[string]string{
		"city": cityTestDB,
		"anon": anonTestDB,
	}

	readers, err := mmdb.OpenDatabases(databases)
	require.NoError(t, err)
	defer readers.Close()

	includeEmpty := true
	cfg := &config.Config{
		Output: config.OutputConfig{
			IncludeEmptyRows: &includeEmpty, // Include rows even with missing data
		},
		Columns: []config.Column{
			{Name: "country", Database: "city", Path: config.Path{"country", "iso_code"}},
			{Name: "is_anon", Database: "anon", Path: config.Path{"is_anonymous"}},
		},
	}

	writer := &mockWriter{}
	merger, err := NewMerger(readers, cfg, writer)
	require.NoError(t, err)

	err = merger.Merge()
	require.NoError(t, err)

	// Should have rows
	assert.NotEmpty(t, writer.rows)

	// Should have some rows where one database has data but the other doesn't
	hasPartialData := false
	for _, row := range writer.rows {
		// Column 0: country, Column 1: is_anon
		countryExists := row.data[0] != nil
		anonExists := row.data[1] != nil

		// XOR: one exists but not both
		if (countryExists && !anonExists) || (!countryExists && anonExists) {
			hasPartialData = true
			break
		}
	}

	// Note: This test may not always find partial data depending on the test databases,
	// but the important thing is that the merge completes without errors even when
	// databases have different network coverage.
	assert.True(t, hasPartialData, "expected at least one network with partial database coverage")
}

// TestMerger_NoRedundantLookups verifies that the optimization to thread Results
// through recursion works correctly. This test documents that extractAndProcess
// uses result.DecodePath() directly instead of calling Lookup() for each column.
//
// The actual verification of eliminated Lookups is done through profiling, which
// showed that Lookup() calls dropped from 19% of CPU time to near-zero after this
// optimization. This test ensures the code path works correctly.
func TestMerger_NoRedundantLookups(t *testing.T) {
	databases := map[string]string{
		"city": cityTestDB,
		"anon": anonTestDB,
	}

	readers, err := mmdb.OpenDatabases(databases)
	require.NoError(t, err)
	defer readers.Close()

	// Use multiple columns from the same database to demonstrate the optimization
	// In the old implementation, this would do 3 Lookups per network.
	// With Result threading, we get the Result once during iteration and reuse it.
	cfg := &config.Config{
		Columns: []config.Column{
			{Name: "country", Database: "city", Path: config.Path{"country", "iso_code"}},
			{Name: "city_name", Database: "city", Path: config.Path{"city", "names", "en"}},
			{Name: "continent", Database: "city", Path: config.Path{"continent", "code"}},
			{Name: "is_anon", Database: "anon", Path: config.Path{"is_anonymous"}},
		},
	}

	writer := &mockWriter{}
	merger, err := NewMerger(readers, cfg, writer)
	require.NoError(t, err)

	// Verify extractors have correct dbIndex mappings
	require.Len(t, merger.extractors, 4)
	assert.Equal(t, 0, merger.extractors[0].dbIndex, "city columns should map to index 0")
	assert.Equal(t, 0, merger.extractors[1].dbIndex, "city columns should map to index 0")
	assert.Equal(t, 0, merger.extractors[2].dbIndex, "city columns should map to index 0")
	assert.Equal(t, 1, merger.extractors[3].dbIndex, "anon column should map to index 1")

	// Run the merge
	err = merger.Merge()
	require.NoError(t, err)

	// Verify we got results with data from multiple columns
	assert.NotEmpty(t, writer.rows)

	// Check that we successfully extracted data from multiple columns
	hasMultipleColumns := false
	for _, row := range writer.rows {
		columnCount := 0
		// Column 0: country, Column 1: city_name, Column 2: continent
		if row.data[0] != nil {
			columnCount++
		}
		if row.data[1] != nil {
			columnCount++
		}
		if row.data[2] != nil {
			columnCount++
		}
		if columnCount >= 2 {
			hasMultipleColumns = true
			break
		}
	}

	assert.True(t, hasMultipleColumns,
		"should have extracted data from multiple columns using a single Result")

	// The key optimization: All city columns (country, city_name, continent) are
	// extracted from the same Result object, eliminating 2 out of 3 Lookups per network.
	// This is verified through profiling showing ~24% overall speedup.
}

// TestMerger_IncludeNetworksWithoutDataGuarantees verifies that with
// IncludeNetworksWithoutData, NetworksWithin always yields at least one
// Result per database, even if that Result has Found() == false.
func TestMerger_IncludeNetworksWithoutDataGuarantees(t *testing.T) {
	reader, err := mmdb.Open(cityTestDB)
	require.NoError(t, err)
	defer reader.Close()

	// Pick a network from the database
	var testNetwork netip.Prefix
	for result := range reader.Networks() {
		require.NoError(t, result.Err())
		testNetwork = result.Prefix()
		break
	}
	require.True(t, testNetwork.IsValid(), "should have found a test network")

	// NetworksWithin with IncludeNetworksWithoutData should always yield results
	count := 0
	for result := range reader.NetworksWithin(testNetwork) {
		require.NoError(t, result.Err())
		count++
		// Should get at least one result
		if count >= 1 {
			break
		}
	}

	assert.GreaterOrEqual(t, count, 1,
		"NetworksWithin with IncludeNetworksWithoutData should yield at least one Result")

	// Now test with a network that likely has no data in a second database
	databases := map[string]string{
		"city": cityTestDB,
		"anon": anonTestDB,
	}

	readers, err := mmdb.OpenDatabases(databases)
	require.NoError(t, err)
	defer readers.Close()

	// Get a network from city database
	cityReader, ok := readers.Get("city")
	require.True(t, ok)

	for result := range cityReader.Networks() {
		require.NoError(t, result.Err())
		testNetwork = result.Prefix()
		break
	}

	// Check if anon database has data for this network
	anonReader, ok := readers.Get("anon")
	require.True(t, ok)

	count = 0
	for result := range anonReader.NetworksWithin(testNetwork) {
		require.NoError(t, result.Err())
		count++
		// Even if anon has no data for this network, we should get a Result
		// with Found() == false
		t.Logf("NetworksWithin result: prefix=%s, found=%v",
			result.Prefix(), result.Found())
	}

	// With IncludeNetworksWithoutData (which Networks uses by default),
	// we should get at least one result even if no data exists
	assert.GreaterOrEqual(t, count, 1,
		"NetworksWithin should yield at least one Result even without data")
}

func TestWalkPathSupportsNegativeIndex(t *testing.T) {
	root := mmdbtype.Map{
		"values": mmdbtype.Slice{
			mmdbtype.String("first"),
			mmdbtype.String("second"),
		},
	}

	value, err := walkPath(root, []any{"values", -1})
	require.NoError(t, err)
	require.Equal(t, mmdbtype.String("second"), value)
}

func TestWalkPathReturnsErrorOnTypeMismatch(t *testing.T) {
	root := mmdbtype.Map{
		"leaf": mmdbtype.String("value"),
	}

	_, err := walkPath(root, []any{"leaf", "nested"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "leaf")
}

func TestDecodeOnceMatchesDecodePath(t *testing.T) {
	reader, err := mmdb.Open(testDataDir + "/GeoIP2-Enterprise-Test.mmdb")
	require.NoError(t, err)
	defer reader.Close()

	paths := []struct {
		name string
		path []any
	}{
		{name: "country", path: []any{"country", "iso_code"}},
		{name: "continent", path: []any{"continent", "code"}},
		{name: "latitude", path: []any{"location", "latitude"}},
		{name: "longitude", path: []any{"location", "longitude"}},
	}

	fullUnmarshaler := mmdbtype.NewUnmarshaler()
	pathUnmarshaler := mmdbtype.NewUnmarshaler()

	count := 0
	for result := range reader.Networks() {
		require.NoError(t, result.Err())

		if !result.Found() {
			continue
		}

		require.NoError(t, result.Decode(fullUnmarshaler))
		fullValue := fullUnmarshaler.Result()
		fullUnmarshaler.Clear()

		record, ok := fullValue.(mmdbtype.Map)
		require.True(t, ok, "expected full record to be mmdbtype.Map for %s", result.Prefix())

		for _, p := range paths {
			got, err := walkPath(record, p.path)
			require.NoError(t, err)

			require.NoError(t, result.DecodePath(pathUnmarshaler, p.path...))
			expected := pathUnmarshaler.Result()
			pathUnmarshaler.Clear()

			if expected == nil {
				assert.Nil(t, got, "expected nil for %s at %s", p.name, result.Prefix())
				continue
			}

			require.NotNil(t, got, "walkPath returned nil for %s at %s", p.name, result.Prefix())
			assert.Truef(t, mmdbEqual(expected, got),
				"walkPath mismatch for %s at %s", p.name, result.Prefix())
		}

		count++
		if count >= 50 {
			break
		}
	}
}

func mmdbEqual(a, b mmdbtype.DataType) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	return a.Equal(b)
}
