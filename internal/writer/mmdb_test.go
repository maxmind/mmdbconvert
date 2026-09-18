package writer

import (
	"net/netip"
	"path/filepath"
	"testing"

	"github.com/maxmind/mmdbwriter/v2/mmdbtype"
	"github.com/oschwald/maxminddb-golang/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/maxmind/mmdbconvert/internal/config"
)

func TestMMDBWriter_RoundTrip(t *testing.T) {
	tests := []struct {
		name      string
		ipVersion int
		prefix    string
		start     string
		end       string
	}{
		{"IPv4", 4, "1.2.3.0/24", "1.2.4.1", "1.2.5.254"},
		{"IPv4 in IPv6", 6, "1.2.3.0/24", "1.2.4.1", "1.2.5.254"},
		{"IPv6", 6, "200f::/120", "200f::101", "200f::2fe"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			recordSize := 28
			includeReserved := false
			cfg := &config.Config{
				Output: config.OutputConfig{
					MMDB: config.MMDBConfig{
						DatabaseType:            "RoundTrip-Test",
						Description:             map[string]string{"en": "Round trip test"},
						Languages:               []string{"en"},
						RecordSize:              &recordSize,
						IncludeReservedNetworks: &includeReserved,
					},
				},
				Columns: []config.Column{
					{Name: "country", OutputPath: &config.Path{"country", "iso_code"}},
					{Name: "count", OutputPath: &config.Path{"count"}},
				},
			}
			path := filepath.Join(t.TempDir(), "output.mmdb")
			writer, err := NewMMDBWriter(path, cfg, tt.ipVersion)
			require.NoError(t, err)

			prefix := netip.MustParsePrefix(tt.prefix)
			start := netip.MustParseAddr(tt.start)
			end := netip.MustParseAddr(tt.end)
			require.NoError(t, writer.WriteRow(prefix, []mmdbtype.DataType{
				mmdbtype.String("US"), mmdbtype.Uint32(42),
			}))
			require.NoError(t, writer.WriteRange(start, end, []mmdbtype.DataType{
				mmdbtype.String("CA"), mmdbtype.Uint32(24),
			}))
			require.NoError(t, writer.Flush())

			reader, err := maxminddb.Open(path)
			require.NoError(t, err)
			defer reader.Close()
			assert.EqualValues(t, tt.ipVersion, reader.Metadata.IPVersion)
			assert.EqualValues(t, recordSize, reader.Metadata.RecordSize)
			assert.Equal(t, cfg.Output.MMDB.DatabaseType, reader.Metadata.DatabaseType)
			assert.Equal(t, cfg.Output.MMDB.Description, reader.Metadata.Description)
			assert.Equal(t, cfg.Output.MMDB.Languages, reader.Metadata.Languages)

			rowData := mmdbtype.Map{
				"country": mmdbtype.Map{"iso_code": mmdbtype.String("US")},
				"count":   mmdbtype.Uint32(42),
			}
			rangeData := mmdbtype.Map{
				"country": mmdbtype.Map{"iso_code": mmdbtype.String("CA")},
				"count":   mmdbtype.Uint32(24),
			}
			for ip := prefix.Addr(); prefix.Contains(ip); ip = ip.Next() {
				var decoded mmdbtype.Unmarshaler
				require.NoError(t, reader.Lookup(ip).Decode(&decoded))
				assert.Equal(t, rowData, decoded.Result(), "IP %s", ip)
			}
			for ip := start; ip.Compare(end) <= 0; ip = ip.Next() {
				var decoded mmdbtype.Unmarshaler
				require.NoError(t, reader.Lookup(ip).Decode(&decoded))
				assert.Equal(t, rangeData, decoded.Result(), "IP %s", ip)
			}
			for _, ip := range []netip.Addr{prefix.Addr().Prev(), start.Prev(), end.Next()} {
				result := reader.Lookup(ip)
				require.NoError(t, result.Err())
				assert.False(t, result.Found(), "IP %s should not have data", ip)
			}
		})
	}
}

func TestMMDBWriter_InvalidNetworks(t *testing.T) {
	recordSize := 28
	includeReserved := true
	cfg := &config.Config{
		Output: config.OutputConfig{
			MMDB: config.MMDBConfig{
				RecordSize:              &recordSize,
				IncludeReservedNetworks: &includeReserved,
			},
		},
		Columns: []config.Column{{Name: "value", OutputPath: &config.Path{"value"}}},
	}
	writer, err := NewMMDBWriter(filepath.Join(t.TempDir(), "output.mmdb"), cfg, 4)
	require.NoError(t, err)
	data := []mmdbtype.DataType{mmdbtype.String("test")}

	for _, prefix := range []netip.Prefix{{}, netip.MustParsePrefix("200f::/120")} {
		err = writer.WriteRow(prefix, data)
		require.ErrorContains(t, err, "inserting ")
	}
	for _, ips := range [][2]netip.Addr{
		{{}, netip.MustParseAddr("1.2.3.4")},
		{netip.MustParseAddr("1.2.3.4"), {}},
		{netip.MustParseAddr("1.2.3.5"), netip.MustParseAddr("1.2.3.4")},
		{netip.MustParseAddr("1.2.3.4"), netip.MustParseAddr("200f::1")},
		{netip.MustParseAddr("200f::1"), netip.MustParseAddr("200f::2")},
	} {
		err = writer.WriteRange(ips[0], ips[1], data)
		require.ErrorContains(t, err, "inserting range ")
	}
}

func TestMergeNestedValue_EmptyPath(t *testing.T) {
	tests := []struct {
		name        string
		root        mmdbtype.Map
		value       mmdbtype.DataType
		expectErr   bool
		errContains string
		expected    mmdbtype.Map
	}{
		{
			name: "merge map into empty root",
			root: make(mmdbtype.Map),
			value: mmdbtype.Map{
				mmdbtype.String("key1"): mmdbtype.String("value1"),
				mmdbtype.String("key2"): mmdbtype.String("value2"),
			},
			expectErr: false,
			expected: mmdbtype.Map{
				mmdbtype.String("key1"): mmdbtype.String("value1"),
				mmdbtype.String("key2"): mmdbtype.String("value2"),
			},
		},
		{
			name: "merge map into root with existing non-conflicting data",
			root: mmdbtype.Map{
				mmdbtype.String("existing"): mmdbtype.String("data"),
			},
			value: mmdbtype.Map{
				mmdbtype.String("new"): mmdbtype.String("value"),
			},
			expectErr: false,
			expected: mmdbtype.Map{
				mmdbtype.String("existing"): mmdbtype.String("data"),
				mmdbtype.String("new"):      mmdbtype.String("value"),
			},
		},
		{
			name: "merge map with conflicting keys",
			root: mmdbtype.Map{
				mmdbtype.String("key1"): mmdbtype.String("original"),
			},
			value: mmdbtype.Map{
				mmdbtype.String("key1"): mmdbtype.String("conflict"),
			},
			expectErr:   true,
			errContains: "field conflict",
		},
		{
			name: "merge nested maps",
			root: mmdbtype.Map{
				mmdbtype.String("location"): mmdbtype.Map{
					mmdbtype.String("latitude"): mmdbtype.Float64(40.7128),
				},
			},
			value: mmdbtype.Map{
				mmdbtype.String("location"): mmdbtype.Map{
					mmdbtype.String("longitude"): mmdbtype.Float64(-74.0060),
				},
			},
			expectErr: false,
			expected: mmdbtype.Map{
				mmdbtype.String("location"): mmdbtype.Map{
					mmdbtype.String("latitude"):  mmdbtype.Float64(40.7128),
					mmdbtype.String("longitude"): mmdbtype.Float64(-74.0060),
				},
			},
		},
		{
			name:        "error on non-map value at empty path",
			root:        make(mmdbtype.Map),
			value:       mmdbtype.String("not a map"),
			expectErr:   true,
			errContains: "cannot set non-map value at root",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := mergeNestedValue(tt.root, []any{}, tt.value)

			if tt.expectErr {
				require.Error(t, err)
				if tt.errContains != "" {
					assert.Contains(t, err.Error(), tt.errContains)
				}
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestMergeNestedValue_MapMergingAtPath(t *testing.T) {
	tests := []struct {
		name        string
		root        mmdbtype.Map
		path        []any
		value       mmdbtype.DataType
		expectErr   bool
		errContains string
		expected    mmdbtype.Map
	}{
		{
			name: "set map at new path",
			root: make(mmdbtype.Map),
			path: []any{"traits"},
			value: mmdbtype.Map{
				mmdbtype.String("is_anonymous"): mmdbtype.Bool(true),
			},
			expectErr: false,
			expected: mmdbtype.Map{
				mmdbtype.String("traits"): mmdbtype.Map{
					mmdbtype.String("is_anonymous"): mmdbtype.Bool(true),
				},
			},
		},
		{
			name: "merge map at existing path",
			root: mmdbtype.Map{
				mmdbtype.String("traits"): mmdbtype.Map{
					mmdbtype.String("is_anonymous"): mmdbtype.Bool(true),
				},
			},
			path: []any{"traits"},
			value: mmdbtype.Map{
				mmdbtype.String("is_vpn"): mmdbtype.Bool(false),
			},
			expectErr: false,
			expected: mmdbtype.Map{
				mmdbtype.String("traits"): mmdbtype.Map{
					mmdbtype.String("is_anonymous"): mmdbtype.Bool(true),
					mmdbtype.String("is_vpn"):       mmdbtype.Bool(false),
				},
			},
		},
		{
			name: "error when merging map into non-map",
			root: mmdbtype.Map{
				mmdbtype.String("traits"): mmdbtype.String("not a map"),
			},
			path: []any{"traits"},
			value: mmdbtype.Map{
				mmdbtype.String("is_anonymous"): mmdbtype.Bool(true),
			},
			expectErr:   true,
			errContains: "cannot merge map into non-map",
		},
		{
			name: "merge map at nested path",
			root: mmdbtype.Map{
				mmdbtype.String("location"): mmdbtype.Map{
					mmdbtype.String("city"): mmdbtype.Map{
						mmdbtype.String("name"): mmdbtype.String("New York"),
					},
				},
			},
			path: []any{"location", "city"},
			value: mmdbtype.Map{
				mmdbtype.String("population"): mmdbtype.Uint32(8000000),
			},
			expectErr: false,
			expected: mmdbtype.Map{
				mmdbtype.String("location"): mmdbtype.Map{
					mmdbtype.String("city"): mmdbtype.Map{
						mmdbtype.String("name"):       mmdbtype.String("New York"),
						mmdbtype.String("population"): mmdbtype.Uint32(8000000),
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := mergeNestedValue(tt.root, tt.path, tt.value)

			if tt.expectErr {
				require.Error(t, err)
				if tt.errContains != "" {
					assert.Contains(t, err.Error(), tt.errContains)
				}
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestMergeMaps(t *testing.T) {
	tests := []struct {
		name        string
		dest        mmdbtype.Map
		source      mmdbtype.Map
		expectErr   bool
		errContains string
		expected    mmdbtype.Map
	}{
		{
			name: "merge into empty dest",
			dest: make(mmdbtype.Map),
			source: mmdbtype.Map{
				mmdbtype.String("key1"): mmdbtype.String("value1"),
			},
			expectErr: false,
			expected: mmdbtype.Map{
				mmdbtype.String("key1"): mmdbtype.String("value1"),
			},
		},
		{
			name: "merge non-overlapping keys",
			dest: mmdbtype.Map{
				mmdbtype.String("key1"): mmdbtype.String("value1"),
			},
			source: mmdbtype.Map{
				mmdbtype.String("key2"): mmdbtype.String("value2"),
			},
			expectErr: false,
			expected: mmdbtype.Map{
				mmdbtype.String("key1"): mmdbtype.String("value1"),
				mmdbtype.String("key2"): mmdbtype.String("value2"),
			},
		},
		{
			name: "recursive merge of nested maps",
			dest: mmdbtype.Map{
				mmdbtype.String("location"): mmdbtype.Map{
					mmdbtype.String("latitude"): mmdbtype.Float64(40.7128),
				},
			},
			source: mmdbtype.Map{
				mmdbtype.String("location"): mmdbtype.Map{
					mmdbtype.String("longitude"): mmdbtype.Float64(-74.0060),
				},
			},
			expectErr: false,
			expected: mmdbtype.Map{
				mmdbtype.String("location"): mmdbtype.Map{
					mmdbtype.String("latitude"):  mmdbtype.Float64(40.7128),
					mmdbtype.String("longitude"): mmdbtype.Float64(-74.0060),
				},
			},
		},
		{
			name: "conflict on scalar values",
			dest: mmdbtype.Map{
				mmdbtype.String("key1"): mmdbtype.String("original"),
			},
			source: mmdbtype.Map{
				mmdbtype.String("key1"): mmdbtype.String("conflict"),
			},
			expectErr:   true,
			errContains: "field conflict",
		},
		{
			name: "conflict when merging map into scalar",
			dest: mmdbtype.Map{
				mmdbtype.String("location"): mmdbtype.String("NYC"),
			},
			source: mmdbtype.Map{
				mmdbtype.String("location"): mmdbtype.Map{
					mmdbtype.String("latitude"): mmdbtype.Float64(40.7128),
				},
			},
			expectErr:   true,
			errContains: "field conflict",
		},
		{
			name: "deeply nested recursive merge",
			dest: mmdbtype.Map{
				mmdbtype.String("a"): mmdbtype.Map{
					mmdbtype.String("b"): mmdbtype.Map{
						mmdbtype.String("c"): mmdbtype.String("value1"),
					},
				},
			},
			source: mmdbtype.Map{
				mmdbtype.String("a"): mmdbtype.Map{
					mmdbtype.String("b"): mmdbtype.Map{
						mmdbtype.String("d"): mmdbtype.String("value2"),
					},
				},
			},
			expectErr: false,
			expected: mmdbtype.Map{
				mmdbtype.String("a"): mmdbtype.Map{
					mmdbtype.String("b"): mmdbtype.Map{
						mmdbtype.String("c"): mmdbtype.String("value1"),
						mmdbtype.String("d"): mmdbtype.String("value2"),
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := mergeMaps(tt.dest, tt.source)

			if tt.expectErr {
				require.Error(t, err)
				if tt.errContains != "" {
					assert.Contains(t, err.Error(), tt.errContains)
				}
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestBuildNestedData_EmptyPath(t *testing.T) {
	// Create a mock MMDB writer for testing
	cfg := &config.Config{
		Columns: []config.Column{
			{
				Name:       "enterprise_all",
				Database:   "enterprise",
				Path:       config.Path{},
				OutputPath: &config.Path{},
			},
		},
	}

	writer := &MMDBWriter{
		config: cfg,
	}

	// Data in column order: enterprise_all
	flatData := []mmdbtype.DataType{
		mmdbtype.Map{
			mmdbtype.String("country"): mmdbtype.Map{
				mmdbtype.String("iso_code"): mmdbtype.String("US"),
			},
			mmdbtype.String("city"): mmdbtype.Map{
				mmdbtype.String("name"): mmdbtype.String("New York"),
			},
		},
	}

	result, err := writer.buildNestedData(flatData)
	require.NoError(t, err)

	// The map should be merged into root
	expected := mmdbtype.Map{
		mmdbtype.String("country"): mmdbtype.Map{
			mmdbtype.String("iso_code"): mmdbtype.String("US"),
		},
		mmdbtype.String("city"): mmdbtype.Map{
			mmdbtype.String("name"): mmdbtype.String("New York"),
		},
	}

	assert.Equal(t, expected, result)
}

func TestMergeMaps_NoMutation(t *testing.T) {
	// Test that mergeMaps doesn't mutate the input maps
	original := mmdbtype.Map{
		mmdbtype.String("is_anonymous"): mmdbtype.Bool(true),
		mmdbtype.String("is_vpn"):       mmdbtype.Bool(false),
	}

	// Create a copy to track original state
	expectedOriginal := mmdbtype.Map{
		mmdbtype.String("is_anonymous"): mmdbtype.Bool(true),
		mmdbtype.String("is_vpn"):       mmdbtype.Bool(false),
	}

	dest1 := make(mmdbtype.Map)
	dest2 := make(mmdbtype.Map)
	source := original

	// Merge same source reference into two different destinations
	result1, err := mergeMaps(dest1, source)
	require.NoError(t, err)

	// Verify original source map was not mutated by first merge
	assert.Equal(t, expectedOriginal, original)

	// Second merge with same source reference should succeed because source wasn't mutated
	result2, err := mergeMaps(dest2, source)
	require.NoError(t, err)

	// Verify original source map still not mutated
	assert.Equal(t, expectedOriginal, original)

	// Both results should have the source data
	assert.NotNil(t, result1)
	assert.NotNil(t, result2)
	assert.Equal(t, result1, result2)
}

func TestMergeNestedValue_NoMutation(t *testing.T) {
	// Test that mergeNestedValue doesn't mutate the input map
	// This simulates the real-world scenario where MMDB reader returns same reference
	sharedMap := mmdbtype.Map{
		mmdbtype.String("is_anonymous"):     mmdbtype.Bool(true),
		mmdbtype.String("is_anonymous_vpn"): mmdbtype.Bool(false),
	}

	expectedShared := mmdbtype.Map{
		mmdbtype.String("is_anonymous"):     mmdbtype.Bool(true),
		mmdbtype.String("is_anonymous_vpn"): mmdbtype.Bool(false),
	}

	// First write
	root1 := make(mmdbtype.Map)
	result1, err := mergeNestedValue(root1, []any{"traits"}, sharedMap)
	require.NoError(t, err)
	assert.NotNil(t, result1)

	// Second write with SAME reference (simulates accumulator reuse)
	root2 := make(mmdbtype.Map)
	result2, err := mergeNestedValue(root2, []any{"traits"}, sharedMap)
	require.NoError(t, err)
	assert.NotNil(t, result2)

	// Verify original sharedMap was not mutated
	assert.Equal(t, expectedShared, sharedMap)

	// Both results should have the data at the correct path
	assert.Equal(t, expectedShared, result1[mmdbtype.String("traits")])
	assert.Equal(t, expectedShared, result2[mmdbtype.String("traits")])
}

func TestBuildNestedData_MapMergingAtPath(t *testing.T) {
	cfg := &config.Config{
		Columns: []config.Column{
			{
				Name:       "col1",
				Database:   "db1",
				Path:       config.Path{},
				OutputPath: &config.Path{"traits"},
			},
			{
				Name:       "col2",
				Database:   "db2",
				Path:       config.Path{},
				OutputPath: &config.Path{"traits"},
			},
		},
	}

	writer := &MMDBWriter{
		config: cfg,
	}

	// Data in column order: col1, col2
	flatData := []mmdbtype.DataType{
		mmdbtype.Map{
			mmdbtype.String("is_anonymous"): mmdbtype.Bool(true),
		},
		mmdbtype.Map{
			mmdbtype.String("is_vpn"): mmdbtype.Bool(false),
		},
	}

	result, err := writer.buildNestedData(flatData)
	require.NoError(t, err)

	// Both maps should be merged under "traits"
	expected := mmdbtype.Map{
		mmdbtype.String("traits"): mmdbtype.Map{
			mmdbtype.String("is_anonymous"): mmdbtype.Bool(true),
			mmdbtype.String("is_vpn"):       mmdbtype.Bool(false),
		},
	}

	assert.Equal(t, expected, result)
}
