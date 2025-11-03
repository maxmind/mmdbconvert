package merger

import (
	"maps"
	"net/netip"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockWriter captures written rows for testing.
type mockWriter struct {
	rows    []mockRow
	stopOn  *netip.Addr
	stopErr error
	found   bool
}

type mockRangeWriter struct {
	rows   []mockRow
	ranges []struct {
		start netip.Addr
		end   netip.Addr
		data  map[string]any
	}
	writeErr error
}

type mockRow struct {
	prefix netip.Prefix
	data   map[string]any
}

func (m *mockWriter) WriteRow(prefix netip.Prefix, data map[string]any) error {
	if m.stopOn != nil && prefix.Contains(*m.stopOn) {
		m.found = true
		if m.stopErr != nil {
			return m.stopErr
		}
	}
	// Deep copy data to avoid mutation issues
	dataCopy := maps.Clone(data)
	m.rows = append(m.rows, mockRow{prefix: prefix, data: dataCopy})
	return nil
}

func (m *mockRangeWriter) WriteRow(prefix netip.Prefix, data map[string]any) error {
	m.rows = append(m.rows, mockRow{prefix: prefix, data: maps.Clone(data)})
	return nil
}

func (m *mockRangeWriter) WriteRange(start, end netip.Addr, data map[string]any) error {
	m.ranges = append(m.ranges, struct {
		start netip.Addr
		end   netip.Addr
		data  map[string]any
	}{
		start: start,
		end:   end,
		data:  maps.Clone(data),
	})
	return m.writeErr
}

func TestAccumulator_SingleNetwork(t *testing.T) {
	writer := &mockWriter{}
	acc := NewAccumulator(writer, true)

	// Process single network
	prefix := netip.MustParsePrefix("10.0.0.0/24")
	data := map[string]any{"country": "US"}

	err := acc.Process(prefix, data)
	require.NoError(t, err)

	// Flush
	err = acc.Flush()
	require.NoError(t, err)

	// Verify output
	require.Len(t, writer.rows, 1)
	assert.Equal(t, prefix, writer.rows[0].prefix)
	assert.Equal(t, data, writer.rows[0].data)
}

func TestAccumulator_AdjacentNetworksWithSameData(t *testing.T) {
	writer := &mockWriter{}
	acc := NewAccumulator(writer, true)

	// Process two adjacent /25 networks with same data
	data := map[string]any{"country": "US"}

	err := acc.Process(netip.MustParsePrefix("10.0.0.0/25"), data)
	require.NoError(t, err)

	err = acc.Process(netip.MustParsePrefix("10.0.0.128/25"), data)
	require.NoError(t, err)

	// Flush
	err = acc.Flush()
	require.NoError(t, err)

	// Should merge into single /24
	require.Len(t, writer.rows, 1)
	assert.Equal(t, netip.MustParsePrefix("10.0.0.0/24"), writer.rows[0].prefix)
	assert.Equal(t, data, writer.rows[0].data)
}

func TestAccumulator_AdjacentNetworksWithDifferentData(t *testing.T) {
	writer := &mockWriter{}
	acc := NewAccumulator(writer, true)

	// Process two adjacent networks with different data
	err := acc.Process(
		netip.MustParsePrefix("10.0.0.0/25"),
		map[string]any{"country": "US"},
	)
	require.NoError(t, err)

	err = acc.Process(
		netip.MustParsePrefix("10.0.0.128/25"),
		map[string]any{"country": "CA"},
	)
	require.NoError(t, err)

	// Flush
	err = acc.Flush()
	require.NoError(t, err)

	// Should NOT merge - different data
	require.Len(t, writer.rows, 2)
	assert.Equal(t, netip.MustParsePrefix("10.0.0.0/25"), writer.rows[0].prefix)
	assert.Equal(t, map[string]any{"country": "US"}, writer.rows[0].data)
	assert.Equal(t, netip.MustParsePrefix("10.0.0.128/25"), writer.rows[1].prefix)
	assert.Equal(t, map[string]any{"country": "CA"}, writer.rows[1].data)
}

func TestAccumulator_NonAdjacentNetworks(t *testing.T) {
	writer := &mockWriter{}
	acc := NewAccumulator(writer, true)

	// Process two non-adjacent networks with same data
	data := map[string]any{"country": "US"}

	err := acc.Process(netip.MustParsePrefix("10.0.0.0/24"), data)
	require.NoError(t, err)

	// Gap: 10.0.1.0/24 is missing
	err = acc.Process(netip.MustParsePrefix("10.0.2.0/24"), data)
	require.NoError(t, err)

	// Flush
	err = acc.Flush()
	require.NoError(t, err)

	// Should NOT merge - not adjacent
	require.Len(t, writer.rows, 2)
	assert.Equal(t, netip.MustParsePrefix("10.0.0.0/24"), writer.rows[0].prefix)
	assert.Equal(t, netip.MustParsePrefix("10.0.2.0/24"), writer.rows[1].prefix)
}

func TestAccumulator_MultipleAdjacentMerges(t *testing.T) {
	writer := &mockWriter{}
	acc := NewAccumulator(writer, true)

	// Process four adjacent /26 networks with same data
	data := map[string]any{"country": "US"}

	err := acc.Process(netip.MustParsePrefix("10.0.0.0/26"), data)
	require.NoError(t, err)

	err = acc.Process(netip.MustParsePrefix("10.0.0.64/26"), data)
	require.NoError(t, err)

	err = acc.Process(netip.MustParsePrefix("10.0.0.128/26"), data)
	require.NoError(t, err)

	err = acc.Process(netip.MustParsePrefix("10.0.0.192/26"), data)
	require.NoError(t, err)

	// Flush
	err = acc.Flush()
	require.NoError(t, err)

	// Should merge into single /24
	require.Len(t, writer.rows, 1)
	assert.Equal(t, netip.MustParsePrefix("10.0.0.0/24"), writer.rows[0].prefix)
}

func TestAccumulator_UnalignedMerge(t *testing.T) {
	writer := &mockWriter{}
	acc := NewAccumulator(writer, true)

	// Process networks that merge into an unaligned range
	data := map[string]any{"country": "US"}

	// 10.0.0.1/32 through 10.0.0.6/32
	err := acc.Process(netip.MustParsePrefix("10.0.0.1/32"), data)
	require.NoError(t, err)

	err = acc.Process(netip.MustParsePrefix("10.0.0.2/32"), data)
	require.NoError(t, err)

	err = acc.Process(netip.MustParsePrefix("10.0.0.3/32"), data)
	require.NoError(t, err)

	err = acc.Process(netip.MustParsePrefix("10.0.0.4/32"), data)
	require.NoError(t, err)

	err = acc.Process(netip.MustParsePrefix("10.0.0.5/32"), data)
	require.NoError(t, err)

	err = acc.Process(netip.MustParsePrefix("10.0.0.6/32"), data)
	require.NoError(t, err)

	// Flush
	err = acc.Flush()
	require.NoError(t, err)

	// Should produce multiple CIDRs for the unaligned range
	// RangeToCIDRs(10.0.0.1, 10.0.0.6) produces:
	// 10.0.0.1/32, 10.0.0.2/31, 10.0.0.4/31, 10.0.0.6/32
	require.Len(t, writer.rows, 4)
	assert.Equal(t, netip.MustParsePrefix("10.0.0.1/32"), writer.rows[0].prefix)
	assert.Equal(t, netip.MustParsePrefix("10.0.0.2/31"), writer.rows[1].prefix)
	assert.Equal(t, netip.MustParsePrefix("10.0.0.4/31"), writer.rows[2].prefix)
	assert.Equal(t, netip.MustParsePrefix("10.0.0.6/32"), writer.rows[3].prefix)
}

func TestAccumulator_RangeWriterReceivesSingleRange(t *testing.T) {
	writer := &mockRangeWriter{}
	acc := NewAccumulator(writer, true)

	data := map[string]any{"country": "CN"}

	require.NoError(t, acc.Process(netip.MustParsePrefix("1.0.1.0/24"), data))
	require.NoError(t, acc.Process(netip.MustParsePrefix("1.0.2.0/23"), data))
	require.NoError(t, acc.Flush())

	require.Len(t, writer.ranges, 1)
	assert.Equal(t, netip.MustParseAddr("1.0.1.0"), writer.ranges[0].start)
	assert.Equal(t, netip.MustParseAddr("1.0.3.255"), writer.ranges[0].end)
	assert.Equal(t, data, writer.ranges[0].data)
	require.Empty(t, writer.rows, "should not fall back to prefix rows")
}

func TestAccumulator_IPv6AdjacentMerging(t *testing.T) {
	writer := &mockWriter{}
	acc := NewAccumulator(writer, true)

	data := map[string]any{"continent": "NA"}

	err := acc.Process(netip.MustParsePrefix("2001:db8::/127"), data)
	require.NoError(t, err)
	err = acc.Process(netip.MustParsePrefix("2001:db8::2/127"), data)
	require.NoError(t, err)

	require.NoError(t, acc.Flush())

	require.Len(t, writer.rows, 1)
	assert.Equal(t, netip.MustParsePrefix("2001:db8::/126"), writer.rows[0].prefix)
}

func TestAccumulator_IPv6UnalignedRange(t *testing.T) {
	writer := &mockWriter{}
	acc := NewAccumulator(writer, true)

	data := map[string]any{"continent": "EU"}

	addresses := []string{"2001:db8::1/128", "2001:db8::2/128", "2001:db8::3/128"}
	for _, cidr := range addresses {
		require.NoError(t, acc.Process(netip.MustParsePrefix(cidr), data))
	}

	require.NoError(t, acc.Flush())

	require.Len(t, writer.rows, 2)
	assert.Equal(t, netip.MustParsePrefix("2001:db8::1/128"), writer.rows[0].prefix)
	assert.Equal(t, netip.MustParsePrefix("2001:db8::2/127"), writer.rows[1].prefix)
}

func TestAccumulator_EmptyFlush(t *testing.T) {
	writer := &mockWriter{}
	acc := NewAccumulator(writer, true)

	// Flush without processing anything
	err := acc.Flush()
	require.NoError(t, err)

	// Should write nothing
	assert.Empty(t, writer.rows)
}

func TestAccumulator_MultipleFlushes(t *testing.T) {
	writer := &mockWriter{}
	acc := NewAccumulator(writer, true)

	// First batch
	err := acc.Process(netip.MustParsePrefix("10.0.0.0/24"), map[string]any{"country": "US"})
	require.NoError(t, err)

	err = acc.Flush()
	require.NoError(t, err)

	// Second batch
	err = acc.Process(netip.MustParsePrefix("10.0.1.0/24"), map[string]any{"country": "CA"})
	require.NoError(t, err)

	err = acc.Flush()
	require.NoError(t, err)

	// Should have two rows
	require.Len(t, writer.rows, 2)
	assert.Equal(t, map[string]any{"country": "US"}, writer.rows[0].data)
	assert.Equal(t, map[string]any{"country": "CA"}, writer.rows[1].data)
}

func TestAccumulator_IPv6(t *testing.T) {
	writer := &mockWriter{}
	acc := NewAccumulator(writer, true)

	// Process adjacent IPv6 networks
	data := map[string]any{"country": "US"}

	err := acc.Process(netip.MustParsePrefix("2001:db8::0/127"), data)
	require.NoError(t, err)

	err = acc.Process(netip.MustParsePrefix("2001:db8::2/127"), data)
	require.NoError(t, err)

	// Flush
	err = acc.Flush()
	require.NoError(t, err)

	// Should merge into /126
	require.Len(t, writer.rows, 1)
	assert.Equal(t, netip.MustParsePrefix("2001:db8::/126"), writer.rows[0].prefix)
}

func TestAccumulator_SkipEmptyRows(t *testing.T) {
	writer := &mockWriter{}
	acc := NewAccumulator(writer, false) // Don't include empty rows

	// Process a network with data
	err := acc.Process(
		netip.MustParsePrefix("10.0.0.0/24"),
		map[string]any{"country": "US"},
	)
	require.NoError(t, err)

	// Process a network with empty data (should be skipped)
	err = acc.Process(
		netip.MustParsePrefix("10.0.1.0/24"),
		map[string]any{},
	)
	require.NoError(t, err)

	// Process another network with data
	err = acc.Process(
		netip.MustParsePrefix("10.0.2.0/24"),
		map[string]any{"country": "CA"},
	)
	require.NoError(t, err)

	// Flush
	err = acc.Flush()
	require.NoError(t, err)

	// Should only have 2 rows (the one with empty data was skipped)
	require.Len(t, writer.rows, 2)
	assert.Equal(t, netip.MustParsePrefix("10.0.0.0/24"), writer.rows[0].prefix)
	assert.Equal(t, map[string]any{"country": "US"}, writer.rows[0].data)
	assert.Equal(t, netip.MustParsePrefix("10.0.2.0/24"), writer.rows[1].prefix)
	assert.Equal(t, map[string]any{"country": "CA"}, writer.rows[1].data)
}

func TestAccumulator_IncludeEmptyRows(t *testing.T) {
	writer := &mockWriter{}
	acc := NewAccumulator(writer, true) // Include empty rows

	// Process a network with data
	err := acc.Process(
		netip.MustParsePrefix("10.0.0.0/24"),
		map[string]any{"country": "US"},
	)
	require.NoError(t, err)

	// Process a network with empty data (should be included)
	err = acc.Process(
		netip.MustParsePrefix("10.0.1.0/24"),
		map[string]any{},
	)
	require.NoError(t, err)

	// Process another network with data
	err = acc.Process(
		netip.MustParsePrefix("10.0.2.0/24"),
		map[string]any{"country": "CA"},
	)
	require.NoError(t, err)

	// Flush
	err = acc.Flush()
	require.NoError(t, err)

	// Should have all 3 rows (including the one with empty data)
	require.Len(t, writer.rows, 3)
	assert.Equal(t, netip.MustParsePrefix("10.0.0.0/24"), writer.rows[0].prefix)
	assert.Equal(t, map[string]any{"country": "US"}, writer.rows[0].data)
	assert.Equal(t, netip.MustParsePrefix("10.0.1.0/24"), writer.rows[1].prefix)
	assert.Equal(t, map[string]any{}, writer.rows[1].data)
	assert.Equal(t, netip.MustParsePrefix("10.0.2.0/24"), writer.rows[2].prefix)
	assert.Equal(t, map[string]any{"country": "CA"}, writer.rows[2].data)
}

func TestAccumulator_SkipEmptyDoesNotAffectMerging(t *testing.T) {
	writer := &mockWriter{}
	acc := NewAccumulator(writer, false) // Don't include empty rows

	// Process three adjacent networks with empty data
	// These would normally merge, but since they're all empty, they should all be skipped
	err := acc.Process(netip.MustParsePrefix("10.0.0.0/26"), map[string]any{})
	require.NoError(t, err)

	err = acc.Process(netip.MustParsePrefix("10.0.0.64/26"), map[string]any{})
	require.NoError(t, err)

	err = acc.Process(netip.MustParsePrefix("10.0.0.128/26"), map[string]any{})
	require.NoError(t, err)

	// Flush
	err = acc.Flush()
	require.NoError(t, err)

	// Should have no rows (all were empty and skipped)
	assert.Empty(t, writer.rows)
}

func TestDataEquals(t *testing.T) {
	tests := []struct {
		name     string
		a        map[string]any
		b        map[string]any
		expected bool
	}{
		{
			name:     "empty maps",
			a:        map[string]any{},
			b:        map[string]any{},
			expected: true,
		},
		{
			name:     "identical maps",
			a:        map[string]any{"country": "US", "city": "NYC"},
			b:        map[string]any{"country": "US", "city": "NYC"},
			expected: true,
		},
		{
			name:     "different values",
			a:        map[string]any{"country": "US"},
			b:        map[string]any{"country": "CA"},
			expected: false,
		},
		{
			name:     "different keys",
			a:        map[string]any{"country": "US"},
			b:        map[string]any{"region": "US"},
			expected: false,
		},
		{
			name:     "different lengths",
			a:        map[string]any{"country": "US"},
			b:        map[string]any{"country": "US", "city": "NYC"},
			expected: false,
		},
		{
			name:     "nil values",
			a:        map[string]any{"country": nil},
			b:        map[string]any{"country": nil},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := dataEquals(tt.a, tt.b)
			assert.Equal(t, tt.expected, result)
		})
	}
}
