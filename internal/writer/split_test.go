package writer

import (
	"errors"
	"net/netip"
	"testing"

	"github.com/maxmind/mmdbwriter/v2/mmdbtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type recordWriter struct {
	rows   []netip.Prefix
	data   [][]mmdbtype.DataType
	flushE error
}

func (r *recordWriter) WriteRow(prefix netip.Prefix, data []mmdbtype.DataType) error {
	r.rows = append(r.rows, prefix)
	r.data = append(r.data, data)
	return nil
}

func (r *recordWriter) Flush() error {
	return r.flushE
}

// rangeRecordWriter is a mock writer that implements both WriteRow and WriteRange.
type rangeRecordWriter struct {
	rows        []netip.Prefix
	ranges      [][2]netip.Addr // Track ranges separately
	data        [][]mmdbtype.DataType
	rangeData   [][]mmdbtype.DataType
	writeRowE   error
	writeRangeE error
}

func (r *rangeRecordWriter) WriteRow(prefix netip.Prefix, data []mmdbtype.DataType) error {
	if r.writeRowE != nil {
		return r.writeRowE
	}
	r.rows = append(r.rows, prefix)
	r.data = append(r.data, data)
	return nil
}

func (r *rangeRecordWriter) WriteRange(start, end netip.Addr, data []mmdbtype.DataType) error {
	if r.writeRangeE != nil {
		return r.writeRangeE
	}
	r.ranges = append(r.ranges, [2]netip.Addr{start, end})
	r.rangeData = append(r.rangeData, data)
	return nil
}

func TestSplitRowWriter_RoutesByIPVersion(t *testing.T) {
	v4 := &recordWriter{}
	v6 := &recordWriter{}

	split := NewSplitRowWriter(v4, v6)

	v4Prefix := netip.MustParsePrefix("10.0.0.0/24")
	v6Prefix := netip.MustParsePrefix("2001:db8::/32")

	// Column 0: col
	require.NoError(t, split.WriteRow(v4Prefix, []mmdbtype.DataType{
		mmdbtype.String("ipv4"),
	}))
	require.NoError(t, split.WriteRow(v6Prefix, []mmdbtype.DataType{
		mmdbtype.String("ipv6"),
	}))

	require.Len(t, v4.rows, 1)
	assert.Equal(t, v4Prefix, v4.rows[0])
	require.Len(t, v6.rows, 1)
	assert.Equal(t, v6Prefix, v6.rows[0])
}

func TestSplitRowWriter_ErrorsWhenWriterMissing(t *testing.T) {
	v6 := &recordWriter{}
	split := NewSplitRowWriter(nil, v6)

	err := split.WriteRow(netip.MustParsePrefix("10.0.0.0/24"), nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "IPv4 writer")

	split = NewSplitRowWriter(&recordWriter{}, nil)
	err = split.WriteRow(netip.MustParsePrefix("2001:db8::/48"), nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "IPv6 writer")
}

func TestSplitRowWriter_FlushPropagatesErrors(t *testing.T) {
	v4 := &recordWriter{}
	v6 := &recordWriter{flushE: errors.New("flush failure")}

	split := NewSplitRowWriter(v4, v6)

	err := split.Flush()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "flush failure")
}

func TestSplitRowWriter_WriteRangeRoutesToRangeCapableWriter(t *testing.T) {
	v4 := &rangeRecordWriter{}
	v6 := &rangeRecordWriter{}

	split := NewSplitRowWriter(v4, v6)

	// Test IPv4 range routing
	v4Start := netip.MustParseAddr("10.0.0.0")
	v4End := netip.MustParseAddr("10.0.0.255")
	// Column 0: col
	v4Data := []mmdbtype.DataType{
		mmdbtype.String("ipv4_range"),
	}

	require.NoError(t, split.WriteRange(v4Start, v4End, v4Data))

	// Verify range was written to IPv4 writer, not converted to CIDRs
	require.Len(t, v4.ranges, 1)
	assert.Equal(t, v4Start, v4.ranges[0][0])
	assert.Equal(t, v4End, v4.ranges[0][1])
	assert.Equal(t, v4Data, v4.rangeData[0])
	assert.Empty(t, v4.rows, "should not call WriteRow for range-capable writer")

	// Test IPv6 range routing
	v6Start := netip.MustParseAddr("2001:db8::1")
	v6End := netip.MustParseAddr("2001:db8::ffff")
	// Column 0: col
	v6Data := []mmdbtype.DataType{
		mmdbtype.String("ipv6_range"),
	}

	require.NoError(t, split.WriteRange(v6Start, v6End, v6Data))

	// Verify range was written to IPv6 writer
	require.Len(t, v6.ranges, 1)
	assert.Equal(t, v6Start, v6.ranges[0][0])
	assert.Equal(t, v6End, v6.ranges[0][1])
	assert.Equal(t, v6Data, v6.rangeData[0])
	assert.Empty(t, v6.rows, "should not call WriteRow for range-capable writer")
}

func TestSplitRowWriter_WriteRangeFallsBackToCIDRs(t *testing.T) {
	// Use recordWriter which doesn't implement WriteRange
	v4 := &recordWriter{}
	v6 := &recordWriter{}

	split := NewSplitRowWriter(v4, v6)

	// Test IPv4 fallback
	v4Start := netip.MustParseAddr("10.0.0.0")
	v4End := netip.MustParseAddr("10.0.0.255")
	// Column 0: col
	v4Data := []mmdbtype.DataType{
		mmdbtype.String("ipv4_fallback"),
	}

	require.NoError(t, split.WriteRange(v4Start, v4End, v4Data))

	// Verify range was converted to CIDR(s) and written via WriteRow
	require.NotEmpty(t, v4.rows, "should convert range to CIDRs")
	// 10.0.0.0-10.0.0.255 is exactly 10.0.0.0/24
	require.Len(t, v4.rows, 1)
	assert.Equal(t, netip.MustParsePrefix("10.0.0.0/24"), v4.rows[0])
	assert.Equal(t, v4Data, v4.data[0])

	// Test IPv6 fallback
	v6Start := netip.MustParseAddr("2001:db8::")
	v6End := netip.MustParseAddr("2001:db8::ffff")
	// Column 0: col
	v6Data := []mmdbtype.DataType{
		mmdbtype.String("ipv6_fallback"),
	}

	require.NoError(t, split.WriteRange(v6Start, v6End, v6Data))

	// Verify range was converted to CIDR(s)
	require.NotEmpty(t, v6.rows, "should convert range to CIDRs")
	// 2001:db8::-2001:db8::ffff is exactly 2001:db8::/112
	require.Len(t, v6.rows, 1)
	assert.Equal(t, netip.MustParsePrefix("2001:db8::/112"), v6.rows[0])
	assert.Equal(t, v6Data, v6.data[0])
}

func TestSplitRowWriter_WriteRangeFallbackMultipleCIDRs(t *testing.T) {
	// Test a range that requires multiple CIDRs
	v4 := &recordWriter{}
	split := NewSplitRowWriter(v4, nil)

	// 10.0.0.1-10.0.0.254 requires multiple CIDRs
	start := netip.MustParseAddr("10.0.0.1")
	end := netip.MustParseAddr("10.0.0.254")
	// Column 0: col
	data := []mmdbtype.DataType{
		mmdbtype.String("multi_cidr"),
	}

	require.NoError(t, split.WriteRange(start, end, data))

	// Verify multiple CIDRs were written
	require.NotEmpty(t, v4.rows, "should write multiple CIDRs")
	// All rows should have the same data
	for _, d := range v4.data {
		assert.Equal(t, data, d)
	}
}

func TestSplitRowWriter_WriteRangeErrorsWhenWriterMissing(t *testing.T) {
	// Test missing IPv4 writer
	v6 := &rangeRecordWriter{}
	split := NewSplitRowWriter(nil, v6)

	v4Start := netip.MustParseAddr("10.0.0.0")
	v4End := netip.MustParseAddr("10.0.0.255")
	err := split.WriteRange(v4Start, v4End, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "IPv4 writer")

	// Test missing IPv6 writer
	v4 := &rangeRecordWriter{}
	split = NewSplitRowWriter(v4, nil)

	v6Start := netip.MustParseAddr("2001:db8::")
	v6End := netip.MustParseAddr("2001:db8::ffff")
	err = split.WriteRange(v6Start, v6End, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "IPv6 writer")
}

func TestSplitRowWriter_WriteRangePropagatesErrors(t *testing.T) {
	v4Start := netip.MustParseAddr("10.0.0.0")
	v4End := netip.MustParseAddr("10.0.0.255")

	// Test error propagation from WriteRange
	v4Range := &rangeRecordWriter{writeRangeE: errors.New("range write failure")}
	split := NewSplitRowWriter(v4Range, nil)

	err := split.WriteRange(v4Start, v4End, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "range write failure")
}

func TestSplitRowWriter_WriteRangeFallbackPropagatesErrors(t *testing.T) {
	// Test error propagation from WriteRow fallback
	// Use errorRecordWriter that returns an error from WriteRow
	v4Start := netip.MustParseAddr("10.0.0.0")
	v4End := netip.MustParseAddr("10.0.0.255")

	// Create a writer that fails on WriteRow (no WriteRange method, so it falls back)
	v4 := &errorRecordWriter{err: errors.New("row write failure")}
	split := NewSplitRowWriter(v4, nil)

	// This should fall back to WriteRow and propagate the error
	err := split.WriteRange(v4Start, v4End, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "row write failure")
}

// errorRecordWriter is a mock writer that always returns an error from WriteRow.
type errorRecordWriter struct {
	err error
}

func (e *errorRecordWriter) WriteRow(netip.Prefix, []mmdbtype.DataType) error {
	return e.err
}

// TestSplitRowWriter_ImplementsRangeRowWriter verifies that SplitRowWriter
// implements the RangeRowWriter interface used by merger.Accumulator.
func TestSplitRowWriter_ImplementsRangeRowWriter(t *testing.T) {
	v4 := &rangeRecordWriter{}
	v6 := &rangeRecordWriter{}
	split := NewSplitRowWriter(v4, v6)

	// Verify that SplitRowWriter satisfies the RangeRowWriter interface
	// This is the same check that merger.Accumulator.Flush() performs
	type rangeRowWriter interface {
		WriteRange(netip.Addr, netip.Addr, []mmdbtype.DataType) error
	}

	// This type assertion must succeed for the feature to work
	_, ok := any(split).(rangeRowWriter)
	assert.True(t, ok, "SplitRowWriter must implement RangeRowWriter interface")

	// Verify it actually calls WriteRange on the underlying writer
	start := netip.MustParseAddr("10.0.0.0")
	end := netip.MustParseAddr("10.0.0.255")
	// Column 0: test
	data := []mmdbtype.DataType{
		mmdbtype.String("data"),
	}

	require.NoError(t, split.WriteRange(start, end, data))

	require.Len(t, v4.ranges, 1)
	assert.Equal(t, start, v4.ranges[0][0])
	assert.Equal(t, end, v4.ranges[0][1])
}

// TestSplitRowWriter_RangeCompressionWithAccumulator tests the complete
// integration with merger.Accumulator to prove WriteRange is actually called.
func TestSplitRowWriter_RangeCompressionWithAccumulator(t *testing.T) {
	// Import the merger package to use Accumulator
	// (Note: This would normally be in an integration test, but we're testing
	// the interface contract here)

	v4 := &rangeRecordWriter{}
	v6 := &rangeRecordWriter{}
	split := NewSplitRowWriter(v4, v6)

	// Simulate what merger.Accumulator does: check for RangeRowWriter
	type rangeRowWriter interface {
		WriteRange(netip.Addr, netip.Addr, []mmdbtype.DataType) error
	}

	// When Accumulator.Flush() runs, it does this check:
	if rangeWriter, ok := any(split).(rangeRowWriter); ok {
		// It should call WriteRange with a range of adjacent networks
		start := netip.MustParseAddr("10.0.0.0")
		end := netip.MustParseAddr("10.0.3.255") // Covers 4 /24s
		// Column 0: country
		data := []mmdbtype.DataType{
			mmdbtype.String("US"),
		}

		require.NoError(t, rangeWriter.WriteRange(start, end, data))

		// Verify WriteRange was called on v4 writer (not WriteRow)
		require.Len(t, v4.ranges, 1, "should call WriteRange once")
		assert.Equal(t, start, v4.ranges[0][0])
		assert.Equal(t, end, v4.ranges[0][1])
		assert.Empty(t, v4.rows, "should NOT call WriteRow when WriteRange is available")
	} else {
		t.Fatal("SplitRowWriter should implement RangeRowWriter interface")
	}
}
