package writer

import (
	"bytes"
	"encoding/csv"
	"math/big"
	"net/netip"
	"strconv"
	"strings"
	"testing"

	"github.com/maxmind/mmdbwriter/mmdbtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/maxmind/mmdbconvert/internal/config"
	"github.com/maxmind/mmdbconvert/internal/network"
)

func TestCSVWriter_SingleRow(t *testing.T) {
	buf := &bytes.Buffer{}

	cfg := &config.Config{
		Output: config.OutputConfig{
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
			{Name: "country", Type: "string"},
			{Name: "city", Type: "string"},
		},
	}

	writer := NewCSVWriter(buf, cfg)

	prefix := netip.MustParsePrefix("10.0.0.0/24")
	// Data in column order: country, city
	data := []mmdbtype.DataType{
		mmdbtype.String("US"),
		mmdbtype.String("New York"),
	}

	err := writer.WriteRow(prefix, data)
	require.NoError(t, err)

	err = writer.Flush()
	require.NoError(t, err)

	expected := "network,country,city\n10.0.0.0/24,US,New York\n"
	assert.Equal(t, expected, buf.String())
}

func TestCSVWriter_MultipleRows(t *testing.T) {
	buf := &bytes.Buffer{}

	cfg := &config.Config{
		Output: config.OutputConfig{
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
			{Name: "value", Type: "string"},
		},
	}

	writer := NewCSVWriter(buf, cfg)

	rows := []struct {
		prefix netip.Prefix
		value  string
	}{
		{netip.MustParsePrefix("10.0.0.0/24"), "first"},
		{netip.MustParsePrefix("10.0.1.0/24"), "second"},
		{netip.MustParsePrefix("10.0.2.0/24"), "third"},
	}

	for _, row := range rows {
		// Data in column order: value
		err := writer.WriteRow(row.prefix, []mmdbtype.DataType{
			mmdbtype.String(row.value),
		})
		require.NoError(t, err)
	}

	err := writer.Flush()
	require.NoError(t, err)

	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	assert.Len(t, lines, 4) // header + 3 rows
	assert.Equal(t, "network,value", lines[0])
	assert.Equal(t, "10.0.0.0/24,first", lines[1])
	assert.Equal(t, "10.0.1.0/24,second", lines[2])
	assert.Equal(t, "10.0.2.0/24,third", lines[3])
}

func TestCSVWriter_NetworkColumns(t *testing.T) {
	tests := []struct {
		name     string
		columns  []config.NetworkColumn
		prefix   string
		expected []string // expected column values (excluding header)
	}{
		{
			name: "cidr only",
			columns: []config.NetworkColumn{
				{Name: "network", Type: "cidr"},
			},
			prefix:   "192.168.1.0/24",
			expected: []string{"192.168.1.0/24"},
		},
		{
			name: "start and end IP",
			columns: []config.NetworkColumn{
				{Name: "start_ip", Type: "start_ip"},
				{Name: "end_ip", Type: "end_ip"},
			},
			prefix:   "192.168.1.0/24",
			expected: []string{"192.168.1.0", "192.168.1.255"},
		},
		{
			name: "start and end integers (IPv4)",
			columns: []config.NetworkColumn{
				{Name: "start_int", Type: "start_int"},
				{Name: "end_int", Type: "end_int"},
			},
			prefix:   "192.168.1.0/24",
			expected: []string{"3232235776", "3232236031"},
		},
		{
			name: "IPv6 start/end integers",
			columns: []config.NetworkColumn{
				{Name: "start_int", Type: "start_int"},
				{Name: "end_int", Type: "end_int"},
			},
			prefix: "2001:db8::/126",
			expected: []string{
				"42540766411282592856903984951653826560",
				"42540766411282592856903984951653826563",
			},
		},
		{
			name: "all network column types",
			columns: []config.NetworkColumn{
				{Name: "network", Type: "cidr"},
				{Name: "start_ip", Type: "start_ip"},
				{Name: "end_ip", Type: "end_ip"},
				{Name: "start_int", Type: "start_int"},
				{Name: "end_int", Type: "end_int"},
			},
			prefix:   "10.0.0.0/24",
			expected: []string{"10.0.0.0/24", "10.0.0.0", "10.0.0.255", "167772160", "167772415"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := &bytes.Buffer{}

			cfg := &config.Config{
				Output: config.OutputConfig{
					CSV: config.CSVConfig{Delimiter: ","},
				},
				Network: config.NetworkConfig{
					Columns: tt.columns,
				},
				Columns: []config.Column{},
			}

			writer := NewCSVWriter(buf, cfg)
			prefix := netip.MustParsePrefix(tt.prefix)

			// No data columns configured
			err := writer.WriteRow(prefix, []mmdbtype.DataType{})
			require.NoError(t, err)

			err = writer.Flush()
			require.NoError(t, err)

			lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
			require.Len(t, lines, 2) // header + 1 row

			values := strings.Split(lines[1], ",")
			assert.Equal(t, tt.expected, values)
		})
	}
}

func TestCSVWriter_WriteRange(t *testing.T) {
	buf := &bytes.Buffer{}

	cfg := &config.Config{
		Output: config.OutputConfig{
			CSV: config.CSVConfig{
				Delimiter: ",",
			},
		},
		Network: config.NetworkConfig{
			Columns: []config.NetworkColumn{
				{Name: "start_ip", Type: "start_ip"},
				{Name: "end_ip", Type: "end_ip"},
			},
		},
		Columns: []config.Column{
			{Name: "country", Type: "string"},
		},
	}

	writer := NewCSVWriter(buf, cfg)
	start := netip.MustParseAddr("1.0.1.0")
	end := netip.MustParseAddr("1.0.3.255")

	// Data in column order: country
	err := writer.WriteRange(start, end, []mmdbtype.DataType{
		mmdbtype.String("CN"),
	})
	require.NoError(t, err)
	require.NoError(t, writer.Flush())

	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	require.Len(t, lines, 2)
	assert.Equal(t, "start_ip,end_ip,country", lines[0])
	assert.Equal(t, "1.0.1.0,1.0.3.255,CN", lines[1])
}

func TestCSVWriter_IPv6(t *testing.T) {
	buf := &bytes.Buffer{}

	cfg := &config.Config{
		Output: config.OutputConfig{
			CSV: config.CSVConfig{Delimiter: ","},
		},
		Network: config.NetworkConfig{
			Columns: []config.NetworkColumn{
				{Name: "network", Type: "cidr"},
				{Name: "start_ip", Type: "start_ip"},
				{Name: "end_ip", Type: "end_ip"},
			},
		},
		Columns: []config.Column{},
	}

	writer := NewCSVWriter(buf, cfg)
	prefix := netip.MustParsePrefix("2001:db8::/32")

	// No data columns configured
	err := writer.WriteRow(prefix, []mmdbtype.DataType{})
	require.NoError(t, err)

	err = writer.Flush()
	require.NoError(t, err)

	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	require.Len(t, lines, 2)

	values := strings.Split(lines[1], ",")
	assert.Equal(t, "2001:db8::/32", values[0])
	assert.Equal(t, "2001:db8::", values[1])
	assert.Equal(t, "2001:db8:ffff:ffff:ffff:ffff:ffff:ffff", values[2])
}

func TestCSVWriter_DisableHeader(t *testing.T) {
	buf := &bytes.Buffer{}
	f := false
	cfg := &config.Config{
		Output: config.OutputConfig{
			CSV: config.CSVConfig{
				Delimiter:     ",",
				IncludeHeader: &f,
			},
		},
		Network: config.NetworkConfig{
			Columns: []config.NetworkColumn{
				{Name: "network", Type: "cidr"},
			},
		},
		Columns: []config.Column{{Name: "value", Type: "string"}},
	}

	writer := NewCSVWriter(buf, cfg)
	// Data in column order: value
	require.NoError(
		t,
		writer.WriteRow(
			netip.MustParsePrefix("10.0.0.0/24"),
			[]mmdbtype.DataType{
				mmdbtype.String("row"),
			},
		),
	)
	require.NoError(t, writer.Flush())

	output := strings.TrimSpace(buf.String())
	assert.Equal(t, "10.0.0.0/24,row", output)
}

func TestCSVWriter_DataTypes(t *testing.T) {
	buf := &bytes.Buffer{}

	cfg := &config.Config{
		Output: config.OutputConfig{
			CSV: config.CSVConfig{Delimiter: ","},
		},
		Network: config.NetworkConfig{
			Columns: []config.NetworkColumn{
				{Name: "network", Type: "cidr"},
			},
		},
		Columns: []config.Column{
			{Name: "string_col", Type: "string"},
			{Name: "int_col", Type: "int64"},
			{Name: "float_col", Type: "float64"},
			{Name: "bool_col", Type: "bool"},
			{Name: "binary_col", Type: "binary"},
		},
	}

	writer := NewCSVWriter(buf, cfg)
	prefix := netip.MustParsePrefix("10.0.0.0/24")

	// Data in column order: string_col, int_col, float_col, bool_col, binary_col
	data := []mmdbtype.DataType{
		mmdbtype.String("hello"),
		mmdbtype.Uint32(42),
		mmdbtype.Float64(3.14),
		mmdbtype.Bool(true),
		mmdbtype.Bytes([]byte{0xde, 0xad, 0xbe, 0xef}),
	}

	err := writer.WriteRow(prefix, data)
	require.NoError(t, err)

	err = writer.Flush()
	require.NoError(t, err)

	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	require.Len(t, lines, 2)

	values := strings.Split(lines[1], ",")
	assert.Equal(t, "10.0.0.0/24", values[0])
	assert.Equal(t, "hello", values[1])
	assert.Equal(t, "42", values[2])
	assert.Equal(t, "3.14", values[3])
	assert.Equal(t, "1", values[4])
	assert.Equal(t, "deadbeef", values[5])
}

func TestCSVWriter_NilValues(t *testing.T) {
	buf := &bytes.Buffer{}

	cfg := &config.Config{
		Output: config.OutputConfig{
			CSV: config.CSVConfig{Delimiter: ","},
		},
		Network: config.NetworkConfig{
			Columns: []config.NetworkColumn{
				{Name: "network", Type: "cidr"},
			},
		},
		Columns: []config.Column{
			{Name: "col1", Type: "string"},
			{Name: "col2", Type: "string"},
			{Name: "col3", Type: "string"},
		},
	}

	writer := NewCSVWriter(buf, cfg)
	prefix := netip.MustParsePrefix("10.0.0.0/24")

	// Data in column order: col1, col2, col3
	data := []mmdbtype.DataType{
		mmdbtype.String("value1"),
		nil, // nil value
		mmdbtype.String("value3"),
	}

	err := writer.WriteRow(prefix, data)
	require.NoError(t, err)

	err = writer.Flush()
	require.NoError(t, err)

	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	require.Len(t, lines, 2)

	// nil should be represented as empty string
	assert.Equal(t, "10.0.0.0/24,value1,,value3", lines[1])
}

func TestCSVWriter_CustomDelimiter(t *testing.T) {
	buf := &bytes.Buffer{}

	cfg := &config.Config{
		Output: config.OutputConfig{
			CSV: config.CSVConfig{
				Delimiter: "\t", // tab delimiter
			},
		},
		Network: config.NetworkConfig{
			Columns: []config.NetworkColumn{
				{Name: "network", Type: "cidr"},
			},
		},
		Columns: []config.Column{
			{Name: "a", Type: "string"},
			{Name: "b", Type: "string"},
		},
	}

	writer := NewCSVWriter(buf, cfg)
	prefix := netip.MustParsePrefix("10.0.0.0/24")

	// Data in column order: a, b
	data := []mmdbtype.DataType{
		mmdbtype.String("foo"),
		mmdbtype.String("bar"),
	}

	err := writer.WriteRow(prefix, data)
	require.NoError(t, err)

	err = writer.Flush()
	require.NoError(t, err)

	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	assert.Equal(t, "network\ta\tb", lines[0])
	assert.Equal(t, "10.0.0.0/24\tfoo\tbar", lines[1])
}

func TestCSVWriter_CSVEscaping(t *testing.T) {
	buf := &bytes.Buffer{}

	cfg := &config.Config{
		Output: config.OutputConfig{
			CSV: config.CSVConfig{Delimiter: ","},
		},
		Network: config.NetworkConfig{
			Columns: []config.NetworkColumn{
				{Name: "network", Type: "cidr"},
			},
		},
		Columns: []config.Column{
			{Name: "value", Type: "string"},
		},
	}

	writer := NewCSVWriter(buf, cfg)
	prefix := netip.MustParsePrefix("10.0.0.0/24")

	// Test value with comma (requires quoting)
	// Data in column order: value
	data := []mmdbtype.DataType{
		mmdbtype.String("hello, world"),
	}

	err := writer.WriteRow(prefix, data)
	require.NoError(t, err)

	err = writer.Flush()
	require.NoError(t, err)

	output := buf.String()
	// The CSV writer should properly quote/escape values with commas
	assert.Contains(t, output, "\"hello, world\"")
}

func TestConvertToString(t *testing.T) {
	tests := []struct {
		name     string
		value    any
		expected string
	}{
		{"nil", nil, ""},
		{"string", mmdbtype.String("hello"), "hello"},
		{"int32", mmdbtype.Int32(42), "42"},
		{"uint16", mmdbtype.Uint16(42), "42"},
		{"uint32", mmdbtype.Uint32(42), "42"},
		{"uint64", mmdbtype.Uint64(42), "42"},
		{"float64", mmdbtype.Float64(3.14), "3.14"},
		{"bool true", mmdbtype.Bool(true), "1"},
		{"bool false", mmdbtype.Bool(false), "0"},
		{"binary", mmdbtype.Bytes([]byte{0xaa, 0xbb}), "aabb"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := convertToString(tt.value)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestCSVWriter_NetworkBucket_IPv4(t *testing.T) {
	tests := []struct {
		name             string
		network          string
		bucketSize       int
		expectedRowCount int
		expectedBuckets  []string
		expectedStartInt string
		expectedEndInt   string
	}{
		{
			name:             "no split - /24 in /16 bucket",
			network:          "1.2.3.0/24",
			bucketSize:       16,
			expectedRowCount: 1,
			expectedBuckets:  []string{csvIPv4ToInt("1.2.0.0")},
			expectedStartInt: csvIPv4ToInt("1.2.3.0"),
			expectedEndInt:   csvIPv4ToInt("1.2.3.255"),
		},
		{
			name:             "split - /15 into two /16 buckets",
			network:          "2.0.0.0/15",
			bucketSize:       16,
			expectedRowCount: 2,
			expectedBuckets: []string{
				csvIPv4ToInt("2.0.0.0"),
				csvIPv4ToInt("2.1.0.0"),
			},
			expectedStartInt: csvIPv4ToInt("2.0.0.0"),
			expectedEndInt:   csvIPv4ToInt("2.1.255.255"),
		},
		{
			name:             "large split - /12 into 16 /16 buckets",
			network:          "1.0.0.0/12",
			bucketSize:       16,
			expectedRowCount: 16,
			expectedBuckets: []string{
				csvIPv4ToInt("1.0.0.0"),
				csvIPv4ToInt("1.1.0.0"),
				csvIPv4ToInt("1.2.0.0"),
				csvIPv4ToInt("1.3.0.0"),
				csvIPv4ToInt("1.4.0.0"),
				csvIPv4ToInt("1.5.0.0"),
				csvIPv4ToInt("1.6.0.0"),
				csvIPv4ToInt("1.7.0.0"),
				csvIPv4ToInt("1.8.0.0"),
				csvIPv4ToInt("1.9.0.0"),
				csvIPv4ToInt("1.10.0.0"),
				csvIPv4ToInt("1.11.0.0"),
				csvIPv4ToInt("1.12.0.0"),
				csvIPv4ToInt("1.13.0.0"),
				csvIPv4ToInt("1.14.0.0"),
				csvIPv4ToInt("1.15.0.0"),
			},
			expectedStartInt: csvIPv4ToInt("1.0.0.0"),
			expectedEndInt:   csvIPv4ToInt("1.15.255.255"),
		},
		{
			name:             "custom bucket size - /23 into /24 buckets",
			network:          "1.2.0.0/23",
			bucketSize:       24,
			expectedRowCount: 2,
			expectedBuckets: []string{
				csvIPv4ToInt("1.2.0.0"),
				csvIPv4ToInt("1.2.1.0"),
			},
			expectedStartInt: csvIPv4ToInt("1.2.0.0"),
			expectedEndInt:   csvIPv4ToInt("1.2.1.255"),
		},
		{
			name:             "single IP /32",
			network:          "192.168.1.100/32",
			bucketSize:       16,
			expectedRowCount: 1,
			expectedBuckets:  []string{csvIPv4ToInt("192.168.0.0")},
			expectedStartInt: csvIPv4ToInt("192.168.1.100"),
			expectedEndInt:   csvIPv4ToInt("192.168.1.100"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := &bytes.Buffer{}

			cfg := &config.Config{
				Output: config.OutputConfig{
					CSV: config.CSVConfig{
						Delimiter:      ",",
						IPv4BucketSize: tt.bucketSize,
					},
				},
				Network: config.NetworkConfig{
					Columns: []config.NetworkColumn{
						{Name: "start_int", Type: "start_int"},
						{Name: "end_int", Type: "end_int"},
						{Name: "network_bucket", Type: "network_bucket"},
					},
				},
				Columns: []config.Column{{Name: "country", Type: "string"}},
			}

			writer := NewCSVWriter(buf, cfg)

			prefix := netip.MustParsePrefix(tt.network)
			err := writer.WriteRow(prefix, []mmdbtype.DataType{mmdbtype.String("XX")})
			require.NoError(t, err)

			err = writer.Flush()
			require.NoError(t, err)

			rows := readCSVRows(t, buf)
			require.Len(t, rows, tt.expectedRowCount)

			for i, row := range rows {
				assert.Equal(t, tt.expectedBuckets[i], row["network_bucket"])
				assert.Equal(t, tt.expectedStartInt, row["start_int"])
				assert.Equal(t, tt.expectedEndInt, row["end_int"])
				assert.Equal(t, "XX", row["country"])
			}
		})
	}
}

func TestCSVWriter_NetworkBucket_IPv6(t *testing.T) {
	tests := []struct {
		name             string
		network          string
		bucketSize       int
		expectedRowCount int
		expectedBuckets  []string
		expectedStartInt string
		expectedEndInt   string
	}{
		{
			name:             "no split - /24 in /16 bucket",
			network:          "2001:0d00::/24",
			bucketSize:       16,
			expectedRowCount: 1,
			expectedBuckets:  []string{"20010000000000000000000000000000"},
			expectedStartInt: csvIPv6ToInt("2001:0d00::"),
			expectedEndInt:   csvIPv6ToInt("2001:0dff:ffff:ffff:ffff:ffff:ffff:ffff"),
		},
		{
			name:             "split - /15 into two /16 buckets",
			network:          "abcc::/15",
			bucketSize:       16,
			expectedRowCount: 2,
			expectedBuckets: []string{
				"abcc0000000000000000000000000000",
				"abcd0000000000000000000000000000",
			},
			expectedStartInt: csvIPv6ToInt("abcc::"),
			expectedEndInt:   csvIPv6ToInt("abcd:ffff:ffff:ffff:ffff:ffff:ffff:ffff"),
		},
		{
			name:             "large split - /12 into 16 /16 buckets",
			network:          "2000::/12",
			bucketSize:       16,
			expectedRowCount: 16,
			expectedBuckets: []string{
				"20000000000000000000000000000000",
				"20010000000000000000000000000000",
				"20020000000000000000000000000000",
				"20030000000000000000000000000000",
				"20040000000000000000000000000000",
				"20050000000000000000000000000000",
				"20060000000000000000000000000000",
				"20070000000000000000000000000000",
				"20080000000000000000000000000000",
				"20090000000000000000000000000000",
				"200a0000000000000000000000000000",
				"200b0000000000000000000000000000",
				"200c0000000000000000000000000000",
				"200d0000000000000000000000000000",
				"200e0000000000000000000000000000",
				"200f0000000000000000000000000000",
			},
			expectedStartInt: csvIPv6ToInt("2000::"),
			expectedEndInt:   csvIPv6ToInt("200f:ffff:ffff:ffff:ffff:ffff:ffff:ffff"),
		},
		{
			name:             "custom bucket size - /23 into /24 buckets",
			network:          "2001:0000::/23",
			bucketSize:       24,
			expectedRowCount: 2,
			expectedBuckets: []string{
				"20010000000000000000000000000000",
				"20010100000000000000000000000000",
			},
			expectedStartInt: csvIPv6ToInt("2001::"),
			expectedEndInt:   csvIPv6ToInt("2001:01ff:ffff:ffff:ffff:ffff:ffff:ffff"),
		},
		{
			name:             "single IP /128",
			network:          "2001:db8::1/128",
			bucketSize:       16,
			expectedRowCount: 1,
			expectedBuckets:  []string{"20010000000000000000000000000000"},
			expectedStartInt: csvIPv6ToInt("2001:db8::1"),
			expectedEndInt:   csvIPv6ToInt("2001:db8::1"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := &bytes.Buffer{}

			cfg := &config.Config{
				Output: config.OutputConfig{
					CSV: config.CSVConfig{
						Delimiter:      ",",
						IPv6BucketSize: tt.bucketSize,
					},
				},
				Network: config.NetworkConfig{
					Columns: []config.NetworkColumn{
						{Name: "start_int", Type: "start_int"},
						{Name: "end_int", Type: "end_int"},
						{Name: "network_bucket", Type: "network_bucket"},
					},
				},
				Columns: []config.Column{{Name: "country", Type: "string"}},
			}

			writer := NewCSVWriter(buf, cfg)

			prefix := netip.MustParsePrefix(tt.network)
			err := writer.WriteRow(prefix, []mmdbtype.DataType{mmdbtype.String("XX")})
			require.NoError(t, err)

			err = writer.Flush()
			require.NoError(t, err)

			rows := readCSVRows(t, buf)
			require.Len(t, rows, tt.expectedRowCount)

			for i, row := range rows {
				assert.Equal(t, tt.expectedBuckets[i], row["network_bucket"])
				assert.Equal(t, tt.expectedStartInt, row["start_int"])
				assert.Equal(t, tt.expectedEndInt, row["end_int"])
				assert.Equal(t, "XX", row["country"])
			}
		})
	}
}

func TestCSVWriter_NetworkBucket_IPv6_Int(t *testing.T) {
	tests := []struct {
		name             string
		network          string
		bucketSize       int
		expectedRowCount int
		expectedBuckets  []string
		expectedStartInt string
		expectedEndInt   string
	}{
		{
			name:             "no split - /24 in /16 bucket",
			network:          "2001:0d00::/24",
			bucketSize:       16,
			expectedRowCount: 1,
			expectedBuckets:  []string{csvIPv6BucketToInt("2001::")},
			expectedStartInt: csvIPv6ToInt("2001:0d00::"),
			expectedEndInt:   csvIPv6ToInt("2001:0dff:ffff:ffff:ffff:ffff:ffff:ffff"),
		},
		{
			name:             "split - /15 into two /16 buckets",
			network:          "abcc::/15",
			bucketSize:       16,
			expectedRowCount: 2,
			expectedBuckets: []string{
				csvIPv6BucketToInt("abcc::"),
				csvIPv6BucketToInt("abcd::"),
			},
			expectedStartInt: csvIPv6ToInt("abcc::"),
			expectedEndInt:   csvIPv6ToInt("abcd:ffff:ffff:ffff:ffff:ffff:ffff:ffff"),
		},
		{
			name:             "large split - /12 into 16 /16 buckets",
			network:          "2000::/12",
			bucketSize:       16,
			expectedRowCount: 16,
			expectedBuckets: []string{
				csvIPv6BucketToInt("2000::"),
				csvIPv6BucketToInt("2001::"),
				csvIPv6BucketToInt("2002::"),
				csvIPv6BucketToInt("2003::"),
				csvIPv6BucketToInt("2004::"),
				csvIPv6BucketToInt("2005::"),
				csvIPv6BucketToInt("2006::"),
				csvIPv6BucketToInt("2007::"),
				csvIPv6BucketToInt("2008::"),
				csvIPv6BucketToInt("2009::"),
				csvIPv6BucketToInt("200a::"),
				csvIPv6BucketToInt("200b::"),
				csvIPv6BucketToInt("200c::"),
				csvIPv6BucketToInt("200d::"),
				csvIPv6BucketToInt("200e::"),
				csvIPv6BucketToInt("200f::"),
			},
			expectedStartInt: csvIPv6ToInt("2000::"),
			expectedEndInt:   csvIPv6ToInt("200f:ffff:ffff:ffff:ffff:ffff:ffff:ffff"),
		},
		{
			name:             "custom bucket size - /23 into /24 buckets",
			network:          "2001:0000::/23",
			bucketSize:       24,
			expectedRowCount: 2,
			expectedBuckets: []string{
				csvIPv6BucketToInt("2001::"),
				csvIPv6BucketToInt("2001:100::"),
			},
			expectedStartInt: csvIPv6ToInt("2001::"),
			expectedEndInt:   csvIPv6ToInt("2001:01ff:ffff:ffff:ffff:ffff:ffff:ffff"),
		},
		{
			name:             "single IP /128",
			network:          "2001:db8::1/128",
			bucketSize:       16,
			expectedRowCount: 1,
			expectedBuckets:  []string{csvIPv6BucketToInt("2001::")},
			expectedStartInt: csvIPv6ToInt("2001:db8::1"),
			expectedEndInt:   csvIPv6ToInt("2001:db8::1"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := &bytes.Buffer{}

			cfg := &config.Config{
				Output: config.OutputConfig{
					CSV: config.CSVConfig{
						Delimiter:      ",",
						IPv6BucketSize: tt.bucketSize,
						IPv6BucketType: config.IPv6BucketTypeInt,
					},
				},
				Network: config.NetworkConfig{
					Columns: []config.NetworkColumn{
						{Name: "start_int", Type: "start_int"},
						{Name: "end_int", Type: "end_int"},
						{Name: "network_bucket", Type: "network_bucket"},
					},
				},
				Columns: []config.Column{{Name: "country", Type: "string"}},
			}

			writer := NewCSVWriter(buf, cfg)

			prefix := netip.MustParsePrefix(tt.network)
			err := writer.WriteRow(prefix, []mmdbtype.DataType{mmdbtype.String("XX")})
			require.NoError(t, err)

			err = writer.Flush()
			require.NoError(t, err)

			rows := readCSVRows(t, buf)
			require.Len(t, rows, tt.expectedRowCount)

			for i, row := range rows {
				assert.Equal(t, tt.expectedBuckets[i], row["network_bucket"])
				assert.Equal(t, tt.expectedStartInt, row["start_int"])
				assert.Equal(t, tt.expectedEndInt, row["end_int"])
				assert.Equal(t, "XX", row["country"])
			}
		})
	}
}

// readCSVRows reads all data rows from a CSV buffer into a slice of maps.
// The first row is treated as headers.
func readCSVRows(t *testing.T, buf *bytes.Buffer) []map[string]string {
	t.Helper()
	reader := csv.NewReader(strings.NewReader(buf.String()))

	records, err := reader.ReadAll()
	require.NoError(t, err)
	require.NotEmpty(t, records)

	headers := records[0]
	rows := make([]map[string]string, 0, len(records)-1)

	for _, record := range records[1:] {
		row := make(map[string]string, len(headers))
		for j, header := range headers {
			row[header] = record[j]
		}
		rows = append(rows, row)
	}
	return rows
}

// csvIPv4ToInt converts an IPv4 address string to its decimal string representation.
func csvIPv4ToInt(s string) string {
	ip := netip.MustParseAddr(s)
	return strconv.FormatUint(uint64(network.IPv4ToUint32(ip)), 10)
}

// csvIPv6ToInt converts an IPv6 address to its full decimal string representation.
func csvIPv6ToInt(s string) string {
	ip := netip.MustParseAddr(s)
	b := ip.As16()
	var i big.Int
	i.SetBytes(b[:])
	return i.String()
}

// csvIPv6BucketToInt converts an IPv6 bucket address string to its 60-bit decimal string.
func csvIPv6BucketToInt(s string) string {
	ip := netip.MustParseAddr(s)
	val, err := network.IPv6BucketToInt64(ip)
	if err != nil {
		panic(err)
	}
	return strconv.FormatInt(val, 10)
}
