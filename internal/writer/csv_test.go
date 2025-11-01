package writer

import (
	"bytes"
	"net/netip"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/maxmind/mmdbconvert/internal/config"
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
	data := map[string]any{
		"country": "US",
		"city":    "New York",
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
		err := writer.WriteRow(row.prefix, map[string]any{"value": row.value})
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

			err := writer.WriteRow(prefix, map[string]any{})
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

	err := writer.WriteRow(prefix, map[string]any{})
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
	require.NoError(
		t,
		writer.WriteRow(
			netip.MustParsePrefix("10.0.0.0/24"),
			map[string]any{"value": "row"},
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

	data := map[string]any{
		"string_col": "hello",
		"int_col":    int64(42),
		"float_col":  float64(3.14),
		"bool_col":   true,
		"binary_col": []byte{0xde, 0xad, 0xbe, 0xef},
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
	assert.Equal(t, "true", values[4])
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

	data := map[string]any{
		"col1": "value1",
		"col2": nil, // nil value
		"col3": "value3",
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

	data := map[string]any{
		"a": "foo",
		"b": "bar",
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
	data := map[string]any{
		"value": "hello, world",
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
		{"string", "hello", "hello"},
		{"int", int(42), "42"},
		{"int64", int64(42), "42"},
		{"uint", uint(42), "42"},
		{"uint64", uint64(42), "42"},
		{"float64", float64(3.14), "3.14"},
		{"bool true", true, "true"},
		{"bool false", false, "false"},
		{"binary", []byte{0xaa, 0xbb}, "aabb"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := convertToString(tt.value)
			assert.Equal(t, tt.expected, result)
		})
	}
}
