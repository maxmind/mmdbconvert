package writer

import (
	"bytes"
	"fmt"
	"net/netip"
	"testing"

	"github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/maxmind/mmdbconvert/internal/config"
)

func TestParquetWriter_SingleRow(t *testing.T) {
	buf := &bytes.Buffer{}

	cfg := &config.Config{
		Output: config.OutputConfig{
			Parquet: config.ParquetConfig{
				Compression:  "snappy",
				RowGroupSize: 500000,
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

	writer, err := NewParquetWriter(buf, cfg)
	require.NoError(t, err)

	prefix := netip.MustParsePrefix("10.0.0.0/24")
	data := map[string]any{
		"country": "US",
		"city":    "New York",
	}

	err = writer.WriteRow(prefix, data)
	require.NoError(t, err)

	err = writer.Flush()
	require.NoError(t, err)

	// Verify we wrote valid Parquet data
	pf, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	require.NoError(t, err)

	// Check basic file properties
	assert.Equal(t, int64(1), pf.NumRows())
	assert.Len(t, pf.Schema().Fields(), 3)
}

func TestParquetWriter_MultipleRows(t *testing.T) {
	buf := &bytes.Buffer{}

	cfg := &config.Config{
		Output: config.OutputConfig{
			Parquet: config.ParquetConfig{
				Compression:  "snappy",
				RowGroupSize: 500000,
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

	writer, err := NewParquetWriter(buf, cfg)
	require.NoError(t, err)

	for i := range 3 {
		prefix := netip.MustParsePrefix(fmt.Sprintf("10.0.%d.0/24", i))
		err := writer.WriteRow(prefix, map[string]any{"value": fmt.Sprintf("row%d", i)})
		require.NoError(t, err)
	}

	err = writer.Flush()
	require.NoError(t, err)

	// Verify valid Parquet file
	pf, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	require.NoError(t, err)

	assert.Equal(t, int64(3), pf.NumRows())
	assert.Len(t, pf.Schema().Fields(), 2)
}

func TestParquetWriter_NetworkColumns(t *testing.T) {
	buf := &bytes.Buffer{}

	cfg := &config.Config{
		Output: config.OutputConfig{
			Parquet: config.ParquetConfig{
				Compression:  "none",
				RowGroupSize: 500000,
			},
		},
		Network: config.NetworkConfig{
			Columns: []config.NetworkColumn{
				{Name: "network", Type: "cidr"},
				{Name: "start_ip", Type: "start_ip"},
				{Name: "end_ip", Type: "end_ip"},
				{Name: "start_int", Type: "start_int"},
				{Name: "end_int", Type: "end_int"},
			},
		},
		Columns: []config.Column{},
	}

	writer, err := NewParquetWriter(buf, cfg)
	require.NoError(t, err)

	prefix := netip.MustParsePrefix("192.168.1.0/24")
	err = writer.WriteRow(prefix, map[string]any{})
	require.NoError(t, err)

	err = writer.Flush()
	require.NoError(t, err)

	// Verify valid Parquet file
	pf, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	require.NoError(t, err)

	assert.Equal(t, int64(1), pf.NumRows())
	assert.Len(t, pf.Schema().Fields(), 5)
}

func TestParquetWriter_DataTypes(t *testing.T) {
	buf := &bytes.Buffer{}

	cfg := &config.Config{
		Output: config.OutputConfig{
			Parquet: config.ParquetConfig{
				Compression:  "none",
				RowGroupSize: 500000,
			},
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

	writer, err := NewParquetWriter(buf, cfg)
	require.NoError(t, err)

	prefix := netip.MustParsePrefix("10.0.0.0/24")
	data := map[string]any{
		"string_col": "hello",
		"int_col":    int64(42),
		"float_col":  float64(3.14),
		"bool_col":   true,
		"binary_col": []byte{0xde, 0xad, 0xbe, 0xef},
	}

	err = writer.WriteRow(prefix, data)
	require.NoError(t, err)

	err = writer.Flush()
	require.NoError(t, err)

	// Verify valid Parquet file with correct schema
	pf, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	require.NoError(t, err)

	assert.Equal(t, int64(1), pf.NumRows())
	assert.Len(t, pf.Schema().Fields(), 6)
}

func TestParquetWriter_NilValues(t *testing.T) {
	buf := &bytes.Buffer{}

	cfg := &config.Config{
		Output: config.OutputConfig{
			Parquet: config.ParquetConfig{
				Compression:  "none",
				RowGroupSize: 500000,
			},
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

	writer, err := NewParquetWriter(buf, cfg)
	require.NoError(t, err)

	prefix := netip.MustParsePrefix("10.0.0.0/24")
	data := map[string]any{
		"col1": "value1",
		"col2": nil, // nil value
		"col3": "value3",
	}

	err = writer.WriteRow(prefix, data)
	require.NoError(t, err)

	err = writer.Flush()
	require.NoError(t, err)

	// Verify valid Parquet file
	pf, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	require.NoError(t, err)

	assert.Equal(t, int64(1), pf.NumRows())
}

func TestParquetWriter_IPv6StartInt(t *testing.T) {
	buf := &bytes.Buffer{}

	cfg := &config.Config{
		Output: config.OutputConfig{
			Parquet: config.ParquetConfig{
				Compression:  "none",
				RowGroupSize: 100,
			},
		},
		Network: config.NetworkConfig{
			Columns: []config.NetworkColumn{
				{Name: "start_int", Type: "start_int"},
				{Name: "end_int", Type: "end_int"},
			},
		},
		Columns: []config.Column{},
	}

	writer, err := NewParquetWriterWithIPVersion(buf, cfg, IPVersion6)
	require.NoError(t, err)

	prefix := netip.MustParsePrefix("2001:db8::/126")
	require.NoError(t, writer.WriteRow(prefix, map[string]any{}))
	require.NoError(t, writer.Flush())

	pf, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	require.NoError(t, err)

	assert.Equal(t, int64(1), pf.NumRows())
	startCol, ok := pf.Schema().Lookup("start_int")
	require.True(t, ok)
	assert.Equal(t, parquet.FixedLenByteArray, startCol.Node.Type().Kind())
	assert.Equal(t, 16, startCol.Node.Type().Length())

	endCol, ok := pf.Schema().Lookup("end_int")
	require.True(t, ok)
	assert.Equal(t, parquet.FixedLenByteArray, endCol.Node.Type().Kind())
	assert.Equal(t, 16, endCol.Node.Type().Length())
}

func TestParquetWriter_Compression(t *testing.T) {
	compressions := []string{"none", "snappy", "gzip", "lz4", "zstd"}

	for _, compression := range compressions {
		t.Run(compression, func(t *testing.T) {
			buf := &bytes.Buffer{}

			cfg := &config.Config{
				Output: config.OutputConfig{
					Parquet: config.ParquetConfig{
						Compression:  compression,
						RowGroupSize: 500000,
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

			writer, err := NewParquetWriter(buf, cfg)
			require.NoError(t, err)

			prefix := netip.MustParsePrefix("10.0.0.0/24")
			err = writer.WriteRow(prefix, map[string]any{"value": "test"})
			require.NoError(t, err)

			err = writer.Flush()
			require.NoError(t, err)

			// Verify valid Parquet file
			pf, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
			require.NoError(t, err)
			assert.Equal(t, int64(1), pf.NumRows())
		})
	}
}

func TestParquetWriter_RowGroupSize(t *testing.T) {
	buf := &bytes.Buffer{}

	cfg := &config.Config{
		Output: config.OutputConfig{
			Parquet: config.ParquetConfig{
				Compression:  "none",
				RowGroupSize: 2, // Small row group for testing
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

	writer, err := NewParquetWriter(buf, cfg)
	require.NoError(t, err)

	// Write 5 rows (should create multiple row groups)
	for i := range 5 {
		prefix := netip.MustParsePrefix(fmt.Sprintf("10.0.%d.0/24", i))
		err := writer.WriteRow(prefix, map[string]any{"value": fmt.Sprintf("row%d", i)})
		require.NoError(t, err)
	}

	err = writer.Flush()
	require.NoError(t, err)

	// Verify all rows were written
	pf, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	require.NoError(t, err)

	assert.Equal(t, int64(5), pf.NumRows())
	// Should have multiple row groups (5 rows / 2 per group = 3 groups)
	assert.GreaterOrEqual(t, len(pf.RowGroups()), 2)
}

func TestConvertToParquetType(t *testing.T) {
	tests := []struct {
		name     string
		value    any
		typeHint string
		expected any
		wantErr  bool
	}{
		{"nil", nil, "string", nil, false},
		{"string", "hello", "string", "hello", false},
		{"int to int64", int(42), "int64", int64(42), false},
		{"int32 to int64", int32(42), "int64", int64(42), false},
		{"uint to int64", uint(42), "int64", int64(42), false},
		{"float32 to float64", float32(3.14), "float64", float64(float32(3.14)), false},
		{"int to float64", int(42), "float64", float64(42), false},
		{"bool", true, "bool", true, false},
		{"binary", []byte{0xaa, 0xbb}, "binary", []byte{0xaa, 0xbb}, false},
		{"invalid int conversion", "hello", "int64", nil, true},
		{"invalid bool conversion", "true", "bool", nil, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := convertToParquetType(tt.value, tt.typeHint)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}
