package writer

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"math"
	"math/big"
	"net/netip"
	"testing"

	"github.com/maxmind/mmdbwriter/v2/mmdbtype"
	"github.com/stretchr/testify/require"

	"github.com/maxmind/mmdbconvert/internal/config"
)

func TestFormatCSVFloat(t *testing.T) {
	for _, precision := range []int{0, 4, 8, 1000} {
		for _, value := range []float64{37.751, 35.68536, -97.822, 0, math.Copysign(0, -1), 0.00001, 1e20, math.Inf(1), math.NaN()} {
			t.Run(fmt.Sprintf("%g/%d", value, precision), func(t *testing.T) {
				format := &config.ColumnFormat{Precision: &precision}
				got, err := formatCSVValue(mmdbtype.Float64(value), format)
				require.NoError(t, err)
				require.Equal(t, fmt.Sprintf("%.*f", precision, value), got)
				got, err = formatCSVValue(mmdbtype.Float32(value), format)
				require.NoError(t, err)
				require.Equal(t, fmt.Sprintf("%.*f", precision, float32(value)), got)
			})
		}
	}
}

func TestFormatCSVBool(t *testing.T) {
	for _, labels := range [][2]string{{"t", "f"}, {"1", "0"}, {"1", ""}, {"true", "false"}, {"", "no"}} {
		t.Run(labels[0]+"/"+labels[1], func(t *testing.T) {
			format := &config.ColumnFormat{
				True:  &labels[0],
				False: &labels[1],
			}
			for i, value := range []mmdbtype.DataType{mmdbtype.Bool(true), mmdbtype.Bool(false), nil} {
				got, err := formatCSVValue(value, format)
				require.NoError(t, err)
				require.Equal(t, []string{labels[0], labels[1], ""}[i], got)
			}
		})
	}
}

func TestCSVWriter_ColumnFormats(t *testing.T) {
	precision := 4
	trueLabel, falseLabel := "yes,\"indeed\"\n", ""
	for _, mode := range []string{"prefix", "range", "bucket"} {
		t.Run(mode, func(t *testing.T) {
			cfg := &config.Config{
				Network: config.NetworkConfig{
					Columns: []config.NetworkColumn{
						{Name: "network", Type: config.NetworkColumnTypeCIDR},
					},
				},
				Columns: []config.Column{
					{
						Name: "latitude",
						Format: &config.ColumnFormat{
							Precision: &precision,
						},
					},
					{Name: "original"},
					{
						Name: "flag",
						Format: &config.ColumnFormat{
							True:  &trueLabel,
							False: &falseLabel,
						},
					},
				},
			}
			if mode == "range" {
				cfg.Network.Columns[0].Type = config.NetworkColumnTypeStartIP
			}
			if mode == "bucket" {
				cfg.Network.Columns = append(
					cfg.Network.Columns,
					config.NetworkColumn{Name: "bucket", Type: config.NetworkColumnTypeBucket},
				)
				cfg.Output.CSV.IPv4BucketSize = 16
			}
			var buf bytes.Buffer
			w := NewCSVWriter(&buf, cfg)
			prefix := netip.MustParsePrefix("10.0.0.0/24")
			write := func(data []mmdbtype.DataType) error {
				if mode == "range" {
					return w.WriteRange(prefix.Addr(), netip.MustParseAddr("10.0.0.255"), data)
				}
				return w.WriteRow(prefix, data)
			}
			require.NoError(
				t,
				write(
					[]mmdbtype.DataType{
						mmdbtype.Float64(37.751),
						mmdbtype.Float64(37.751),
						mmdbtype.Bool(true),
					},
				),
			)
			require.NoError(
				t,
				write([]mmdbtype.DataType{nil, mmdbtype.Float32(3.14), mmdbtype.Bool(false)}),
			)
			require.NoError(t, w.Flush())
			rows, err := csv.NewReader(&buf).ReadAll()
			require.NoError(t, err)
			require.Len(t, rows, 3)
			offset := len(cfg.Network.Columns)
			require.Equal(t, []string{"37.7510", "37.751", trueLabel}, rows[1][offset:])
			require.Equal(t, []string{"", "3.14", ""}, rows[2][offset:])
			err = write([]mmdbtype.DataType{mmdbtype.String("37.751"), nil, nil})
			require.ErrorContains(t, err, "column 'latitude'")
			require.ErrorContains(t, err, "requires a floating-point value")
			err = write([]mmdbtype.DataType{nil, nil, mmdbtype.Uint16(1)})
			require.ErrorContains(t, err, "column 'flag'")
			require.ErrorContains(t, err, "require a boolean value")
		})
	}
}

func TestParquetStringConversion_DefaultFormatting(t *testing.T) {
	got, err := convertToParquetType(mmdbtype.Float64(37.751), config.ColumnTypeString)
	require.NoError(t, err)
	require.Equal(t, "37.751", got)
}

func TestCSVWriter_FormatTypeMismatch(t *testing.T) {
	precision := 4
	trueLabel, falseLabel := "1", "0"
	type testValue struct {
		value    mmdbtype.DataType
		typeName string
	}
	for _, tt := range []struct {
		name   string
		format *config.ColumnFormat
		values []testValue
		want   string
	}{
		{
			name:   "precision",
			format: &config.ColumnFormat{Precision: &precision},
			values: []testValue{
				{mmdbtype.Bool(true), "boolean"},
				{mmdbtype.Bytes{1}, "bytes"},
				{mmdbtype.Int32(-1), "int32"},
				{mmdbtype.Uint16(1), "uint16"},
				{mmdbtype.Uint32(1), "uint32"},
				{mmdbtype.Uint64(1), "uint64"},
				{(*mmdbtype.Uint128)(big.NewInt(1)), "uint128"},
				{mmdbtype.String("1.2"), "string"},
				{mmdbtype.Map{"value": mmdbtype.Float64(1.2)}, "map"},
				{mmdbtype.Slice{mmdbtype.Float64(1.2)}, "array"},
			},
			want: "precision requires a floating-point value",
		},
		{
			name:   "boolean labels",
			format: &config.ColumnFormat{True: &trueLabel, False: &falseLabel},
			values: []testValue{
				{mmdbtype.Float32(1), "float"},
				{mmdbtype.Float64(1), "double"},
				{mmdbtype.Uint32(1), "uint32"},
				{mmdbtype.String("true"), "string"},
				{mmdbtype.Map{"value": mmdbtype.Bool(true)}, "map"},
				{mmdbtype.Slice{mmdbtype.Bool(true)}, "array"},
			},
			want: "true/false labels require a boolean value",
		},
	} {
		for _, value := range tt.values {
			t.Run(tt.name+"/"+value.typeName, func(t *testing.T) {
				cfg := &config.Config{
					Columns: []config.Column{{Name: "value", Format: tt.format}},
				}
				var buf bytes.Buffer
				w := NewCSVWriter(&buf, cfg)
				err := w.WriteRow(
					netip.MustParsePrefix("10.0.0.0/24"),
					[]mmdbtype.DataType{value.value},
				)
				require.ErrorContains(t, err, "column 'value'")
				require.ErrorContains(t, err, tt.want+", got "+value.typeName)
				require.NotContains(t, err.Error(), "mmdbtype.")
			})
		}
	}
}
