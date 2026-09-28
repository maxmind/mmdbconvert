package writer

import (
	"encoding/json"
	"math"
	"testing"

	"github.com/maxmind/mmdbwriter/v2/mmdbtype"
	"github.com/stretchr/testify/require"
)

func TestConvertToString_JSON(t *testing.T) {
	sharedMap := mmdbtype.Map{"n": mmdbtype.Uint128{Low: 42}}
	sharedSlice := mmdbtype.Slice{mmdbtype.Uint128{Low: 1}, mmdbtype.Uint128{High: 1, Low: 1}}
	for _, tt := range []struct {
		name  string
		value mmdbtype.DataType
		want  string
	}{
		{"map", sharedMap, `{"n":42}`},
		{"slice", mmdbtype.Slice{mmdbtype.Uint128{}, mmdbtype.Uint128{High: 1, Low: 1}}, `[0,18446744073709551617]`},
		{
			"mixed nesting",
			mmdbtype.Map{"values": mmdbtype.Slice{mmdbtype.Map{"n": mmdbtype.Uint128{High: math.MaxUint64, Low: math.MaxUint64}}}},
			`{"values":[{"n":340282366920938463463374607431768211455}]}`,
		},
		{
			"other scalars",
			mmdbtype.Slice{mmdbtype.Bool(true), mmdbtype.Bytes{0xaa, 0xbb}, mmdbtype.Int32(-1), mmdbtype.Float64(1.5), mmdbtype.String("text"), nil},
			`[true,"qrs=",-1,1.5,"text",null]`,
		},
		{"nil map", mmdbtype.Map(nil), `null`},
		{"empty map", mmdbtype.Map{}, `{}`},
		{"nil slice", mmdbtype.Slice(nil), `null`},
		{"empty slice", mmdbtype.Slice{}, `[]`},
		{"shared map", mmdbtype.Slice{sharedMap, sharedMap}, `[{"n":42},{"n":42}]`},
		{"shared slice", mmdbtype.Slice{sharedSlice, sharedSlice}, `[[1,18446744073709551617],[1,18446744073709551617]]`},
		{"slice prefixes", mmdbtype.Slice{sharedSlice[:1], sharedSlice}, `[[1],[1,18446744073709551617]]`},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got, err := convertToString(tt.value)
			require.NoError(t, err)
			// Exact text preserves integer precision that JSON float decoding loses.
			require.Equal(t, tt.want, got)
		})
	}
	// Normalizing a value must not replace leaves in the original containers.
	require.Equal(t, mmdbtype.Uint128{Low: 42}, sharedMap["n"])
	require.Equal(t, mmdbtype.Uint128{High: 1, Low: 1}, sharedSlice[1])
}

func TestConvertToString_JSONErrors(t *testing.T) {
	cyclicMap := mmdbtype.Map{}
	cyclicMap["self"] = cyclicMap
	cyclicSlice := make(mmdbtype.Slice, 1)
	cyclicSlice[0] = cyclicSlice
	for _, tt := range []struct {
		name    string
		value   mmdbtype.DataType
		context string
	}{
		{"map cycle", cyclicMap, "marshaling map to JSON"},
		{"slice cycle", cyclicSlice, "marshaling slice to JSON"},
		{"nested NaN", mmdbtype.Map{"values": mmdbtype.Slice{mmdbtype.Float64(math.NaN())}}, "marshaling map to JSON"},
		{"nested infinity", mmdbtype.Slice{mmdbtype.Map{"n": mmdbtype.Float64(math.Inf(1))}}, "marshaling slice to JSON"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got, err := convertToString(tt.value)
			require.Empty(t, got)
			require.ErrorContains(t, err, tt.context)
			var unsupported *json.UnsupportedValueError
			require.ErrorAs(t, err, &unsupported)
		})
	}
}

func TestConvertToParquetType_JSON(t *testing.T) {
	value := mmdbtype.Map{"n": mmdbtype.Slice{mmdbtype.Uint128{High: 1, Low: 1}}}
	for _, typeHint := range []string{"", "string"} {
		t.Run("type="+typeHint, func(t *testing.T) {
			got, err := convertToParquetType(value, typeHint)
			require.NoError(t, err)
			//nolint:testifylint // JSONEq loses integer precision through float64 decoding.
			require.Equal(t, `{"n":[18446744073709551617]}`, got)
		})
	}
}
