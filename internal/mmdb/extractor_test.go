package mmdb

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNormalizeSegments(t *testing.T) {
	tests := []struct {
		name     string
		path     []any
		expected []any
		wantErr  bool
	}{
		{
			name:    "empty path",
			path:    []any{},
			wantErr: true,
		},
		{
			name:     "simple path",
			path:     []any{"country", "iso_code"},
			expected: []any{"country", "iso_code"},
		},
		{
			name:     "array path with index",
			path:     []any{"subdivisions", int64(0), "iso_code"},
			expected: []any{"subdivisions", 0, "iso_code"},
		},
		{
			name:     "nested path",
			path:     []any{"location", "latitude"},
			expected: []any{"location", "latitude"},
		},
		{
			name:     "negative array index",
			path:     []any{"subdivisions", int64(-1), "names", "en"},
			expected: []any{"subdivisions", -1, "names", "en"},
		},
		{
			name:     "multiple array indices",
			path:     []any{"path", int64(0), "sub", int64(1), "value"},
			expected: []any{"path", 0, "sub", 1, "value"},
		},
		{
			name:    "invalid type",
			path:    []any{1.23},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := normalizeSegments(tt.path)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestConvertToString(t *testing.T) {
	tests := []struct {
		name     string
		value    any
		expected string
	}{
		{
			name:     "string value",
			value:    "hello",
			expected: "hello",
		},
		{
			name:     "int value",
			value:    int64(42),
			expected: "42",
		},
		{
			name:     "int32 value",
			value:    int32(42),
			expected: "42",
		},
		{
			name:     "uint value",
			value:    uint32(42),
			expected: "42",
		},
		{
			name:     "float value",
			value:    37.751,
			expected: "37.751",
		},
		{
			name:     "bool value",
			value:    true,
			expected: "true",
		},
		{
			name:     "map value (JSON encoded)",
			value:    map[string]any{"key": "value"},
			expected: `{"key":"value"}`,
		},
		{
			name:     "array value (JSON encoded)",
			value:    []any{"a", "b", "c"},
			expected: `["a","b","c"]`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := convertToString(tt.value)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestConvertToInt64(t *testing.T) {
	tests := []struct {
		name     string
		value    any
		expected int64
		wantErr  bool
	}{
		{
			name:     "int",
			value:    42,
			expected: 42,
		},
		{
			name:     "int8",
			value:    int8(42),
			expected: 42,
		},
		{
			name:     "int16",
			value:    int16(1000),
			expected: 1000,
		},
		{
			name:     "int32",
			value:    int32(100000),
			expected: 100000,
		},
		{
			name:     "int64",
			value:    int64(1000000),
			expected: 1000000,
		},
		{
			name:     "uint32",
			value:    uint32(42),
			expected: 42,
		},
		{
			name:     "uint64 valid",
			value:    uint64(42),
			expected: 42,
		},
		{
			name:    "uint64 overflow",
			value:   uint64(1 << 63),
			wantErr: true,
		},
		{
			name:     "negative int",
			value:    int64(-42),
			expected: -42,
		},
		{
			name:    "string - should fail",
			value:   "42",
			wantErr: true,
		},
		{
			name:    "float - should fail",
			value:   42.5,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := convertToInt64(tt.value)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestConvertToFloat64(t *testing.T) {
	tests := []struct {
		name     string
		value    any
		expected float64
		wantErr  bool
	}{
		{
			name:     "float32",
			value:    float32(3.14),
			expected: float64(float32(3.14)),
		},
		{
			name:     "float64",
			value:    37.751,
			expected: 37.751,
		},
		{
			name:     "int to float",
			value:    42,
			expected: 42.0,
		},
		{
			name:     "int64 to float",
			value:    int64(42),
			expected: 42.0,
		},
		{
			name:     "uint to float",
			value:    uint32(42),
			expected: 42.0,
		},
		{
			name:    "string - should fail",
			value:   "3.14",
			wantErr: true,
		},
		{
			name:    "bool - should fail",
			value:   true,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := convertToFloat64(tt.value)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.InDelta(t, tt.expected, result, 0.0001)
		})
	}
}

func TestConvertToBool(t *testing.T) {
	tests := []struct {
		name     string
		value    any
		expected bool
		wantErr  bool
	}{
		{
			name:     "bool true",
			value:    true,
			expected: true,
		},
		{
			name:     "bool false",
			value:    false,
			expected: false,
		},
		{
			name:    "int - should fail",
			value:   1,
			wantErr: true,
		},
		{
			name:    "string - should fail",
			value:   "true",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := convertToBool(tt.value)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestConvertToBinary(t *testing.T) {
	tests := []struct {
		name     string
		value    any
		expected []byte
		wantErr  bool
	}{
		{
			name:     "byte slice",
			value:    []byte{0x01, 0x02, 0x03},
			expected: []byte{0x01, 0x02, 0x03},
		},
		{
			name:    "string - should fail",
			value:   "hello",
			wantErr: true,
		},
		{
			name:    "int - should fail",
			value:   42,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := convertToBinary(tt.value)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.expected, result)
		})
	}
}
