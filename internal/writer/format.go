package writer

import (
	"errors"
	"fmt"
	"strconv"

	"github.com/maxmind/mmdbwriter/v2/mmdbtype"

	"github.com/maxmind/mmdbconvert/internal/config"
)

// formatCSVValue formats values after filtering and merging.
func formatCSVValue(value mmdbtype.DataType, format *config.ColumnFormat) (string, error) {
	if value == nil || format == nil {
		return convertToString(value)
	}
	switch {
	case format.Precision != nil:
		switch v := value.(type) {
		case mmdbtype.Float32:
			return strconv.FormatFloat(float64(v), 'f', *format.Precision, 32), nil
		case mmdbtype.Float64:
			return strconv.FormatFloat(float64(v), 'f', *format.Precision, 64), nil
		default:
			return "", fmt.Errorf(
				"precision requires a floating-point value, got %s",
				mmdbTypeName(value),
			)
		}
	case format.True != nil && format.False != nil:
		v, ok := value.(mmdbtype.Bool)
		if !ok {
			return "", fmt.Errorf(
				"true/false labels require a boolean value, got %s",
				mmdbTypeName(value),
			)
		}
		if v {
			return *format.True, nil
		}
		return *format.False, nil
	default:
		return "", errors.New("internal error: invalid CSV column format")
	}
}

func mmdbTypeName(value mmdbtype.DataType) string {
	switch value.(type) {
	case mmdbtype.Bool:
		return "boolean"
	case mmdbtype.Bytes:
		return "bytes"
	case mmdbtype.Float32:
		return "float"
	case mmdbtype.Float64:
		return "double"
	case mmdbtype.Int32:
		return "int32"
	case mmdbtype.Map:
		return "map"
	case mmdbtype.Pointer:
		return "pointer"
	case mmdbtype.Slice:
		return "array"
	case mmdbtype.String:
		return "string"
	case mmdbtype.Uint16:
		return "uint16"
	case mmdbtype.Uint32:
		return "uint32"
	case mmdbtype.Uint64:
		return "uint64"
	case *mmdbtype.Uint128:
		return "uint128"
	default:
		return "unknown"
	}
}
