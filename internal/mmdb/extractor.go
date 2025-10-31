package mmdb

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/netip"
	"strconv"
	"strings"
)

// ExtractValue extracts a value from an MMDB database for a given network and path.
// The path uses a simplified JSON pointer syntax (e.g., "/country/iso_code" or
// "/subdivisions/0/names/en").
// Array indices can be negative (e.g., "/subdivisions/-1/names/en").
// The typeHint specifies the desired output type (empty/"string", "int64", "float64", "bool",
// "binary").
// Returns nil if the path doesn't exist (not an error).
// Returns error if type conversion fails or JSON encoding fails.
func ExtractValue(
	reader *Reader,
	network netip.Prefix,
	path, typeHint string,
) (any, error) {
	// Parse path into segments for DecodePath
	segments, err := parsePath(path)
	if err != nil {
		return nil, fmt.Errorf("invalid path '%s': %w", path, err)
	}

	// Look up the network in the database
	result := reader.Lookup(network.Addr())
	if !result.Found() {
		// Network not found in database - return nil (not an error)
		return nil, nil
	}

	// Decode once to any
	var value any
	if err := result.DecodePath(&value, segments...); err != nil {
		return nil, fmt.Errorf("failed to decode path '%s': %w", path, err)
	}

	if value == nil {
		return nil, nil
	}

	// Default to string if no type hint
	if typeHint == "" {
		typeHint = "string"
	}

	// Convert to target type using type switch
	switch typeHint {
	case "string":
		return convertToString(value)

	case "int64":
		return convertToInt64(value)

	case "float64":
		return convertToFloat64(value)

	case "bool":
		return convertToBool(value)

	case "binary":
		return convertToBinary(value)

	default:
		return nil, fmt.Errorf("unsupported type hint: %s", typeHint)
	}
}

// convertToString converts any value to a string.
func convertToString(value any) (any, error) {
	switch v := value.(type) {
	case string:
		return v, nil
	case int, int8, int16, int32, int64:
		return fmt.Sprintf("%d", v), nil
	case uint, uint8, uint16, uint32, uint64:
		return fmt.Sprintf("%d", v), nil
	case float32, float64:
		return fmt.Sprintf("%g", v), nil
	case bool:
		return strconv.FormatBool(v), nil
	default:
		// Complex types: JSON encode
		jsonBytes, err := json.Marshal(value)
		if err != nil {
			return nil, fmt.Errorf("failed to JSON encode value: %w", err)
		}
		return string(jsonBytes), nil
	}
}

// convertToInt64 converts any value to int64.
func convertToInt64(value any) (any, error) {
	switch v := value.(type) {
	case int:
		return int64(v), nil
	case int8:
		return int64(v), nil
	case int16:
		return int64(v), nil
	case int32:
		return int64(v), nil
	case int64:
		return v, nil
	case uint:
		if v > (1<<63 - 1) {
			return nil, fmt.Errorf("uint value %d too large for int64", v)
		}
		return int64(v), nil
	case uint8:
		return int64(v), nil
	case uint16:
		return int64(v), nil
	case uint32:
		return int64(v), nil
	case uint64:
		if v > (1<<63 - 1) {
			return nil, fmt.Errorf("uint64 value %d too large for int64", v)
		}
		return int64(v), nil
	default:
		return nil, fmt.Errorf("cannot convert %T to int64", value)
	}
}

// convertToFloat64 converts any value to float64.
func convertToFloat64(value any) (any, error) {
	switch v := value.(type) {
	case float32:
		return float64(v), nil
	case float64:
		return v, nil
	case int:
		return float64(v), nil
	case int8:
		return float64(v), nil
	case int16:
		return float64(v), nil
	case int32:
		return float64(v), nil
	case int64:
		return float64(v), nil
	case uint:
		return float64(v), nil
	case uint8:
		return float64(v), nil
	case uint16:
		return float64(v), nil
	case uint32:
		return float64(v), nil
	case uint64:
		return float64(v), nil
	default:
		return nil, fmt.Errorf("cannot convert %T to float64", value)
	}
}

// convertToBool converts any value to bool.
func convertToBool(value any) (any, error) {
	if v, ok := value.(bool); ok {
		return v, nil
	}
	return nil, fmt.Errorf("cannot convert %T to bool", value)
}

// convertToBinary converts any value to []byte.
func convertToBinary(value any) (any, error) {
	if v, ok := value.([]byte); ok {
		return v, nil
	}
	return nil, fmt.Errorf("cannot convert %T to binary", value)
}

// parsePath parses a path like "/country/iso_code" or "/subdivisions/0/names/en"
// into segments suitable for DecodePath. Array indices are converted to integers.
func parsePath(path string) ([]any, error) {
	if path == "" {
		return []any{}, nil
	}
	if !strings.HasPrefix(path, "/") {
		return nil, errors.New("path must start with '/'")
	}

	// Split by '/' and skip the first empty element
	parts := strings.Split(path[1:], "/")
	if len(parts) == 0 {
		return []any{}, nil
	}

	// Convert numeric strings to integers for array indices
	segments := make([]any, len(parts))
	for i, part := range parts {
		// Try to parse as integer (for array indices)
		if num, err := strconv.Atoi(part); err == nil {
			segments[i] = num
		} else {
			segments[i] = part
		}
	}

	return segments, nil
}
