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
// The path must be a slice of map keys (strings) and/or array indices (ints).
// The typeHint specifies the desired output type (empty/"string", "int64", "float64", "bool",
// "binary"). Returns nil if the path doesn't exist (not an error).
func ExtractValue(
	reader *Reader,
	network netip.Prefix,
	path []any,
	typeHint string,
) (any, error) {
	segments, err := normalizeSegments(path)
	if err != nil {
		return nil, fmt.Errorf("invalid path %v: %w", path, err)
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
		return nil, fmt.Errorf("failed to decode path %s: %w", describePath(segments), err)
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

func normalizeSegments(path []any) ([]any, error) {
	if len(path) == 0 {
		return nil, errors.New("path must contain at least one segment")
	}

	segments := make([]any, len(path))
	for i, seg := range path {
		switch v := seg.(type) {
		case string:
			segments[i] = v
		case int:
			segments[i] = v
		case int64:
			if v > int64(int(^uint(0)>>1)) || v < int64(minInt()) {
				return nil, fmt.Errorf("path index %d out of range", v)
			}
			segments[i] = int(v)
		default:
			return nil, fmt.Errorf("unsupported path segment type %T", seg)
		}
	}

	return segments, nil
}

func describePath(segments []any) string {
	var b strings.Builder
	b.WriteString("[")
	for i, seg := range segments {
		if i > 0 {
			b.WriteString(" ")
		}
		switch v := seg.(type) {
		case string:
			b.WriteString(v)
		case int:
			b.WriteString(strconv.Itoa(v))
		default:
			b.WriteString(fmt.Sprintf("%v", v))
		}
	}
	b.WriteString("]")
	return b.String()
}

func minInt() int {
	n := ^uint(0) >> 1
	return -int(n) - 1
}
