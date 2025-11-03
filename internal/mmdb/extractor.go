package mmdb

import (
	"fmt"
	"net/netip"
	"strconv"
	"strings"

	"github.com/maxmind/mmdbwriter/mmdbtype"
)

// ExtractValue extracts a value from an MMDB database for a given network and path.
// The path must be a slice of map keys (strings) and/or array indices (ints).
// Returns nil if the path doesn't exist (not an error).
// The unmarshaler is reused across calls for caching efficiency and must be cleared
// after retrieving the result.
func ExtractValue(
	reader *Reader,
	network netip.Prefix,
	path []any,
	unmarshaler *mmdbtype.Unmarshaler,
) (mmdbtype.DataType, error) {
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

	// Decode using unmarshaler (with caching!)
	if err := result.DecodePath(unmarshaler, segments...); err != nil {
		return nil, fmt.Errorf("decoding path %s: %w", describePath(segments), err)
	}

	value := unmarshaler.Result()
	unmarshaler.Clear() // Reset for next column

	return value, nil
}

func normalizeSegments(path []any) ([]any, error) {
	// Empty path is allowed - it means "decode entire record"
	if len(path) == 0 {
		return []any{}, nil
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
