package writer

import (
	"encoding/json"
	"reflect"

	"github.com/maxmind/mmdbwriter/v2/mmdbtype"
)

func normalizeMMDBJSON(value mmdbtype.DataType) any {
	return jsonValue(value, map[jsonContainer]any{})
}

type jsonContainer struct {
	pointer uintptr
	length  int
	kind    reflect.Kind
}

// jsonValue replaces Uint128 leaves with exact JSON numbers. Memoized copies
// preserve sharing and cycles without changing the input; json.Marshal retains
// responsibility for rejecting cycles and unsupported scalar values.
func jsonValue(value mmdbtype.DataType, seen map[jsonContainer]any) any {
	switch v := value.(type) {
	case mmdbtype.Uint128:
		return json.Number(formatUint128(v))
	case mmdbtype.Map:
		if v == nil {
			return nil
		}
		key := jsonContainer{
			pointer: reflect.ValueOf(v).Pointer(),
			length:  len(v),
			kind:    reflect.Map,
		}
		if previous, ok := seen[key]; ok {
			return previous
		}
		result := make(map[string]any, len(v))
		seen[key] = result
		for name, child := range v {
			result[string(name)] = jsonValue(child, seen)
		}
		return result
	case mmdbtype.Slice:
		if v == nil {
			return nil
		}
		key := jsonContainer{
			pointer: reflect.ValueOf(v).Pointer(),
			length:  len(v),
			kind:    reflect.Slice,
		}
		if previous, ok := seen[key]; ok {
			return previous
		}
		result := make([]any, len(v))
		seen[key] = result
		for i, child := range v {
			result[i] = jsonValue(child, seen)
		}
		return result
	default:
		return value
	}
}
