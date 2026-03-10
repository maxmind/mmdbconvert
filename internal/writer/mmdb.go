package writer

import (
	"fmt"
	"maps"
	"net/netip"
	"os"

	"github.com/maxmind/mmdbwriter"
	"github.com/maxmind/mmdbwriter/mmdbtype"
	"go4.org/netipx"

	"github.com/maxmind/mmdbconvert/internal/config"
)

// MMDBWriter writes merged MMDB data to MMDB format.
type MMDBWriter struct {
	tree     *mmdbwriter.Tree
	config   *config.Config
	filePath string
}

// NewMMDBWriter creates a new MMDB writer.
func NewMMDBWriter(outputPath string, cfg *config.Config, ipVersion int) (*MMDBWriter, error) {
	if ipVersion != 4 && ipVersion != 6 {
		return nil, fmt.Errorf("invalid IP version: %d", ipVersion)
	}

	tree, err := mmdbwriter.New(mmdbwriter.Options{
		DatabaseType:            cfg.Output.MMDB.DatabaseType,
		Description:             cfg.Output.MMDB.Description,
		Languages:               cfg.Output.MMDB.Languages,
		RecordSize:              *cfg.Output.MMDB.RecordSize,
		IPVersion:               ipVersion,
		IncludeReservedNetworks: *cfg.Output.MMDB.IncludeReservedNetworks,
	})
	if err != nil {
		return nil, fmt.Errorf("creating MMDB tree: %w", err)
	}

	return &MMDBWriter{
		tree:     tree,
		config:   cfg,
		filePath: outputPath,
	}, nil
}

// WriteRow writes a single row with network prefix and column data.
func (w *MMDBWriter) WriteRow(prefix netip.Prefix, data []mmdbtype.DataType) error {
	nested, err := w.buildNestedData(data)
	if err != nil {
		return fmt.Errorf("building nested data: %w", err)
	}

	ipnet := netipx.PrefixIPNet(prefix)
	if err := w.tree.Insert(ipnet, nested); err != nil {
		return fmt.Errorf("inserting %s: %w", prefix, err)
	}

	return nil
}

// WriteRange writes a range of IP addresses with the same data.
func (w *MMDBWriter) WriteRange(start, end netip.Addr, data []mmdbtype.DataType) error {
	nested, err := w.buildNestedData(data)
	if err != nil {
		return fmt.Errorf("building nested data: %w", err)
	}

	cidrs := netipx.IPRangeFrom(start, end).Prefixes()
	for _, cidr := range cidrs {
		ipnet := netipx.PrefixIPNet(cidr)
		if err := w.tree.Insert(ipnet, nested); err != nil {
			return fmt.Errorf("inserting %s: %w", cidr, err)
		}
	}
	return nil
}

// Flush writes the MMDB tree to disk.
func (w *MMDBWriter) Flush() error {
	f, err := os.Create(w.filePath)
	if err != nil {
		return fmt.Errorf("creating output file: %w", err)
	}
	defer f.Close()

	_, err = w.tree.WriteTo(f)
	if err != nil {
		return fmt.Errorf("writing MMDB to file: %w", err)
	}

	return nil
}

// buildNestedData converts flat column data to nested mmdbtype.Map.
func (w *MMDBWriter) buildNestedData(flatData []mmdbtype.DataType) (mmdbtype.Map, error) {
	root := make(mmdbtype.Map)

	if len(flatData) < len(w.config.Columns) {
		return nil, fmt.Errorf(
			"data slice length %d is less than column count %d",
			len(flatData),
			len(w.config.Columns),
		)
	}

	for i, col := range w.config.Columns {
		value := flatData[i] //nolint:gosec // G602: bounds checked above
		if value == nil {
			continue
		}

		// Use output_path if set, otherwise use [name] for flat structure
		path := col.OutputPath
		if path == nil {
			path = &config.Path{col.Name}
		}

		var err error
		root, err = mergeNestedValue(root, path.Segments(), value)
		if err != nil {
			return nil, fmt.Errorf("setting column %s: %w", col.Name, err)
		}
	}

	return root, nil
}

// mergeNestedValue returns a new map with value merged at the specified path.
// If the value is a Map and a Map already exists at the target location, they are merged.
// Neither root nor value are modified.
func mergeNestedValue(
	root mmdbtype.Map,
	path []any,
	value mmdbtype.DataType,
) (mmdbtype.Map, error) {
	// Special case: empty path means merge into root
	if len(path) == 0 {
		valueMap, ok := value.(mmdbtype.Map)
		if !ok {
			return nil, fmt.Errorf(
				"cannot set non-map value at root with empty path, got %T",
				value,
			)
		}
		return mergeMaps(root, valueMap)
	}

	// Copy root to avoid mutation
	result := make(mmdbtype.Map, len(root))
	maps.Copy(result, root)

	// Navigate to parent, copying and creating nested maps as needed
	current := result
	for i := range len(path) - 1 {
		key, ok := path[i].(string)
		if !ok {
			return nil, fmt.Errorf("non-string key: %v", path[i])
		}

		mmdbKey := mmdbtype.String(key)

		if existing, exists := current[mmdbKey]; exists {
			// Must be a map to navigate further
			existingMap, ok := existing.(mmdbtype.Map)
			if !ok {
				return nil, fmt.Errorf("path conflict at %s: expected map, got %T", key, existing)
			}
			// Copy the nested map to avoid mutation
			next := make(mmdbtype.Map, len(existingMap))
			maps.Copy(next, existingMap)
			current[mmdbKey] = next
			current = next
		} else {
			// Create new nested map
			next := make(mmdbtype.Map)
			current[mmdbKey] = next
			current = next
		}
	}

	// Handle final key
	finalKey, ok := path[len(path)-1].(string)
	if !ok {
		return nil, fmt.Errorf("non-string final key: %v", path[len(path)-1])
	}

	mmdbKey := mmdbtype.String(finalKey)

	// If value is a Map and something already exists at this key, try to merge
	if valueMap, ok := value.(mmdbtype.Map); ok {
		if existing, exists := current[mmdbKey]; exists {
			// If existing is also a Map, merge them
			if existingMap, ok := existing.(mmdbtype.Map); ok {
				merged, err := mergeMaps(existingMap, valueMap)
				if err != nil {
					return nil, err
				}
				current[mmdbKey] = merged
				return result, nil
			}
			// Cannot merge map into non-map
			return nil, fmt.Errorf(
				"cannot merge map into non-map at path %v: existing value is %T",
				path,
				existing,
			)
		}
	}

	// No conflict or not a map - just set the value
	current[mmdbKey] = value
	return result, nil
}

// mergeMaps returns a new map with contents merged from dest and source.
// If both maps have the same key:
// - If both values are maps, merge recursively.
// - Otherwise, return error (fail-fast on conflicts).
// Neither dest nor source are modified.
func mergeMaps(dest, source mmdbtype.Map) (mmdbtype.Map, error) {
	// Pre-allocate for efficiency
	result := make(mmdbtype.Map, len(dest)+len(source))
	maps.Copy(result, dest)

	for key, sourceValue := range source {
		if destValue, exists := result[key]; exists {
			// Both have this key - check if both are maps
			destMap, destIsMap := destValue.(mmdbtype.Map)
			sourceMap, sourceIsMap := sourceValue.(mmdbtype.Map)

			if destIsMap && sourceIsMap {
				// Both are maps - merge recursively
				merged, err := mergeMaps(destMap, sourceMap)
				if err != nil {
					return nil, err
				}
				result[key] = merged
				continue
			}

			// Conflict: same key but at least one is not a map
			return nil, fmt.Errorf(
				"field conflict: key %s already exists (cannot merge %T with %T)",
				key,
				destValue,
				sourceValue,
			)
		}

		// No conflict - add to result
		result[key] = sourceValue
	}

	return result, nil
}
