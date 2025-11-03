package writer

import (
	"errors"
	"fmt"
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
func (w *MMDBWriter) WriteRow(prefix netip.Prefix, data map[string]any) error {
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
func (w *MMDBWriter) WriteRange(start, end netip.Addr, data map[string]any) error {
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
func (w *MMDBWriter) buildNestedData(flatData map[string]any) (mmdbtype.Map, error) {
	root := make(mmdbtype.Map)

	for _, col := range w.config.Columns {
		value := flatData[col.Name]
		if value == nil {
			continue
		}

		// Value should already be mmdbtype.DataType - use as-is!
		dt, ok := value.(mmdbtype.DataType)
		if !ok {
			return nil, fmt.Errorf("column %s: expected mmdbtype.DataType, got %T", col.Name, value)
		}

		// Use output_path if set, otherwise use [name] for flat structure
		path := col.OutputPath
		if path == nil {
			path = &config.Path{col.Name}
		}

		if err := setNestedValue(root, path.Segments(), dt); err != nil {
			return nil, fmt.Errorf("setting column %s: %w", col.Name, err)
		}
	}

	return root, nil
}

// setNestedValue sets a mmdbtype.DataType at a nested path in mmdbtype.Map.
func setNestedValue(root mmdbtype.Map, path []any, value mmdbtype.DataType) error {
	if len(path) == 0 {
		return errors.New("empty path")
	}

	// Navigate to parent, creating nested maps as needed
	current := root
	for i := range len(path) - 1 {
		key, ok := path[i].(string)
		if !ok {
			return fmt.Errorf("non-string key: %v", path[i])
		}

		mmdbKey := mmdbtype.String(key)

		// Create nested map if needed
		if _, exists := current[mmdbKey]; !exists {
			current[mmdbKey] = make(mmdbtype.Map)
		}

		// Must be a map
		next, ok := current[mmdbKey].(mmdbtype.Map)
		if !ok {
			return fmt.Errorf("path conflict at %s: expected map, got %T", key, current[mmdbKey])
		}
		current = next
	}

	// Set final value
	finalKey, ok := path[len(path)-1].(string)
	if !ok {
		return fmt.Errorf("non-string final key: %v", path[len(path)-1])
	}

	current[mmdbtype.String(finalKey)] = value
	return nil
}
