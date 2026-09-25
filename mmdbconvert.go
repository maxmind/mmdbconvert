// Package mmdbconvert provides a Go library for merging MaxMind MMDB databases
// and exporting the merged data to CSV, Parquet, or MMDB format.
package mmdbconvert

import (
	"errors"
	"fmt"

	"github.com/maxmind/mmdbconvert/internal/config"
	"github.com/maxmind/mmdbconvert/internal/merger"
	"github.com/maxmind/mmdbconvert/internal/mmdb"
)

// Options configures the conversion behavior.
type Options struct {
	// ConfigPath is the path to a TOML configuration file (required).
	ConfigPath string

	// DisableCache disables MMDB unmarshaler caching to reduce memory usage.
	// This makes processing several times slower but uses less memory.
	DisableCache bool
}

// Run performs the MMDB conversion using the specified options.
func Run(opts Options) error {
	if opts.ConfigPath == "" {
		return errors.New("config path is required")
	}

	cfg, err := config.LoadConfig(opts.ConfigPath)
	if err != nil {
		return fmt.Errorf("loading config: %w", err)
	}

	if opts.DisableCache {
		cfg.DisableCache = true
	}

	databases := make(map[string]string, len(cfg.Databases))
	for _, db := range cfg.Databases {
		databases[db.Name] = db.Path
	}

	readers, err := mmdb.OpenDatabases(databases)
	if err != nil {
		return fmt.Errorf("opening databases: %w", err)
	}
	defer readers.Close()

	if err := validateParquetNetworkColumns(cfg, readers); err != nil {
		return fmt.Errorf("validating network columns: %w", err)
	}

	rowWriter, outputs, err := prepareRowWriter(cfg, readers)
	if err != nil {
		return err
	}
	return convert(cfg, readers, rowWriter, outputs)
}

// convert owns the prepared outputs through conversion, publication, and cleanup.
func convert(
	cfg *config.Config,
	readers *mmdb.Readers,
	rowWriter merger.RowWriter,
	outputs []pendingOutput,
) (retErr error) {
	defer func() {
		retErr = errors.Join(retErr, cleanupOutputs(outputs))
	}()

	m, err := merger.NewMerger(readers, cfg, rowWriter)
	if err != nil {
		return fmt.Errorf("creating merger: %w", err)
	}
	if err := m.Merge(); err != nil {
		return fmt.Errorf("merging databases: %w", err)
	}

	return flushAndCommit(rowWriter, outputs)
}

func detectIPVersionFromDatabases(cfg *config.Config, readers *mmdb.Readers) (int, error) {
	// Get the first database from config to detect IP version
	// In practice, all databases in the merge should have the same IP version
	// due to validation in merger
	if len(cfg.Databases) == 0 {
		return 0, errors.New("no databases configured")
	}

	firstDB := cfg.Databases[0].Name
	reader, ok := readers.Get(firstDB)
	if !ok {
		return 0, fmt.Errorf("database '%s' not found", firstDB)
	}

	metadata := reader.Metadata()
	ipVersion := int(metadata.IPVersion)

	if ipVersion != 4 && ipVersion != 6 {
		return 0, fmt.Errorf("invalid IP version %d in database '%s'", ipVersion, firstDB)
	}

	return ipVersion, nil
}

func validateParquetNetworkColumns(cfg *config.Config, readers *mmdb.Readers) error {
	if cfg.Output.Format != config.OutputFormatParquet {
		return nil
	}

	if !hasIntegerNetworkColumns(cfg.Network.Columns) {
		return nil
	}

	// Already split output, so integer columns are safe (each writer enforces a single IP family).
	if cfg.Output.IPv4File != "" && cfg.Output.IPv6File != "" {
		return nil
	}

	ipVersion, err := detectIPVersionFromDatabases(cfg, readers)
	if err != nil {
		return err
	}

	if ipVersion == 6 {
		return errors.New(
			"network column types 'start_int' and 'end_int' require split IPv4/IPv6 outputs when processing IPv6 databases; set output.ipv4_file and output.ipv6_file or switch to start_ip/end_ip",
		)
	}

	return nil
}

func hasIntegerNetworkColumns(cols []config.NetworkColumn) bool {
	for _, col := range cols {
		switch col.Type {
		case config.NetworkColumnTypeStartInt, config.NetworkColumnTypeEndInt:
			return true
		}
	}
	return false
}
