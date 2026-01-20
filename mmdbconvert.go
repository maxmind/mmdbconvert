// Package mmdbconvert provides a Go library for merging MaxMind MMDB databases
// and exporting the merged data to CSV, Parquet, or MMDB format.
package mmdbconvert

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/maxmind/mmdbconvert/internal/config"
	"github.com/maxmind/mmdbconvert/internal/merger"
	"github.com/maxmind/mmdbconvert/internal/mmdb"
	"github.com/maxmind/mmdbconvert/internal/writer"
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

	rowWriter, closers, err := prepareRowWriter(cfg, readers)
	if err != nil {
		return err
	}
	defer func() {
		for _, closer := range closers {
			closer.Close()
		}
	}()

	m, err := merger.NewMerger(readers, cfg, rowWriter)
	if err != nil {
		return fmt.Errorf("creating merger: %w", err)
	}
	if err := m.Merge(); err != nil {
		return fmt.Errorf("merging databases: %w", err)
	}

	if flusher, ok := rowWriter.(interface{ Flush() error }); ok {
		if err := flusher.Flush(); err != nil {
			return fmt.Errorf("flushing output: %w", err)
		}
	}

	return nil
}

func prepareRowWriter(
	cfg *config.Config,
	readers *mmdb.Readers,
) (merger.RowWriter, []io.Closer, error) {
	var closers []io.Closer

	closeAll := func() {
		for _, closer := range closers {
			closer.Close()
		}
	}

	switch cfg.Output.Format {
	case "csv":
		if cfg.Output.IPv4File != "" && cfg.Output.IPv6File != "" {
			ipv4Path, ipv6Path := splitConfiguredPaths(
				cfg.Output.File,
				cfg.Output.IPv4File,
				cfg.Output.IPv6File,
			)
			ipv4File, err := createOutputFile(ipv4Path)
			if err != nil {
				closeAll()
				return nil, nil, fmt.Errorf("creating IPv4 output file: %w", err)
			}
			closers = append(closers, ipv4File)

			ipv6File, err := createOutputFile(ipv6Path)
			if err != nil {
				closeAll()
				return nil, nil, fmt.Errorf("creating IPv6 output file: %w", err)
			}
			closers = append(closers, ipv6File)

			return writer.NewSplitRowWriter(
				writer.NewCSVWriter(ipv4File, cfg),
				writer.NewCSVWriter(ipv6File, cfg),
			), closers, nil
		}

		outputFile, err := createOutputFile(cfg.Output.File)
		if err != nil {
			closeAll()
			return nil, nil, fmt.Errorf("creating output file: %w", err)
		}
		closers = append(closers, outputFile)
		return writer.NewCSVWriter(outputFile, cfg), closers, nil

	case "parquet":
		if cfg.Output.IPv4File != "" && cfg.Output.IPv6File != "" {
			ipv4Path, ipv6Path := splitConfiguredPaths(
				cfg.Output.File,
				cfg.Output.IPv4File,
				cfg.Output.IPv6File,
			)

			ipv4File, err := createOutputFile(ipv4Path)
			if err != nil {
				closeAll()
				return nil, nil, fmt.Errorf("creating IPv4 output file: %w", err)
			}
			closers = append(closers, ipv4File)

			ipv6File, err := createOutputFile(ipv6Path)
			if err != nil {
				closeAll()
				return nil, nil, fmt.Errorf("creating IPv6 output file: %w", err)
			}
			closers = append(closers, ipv6File)

			ipv4Writer, err := writer.NewParquetWriterWithIPVersion(
				ipv4File,
				cfg,
				writer.IPVersion4,
			)
			if err != nil {
				closeAll()
				return nil, nil, fmt.Errorf("creating IPv4 Parquet writer: %w", err)
			}
			ipv6Writer, err := writer.NewParquetWriterWithIPVersion(
				ipv6File,
				cfg,
				writer.IPVersion6,
			)
			if err != nil {
				closeAll()
				return nil, nil, fmt.Errorf("creating IPv6 Parquet writer: %w", err)
			}
			return writer.NewSplitRowWriter(ipv4Writer, ipv6Writer), closers, nil
		}

		outputFile, err := createOutputFile(cfg.Output.File)
		if err != nil {
			closeAll()
			return nil, nil, fmt.Errorf("creating output file: %w", err)
		}
		closers = append(closers, outputFile)

		parquetWriter, err := writer.NewParquetWriter(outputFile, cfg)
		if err != nil {
			closeAll()
			return nil, nil, fmt.Errorf("creating Parquet writer: %w", err)
		}
		return parquetWriter, closers, nil

	case "mmdb":
		ipVersion, err := detectIPVersionFromDatabases(cfg, readers)
		if err != nil {
			closeAll()
			return nil, nil, fmt.Errorf("detecting IP version: %w", err)
		}

		mmdbWriter, err := writer.NewMMDBWriter(cfg.Output.File, cfg, ipVersion)
		if err != nil {
			closeAll()
			return nil, nil, fmt.Errorf("creating MMDB writer: %w", err)
		}

		return mmdbWriter, closers, nil
	}

	closeAll()
	return nil, nil, fmt.Errorf("unsupported output format: %s", cfg.Output.Format)
}

func createOutputFile(path string) (*os.File, error) {
	// #nosec G304 -- paths come from trusted configuration
	file, err := os.Create(path)
	if err != nil {
		return nil, fmt.Errorf("creating %s: %w", path, err)
	}
	return file, nil
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
	//nolint:gosec // IPVersion is always 4 or 6, no overflow risk
	ipVersion := int(metadata.IPVersion)

	if ipVersion != 4 && ipVersion != 6 {
		return 0, fmt.Errorf("invalid IP version %d in database '%s'", ipVersion, firstDB)
	}

	return ipVersion, nil
}

func splitConfiguredPaths(base, ipv4Override, ipv6Override string) (ipv4, ipv6 string) {
	ext := filepath.Ext(base)
	name := strings.TrimSuffix(base, ext)
	if name == "" {
		name = base
	}
	if ext == "" {
		ext = ".parquet"
	}

	defaultIPv4 := fmt.Sprintf("%s_ipv4%s", name, ext)
	defaultIPv6 := fmt.Sprintf("%s_ipv6%s", name, ext)

	ipv4 = ipv4Override
	if ipv4 == "" {
		ipv4 = defaultIPv4
	}
	ipv6 = ipv6Override
	if ipv6 == "" {
		ipv6 = defaultIPv6
	}

	return ipv4, ipv6
}

func validateParquetNetworkColumns(cfg *config.Config, readers *mmdb.Readers) error {
	if cfg.Output.Format != "parquet" {
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
		case writer.NetworkColumnStartInt, writer.NetworkColumnEndInt:
			return true
		}
	}
	return false
}
