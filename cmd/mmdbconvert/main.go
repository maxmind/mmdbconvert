// mmdbconvert merges multiple MaxMind MMDB databases and exports to CSV, Parquet, or MMDB format.
package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/maxmind/mmdbconvert/internal/config"
	"github.com/maxmind/mmdbconvert/internal/merger"
	"github.com/maxmind/mmdbconvert/internal/mmdb"
	"github.com/maxmind/mmdbconvert/internal/writer"
)

const version = "0.1.0"

func main() {
	// Define command-line flags
	var (
		configPath string
		quiet      bool
		showHelp   bool
		showVer    bool
	)

	flag.StringVar(&configPath, "config", "", "Path to TOML configuration file")
	flag.BoolVar(&quiet, "quiet", false, "Suppress progress output")
	flag.BoolVar(&showHelp, "help", false, "Show usage information")
	flag.BoolVar(&showVer, "version", false, "Show version information")

	flag.Usage = usage
	flag.Parse()

	// Handle version flag
	if showVer {
		fmt.Printf("mmdbconvert version %s\n", version)
		os.Exit(0)
	}

	// Handle help flag
	if showHelp {
		usage()
		os.Exit(0)
	}

	// Get config path from positional argument if not specified with flag
	if configPath == "" {
		if flag.NArg() == 0 {
			fmt.Fprint(os.Stderr, "Error: config file path required\n\n")
			usage()
			os.Exit(1)
		}
		configPath = flag.Arg(0)
	}

	// Run the conversion
	if err := run(configPath, quiet); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

// run performs the main conversion process.
func run(configPath string, quiet bool) error {
	startTime := time.Now()

	if !quiet {
		fmt.Printf("mmdbconvert v%s\n", version)
		fmt.Printf("Loading configuration from %s...\n", configPath)
	}

	// Load configuration
	cfg, err := config.LoadConfig(configPath)
	if err != nil {
		return fmt.Errorf("failed to load config: %w", err)
	}

	if !quiet {
		fmt.Printf("Output format: %s\n", cfg.Output.Format)
		if cfg.Output.File != "" {
			fmt.Printf("Output file: %s\n", cfg.Output.File)
		} else {
			fmt.Printf("Output files: IPv4=%s, IPv6=%s\n", cfg.Output.IPv4File, cfg.Output.IPv6File)
		}
		fmt.Printf("Databases: %d\n", len(cfg.Databases))
		fmt.Printf("Data columns: %d\n", len(cfg.Columns))
		fmt.Printf("Network columns: %d\n", len(cfg.Network.Columns))
		fmt.Println()
	}

	// Open MMDB databases
	if !quiet {
		fmt.Println("Opening MMDB databases...")
	}

	databases := map[string]string{}
	for _, db := range cfg.Databases {
		databases[db.Name] = db.Path
		if !quiet {
			fmt.Printf("  - %s: %s\n", db.Name, db.Path)
		}
	}

	readers, err := mmdb.OpenDatabases(databases)
	if err != nil {
		return fmt.Errorf("failed to open databases: %w", err)
	}
	defer readers.Close()

	rowWriter, closers, outputPaths, err := prepareRowWriter(cfg, readers, quiet)
	if err != nil {
		return err
	}
	defer func() {
		for _, closer := range closers {
			closer.Close()
		}
	}()

	if !quiet {
		fmt.Println("Merging databases and writing output...")
	}

	// Create merger and run
	m := merger.NewMerger(readers, cfg, rowWriter)
	if err := m.Merge(); err != nil {
		return fmt.Errorf("merge failed: %w", err)
	}

	// Flush writer
	if flusher, ok := rowWriter.(interface{ Flush() error }); ok {
		if err := flusher.Flush(); err != nil {
			return fmt.Errorf("failed to flush output: %w", err)
		}
	}

	if !quiet {
		elapsed := time.Since(startTime)
		fmt.Println()
		fmt.Printf("✓ Successfully completed in %v\n", elapsed.Round(time.Millisecond))
		if len(outputPaths) == 1 {
			fmt.Printf("Output written to: %s\n", outputPaths[0])
		} else {
			fmt.Println("Output written to:")
			for _, path := range outputPaths {
				fmt.Printf("  - %s\n", path)
			}
		}
	}

	return nil
}

func prepareRowWriter(
	cfg *config.Config,
	readers *mmdb.Readers,
	quiet bool,
) (merger.RowWriter, []io.Closer, []string, error) {
	var (
		closers     []io.Closer
		outputPaths []string
	)

	closeAll := func() {
		for _, closer := range closers {
			closer.Close()
		}
	}

	switch cfg.Output.Format {
	case "csv":
		if cfg.Output.IPv4File != "" && cfg.Output.IPv6File != "" {
			if !quiet {
				fmt.Println()
				fmt.Println("Creating output files...")
			}
			ipv4Path, ipv6Path := splitConfiguredPaths(
				cfg.Output.File,
				cfg.Output.IPv4File,
				cfg.Output.IPv6File,
			)
			ipv4File, err := createOutputFile(ipv4Path)
			if err != nil {
				closeAll()
				return nil, nil, nil, fmt.Errorf("failed to create IPv4 output file: %w", err)
			}
			closers = append(closers, ipv4File)
			outputPaths = append(outputPaths, ipv4Path)

			ipv6File, err := createOutputFile(ipv6Path)
			if err != nil {
				closeAll()
				return nil, nil, nil, fmt.Errorf("failed to create IPv6 output file: %w", err)
			}
			closers = append(closers, ipv6File)
			outputPaths = append(outputPaths, ipv6Path)

			return writer.NewSplitRowWriter(
				writer.NewCSVWriter(ipv4File, cfg),
				writer.NewCSVWriter(ipv6File, cfg),
			), closers, outputPaths, nil
		}

		if !quiet {
			fmt.Println()
			fmt.Println("Creating output file...")
		}
		outputFile, err := createOutputFile(cfg.Output.File)
		if err != nil {
			closeAll()
			return nil, nil, nil, fmt.Errorf("failed to create output file: %w", err)
		}
		closers = append(closers, outputFile)
		outputPaths = append(outputPaths, cfg.Output.File)
		return writer.NewCSVWriter(outputFile, cfg), closers, outputPaths, nil

	case "parquet":
		if cfg.Output.IPv4File != "" && cfg.Output.IPv6File != "" {
			if !quiet {
				fmt.Println()
				fmt.Println("Creating output files...")
			}
			ipv4Path, ipv6Path := splitConfiguredPaths(
				cfg.Output.File,
				cfg.Output.IPv4File,
				cfg.Output.IPv6File,
			)

			ipv4File, err := createOutputFile(ipv4Path)
			if err != nil {
				closeAll()
				return nil, nil, nil, fmt.Errorf("failed to create IPv4 output file: %w", err)
			}
			closers = append(closers, ipv4File)
			outputPaths = append(outputPaths, ipv4Path)

			ipv6File, err := createOutputFile(ipv6Path)
			if err != nil {
				closeAll()
				return nil, nil, nil, fmt.Errorf("failed to create IPv6 output file: %w", err)
			}
			closers = append(closers, ipv6File)
			outputPaths = append(outputPaths, ipv6Path)

			ipv4Writer, err := writer.NewParquetWriterWithIPVersion(
				ipv4File,
				cfg,
				writer.IPVersion4,
			)
			if err != nil {
				closeAll()
				return nil, nil, nil, fmt.Errorf("failed to create IPv4 Parquet writer: %w", err)
			}
			ipv6Writer, err := writer.NewParquetWriterWithIPVersion(
				ipv6File,
				cfg,
				writer.IPVersion6,
			)
			if err != nil {
				closeAll()
				return nil, nil, nil, fmt.Errorf("failed to create IPv6 Parquet writer: %w", err)
			}
			return writer.NewSplitRowWriter(ipv4Writer, ipv6Writer), closers, outputPaths, nil
		}

		if !quiet {
			fmt.Println()
			fmt.Println("Creating output file...")
		}
		outputFile, err := createOutputFile(cfg.Output.File)
		if err != nil {
			closeAll()
			return nil, nil, nil, fmt.Errorf("failed to create output file: %w", err)
		}
		closers = append(closers, outputFile)
		outputPaths = append(outputPaths, cfg.Output.File)

		parquetWriter, err := writer.NewParquetWriter(outputFile, cfg)
		if err != nil {
			closeAll()
			return nil, nil, nil, fmt.Errorf("failed to create Parquet writer: %w", err)
		}
		return parquetWriter, closers, outputPaths, nil

	case "mmdb":
		if !quiet {
			fmt.Println()
			fmt.Println("Creating output file...")
		}

		// Detect IP version from databases
		ipVersion, err := detectIPVersionFromDatabases(cfg, readers)
		if err != nil {
			closeAll()
			return nil, nil, nil, fmt.Errorf("detecting IP version: %w", err)
		}

		mmdbWriter, err := writer.NewMMDBWriter(cfg.Output.File, cfg, ipVersion)
		if err != nil {
			closeAll()
			return nil, nil, nil, fmt.Errorf("creating MMDB writer: %w", err)
		}

		outputPaths = append(outputPaths, cfg.Output.File)
		return mmdbWriter, closers, outputPaths, nil
	}

	closeAll()
	return nil, nil, nil, fmt.Errorf("unsupported output format: %s", cfg.Output.Format)
}

func createOutputFile(path string) (*os.File, error) {
	// #nosec G304 -- paths come from trusted configuration
	file, err := os.Create(path)
	if err != nil {
		return nil, fmt.Errorf("failed to create %s: %w", path, err)
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

func usage() {
	fmt.Fprint(
		os.Stderr,
		`mmdbconvert - Merge MaxMind MMDB databases and export to CSV, Parquet, or MMDB

USAGE:
    mmdbconvert [OPTIONS] <config-file>
    mmdbconvert --config <config-file> [OPTIONS]

OPTIONS:
    --config <file>    Path to TOML configuration file
    --quiet            Suppress progress output
    --help             Show this help message
    --version          Show version information

EXAMPLES:
    # Basic usage with config file
    mmdbconvert config.toml

    # Using explicit flag
    mmdbconvert --config config.toml

    # Suppress progress output
    mmdbconvert --config config.toml --quiet

CONFIGURATION:
    See docs/config.md for configuration file format and options.

MORE INFORMATION:
    Documentation: https://github.com/maxmind/mmdbconvert
    Report issues: https://github.com/maxmind/mmdbconvert/issues

`)
}
