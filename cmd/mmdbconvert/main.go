// mmdbconvert merges multiple MaxMind MMDB databases and exports to CSV or Parquet format.
package main

import (
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
		fmt.Printf("Output file: %s\n", cfg.Output.File)
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

	var (
		rowWriter   merger.RowWriter
		closers     []io.Closer
		outputPaths []string
	)
	defer func() {
		for _, closer := range closers {
			closer.Close()
		}
	}()

	switch cfg.Output.Format {
	case "csv":
		if !quiet {
			fmt.Println()
			fmt.Println("Creating output file...")
		}
		outputFile, err := os.Create(cfg.Output.File)
		if err != nil {
			return fmt.Errorf("failed to create output file: %w", err)
		}
		closers = append(closers, outputFile)
		outputPaths = append(outputPaths, cfg.Output.File)
		rowWriter = writer.NewCSVWriter(outputFile, cfg)
	case "parquet":
		if cfg.Output.Parquet.SeparateIPVersions {
			if !quiet {
				fmt.Println()
				fmt.Println("Creating output files...")
			}
			ipv4Path, ipv6Path := splitOutputPaths(cfg.Output.File)
			// #nosec G304 -- paths come from trusted configuration
			ipv4File, err := os.Create(ipv4Path)
			if err != nil {
				return fmt.Errorf("failed to create IPv4 output file: %w", err)
			}
			closers = append(closers, ipv4File)
			// #nosec G304 -- paths come from trusted configuration
			ipv6File, err := os.Create(ipv6Path)
			if err != nil {
				return fmt.Errorf("failed to create IPv6 output file: %w", err)
			}
			closers = append(closers, ipv6File)
			outputPaths = append(outputPaths, ipv4Path, ipv6Path)

			ipv4Writer, err := writer.NewParquetWriterWithIPVersion(
				ipv4File,
				cfg,
				writer.IPVersion4,
			)
			if err != nil {
				return fmt.Errorf("failed to create IPv4 Parquet writer: %w", err)
			}
			ipv6Writer, err := writer.NewParquetWriterWithIPVersion(
				ipv6File,
				cfg,
				writer.IPVersion6,
			)
			if err != nil {
				return fmt.Errorf("failed to create IPv6 Parquet writer: %w", err)
			}
			rowWriter = writer.NewSplitRowWriter(ipv4Writer, ipv6Writer)
		} else {
			if !quiet {
				fmt.Println()
				fmt.Println("Creating output file...")
			}
			// #nosec G304 -- paths come from trusted configuration
			outputFile, err := os.Create(cfg.Output.File)
			if err != nil {
				return fmt.Errorf("failed to create output file: %w", err)
			}
			closers = append(closers, outputFile)
			outputPaths = append(outputPaths, cfg.Output.File)

			parquetWriter, err := writer.NewParquetWriter(outputFile, cfg)
			if err != nil {
				return fmt.Errorf("failed to create Parquet writer: %w", err)
			}
			rowWriter = parquetWriter
		}
	default:
		return fmt.Errorf("unsupported output format: %s", cfg.Output.Format)
	}

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

func splitOutputPaths(base string) (ipv4Path, ipv6Path string) {
	ext := filepath.Ext(base)
	name := strings.TrimSuffix(base, ext)
	if name == "" {
		name = base
	}
	if ext == "" {
		ext = ".parquet"
	}
	ipv4Path = fmt.Sprintf("%s_ipv4%s", name, ext)
	ipv6Path = fmt.Sprintf("%s_ipv6%s", name, ext)
	return ipv4Path, ipv6Path
}

func usage() {
	fmt.Fprint(os.Stderr, `mmdbconvert - Merge MaxMind MMDB databases and export to CSV or Parquet

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
