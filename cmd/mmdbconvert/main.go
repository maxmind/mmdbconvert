// mmdbconvert merges multiple MaxMind MMDB databases and exports to CSV, Parquet, or MMDB format.
package main

import (
	"flag"
	"fmt"
	"os"
	"runtime/pprof"
	"time"

	"github.com/maxmind/mmdbconvert"
)

const version = "0.1.0"

func main() {
	// Define command-line flags
	var (
		configPath   string
		quiet        bool
		showHelp     bool
		showVer      bool
		cpuprofile   string
		memprofile   string
		disableCache bool
	)

	flag.StringVar(&configPath, "config", "", "Path to TOML configuration file")
	flag.BoolVar(&quiet, "quiet", false, "Suppress progress output")
	flag.BoolVar(&showHelp, "help", false, "Show usage information")
	flag.BoolVar(&showVer, "version", false, "Show version information")
	flag.StringVar(&cpuprofile, "cpuprofile", "", "Write CPU profile to file")
	flag.StringVar(&memprofile, "memprofile", "", "Write memory profile to file")
	flag.BoolVar(
		&disableCache,
		"disable-cache",
		false,
		"Disable MMDB unmarshaler caching to reduce memory usage (several times slower)",
	)

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

	// Start CPU profiling if requested
	var cpuProfileFile *os.File
	if cpuprofile != "" {
		// #nosec G304 -- cpuprofile path comes from trusted command-line flag
		f, err := os.Create(cpuprofile)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error creating CPU profile: %v\n", err)
			os.Exit(1)
		}
		cpuProfileFile = f
		if err := pprof.StartCPUProfile(f); err != nil {
			fmt.Fprintf(os.Stderr, "Error starting CPU profile: %v\n", err)
			f.Close()
			os.Exit(1)
		}
	}

	// Run the conversion
	runErr := run(configPath, quiet, disableCache)

	// Stop CPU profiling and close file before potentially exiting
	if cpuProfileFile != nil {
		pprof.StopCPUProfile()
		cpuProfileFile.Close()
	}

	// Write memory profile if requested
	if memprofile != "" {
		// #nosec G304 -- memprofile path comes from trusted command-line flag
		f, err := os.Create(memprofile)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error creating memory profile: %v\n", err)
			os.Exit(1)
		}
		if err := pprof.WriteHeapProfile(f); err != nil {
			f.Close()
			fmt.Fprintf(os.Stderr, "Error writing memory profile: %v\n", err)
			os.Exit(1)
		}
		f.Close()
	}

	// Check for run errors after profiling is complete
	if runErr != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", runErr)
		os.Exit(1)
	}
}

// run performs the main conversion process.
func run(configPath string, quiet, disableCache bool) error {
	startTime := time.Now()

	if !quiet {
		fmt.Printf("mmdbconvert v%s\n", version)
		fmt.Printf("Loading configuration from %s...\n", configPath)
		fmt.Println("Merging databases and writing output...")
		if disableCache {
			fmt.Println("  (unmarshaler caching disabled)")
		}
	}

	err := mmdbconvert.Run(mmdbconvert.Options{
		ConfigPath:   configPath,
		DisableCache: disableCache,
	})
	if err != nil {
		return err
	}

	if !quiet {
		elapsed := time.Since(startTime)
		fmt.Println()
		fmt.Printf("✓ Successfully completed in %v\n", elapsed.Round(time.Millisecond))
	}

	return nil
}

func usage() {
	fmt.Fprint(
		os.Stderr,
		`mmdbconvert - Merge MaxMind MMDB databases and export to CSV, Parquet, or MMDB

USAGE:
    mmdbconvert [OPTIONS] <config-file>
    mmdbconvert --config <config-file> [OPTIONS]

OPTIONS:
    --config <file>        Path to TOML configuration file
    --quiet                Suppress progress output
    --disable-cache        Disable MMDB unmarshaler caching to reduce memory (several times slower)
    --cpuprofile <file>    Write CPU profile to file
    --memprofile <file>    Write memory profile to file
    --help                 Show this help message
    --version              Show version information

EXAMPLES:
    # Basic usage with config file
    mmdbconvert config.toml

    # Using explicit flag
    mmdbconvert --config config.toml

    # Suppress progress output
    mmdbconvert --config config.toml --quiet

    # Profile performance
    mmdbconvert --config config.toml --cpuprofile cpu.prof --memprofile mem.prof --quiet

CONFIGURATION:
    See docs/config.md for configuration file format and options.

MORE INFORMATION:
    Documentation: https://github.com/maxmind/mmdbconvert
    Report issues: https://github.com/maxmind/mmdbconvert/issues

`)
}
