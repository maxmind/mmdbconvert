// mmdbconvert merges multiple MaxMind MMDB databases and exports to CSV, Parquet, or MMDB format.
package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"runtime/pprof"
	"time"

	"golang.org/x/term"

	"github.com/maxmind/mmdbconvert"
)

const (
	unknownVersion = "unknown"
	logFormatAuto  = "auto"
	logFormatJSON  = "json"
	logFormatText  = "text"
)

var version = unknownVersion

func main() {
	os.Exit(runCLI(os.Args[1:], os.Stdout, os.Stderr, term.IsTerminal(int(os.Stderr.Fd()))))
}

func runCLI(args []string, stdout, stderr io.Writer, stderrIsTerminal bool) int {
	// Define command-line flags
	var (
		configPath   string
		logFormat    string
		quiet        bool
		showHelp     bool
		showVer      bool
		cpuprofile   string
		memprofile   string
		disableCache bool
	)

	flags := flag.NewFlagSet("mmdbconvert", flag.ContinueOnError)
	flags.StringVar(&configPath, "config", "", "Path to TOML configuration file")
	flags.StringVar(
		&logFormat,
		"log-format",
		logFormatAuto,
		"Diagnostic format: auto, json, or text",
	)
	flags.BoolVar(&quiet, "quiet", false, "Suppress progress output")
	flags.BoolVar(&showHelp, "help", false, "Show usage information")
	flags.BoolVar(&showVer, "version", false, "Show version information")
	flags.StringVar(&cpuprofile, "cpuprofile", "", "Write CPU profile to file")
	flags.StringVar(&memprofile, "memprofile", "", "Write memory profile to file")
	flags.BoolVar(
		&disableCache,
		"disable-cache",
		false,
		"Disable MMDB unmarshaler caching to reduce memory usage (several times slower)",
	)

	// Handle parser errors ourselves so usage text cannot leak into JSON logs.
	flags.SetOutput(io.Discard)
	flags.Usage = func() {}
	parseErr := flags.Parse(args)
	jsonLogs := !stderrIsTerminal
	switch logFormat {
	case logFormatAuto:
	case logFormatJSON:
		jsonLogs = true
	case logFormatText:
		jsonLogs = false
	default:
		parseErr = fmt.Errorf("invalid log format %q (expected auto, json, or text)", logFormat)
	}
	if errors.Is(parseErr, flag.ErrHelp) {
		usage(stderr)
		return 0
	}
	logger := newLogger(stderr, jsonLogs, quiet)
	if parseErr != nil {
		logger.Error("Parsing command-line flags", "error", parseErr)
		if !jsonLogs {
			usage(stderr)
		}
		return 2
	}

	// Handle version flag
	if showVer {
		if _, err := fmt.Fprintf(stdout, "mmdbconvert version %s\n", version); err != nil {
			logger.Error("Writing version", "error", err)
			return 1
		}
		return 0
	}

	// Handle help flag
	if showHelp {
		usage(stderr)
		return 0
	}

	// Get config path from positional argument if not specified with flag
	if configPath == "" {
		if flags.NArg() == 0 {
			logger.Error("Config file path required")
			if !jsonLogs {
				usage(stderr)
			}
			return 1
		}
		configPath = flags.Arg(0)
	}

	// Start CPU profiling if requested
	var cpuProfileFile *os.File
	if cpuprofile != "" {
		// #nosec G304 -- cpuprofile path comes from trusted command-line flag
		f, err := os.Create(cpuprofile)
		if err != nil {
			logger.Error("Creating CPU profile", "error", err)
			return 1
		}
		cpuProfileFile = f
		if err := pprof.StartCPUProfile(f); err != nil {
			logger.Error("Starting CPU profile", "error", err)
			f.Close()
			return 1
		}
	}

	// Run the conversion
	runErr := run(configPath, disableCache, logger)

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
			logger.Error("Creating memory profile", "error", err)
			return 1
		}
		if err := pprof.WriteHeapProfile(f); err != nil {
			f.Close()
			logger.Error("Writing memory profile", "error", err)
			return 1
		}
		f.Close()
	}

	// Check for run errors after profiling is complete
	if runErr != nil {
		logger.Error("Converting databases", "error", runErr)
		return 1
	}
	return 0
}

// run performs the main conversion process.
func run(configPath string, disableCache bool, logger *slog.Logger) error {
	startTime := time.Now()

	logger.Info("mmdbconvert", "version", version)
	logger.Info("Loading configuration", "config_path", configPath)
	logger.Info("Merging databases and writing output")
	if disableCache {
		logger.Info("Unmarshaler caching disabled", "disable_cache", true)
	}

	err := mmdbconvert.Run(mmdbconvert.Options{
		ConfigPath:   configPath,
		DisableCache: disableCache,
	})
	if err != nil {
		return err
	}

	logger.Info(
		"Successfully completed",
		"elapsed_ms",
		time.Since(startTime).Round(time.Millisecond).Milliseconds(),
	)

	return nil
}

func usage(w io.Writer) {
	//nolint:errcheck // Usage is best effort; a write error means stderr is unavailable.
	fmt.Fprint(
		w,
		`mmdbconvert - Merge MaxMind MMDB databases and export to CSV, Parquet, or MMDB

USAGE:
    mmdbconvert [OPTIONS] <config-file>
    mmdbconvert --config <config-file> [OPTIONS]

OPTIONS:
    --config <file>         Path to TOML configuration file
    --log-format <format>   Diagnostic format: auto (default), json, or text
    --quiet                 Suppress progress output; errors remain visible
    --disable-cache         Disable MMDB unmarshaler caching to reduce memory (several times slower)
    --cpuprofile <file>      Write CPU profile to file
    --memprofile <file>      Write memory profile to file
    --help                  Show this help message
    --version               Show version information

LOGGING:
    Progress and errors go to stderr. The auto format selects text when stderr
    is a terminal and JSON otherwise. JSON records contain time, level, and
    message fields, plus relevant attributes such as error or elapsed_ms.
    Explicit help and version output remain plain text.

EXAMPLES:
    # Basic usage with config file
    mmdbconvert config.toml

    # Using explicit flag
    mmdbconvert --config config.toml

    # Suppress progress output
    mmdbconvert --config config.toml --quiet

    # Force a diagnostic format
    mmdbconvert --log-format=json config.toml
    mmdbconvert --log-format=text config.toml

    # Profile performance
    mmdbconvert --config config.toml --cpuprofile cpu.prof --memprofile mem.prof --quiet

CONFIGURATION:
    See docs/config.md for configuration file format and options.

MORE INFORMATION:
    Documentation: https://github.com/maxmind/mmdbconvert
    Report issues: https://github.com/maxmind/mmdbconvert/issues

`)
}
