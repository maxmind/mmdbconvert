package mmdbconvert

import (
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/maxmind/mmdbconvert/internal/config"
	"github.com/maxmind/mmdbconvert/internal/merger"
	"github.com/maxmind/mmdbconvert/internal/mmdb"
	"github.com/maxmind/mmdbconvert/internal/writer"
)

// pendingOutput keeps publication separate from format-specific serialization.
// Cleanup must leave a successfully committed destination in place.
type pendingOutput interface {
	io.Writer
	Commit() error
	Cleanup() error
}

func prepareRowWriter(
	cfg *config.Config,
	readers *mmdb.Readers,
) (merger.RowWriter, []pendingOutput, error) {
	paths := []string{cfg.Output.File}
	versions := []int{writer.IPVersionAny}
	if cfg.Output.IPv4File != "" && cfg.Output.IPv6File != "" {
		paths = []string{cfg.Output.IPv4File, cfg.Output.IPv6File}
		versions = []int{writer.IPVersion4, writer.IPVersion6}
	}

	var outputs []pendingOutput
	var writers []merger.RowWriter
	for i, path := range paths {
		file, err := newPendingOutput(path)
		if err != nil {
			return nil, nil, errors.Join(err, cleanupOutputs(outputs))
		}
		outputs = append(outputs, file)
		rowWriter, err := newRowWriter(file, cfg, readers, versions[i])
		if err != nil {
			return nil, nil, errors.Join(
				fmt.Errorf("preparing output %s: %w", path, err),
				cleanupOutputs(outputs),
			)
		}
		writers = append(writers, rowWriter)
	}
	if len(writers) == 2 {
		return writer.NewSplitRowWriter(writers[0], writers[1]), outputs, nil
	}
	return writers[0], outputs, nil
}

func newRowWriter(
	w io.Writer,
	cfg *config.Config,
	readers *mmdb.Readers,
	ipVersion int,
) (merger.RowWriter, error) {
	switch cfg.Output.Format {
	case config.OutputFormatCSV:
		return writer.NewCSVWriter(w, cfg), nil
	case config.OutputFormatParquet:
		return writer.NewParquetWriterWithIPVersion(w, cfg, ipVersion)
	case config.OutputFormatMMDB:
		version, err := detectIPVersionFromDatabases(cfg, readers)
		if err != nil {
			return nil, err
		}
		return writer.NewMMDBWriter(w, cfg, version)
	default:
		return nil, fmt.Errorf("unsupported output format: %s", cfg.Output.Format)
	}
}

// inspectOutput accepts regular files and returns nil for missing files.
func inspectOutput(path string) (os.FileInfo, error) {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("checking output %s: %w", path, err)
	}
	var kind string
	switch mode := info.Mode(); {
	case mode.IsRegular():
		return info, nil
	case mode&os.ModeSymlink != 0:
		kind = "symlink"
	case mode.IsDir():
		kind = "directory"
	case mode&os.ModeDevice != 0:
		kind = "device"
	case mode&os.ModeNamedPipe != 0:
		kind = "named pipe"
	case mode&os.ModeSocket != 0:
		kind = "socket"
	default:
		kind = "special file"
	}
	return nil, fmt.Errorf("output %s is not a regular file (%s)", path, kind)
}

func flushAndCommit(rowWriter merger.RowWriter, outputs []pendingOutput) error {
	if flusher, ok := rowWriter.(interface{ Flush() error }); ok {
		if err := flusher.Flush(); err != nil {
			return fmt.Errorf("flushing output: %w", err)
		}
	}
	// All writers must flush before any output is published.
	for _, file := range outputs {
		if err := file.Commit(); err != nil {
			return err
		}
	}
	return nil
}

func cleanupOutputs(outputs []pendingOutput) error {
	var errs []error
	for _, file := range outputs {
		if err := file.Cleanup(); err != nil {
			errs = append(errs, fmt.Errorf("cleaning up output: %w", err))
		}
	}
	return errors.Join(errs...)
}
