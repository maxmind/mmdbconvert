//go:build !windows

package mmdbconvert

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/google/renameio/v2"
)

type pendingFile struct {
	*renameio.PendingFile

	path string
}

func newPendingOutput(path string) (*pendingFile, error) {
	return createPendingOutput(path, restoreOutputPermissions)
}

func createPendingOutput(
	path string,
	restorePermissions func(*os.File, os.FileMode) error,
) (_ *pendingFile, retErr error) {
	info, err := inspectOutput(path)
	if err != nil {
		return nil, err
	}
	mode := os.FileMode(0o666)
	preserveMode := info != nil && info.Mode().IsRegular()
	if preserveMode {
		mode = info.Mode().Perm()
	}

	// Restore permissions ourselves: WithExistingPermissions can leak the
	// temporary file if chmod fails before it returns a handle.
	file, err := renameio.NewPendingFile(path,
		renameio.WithTempDir(filepath.Dir(path)),
		renameio.WithPermissions(mode),
	)
	if err != nil {
		return nil, fmt.Errorf("creating pending output %s: %w", path, err)
	}
	defer func() {
		if retErr != nil {
			if err := file.Cleanup(); err != nil {
				retErr = errors.Join(
					retErr,
					fmt.Errorf("cleaning up pending output %s: %w", path, err),
				)
			}
		}
	}()
	if preserveMode {
		if err := restorePermissions(file.File, mode); err != nil {
			return nil, fmt.Errorf("restoring output permissions %s: %w", path, err)
		}
	}
	return &pendingFile{PendingFile: file, path: path}, nil
}

func restoreOutputPermissions(file *os.File, mode os.FileMode) error {
	info, err := file.Stat()
	if err != nil {
		return fmt.Errorf("checking temporary file permissions: %w", err)
	}
	if info.Mode().Perm() != mode {
		if err := file.Chmod(mode); err != nil {
			return fmt.Errorf("setting temporary file permissions: %w", err)
		}
	}
	return nil
}

func (f *pendingFile) Commit() error {
	if err := f.CloseAtomicallyReplace(); err != nil {
		return fmt.Errorf("publishing output %s: %w", f.path, err)
	}
	return nil
}
