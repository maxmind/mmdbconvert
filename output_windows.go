package mmdbconvert

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// Windows also stages output, but os.Rename does not guarantee atomicity there.
type pendingFile struct {
	*os.File

	path   string
	closed bool
	done   bool
}

func newPendingOutput(path string) (*pendingFile, error) {
	if _, err := inspectOutput(path); err != nil {
		return nil, err
	}
	file, err := os.CreateTemp(filepath.Dir(path), "."+filepath.Base(path)+"-*")
	if err != nil {
		return nil, fmt.Errorf("creating pending output %s: %w", path, err)
	}
	return &pendingFile{File: file, path: path}, nil
}

func (f *pendingFile) Commit() error {
	if err := f.Sync(); err != nil {
		return fmt.Errorf("syncing output %s: %w", f.path, err)
	}
	f.closed = true
	if err := f.Close(); err != nil {
		return fmt.Errorf("closing output %s: %w", f.path, err)
	}
	if err := os.Rename(f.Name(), f.path); err != nil {
		return fmt.Errorf("publishing output %s: %w", f.path, err)
	}
	f.done = true
	return nil
}

func (f *pendingFile) Cleanup() error {
	if f.done {
		return nil
	}
	var closeErr error
	if !f.closed {
		f.closed = true
		closeErr = f.Close()
	}
	removeErr := os.Remove(f.Name())
	if removeErr == nil {
		f.done = true
	}
	return errors.Join(closeErr, removeErr)
}
