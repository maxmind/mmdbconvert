package mmdbconvert

import (
	"crypto/rand"
	"errors"
	"fmt"
	"os"
)

type pendingFile struct {
	*os.File

	path   string
	closed bool
	done   bool
}

func newPendingOutput(destination outputDestination) (_ *pendingFile, retErr error) {
	dir := destination.dir
	// Append without cleaning so the filesystem resolves symlinks and "..".
	if !os.IsPathSeparator(dir[len(dir)-1]) {
		dir += string(os.PathSeparator)
	}
	name := dir + ".mmdbconvert-" + rand.Text()
	mode := outputFileMode(destination.info)
	// #nosec G304 -- stage beside the user-configured output, using exclusive creation.
	file, err := os.OpenFile(name, os.O_RDWR|os.O_CREATE|os.O_EXCL, mode)
	if err != nil {
		return nil, fmt.Errorf("creating pending output %s: %w", destination.path, err)
	}
	pending := &pendingFile{File: file, path: destination.path}
	defer func() {
		if retErr != nil {
			if err := pending.Cleanup(); err != nil {
				retErr = errors.Join(
					retErr,
					fmt.Errorf("cleaning up pending output %s: %w", destination.path, err),
				)
			}
		}
	}()
	if destination.info != nil {
		if err := restoreOutputPermissions(file, mode); err != nil {
			return nil, fmt.Errorf("restoring output permissions %s: %w", destination.path, err)
		}
	}
	return pending, nil
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
