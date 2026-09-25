//go:build !windows

package mmdbconvert

import (
	"fmt"
	"os"
)

func outputFileMode(info os.FileInfo) os.FileMode {
	if info != nil {
		return info.Mode().Perm()
	}
	return 0o666
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
