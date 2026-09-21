//go:build !windows

package mmdbconvert

import (
	"net"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPendingOutput_Permissions(t *testing.T) {
	for _, tt := range []struct {
		name     string
		existing bool
	}{{"new", false}, {"existing", true}} {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "output")
			control := filepath.Join(dir, "control")
			require.NoError(t, os.WriteFile(control, nil, 0o666))
			info, err := os.Stat(control)
			require.NoError(t, err)
			want := info.Mode().Perm()
			if tt.existing {
				require.NoError(t, os.WriteFile(path, nil, 0o600))
				want = 0o640
				require.NoError(t, os.Chmod(path, want))
			}
			file, err := newPendingOutput(path)
			require.NoError(t, err)
			defer file.Cleanup()
			require.NoError(t, file.Commit())
			info, err = os.Stat(path)
			require.NoError(t, err)
			require.Equal(t, want, info.Mode().Perm())
		})
	}
}

func TestPendingOutput_RejectsSymlink(t *testing.T) {
	for _, tt := range []struct {
		name     string
		existing bool
	}{{"missing target", false}, {"regular target", true}} {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			target := filepath.Join(dir, "target")
			path := filepath.Join(dir, "output")
			paths := []string{path}
			if tt.existing {
				require.NoError(t, os.WriteFile(target, []byte("target contents"), 0o600))
				paths = append(paths, target)
			}
			require.NoError(t, os.Symlink(target, path))
			file, err := newPendingOutput(path)
			require.Nil(t, file)
			require.ErrorContains(t, err, "symlink")
			require.ErrorContains(t, err, path)
			link, err := os.Readlink(path)
			require.NoError(t, err)
			require.Equal(t, target, link)
			if tt.existing {
				assertFileContent(t, target, "target contents")
			} else {
				require.NoFileExists(t, target)
			}
			assertOutputDirectory(t, paths)
		})
	}
}

func TestPendingOutput_RejectsSpecialFiles(t *testing.T) {
	dir := t.TempDir()
	t.Chdir(dir)
	fifo := filepath.Join(dir, "fifo")
	socket := filepath.Join(dir, "socket")
	directory := filepath.Join(dir, "dir")
	require.NoError(t, syscall.Mkfifo(fifo, 0o600))
	// A relative socket path avoids the short Unix socket path limit on macOS.
	listener, err := net.ListenUnix("unix", &net.UnixAddr{Name: "socket", Net: "unix"})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, listener.Close()) })
	require.NoError(t, os.Mkdir(directory, 0o700))
	for _, tt := range []struct {
		target string
		kind   string
	}{{fifo, "named pipe"}, {socket, "socket"}, {directory, "directory"}, {os.DevNull, "device"}} {
		target := tt.target
		t.Run(tt.kind, func(t *testing.T) {
			before, statErr := os.Stat(target)
			require.NoError(t, statErr)
			link := filepath.Join(dir, "link")
			require.NoError(t, os.Symlink(target, link))
			defer os.Remove(link)
			paths := []string{link}
			// Exercise direct special-file destinations only inside the test directory.
			if target != os.DevNull {
				paths = append(paths, target)
			}
			for _, path := range paths {
				file, outputErr := newPendingOutput(path)
				require.ErrorContains(t, outputErr, "not a regular file")
				require.ErrorContains(t, outputErr, path)
				kind := tt.kind
				if path == link {
					kind = "symlink"
				}
				require.ErrorContains(t, outputErr, "("+kind+")")
				require.Nil(t, file)
			}
			after, statErr := os.Stat(target)
			require.NoError(t, statErr)
			require.True(t, os.SameFile(before, after))
			require.Equal(t, before.Mode(), after.Mode())
			linkTarget, readErr := os.Readlink(link)
			require.NoError(t, readErr)
			require.Equal(t, target, linkTarget)
			assertOutputDirectory(t, []string{fifo, socket, directory, link})
		})
	}
}

func TestPendingOutput_RejectsSymlinkLoop(t *testing.T) {
	path := filepath.Join(t.TempDir(), "loop")
	require.NoError(t, os.Symlink(path, path))
	file, err := newPendingOutput(path)
	require.Nil(t, file)
	require.ErrorContains(t, err, "symlink")
	assertOutputDirectory(t, []string{path})
}

func TestPendingOutput_PermissionFailureCleansUp(t *testing.T) {
	path := filepath.Join(t.TempDir(), "output")
	require.NoError(t, os.WriteFile(path, []byte("previous output"), 0o600))
	// #nosec G302 -- reproduce restoration of an existing file's 0666 permissions.
	require.NoError(t, os.Chmod(path, 0o666))
	var staged *os.File
	var chmodErr error
	file, err := createPendingOutput(path, func(f *os.File, mode os.FileMode) error {
		staged = f
		require.Equal(t, os.FileMode(0o666), mode)
		require.FileExists(t, f.Name())
		chmodErr = &os.PathError{Op: "chmod", Path: f.Name(), Err: syscall.EPERM}
		return chmodErr
	})
	require.Nil(t, file)
	require.ErrorIs(t, err, chmodErr)
	require.ErrorIs(t, err, syscall.EPERM)
	require.ErrorContains(t, err, "restoring output permissions")
	require.NotNil(t, staged)
	_, err = staged.Stat()
	require.ErrorIs(t, err, os.ErrClosed)
	require.NoFileExists(t, staged.Name())
	assertFileContent(t, path, "previous output")
	assertOutputDirectory(t, []string{path})
}

func TestRestoreOutputPermissions(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "staged")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, file.Close()) })
	// Restore permission bits regardless of the process umask.
	require.NoError(t, file.Chmod(0o600))
	require.NoError(t, restoreOutputPermissions(file, 0o666))
	info, err := file.Stat()
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o666), info.Mode().Perm())
}
