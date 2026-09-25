package mmdbconvert

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestPendingOutput_DriveRelativePath(t *testing.T) {
	dir := t.TempDir()
	t.Chdir(dir)
	drive := filepath.VolumeName(dir)
	if len(drive) != 2 || drive[1] != ':' {
		t.Skip("test requires a drive letter")
	}
	file, err := preparePendingOutput(drive + "out.csv")
	require.NoError(t, err)
	defer func() { require.NoError(t, file.Cleanup()) }()
	parent, err := os.Stat(filepath.Dir(file.Name()))
	require.NoError(t, err)
	wantParent, err := os.Stat(dir)
	require.NoError(t, err)
	require.True(t, os.SameFile(parent, wantParent), "stage in the output directory")
	_, err = file.WriteString("published")
	require.NoError(t, err)
	require.NoError(t, file.Commit())
	path := filepath.Join(dir, "out.csv")
	assertFileContent(t, path, "published")
	assertOutputDirectory(t, []string{path})
}

func TestOutput_SubstDrive(t *testing.T) {
	dir := t.TempDir()
	var drive string
	for letter := 'Z'; letter >= 'D'; letter-- {
		candidate := string(letter) + ":"
		if _, err := os.Stat(candidate + `\`); errors.Is(err, os.ErrNotExist) {
			drive = candidate
			break
		}
	}
	if drive == "" {
		t.Skip("no free drive letter for subst")
	}
	// #nosec G204 -- subst maps only a free drive letter to this test's temporary directory.
	result, err := exec.CommandContext(t.Context(), "subst", drive, dir).CombinedOutput()
	require.NoError(t, err, string(result))
	t.Cleanup(func() {
		// t.Context is canceled before cleanup, so allow subst to finish separately.
		ctx, cancel := context.WithTimeout(context.WithoutCancel(t.Context()), 10*time.Second)
		defer cancel()
		// #nosec G204 -- remove only the drive mapping created by this test.
		cleanupResult, err := exec.CommandContext(ctx, "subst", drive, "/D").CombinedOutput()
		require.NoError(t, err, string(cleanupResult))
	})
	path := drive + `\out.csv`
	file, err := preparePendingOutput(path)
	require.NoError(t, err)
	defer func() { require.NoError(t, file.Cleanup()) }()
	_, err = file.WriteString("published")
	require.NoError(t, err)
	require.NoError(t, file.Commit())
	assertFileContent(t, filepath.Join(dir, "out.csv"), "published")
	_, err = prepareOutputPaths([]string{path, filepath.Join(dir, "out.csv")})
	require.ErrorContains(t, err, "ignoring case")
}

func TestOutput_ShortNameAlias(t *testing.T) {
	path := filepath.Join(t.TempDir(), "long-output-filename.csv")
	require.NoError(t, os.WriteFile(path, []byte("previous output"), 0o600))
	longPath, err := syscall.UTF16PtrFromString(path)
	require.NoError(t, err)
	const bufferSize = 32768
	buf := make([]uint16, bufferSize)
	n, err := syscall.GetShortPathName(longPath, &buf[0], bufferSize)
	require.NoError(t, err)
	require.Less(t, int(n), len(buf))
	shortPath := syscall.UTF16ToString(buf[:n])
	if strings.EqualFold(filepath.Base(path), filepath.Base(shortPath)) {
		t.Skip("filesystem does not assign 8.3 short names")
	}
	_, err = prepareOutputPaths([]string{path, shortPath})
	require.ErrorContains(t, err, "refer to the same file")
	assertFileContent(t, path, "previous output")
	assertOutputDirectory(t, []string{path})
}
