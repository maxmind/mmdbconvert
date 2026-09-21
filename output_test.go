package mmdbconvert

import (
	"bytes"
	"encoding/csv"
	"errors"
	"fmt"
	"net/netip"
	"os"
	"path/filepath"
	"testing"

	"github.com/maxmind/mmdbwriter/v2/mmdbtype"
	maxminddb "github.com/oschwald/maxminddb-golang/v2"
	"github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/require"

	"github.com/maxmind/mmdbconvert/internal/config"
	"github.com/maxmind/mmdbconvert/internal/mmdb"
	"github.com/maxmind/mmdbconvert/internal/writer"
)

func TestRun_OutputPublication(t *testing.T) {
	for _, format := range []string{"csv", "parquet", "mmdb"} {
		for _, split := range []bool{false, true} {
			if split && format == "mmdb" {
				continue
			}
			t.Run(fmt.Sprintf("%s/split=%t", format, split), func(t *testing.T) {
				configPath, _, paths := outputTestConfig(
					t,
					format,
					split,
					`["country", "iso_code"]`,
				)
				for _, path := range paths {
					require.NoError(t, os.WriteFile(path, []byte("previous output"), 0o600))
				}
				require.NoError(t, Run(Options{ConfigPath: configPath}))
				for _, path := range paths {
					data, err := os.ReadFile(filepath.Clean(path))
					require.NoError(t, err)
					switch format {
					case "csv":
						rows, readErr := csv.NewReader(bytes.NewReader(data)).ReadAll()
						require.NoError(t, readErr)
						require.Greater(t, len(rows), 1)
						require.Equal(t, []string{"network", "country_code"}, rows[0])
					case "parquet":
						file, readErr := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
						require.NoError(t, readErr)
						require.Positive(t, file.NumRows())
					case "mmdb":
						db, readErr := maxminddb.Open(path)
						require.NoError(t, readErr)
						defer db.Close()
						var record struct {
							Country string `maxminddb:"country_code"`
						}
						require.NoError(
							t,
							db.Lookup(netip.MustParseAddr("2.125.160.216")).Decode(&record),
						)
						require.Equal(t, "GB", record.Country)
					}
				}
				assertOutputDirectory(t, paths)
			})
		}
	}
}

func TestRun_ConversionFailurePreservesOutputs(t *testing.T) {
	for _, format := range []string{"csv", "parquet", "mmdb"} {
		for _, split := range []bool{false, true} {
			if split && format == "mmdb" {
				continue
			}
			t.Run(fmt.Sprintf("%s/split=%t", format, split), func(t *testing.T) {
				configPath, _, paths := outputTestConfig(
					t,
					format,
					split,
					`["country", "iso_code", "invalid"]`,
				)
				for _, path := range paths {
					require.NoError(t, os.WriteFile(path, []byte("previous output"), 0o600))
				}
				require.ErrorContains(t, Run(Options{ConfigPath: configPath}), "merging databases")
				for _, path := range paths {
					assertFileContent(t, path, "previous output")
				}
				assertOutputDirectory(t, paths)
			})
		}
	}
}

func TestPrepareRowWriter_SecondOutputFailure(t *testing.T) {
	for _, format := range []string{"csv", "parquet"} {
		t.Run(format, func(t *testing.T) {
			_, cfg, paths := outputTestConfig(t, format, true, `["country", "iso_code"]`)
			require.NoError(t, os.WriteFile(paths[0], []byte("previous output"), 0o600))
			cfg.Output.IPv6File = filepath.Join(filepath.Dir(paths[1]), "missing", "out")
			readers := openOutputTestReaders(t, cfg)
			_, _, err := prepareRowWriter(cfg, readers)
			require.ErrorContains(t, err, cfg.Output.IPv6File)
			assertFileContent(t, paths[0], "previous output")
			assertOutputDirectory(t, paths[:1])
		})
	}
}

func TestRun_RejectsDirectoryBeforeConversion(t *testing.T) {
	for _, format := range []string{"csv", "parquet", "mmdb"} {
		for _, split := range []bool{false, true} {
			if split && format == "mmdb" {
				continue
			}
			t.Run(fmt.Sprintf("%s/split=%t", format, split), func(t *testing.T) {
				// This projection would fail during conversion if destination
				// validation did not reject the directory first.
				configPath, _, paths := outputTestConfig(
					t, format, split, `["country", "iso_code", "invalid"]`,
				)
				if split {
					require.NoError(t, os.WriteFile(paths[0], []byte("previous output"), 0o600))
				}
				dir := paths[len(paths)-1]
				require.NoError(t, os.Mkdir(dir, 0o700))
				err := Run(Options{ConfigPath: configPath})
				require.ErrorContains(t, err, "not a regular file")
				require.Contains(t, filepath.ToSlash(err.Error()), filepath.ToSlash(dir))
				require.DirExists(t, dir)
				if split {
					assertFileContent(t, paths[0], "previous output")
				}
				assertOutputDirectory(t, paths)
			})
		}
	}
}

func TestInspectOutput_RejectsDevice(t *testing.T) {
	// Inspect the real device without ever attempting to stage or replace it.
	info, err := inspectOutput(os.DevNull)
	require.Nil(t, info)
	require.ErrorContains(t, err, "not a regular file")
	require.ErrorContains(t, err, os.DevNull)
}

func TestOutput_FlushFailure(t *testing.T) {
	for _, format := range []string{"csv", "parquet", "mmdb"} {
		t.Run(format, func(t *testing.T) {
			_, cfg, paths := outputTestConfig(t, format, false, `["country", "iso_code"]`)
			require.NoError(t, os.WriteFile(paths[0], []byte("previous output"), 0o600))
			file, err := newPendingOutput(paths[0])
			require.NoError(t, err)
			defer func() { require.NoError(t, file.Cleanup()) }()
			writeErr := errors.New("injected write failure")
			rowWriter, err := newRowWriter(
				failingWriter{writeErr},
				cfg,
				openOutputTestReaders(t, cfg),
				writer.IPVersionAny,
			)
			require.NoError(t, err)
			require.NoError(
				t,
				rowWriter.WriteRow(
					netip.MustParsePrefix("2.125.160.216/29"),
					[]mmdbtype.DataType{mmdbtype.String("GB")},
				),
			)
			err = flushAndCommit(rowWriter, []pendingOutput{file})
			require.ErrorIs(t, err, writeErr)
			require.NoError(t, file.Cleanup())
			assertFileContent(t, paths[0], "previous output")
			assertOutputDirectory(t, paths)
		})
	}
}

func TestOutput_SplitFlushFailure(t *testing.T) {
	_, cfg, paths := outputTestConfig(t, "csv", true, `["country", "iso_code"]`)
	var files []pendingOutput
	for _, path := range paths {
		require.NoError(t, os.WriteFile(path, []byte("previous output"), 0o600))
		file, err := newPendingOutput(path)
		require.NoError(t, err)
		files = append(files, file)
	}
	defer func() { require.NoError(t, cleanupOutputs(files)) }()
	ipv4 := writer.NewCSVWriter(files[0], cfg)
	writeErr := errors.New("IPv6 flush failure")
	ipv6 := writer.NewCSVWriter(failingWriter{writeErr}, cfg)
	split := writer.NewSplitRowWriter(ipv4, ipv6)
	for _, prefix := range []string{"2.0.0.0/24", "2001:218::/32"} {
		require.NoError(
			t,
			split.WriteRow(
				netip.MustParsePrefix(prefix),
				[]mmdbtype.DataType{mmdbtype.String("GB")},
			),
		)
	}
	require.ErrorIs(t, flushAndCommit(split, files), writeErr)
	require.NoError(t, cleanupOutputs(files))
	for _, path := range paths {
		assertFileContent(t, path, "previous output")
	}
	assertOutputDirectory(t, paths)
}

func TestPendingOutput(t *testing.T) {
	for _, existing := range []bool{false, true} {
		for _, commit := range []bool{false, true} {
			t.Run(fmt.Sprintf("existing=%t/commit=%t", existing, commit), func(t *testing.T) {
				dir := t.TempDir()
				path := filepath.Join(dir, "output")
				if existing {
					require.NoError(t, os.WriteFile(path, []byte("old"), 0o600))
				}
				file, err := newPendingOutput(path)
				require.NoError(t, err)
				defer func() { require.NoError(t, file.Cleanup()) }()
				_, err = file.WriteString("new")
				require.NoError(t, err)
				if existing {
					assertFileContent(t, path, "old")
				} else {
					require.NoFileExists(t, path)
				}
				if commit {
					require.NoError(t, file.Commit())
				}
				require.NoError(t, file.Cleanup())
				switch {
				case commit:
					assertFileContent(t, path, "new")
				case existing:
					assertFileContent(t, path, "old")
				default:
					require.NoFileExists(t, path)
				}
				entries, err := os.ReadDir(dir)
				require.NoError(t, err)
				if commit || existing {
					require.Len(t, entries, 1)
				} else {
					require.Empty(t, entries)
				}
			})
		}
	}
}

func TestOutput_CommitFailure(t *testing.T) {
	_, cfg, paths := outputTestConfig(t, "csv", true, `["country", "iso_code"]`)
	var files []pendingOutput
	for _, path := range paths {
		file, err := newPendingOutput(path)
		require.NoError(t, err)
		files = append(files, file)
	}
	defer func() { require.NoError(t, cleanupOutputs(files)) }()
	// The second destination becomes a directory after staging. The first can
	// commit, but the second replacement must fail and be reported.
	require.NoError(t, os.Mkdir(paths[1], 0o700))
	split := writer.NewSplitRowWriter(
		writer.NewCSVWriter(files[0], cfg),
		writer.NewCSVWriter(files[1], cfg),
	)
	require.NoError(
		t,
		split.WriteRow(
			netip.MustParsePrefix("2.0.0.0/24"),
			[]mmdbtype.DataType{mmdbtype.String("GB")},
		),
	)
	require.ErrorContains(t, flushAndCommit(split, files), paths[1])
	require.NoError(t, cleanupOutputs(files))
	assertFileContent(t, paths[0], "network,country_code\n2.0.0.0/24,GB\n")
	require.DirExists(t, paths[1])
	assertOutputDirectory(t, paths)
}

func TestCleanupOutputs_AggregatesFailures(t *testing.T) {
	firstErr := errors.New("first cleanup failure")
	lastErr := errors.New("last cleanup failure")
	first := &cleanupErrorOutput{cleanupErr: firstErr}
	middle := &cleanupErrorOutput{}
	last := &cleanupErrorOutput{cleanupErr: lastErr}
	err := cleanupOutputs([]pendingOutput{first, middle, last})
	require.ErrorIs(t, err, firstErr)
	require.ErrorIs(t, err, lastErr)
	for _, output := range []*cleanupErrorOutput{first, middle, last} {
		require.Equal(t, 1, output.cleanups)
	}
}

func TestConvert_PreservesPrimaryAndCleanupErrors(t *testing.T) {
	for _, stage := range []string{"conversion", "flush", "publication", "cleanup only"} {
		t.Run(stage, func(t *testing.T) {
			_, cfg, _ := outputTestConfig(t, "csv", false, `["country", "iso_code"]`)
			readers := openOutputTestReaders(t, cfg)
			primaryErr := errors.New("primary failure")
			firstCleanupErr := errors.New("first cleanup failure")
			secondCleanupErr := errors.New("second cleanup failure")
			first := &cleanupErrorOutput{cleanupErr: firstCleanupErr}
			second := &cleanupErrorOutput{cleanupErr: secondCleanupErr}
			rowWriter := &errorRowWriter{}
			switch stage {
			case "conversion":
				rowWriter.writeErr = primaryErr
			case "flush":
				rowWriter.flushErr = primaryErr
			case "publication":
				first.commitErr = primaryErr
			}
			err := convert(cfg, readers, rowWriter, []pendingOutput{first, second})
			if stage != "cleanup only" {
				require.ErrorIs(t, err, primaryErr)
			}
			require.ErrorIs(t, err, firstCleanupErr)
			require.ErrorIs(t, err, secondCleanupErr)
			require.Equal(t, 1, first.cleanups)
			require.Equal(t, 1, second.cleanups)
			switch stage {
			case "conversion", "flush":
				require.Zero(t, first.commits)
				require.Zero(t, second.commits)
			case "publication":
				require.Equal(t, 1, first.commits)
				require.Zero(t, second.commits)
			case "cleanup only":
				require.Equal(t, 1, first.commits)
				require.Equal(t, 1, second.commits)
			}
		})
	}
}

type cleanupErrorOutput struct {
	cleanupErr error
	commitErr  error
	cleanups   int
	commits    int
}

func (*cleanupErrorOutput) Write(p []byte) (int, error) { return len(p), nil }

func (o *cleanupErrorOutput) Commit() error {
	o.commits++
	return o.commitErr
}

func (o *cleanupErrorOutput) Cleanup() error {
	o.cleanups++
	return o.cleanupErr
}

type errorRowWriter struct {
	writeErr error
	flushErr error
}

func (w *errorRowWriter) WriteRow(netip.Prefix, []mmdbtype.DataType) error {
	return w.writeErr
}

func (w *errorRowWriter) Flush() error { return w.flushErr }

type failingWriter struct{ err error }

func TestPendingOutput_SyncFailure(t *testing.T) {
	path := filepath.Join(t.TempDir(), "output")
	require.NoError(t, os.WriteFile(path, []byte("previous output"), 0o600))
	file, err := newPendingOutput(path)
	require.NoError(t, err)
	require.NoError(t, file.Close())
	require.ErrorIs(t, file.Commit(), os.ErrClosed)
	// Cleanup reports the already-closed descriptor but still removes the temp.
	require.ErrorIs(t, file.Cleanup(), os.ErrClosed)
	assertFileContent(t, path, "previous output")
	assertOutputDirectory(t, []string{path})
}

func (w failingWriter) Write([]byte) (int, error) { return 0, w.err }

func outputTestConfig(
	t *testing.T,
	format string,
	split bool,
	columnPath string,
) (string, *config.Config, []string) {
	t.Helper()
	dir := t.TempDir()
	paths := []string{filepath.Join(dir, "output."+format)}
	output := fmt.Sprintf("file = %q", tomlPath(paths[0]))
	if split {
		paths = []string{filepath.Join(dir, "ipv4."+format), filepath.Join(dir, "ipv6."+format)}
		output = fmt.Sprintf(
			"ipv4_file = %q\nipv6_file = %q",
			tomlPath(paths[0]),
			tomlPath(paths[1]),
		)
	}
	content := fmt.Sprintf(`
[output]
format = %q
%s
[output.mmdb]
database_type = "Test"
[[network.columns]]
name = "network"
type = "cidr"
[[databases]]
name = "city"
path = %q
[[columns]]
name = "country_code"
database = "city"
path = %s
output_path = ["country_code"]
`, format, output, tomlPath(filepath.Join(testDataDir, "GeoIP2-City-Test.mmdb")), columnPath)
	configPath := filepath.Join(t.TempDir(), "config.toml")
	require.NoError(t, os.WriteFile(configPath, []byte(content), 0o600))
	cfg, err := config.LoadConfig(configPath)
	require.NoError(t, err)
	return configPath, cfg, paths
}

func openOutputTestReaders(t *testing.T, cfg *config.Config) *mmdb.Readers {
	t.Helper()
	readers, err := mmdb.OpenDatabases(
		map[string]string{cfg.Databases[0].Name: cfg.Databases[0].Path},
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, readers.Close()) })
	return readers
}

func assertFileContent(t *testing.T, path, expected string) {
	t.Helper()
	data, err := os.ReadFile(filepath.Clean(path))
	require.NoError(t, err)
	require.Equal(t, expected, string(data))
}

func assertOutputDirectory(t *testing.T, paths []string) {
	t.Helper()
	entries, err := os.ReadDir(filepath.Dir(paths[0]))
	require.NoError(t, err)
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		names = append(names, entry.Name())
	}
	want := make([]string, 0, len(paths))
	for _, path := range paths {
		want = append(want, filepath.Base(path))
	}
	require.ElementsMatch(t, want, names)
}
