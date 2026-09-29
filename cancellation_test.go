package mmdbconvert

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/netip"
	"os"
	"testing"

	"github.com/maxmind/mmdbwriter/v2/mmdbtype"
	"github.com/stretchr/testify/require"

	"github.com/maxmind/mmdbconvert/internal/merger"
	"github.com/maxmind/mmdbconvert/internal/writer"
)

func TestRun_Canceled(t *testing.T) {
	for _, format := range []string{"csv", "parquet", "mmdb"} {
		t.Run(format, func(t *testing.T) {
			configPath, _, paths := outputTestConfig(t, format, false, `["country", "iso_code"]`)
			require.NoError(t, os.WriteFile(paths[0], []byte("previous output"), 0o600))
			ctx, cancel := context.WithCancel(t.Context())
			cancel()
			require.ErrorIs(t, Run(ctx, Options{ConfigPath: configPath}), context.Canceled)
			assertFileContent(t, paths[0], "previous output")
			assertOutputDirectory(t, paths)
		})
	}
}

func TestConvert_CancellationCleansOutputs(t *testing.T) {
	for _, format := range []string{"csv", "parquet", "mmdb"} {
		for _, split := range []bool{false, true} {
			if split && format == "mmdb" {
				continue
			}
			for _, stage := range []string{"merge", "flush", "after flush"} {
				t.Run(fmt.Sprintf("%s/split=%t/%s", format, split, stage), func(t *testing.T) {
					_, cfg, paths := outputTestConfig(t, format, split, `["country", "iso_code"]`)
					for _, path := range paths {
						require.NoError(t, os.WriteFile(path, []byte("previous output"), 0o600))
					}
					ctx, cancel := context.WithCancel(t.Context())
					defer cancel()
					readers := openOutputTestReaders(t, cfg)
					rowWriter, outputs, err := prepareRowWriter(ctx, cfg, readers)
					require.NoError(t, err)
					wrapped := &cancelRowWriter{RowWriter: rowWriter, cancel: cancel, stage: stage}
					var observed merger.RowWriter = wrapped
					_, supportsRanges := rowWriter.(merger.RangeRowWriter)
					if supportsRanges {
						observed = &cancelRangeRowWriter{cancelRowWriter: wrapped}
					}
					err = convert(ctx, cfg, readers, observed, outputs)
					require.ErrorIs(t, err, context.Canceled)
					if supportsRanges {
						require.Positive(t, wrapped.ranges)
						require.Zero(t, wrapped.rows)
					} else {
						require.Positive(t, wrapped.rows)
					}
					if stage == "flush" {
						// The real serializer must stop; convert's post-flush check
						// alone would allow a canceled flush to write the full file.
						require.ErrorIs(t, wrapped.flushErr, context.Canceled)
					}
					for _, path := range paths {
						assertFileContent(t, path, "previous output")
					}
					assertOutputDirectory(t, paths)
				})
			}
		}
	}
}

func TestConvert_CancellationDuringFinalAccumulatorFlush(t *testing.T) {
	_, cfg, paths := outputTestConfig(t, "mmdb", false, `["value"]`)
	require.NoError(t, os.WriteFile(paths[0], []byte("previous output"), 0o600))
	// A single data range stays in the accumulator until Merge's final Flush.
	cfg.Databases[0].Path = createCSVTestDatabase(t, 4, []string{"1.0.0.0/24"})

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	readers := openOutputTestReaders(t, cfg)
	rowWriter, outputs, err := prepareRowWriter(ctx, cfg, readers)
	require.NoError(t, err)
	wrapped := &cancelRowWriter{RowWriter: rowWriter, cancel: cancel, stage: "merge"}
	err = convert(ctx, cfg, readers, &cancelRangeRowWriter{cancelRowWriter: wrapped}, outputs)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, 1, wrapped.ranges)
	require.Zero(t, wrapped.flushes, "cancellation must prevent format serialization")
	assertFileContent(t, paths[0], "previous output")
	assertOutputDirectory(t, paths)
}

type cancelRowWriter struct {
	merger.RowWriter

	cancel   context.CancelFunc
	stage    string
	rows     int
	ranges   int
	flushes  int
	flushErr error
}

func (w *cancelRowWriter) WriteRow(prefix netip.Prefix, data []mmdbtype.DataType) error {
	w.rows++
	if err := w.RowWriter.WriteRow(prefix, data); err != nil {
		return err
	}
	if w.stage == "merge" {
		w.cancel()
	}
	return nil
}

func (w *cancelRowWriter) Flush() error {
	w.flushes++
	if w.stage == "flush" {
		w.cancel()
	}
	w.flushErr = w.RowWriter.(interface{ Flush() error }).Flush()
	if w.flushErr != nil {
		return w.flushErr
	}
	if w.stage == "after flush" {
		w.cancel()
	}
	return nil
}

// Preserve the prepared writer's optional range capability in the observer.
type cancelRangeRowWriter struct {
	*cancelRowWriter
}

func (w *cancelRangeRowWriter) WriteRange(start, end netip.Addr, data []mmdbtype.DataType) error {
	w.ranges++
	if err := w.RowWriter.(merger.RangeRowWriter).WriteRange(start, end, data); err != nil {
		return err
	}
	if w.stage == "merge" {
		w.cancel()
	}
	return nil
}

func TestContextWriter_Cancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	var buf bytes.Buffer
	w := contextWriter{ctx: ctx, writer: &buf}
	n, err := w.Write([]byte("first"))
	require.NoError(t, err)
	require.Equal(t, 5, n)
	cancel()
	n, err = w.Write([]byte("second"))
	require.ErrorIs(t, err, context.Canceled)
	require.Zero(t, n)
	require.Equal(t, "first", buf.String())
}

func TestConvert_CancellationAfterOutputWrite(t *testing.T) {
	for _, format := range []string{"csv", "parquet", "mmdb"} {
		t.Run(format, func(t *testing.T) {
			_, cfg, paths := outputTestConfig(t, format, false, `["country", "iso_code"]`)
			require.NoError(t, os.WriteFile(paths[0], []byte("previous output"), 0o600))
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			file, err := preparePendingOutput(paths[0])
			require.NoError(t, err)
			defer func() { require.NoError(t, file.Cleanup()) }()
			// Cancel after an underlying write succeeds. Even if serialization
			// finishes in that write, the publication check must preserve the old output.
			output := &cancelWriteOutput{pendingOutput: file, cancel: cancel}
			readers := openOutputTestReaders(t, cfg)
			rowWriter, err := newRowWriter(
				contextWriter{ctx: ctx, writer: output},
				cfg,
				readers,
				writer.IPVersionAny,
			)
			require.NoError(t, err)
			err = convert(ctx, cfg, readers, rowWriter, []pendingOutput{file})
			require.ErrorIs(t, err, context.Canceled)
			require.Positive(t, output.bytesWritten)
			assertFileContent(t, paths[0], "previous output")
			assertOutputDirectory(t, paths)
		})
	}
}

type cancelWriteOutput struct {
	pendingOutput

	cancel       context.CancelFunc
	bytesWritten int
}

func (o *cancelWriteOutput) Write(p []byte) (int, error) {
	n, err := o.pendingOutput.Write(p)
	o.bytesWritten += n
	o.cancel()
	return n, err
}

func TestConvert_CancellationPreservesCleanupErrors(t *testing.T) {
	_, cfg, _ := outputTestConfig(t, "csv", false, `["country", "iso_code"]`)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	cleanupErr := errors.New("cleanup failure")
	output := &cleanupErrorOutput{cleanupErr: cleanupErr}
	err := convert(
		ctx,
		cfg,
		openOutputTestReaders(t, cfg),
		&errorRowWriter{},
		[]pendingOutput{output},
	)
	require.ErrorIs(t, err, context.Canceled)
	require.ErrorIs(t, err, cleanupErr)
	require.Equal(t, 1, output.cleanups)
	require.Zero(t, output.commits)
}

func TestPublication_CompletesAfterCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	first := &cancelCommitOutput{cancel: cancel}
	second := &cleanupErrorOutput{}
	require.NoError(t, flushAndCommit(ctx, &errorRowWriter{}, []pendingOutput{first, second}))
	require.ErrorIs(t, ctx.Err(), context.Canceled)
	require.Equal(t, 1, first.commits)
	require.Equal(t, 1, second.commits)
}

type cancelCommitOutput struct {
	cleanupErrorOutput

	cancel context.CancelFunc
}

func (o *cancelCommitOutput) Commit() error {
	o.cancel()
	return o.cleanupErrorOutput.Commit()
}
