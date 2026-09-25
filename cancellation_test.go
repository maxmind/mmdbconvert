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

func TestRunContext_Canceled(t *testing.T) {
	for _, format := range []string{"csv", "parquet", "mmdb"} {
		t.Run(format, func(t *testing.T) {
			configPath, _, paths := outputTestConfig(t, format, false, `["country", "iso_code"]`)
			require.NoError(t, os.WriteFile(paths[0], []byte("previous output"), 0o600))
			ctx, cancel := context.WithCancel(t.Context())
			cancel()
			require.ErrorIs(t, RunContext(ctx, Options{ConfigPath: configPath}), context.Canceled)
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
					err = convert(ctx, cfg, readers, wrapped, outputs)
					require.ErrorIs(t, err, context.Canceled)
					require.Positive(t, wrapped.rows)
					for _, path := range paths {
						assertFileContent(t, path, "previous output")
					}
					assertOutputDirectory(t, paths)
				})
			}
		}
	}
}

type cancelRowWriter struct {
	merger.RowWriter

	cancel context.CancelFunc
	stage  string
	rows   int
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
	if w.stage == "flush" {
		w.cancel()
	}
	if err := w.RowWriter.(interface{ Flush() error }).Flush(); err != nil {
		return err
	}
	if w.stage == "after flush" {
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

func TestConvert_CancellationDuringWrite(t *testing.T) {
	for _, format := range []string{"csv", "parquet", "mmdb"} {
		t.Run(format, func(t *testing.T) {
			_, cfg, paths := outputTestConfig(t, format, false, `["country", "iso_code"]`)
			require.NoError(t, os.WriteFile(paths[0], []byte("previous output"), 0o600))
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			file, err := preparePendingOutput(paths[0])
			require.NoError(t, err)
			defer func() { require.NoError(t, file.Cleanup()) }()
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
