package main

import (
	"io"
	"log/slog"
)

func newLogger(w io.Writer, format string, level slog.Leveler) *slog.Logger {
	opts := &slog.HandlerOptions{
		Level: level,
		ReplaceAttr: func(groups []string, attr slog.Attr) slog.Attr {
			if len(groups) == 0 && attr.Key == slog.MessageKey {
				attr.Key = "message"
			}
			if len(groups) == 0 && format == logFormatJSON && attr.Key == "elapsed" &&
				attr.Value.Kind() == slog.KindDuration {
				return slog.Int64("elapsed_ms", attr.Value.Duration().Milliseconds())
			}
			return attr
		},
	}
	if format == logFormatJSON {
		return slog.New(slog.NewJSONHandler(w, opts))
	}
	return slog.New(slog.NewTextHandler(w, opts))
}
