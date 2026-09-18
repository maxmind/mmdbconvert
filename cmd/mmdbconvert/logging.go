package main

import (
	"io"
	"log/slog"
)

func newLogger(w io.Writer, jsonLogs, quiet bool) *slog.Logger {
	opts := &slog.HandlerOptions{
		ReplaceAttr: func(groups []string, attr slog.Attr) slog.Attr {
			if len(groups) == 0 && attr.Key == slog.MessageKey {
				attr.Key = "message"
			}
			return attr
		},
	}
	if quiet {
		opts.Level = slog.LevelWarn
	}
	if jsonLogs {
		return slog.New(slog.NewJSONHandler(w, opts))
	}
	return slog.New(slog.NewTextHandler(w, opts))
}
