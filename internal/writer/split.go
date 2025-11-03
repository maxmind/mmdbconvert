package writer

import (
	"errors"
	"fmt"
	"net/netip"
)

type rowWriter interface {
	WriteRow(netip.Prefix, map[string]any) error
}

// SplitRowWriter routes rows to IPv4 or IPv6 writers based on the prefix.
type SplitRowWriter struct {
	ipv4 rowWriter
	ipv6 rowWriter
}

// NewSplitRowWriter constructs a row writer that dispatches rows by IP version.
func NewSplitRowWriter(ipv4, ipv6 rowWriter) *SplitRowWriter {
	return &SplitRowWriter{ipv4: ipv4, ipv6: ipv6}
}

// WriteRow writes the row to the underlying IPv4 or IPv6 writer.
func (s *SplitRowWriter) WriteRow(prefix netip.Prefix, data map[string]any) error {
	if prefix.Addr().Is4() {
		if s.ipv4 == nil {
			return errors.New("no IPv4 writer configured")
		}
		return s.ipv4.WriteRow(prefix, data)
	}
	if s.ipv6 == nil {
		return errors.New("no IPv6 writer configured")
	}
	return s.ipv6.WriteRow(prefix, data)
}

// Flush flushes both underlying writers when supported.
func (s *SplitRowWriter) Flush() error {
	if flusher, ok := s.ipv4.(interface{ Flush() error }); ok {
		if err := flusher.Flush(); err != nil {
			return fmt.Errorf("flushing IPv4 writer: %w", err)
		}
	}
	if flusher, ok := s.ipv6.(interface{ Flush() error }); ok {
		if err := flusher.Flush(); err != nil {
			return fmt.Errorf("flushing IPv6 writer: %w", err)
		}
	}
	return nil
}
