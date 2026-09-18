package writer

import (
	"errors"
	"fmt"
	"net/netip"

	"github.com/maxmind/mmdbwriter/v2/mmdbtype"
	"go4.org/netipx"
)

type rowWriter interface {
	WriteRow(netip.Prefix, []mmdbtype.DataType) error
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
func (s *SplitRowWriter) WriteRow(prefix netip.Prefix, data []mmdbtype.DataType) error {
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

// WriteRange writes an IP range to the underlying IPv4 or IPv6 writer.
// This method implements the merger.RangeRowWriter interface, enabling range
// compression when the underlying writers support it.
//
// The start and end addresses are guaranteed to be the same IP version by the
// Accumulator. This method checks if the underlying writer implements WriteRange;
// if so, it delegates directly. Otherwise, it converts the range to CIDRs and
// calls WriteRow for each prefix.
func (s *SplitRowWriter) WriteRange(start, end netip.Addr, data []mmdbtype.DataType) error {
	if start.Is4() {
		if s.ipv4 == nil {
			return errors.New("no IPv4 writer configured")
		}
		// Check if the underlying writer implements WriteRange
		if rangeWriter, ok := s.ipv4.(interface {
			WriteRange(netip.Addr, netip.Addr, []mmdbtype.DataType) error
		}); ok {
			if err := rangeWriter.WriteRange(start, end, data); err != nil {
				return fmt.Errorf("writing IPv4 range to underlying writer: %w", err)
			}
			return nil
		}
		// Fallback: convert range to CIDRs and write each prefix
		cidrs := netipx.IPRangeFrom(start, end).Prefixes()
		for _, cidr := range cidrs {
			if err := s.ipv4.WriteRow(cidr, data); err != nil {
				return fmt.Errorf("writing IPv4 CIDR %s: %w", cidr, err)
			}
		}
		return nil
	}
	if s.ipv6 == nil {
		return errors.New("no IPv6 writer configured")
	}
	// Check if the underlying writer implements WriteRange
	if rangeWriter, ok := s.ipv6.(interface {
		WriteRange(netip.Addr, netip.Addr, []mmdbtype.DataType) error
	}); ok {
		if err := rangeWriter.WriteRange(start, end, data); err != nil {
			return fmt.Errorf("writing IPv6 range to underlying writer: %w", err)
		}
		return nil
	}
	// Fallback: convert range to CIDRs and write each prefix
	cidrs := netipx.IPRangeFrom(start, end).Prefixes()
	for _, cidr := range cidrs {
		if err := s.ipv6.WriteRow(cidr, data); err != nil {
			return fmt.Errorf("writing IPv6 CIDR %s: %w", cidr, err)
		}
	}
	return nil
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
