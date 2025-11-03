package merger

import (
	"fmt"
	"net/netip"
	"reflect"

	"go4.org/netipx"

	"github.com/maxmind/mmdbconvert/internal/network"
)

// AccumulatedRange represents a continuous IP range with associated data.
type AccumulatedRange struct {
	StartIP netip.Addr
	EndIP   netip.Addr
	Data    map[string]any // column_name -> value
}

// RowWriter defines the interface for writing output rows.
type RowWriter interface {
	// WriteRow writes a single row with network prefix and column data.
	WriteRow(prefix netip.Prefix, data map[string]any) error
}

// RangeRowWriter can accept full start/end ranges instead of prefixes.
type RangeRowWriter interface {
	WriteRange(start, end netip.Addr, data map[string]any) error
}

// Accumulator accumulates adjacent networks with identical data and flushes
// them as CIDRs when data changes. This enables O(1) memory usage.
type Accumulator struct {
	current          *AccumulatedRange
	writer           RowWriter
	includeEmptyRows bool
}

// NewAccumulator creates a new streaming accumulator.
func NewAccumulator(writer RowWriter, includeEmptyRows bool) *Accumulator {
	return &Accumulator{
		writer:           writer,
		includeEmptyRows: includeEmptyRows,
	}
}

// Process handles an incoming network with its data. If the network is adjacent
// to the current accumulated range and has identical data, it extends the range.
// Otherwise, it flushes the current range and starts a new accumulation.
func (a *Accumulator) Process(prefix netip.Prefix, data map[string]any) error {
	// Skip rows with no data if includeEmptyRows is false (default)
	if !a.includeEmptyRows && len(data) == 0 {
		return nil
	}

	addr := prefix.Addr()
	endIP := network.CalculateEndIP(prefix)

	// First network
	if a.current == nil {
		a.current = &AccumulatedRange{
			StartIP: addr,
			EndIP:   endIP,
			Data:    data,
		}
		return nil
	}

	// Check if we can extend current accumulation
	canExtend := network.IsAdjacent(a.current.EndIP, addr) && dataEquals(a.current.Data, data)

	if canExtend {
		// Extend the current range
		a.current.EndIP = endIP
		return nil
	}

	// Data changed or not adjacent - flush current range
	if err := a.Flush(); err != nil {
		return err
	}

	// Start new accumulation
	a.current = &AccumulatedRange{
		StartIP: addr,
		EndIP:   endIP,
		Data:    data,
	}

	return nil
}

// Flush writes the current accumulated range as one or more CIDR rows.
// An accumulated range may produce multiple CIDRs if it doesn't align perfectly.
func (a *Accumulator) Flush() error {
	if a.current == nil {
		return nil
	}

	if rangeWriter, ok := a.writer.(RangeRowWriter); ok {
		if err := rangeWriter.WriteRange(a.current.StartIP, a.current.EndIP, a.current.Data); err != nil {
			return fmt.Errorf(
				"failed to write range %s-%s: %w",
				a.current.StartIP,
				a.current.EndIP,
				err,
			)
		}
		a.current = nil
		return nil
	}

	// Convert the IP range to valid CIDRs
	cidrs := netipx.IPRangeFrom(a.current.StartIP, a.current.EndIP).Prefixes()

	// Write each CIDR as a separate row
	for _, cidr := range cidrs {
		if err := a.writer.WriteRow(cidr, a.current.Data); err != nil {
			return fmt.Errorf("failed to write row for %s: %w", cidr, err)
		}
	}

	// Clear current accumulation
	a.current = nil
	return nil
}

// dataEquals performs deep equality check for data maps.
func dataEquals(a, b map[string]any) bool {
	if len(a) != len(b) {
		return false
	}
	for key, av := range a {
		bv, ok := b[key]
		if !ok {
			return false
		}
		if !reflect.DeepEqual(av, bv) {
			return false
		}
	}
	return true
}
