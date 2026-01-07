package merger

import (
	"fmt"
	"net/netip"

	"github.com/maxmind/mmdbwriter/mmdbtype"
	"go4.org/netipx"

	"github.com/maxmind/mmdbconvert/internal/network"
)

// AccumulatedRange represents a continuous IP range with associated data.
type AccumulatedRange struct {
	StartIP netip.Addr
	EndIP   netip.Addr
	Data    []mmdbtype.DataType // column values ordered by config.Columns
}

// RowWriter defines the interface for writing output rows.
type RowWriter interface {
	// WriteRow writes a single row with network prefix and column data.
	WriteRow(prefix netip.Prefix, data []mmdbtype.DataType) error
}

// RangeRowWriter can accept full start/end ranges instead of prefixes.
type RangeRowWriter interface {
	WriteRange(start, end netip.Addr, data []mmdbtype.DataType) error
}

// Accumulator accumulates adjacent networks with identical data and flushes
// them as CIDRs when data changes. This enables O(1) memory usage.
type Accumulator struct {
	current          *AccumulatedRange
	writer           RowWriter
	includeEmptyRows bool
	pool             *slicePool // Pool for returning slices when flushing
}

// NewAccumulator creates a new streaming accumulator.
func NewAccumulator(writer RowWriter, includeEmptyRows bool, pool *slicePool) *Accumulator {
	return &Accumulator{
		writer:           writer,
		includeEmptyRows: includeEmptyRows,
		pool:             pool,
	}
}

// Process handles an incoming network with its data. If the network is adjacent
// to the current accumulated range and has identical data, it extends the range.
// Otherwise, it flushes the current range and starts a new accumulation.
func (a *Accumulator) Process(prefix netip.Prefix, data []mmdbtype.DataType) error {
	// Skip rows with no data if includeEmptyRows is false (default)
	if !a.includeEmptyRows && isEmptyData(data) {
		return nil
	}

	addr := prefix.Addr()
	endIP := netipx.PrefixLastIP(prefix)

	// First network - get a slice from pool and copy data
	if a.current == nil {
		pooledSlice := a.pool.Get()
		copy(pooledSlice, data)
		a.current = &AccumulatedRange{
			StartIP: addr,
			EndIP:   endIP,
			Data:    pooledSlice,
		}
		return nil
	}

	// Check if we can extend current accumulation
	canExtend := network.IsAdjacent(a.current.EndIP, addr) && dataEquals(a.current.Data, data)

	if canExtend {
		// Extend the current range (no allocation needed)
		a.current.EndIP = endIP
		return nil
	}

	// Data changed or not adjacent - flush current range
	if err := a.Flush(); err != nil {
		return err
	}

	// Start new accumulation - get a new slice from pool and copy data
	pooledSlice := a.pool.Get()
	copy(pooledSlice, data)
	a.current = &AccumulatedRange{
		StartIP: addr,
		EndIP:   endIP,
		Data:    pooledSlice,
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
		err := rangeWriter.WriteRange(
			a.current.StartIP,
			a.current.EndIP,
			a.current.Data,
		)
		if err != nil {
			return fmt.Errorf(
				"writing range %s-%s: %w",
				a.current.StartIP,
				a.current.EndIP,
				err,
			)
		}
		// Return the slice to the pool after writing
		a.pool.Put(a.current.Data)
		a.current = nil
		return nil
	}

	// Convert the IP range to valid CIDRs
	cidrs := netipx.IPRangeFrom(a.current.StartIP, a.current.EndIP).Prefixes()

	// Write each CIDR as a separate row
	for _, cidr := range cidrs {
		if err := a.writer.WriteRow(cidr, a.current.Data); err != nil {
			return fmt.Errorf("writing row for %s: %w", cidr, err)
		}
	}

	// Return the slice to the pool after writing all rows
	a.pool.Put(a.current.Data)

	// Clear current accumulation
	a.current = nil
	return nil
}

// dataEquals compares two data slices for equality.
// Treats nil values as equal (both represent missing data).
func dataEquals(a, b []mmdbtype.DataType) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !dataValueEquals(a[i], b[i]) {
			return false
		}
	}
	return true
}

// dataValueEquals compares two individual values, handling nil.
func dataValueEquals(a, b mmdbtype.DataType) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	// Use mmdbtype.DataType.Equal() for actual values
	return a.Equal(b)
}

// isEmptyData checks if all values in the slice are nil.
func isEmptyData(data []mmdbtype.DataType) bool {
	for _, v := range data {
		if v != nil {
			return false
		}
	}
	return true
}
