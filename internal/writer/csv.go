package writer

import (
	"encoding/csv"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net/netip"
	"strconv"
	"sync"

	"github.com/maxmind/mmdbwriter/mmdbtype"
	"go4.org/netipx"

	"github.com/maxmind/mmdbconvert/internal/config"
	"github.com/maxmind/mmdbconvert/internal/network"
)

// CSVWriter writes merged MMDB data to CSV format.
type CSVWriter struct {
	writer        *csv.Writer
	config        *config.Config
	headerWritten bool
	headerEnabled bool
	rangeCapable  bool
	hasBucket     bool       // Whether network_bucket column is configured
	bigIntPool    *sync.Pool // Pool of big.Int for IPv6 integer conversion
	rowBatch      [][]string // Batch buffer for rows
	batchSize     int        // Number of rows to batch before writing
}

// NewCSVWriter creates a new CSV writer.
func NewCSVWriter(w io.Writer, cfg *config.Config) *CSVWriter {
	csvWriter := csv.NewWriter(w)

	// Set delimiter (rune conversion)
	if cfg.Output.CSV.Delimiter != "" {
		csvWriter.Comma = rune(cfg.Output.CSV.Delimiter[0])
	}

	headerEnabled := true
	if cfg.Output.CSV.IncludeHeader != nil {
		headerEnabled = *cfg.Output.CSV.IncludeHeader
	}

	rangeCapable := true
	for _, col := range cfg.Network.Columns {
		switch col.Type {
		case NetworkColumnStartIP, NetworkColumnEndIP, NetworkColumnStartInt, NetworkColumnEndInt:
			// supported
		default:
			rangeCapable = false
		}
	}

	const defaultBatchSize = 1000
	return &CSVWriter{
		writer:        csvWriter,
		config:        cfg,
		headerEnabled: headerEnabled,
		headerWritten: !headerEnabled,
		rangeCapable:  rangeCapable,
		hasBucket:     hasNetworkBucketColumn(cfg),
		bigIntPool: &sync.Pool{
			New: func() any {
				return new(big.Int)
			},
		},
		rowBatch:  make([][]string, 0, defaultBatchSize),
		batchSize: defaultBatchSize,
	}
}

// getBucketSize returns the bucket prefix length for the given IP version.
func (w *CSVWriter) getBucketSize(isIPv6 bool) int {
	if isIPv6 {
		return w.config.Output.CSV.IPv6BucketSize
	}
	return w.config.Output.CSV.IPv4BucketSize
}

// WriteRow writes a single row with network prefix and column data.
// If a network_bucket column is configured, this may write multiple rows
// (one per bucket the network spans).
func (w *CSVWriter) WriteRow(prefix netip.Prefix, data []mmdbtype.DataType) error {
	if w.hasBucket {
		return w.writeRowsWithBucketing(prefix, data)
	}
	return w.writeSingleRow(prefix, netip.Prefix{}, data)
}

// writeRowsWithBucketing writes one row per bucket that the network spans.
func (w *CSVWriter) writeRowsWithBucketing(
	prefix netip.Prefix,
	data []mmdbtype.DataType,
) error {
	bucketSize := w.getBucketSize(prefix.Addr().Is6())

	buckets, err := network.SplitPrefix(prefix, bucketSize)
	if err != nil {
		return fmt.Errorf("splitting prefix into buckets: %w", err)
	}

	for _, bucket := range buckets {
		// Truncate to the bucket size boundary for the bucket column value.
		//
		// SplitPrefix returns the network unchanged when it's smaller than the
		// bucket size (e.g., 1.2.3.0/24 with /16 buckets returns [1.2.3.0/24]).
		// This is correct for determining row count (1 row), but queries compute
		// buckets as NET.IP_TRUNC(ip, 16) = 1.2.0.0, so we must store 1.2.0.0,
		// not 1.2.3.0. Without this truncation, queries would fail to find the
		// network.
		//
		// For networks larger than the bucket size (e.g., 2.0.0.0/15 with /16
		// buckets), SplitPrefix returns [2.0.0.0/16, 2.1.0.0/16] which are
		// already bucket-aligned, so Masked() is a no-op.
		bucketPrefix := bucket
		if bucket.Bits() > bucketSize {
			bucketPrefix = netip.PrefixFrom(bucket.Addr(), bucketSize).Masked()
		}

		if err := w.writeSingleRow(prefix, bucketPrefix, data); err != nil {
			return err
		}
	}
	return nil
}

// writeSingleRow writes a single row with the given prefix and optional bucket.
// If bucket.IsValid() is false, the bucket column is not written.
func (w *CSVWriter) writeSingleRow(
	prefix netip.Prefix,
	bucket netip.Prefix,
	data []mmdbtype.DataType,
) error {
	if err := w.ensureHeader(); err != nil {
		return err
	}

	// Build row with network columns + data columns
	row := make([]string, 0, len(w.config.Network.Columns)+len(w.config.Columns))

	// Add network column values
	for _, netCol := range w.config.Network.Columns {
		value, err := w.generateNetworkColumnValue(prefix, bucket, netCol.Type)
		if err != nil {
			return fmt.Errorf("generating network column '%s': %w", netCol.Name, err)
		}
		row = append(row, value)
	}

	// Add data column values (in config order)
	for i, col := range w.config.Columns {
		value := data[i]
		strValue, err := convertToString(value)
		if err != nil {
			return fmt.Errorf("converting column '%s' to string: %w", col.Name, err)
		}
		row = append(row, strValue)
	}

	// Add row to batch
	w.rowBatch = append(w.rowBatch, row)

	// Flush batch if it's full
	if len(w.rowBatch) >= w.batchSize {
		return w.flushBatch()
	}

	return nil
}

// flushBatch writes all batched rows to the CSV writer.
func (w *CSVWriter) flushBatch() error {
	for _, row := range w.rowBatch {
		if err := w.writer.Write(row); err != nil {
			return fmt.Errorf("writing CSV row: %w", err)
		}
	}
	// Clear the batch
	w.rowBatch = w.rowBatch[:0]
	return nil
}

// Flush ensures all buffered data is written.
func (w *CSVWriter) Flush() error {
	// Flush any remaining batched rows
	if err := w.flushBatch(); err != nil {
		return err
	}
	w.writer.Flush()
	if err := w.writer.Error(); err != nil {
		return fmt.Errorf("CSV flush error: %w", err)
	}
	return nil
}

// WriteRange implements merger.RangeRowWriter, emitting a single row when the
// configured network columns support ranges, or falling back to prefix output
// otherwise.
func (w *CSVWriter) WriteRange(start, end netip.Addr, data []mmdbtype.DataType) error {
	if !w.rangeCapable {
		cidrs := netipx.IPRangeFrom(start, end).Prefixes()
		for _, cidr := range cidrs {
			if err := w.WriteRow(cidr, data); err != nil {
				return err
			}
		}
		return nil
	}
	if err := w.ensureHeader(); err != nil {
		return err
	}

	row := make([]string, 0, len(w.config.Network.Columns)+len(w.config.Columns))

	for _, netCol := range w.config.Network.Columns {
		value, err := w.generateRangeNetworkValue(start, end, netCol.Type)
		if err != nil {
			return fmt.Errorf("generating network column '%s': %w", netCol.Name, err)
		}
		row = append(row, value)
	}

	for i, col := range w.config.Columns {
		value := data[i]
		strValue, err := convertToString(value)
		if err != nil {
			return fmt.Errorf("converting column '%s' to string: %w", col.Name, err)
		}
		row = append(row, strValue)
	}

	// Add row to batch
	w.rowBatch = append(w.rowBatch, row)

	// Flush batch if it's full
	if len(w.rowBatch) >= w.batchSize {
		return w.flushBatch()
	}

	return nil
}

// writeHeader writes the CSV header row.
func (w *CSVWriter) writeHeader() error {
	header := make([]string, 0, len(w.config.Network.Columns)+len(w.config.Columns))

	// Add network column names
	for _, netCol := range w.config.Network.Columns {
		header = append(header, string(netCol.Name))
	}

	// Add data column names
	for _, col := range w.config.Columns {
		header = append(header, string(col.Name))
	}

	if err := w.writer.Write(header); err != nil {
		return fmt.Errorf("writing header: %w", err)
	}

	return nil
}

func (w *CSVWriter) ensureHeader() error {
	if w.headerEnabled && !w.headerWritten {
		if err := w.writeHeader(); err != nil {
			return fmt.Errorf("writing CSV header: %w", err)
		}
		w.headerWritten = true
	}
	return nil
}

// generateNetworkColumnValue generates the value for a network column.
// bucket is only used for NetworkColumnBucket; for other column types it is ignored.
func (w *CSVWriter) generateNetworkColumnValue(
	prefix netip.Prefix,
	bucket netip.Prefix,
	colType string,
) (string, error) {
	addr := prefix.Addr()

	switch colType {
	case NetworkColumnCIDR:
		return prefix.String(), nil

	case NetworkColumnStartIP:
		return addr.String(), nil

	case NetworkColumnEndIP:
		endIP := netipx.PrefixLastIP(prefix)
		return endIP.String(), nil

	case NetworkColumnStartInt:
		if addr.Is4() {
			return strconv.FormatUint(uint64(network.IPv4ToUint32(addr)), 10), nil
		}
		return w.formatIPv6AsInt(addr), nil

	case NetworkColumnEndInt:
		endIP := netipx.PrefixLastIP(prefix)
		if endIP.Is4() {
			return strconv.FormatUint(uint64(network.IPv4ToUint32(endIP)), 10), nil
		}
		return w.formatIPv6AsInt(endIP), nil

	case NetworkColumnBucket:
		if !bucket.IsValid() {
			return "", errors.New("invalid bucket but network_bucket column requested")
		}
		bucketAddr := bucket.Addr()
		if bucketAddr.Is4() {
			return strconv.FormatUint(uint64(network.IPv4ToUint32(bucketAddr)), 10), nil
		}
		// IPv6: hex string by default, decimal int when configured
		if w.config.Output.CSV.IPv6BucketType != config.IPv6BucketTypeInt {
			return fmt.Sprintf("%x", bucketAddr.As16()), nil
		}
		val, err := network.IPv6BucketToInt64(bucketAddr)
		if err != nil {
			return "", fmt.Errorf("converting IPv6 bucket to int64: %w", err)
		}
		return strconv.FormatInt(val, 10), nil

	default:
		return "", fmt.Errorf("unknown network column type: %s", colType)
	}
}

func (w *CSVWriter) generateRangeNetworkValue(
	start netip.Addr,
	end netip.Addr,
	colType string,
) (string, error) {
	switch colType {
	case NetworkColumnStartIP:
		return start.String(), nil
	case NetworkColumnEndIP:
		return end.String(), nil
	case NetworkColumnStartInt:
		if start.Is4() {
			return strconv.FormatUint(uint64(network.IPv4ToUint32(start)), 10), nil
		}
		return w.formatIPv6AsInt(start), nil
	case NetworkColumnEndInt:
		if end.Is4() {
			return strconv.FormatUint(uint64(network.IPv4ToUint32(end)), 10), nil
		}
		return w.formatIPv6AsInt(end), nil
	default:
		return "", fmt.Errorf("unsupported network column type '%s' for range output", colType)
	}
}

// formatIPv6AsInt formats an IPv6 address as a decimal integer string using a
// pooled big.Int to reduce allocations.
func (w *CSVWriter) formatIPv6AsInt(addr netip.Addr) string {
	// Get a big.Int from the pool
	i := w.bigIntPool.Get().(*big.Int)
	defer func() {
		// Reset and return to pool
		i.SetInt64(0)
		w.bigIntPool.Put(i)
	}()

	// Convert IPv6 address to big.Int
	b := addr.As16()
	i.SetBytes(b[:])
	return i.String()
}

// convertToString converts a value to its CSV string representation.
// Handles mmdbtype.DataType values from the extractor.
func convertToString(value any) (string, error) {
	if value == nil {
		return "", nil
	}

	// Handle mmdbtype.DataType values
	switch v := value.(type) {
	case mmdbtype.Bool:
		if bool(v) {
			return "1", nil
		}
		return "0", nil
	case mmdbtype.String:
		return string(v), nil
	case mmdbtype.Int32:
		return strconv.FormatInt(int64(v), 10), nil
	case mmdbtype.Uint16:
		return strconv.FormatUint(uint64(v), 10), nil
	case mmdbtype.Uint32:
		return strconv.FormatUint(uint64(v), 10), nil
	case mmdbtype.Uint64:
		return strconv.FormatUint(uint64(v), 10), nil
	case *mmdbtype.Uint128:
		return (*big.Int)(v).String(), nil
	case mmdbtype.Float32:
		return strconv.FormatFloat(float64(v), 'g', -1, 32), nil
	case mmdbtype.Float64:
		return strconv.FormatFloat(float64(v), 'g', -1, 64), nil
	case mmdbtype.Bytes:
		return hex.EncodeToString([]byte(v)), nil
	case mmdbtype.Map:
		b, err := json.Marshal(v)
		if err != nil {
			return "", fmt.Errorf("marshaling map to JSON: %w", err)
		}
		return string(b), nil
	case mmdbtype.Slice:
		b, err := json.Marshal(v)
		if err != nil {
			return "", fmt.Errorf("marshaling slice to JSON: %w", err)
		}
		return string(b), nil
	default:
		// Fallback for any unexpected types
		return fmt.Sprintf("%v", v), nil
	}
}
