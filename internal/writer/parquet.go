package writer

import (
	"errors"
	"fmt"
	"io"
	"math/big"
	"net/netip"

	"github.com/maxmind/mmdbwriter/mmdbtype"
	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/compress"
	"go4.org/netipx"

	"github.com/maxmind/mmdbconvert/internal/config"
	"github.com/maxmind/mmdbconvert/internal/network"
)

const (
	ipVersionAny = 0
	ipVersion4   = 4
	ipVersion6   = 6

	// IPVersionAny represents a Parquet writer that accepts both IPv4 and IPv6 rows.
	IPVersionAny = ipVersionAny
	// IPVersion4 constrains a Parquet writer to IPv4 rows.
	IPVersion4 = ipVersion4
	// IPVersion6 constrains a Parquet writer to IPv6 rows.
	IPVersion6 = ipVersion6
)

// ParquetWriter writes merged MMDB data to Parquet format.
type ParquetWriter struct {
	writer       *parquet.GenericWriter[map[string]any]
	config       *config.Config
	schema       *parquet.Schema
	rowGroupSize int
	rowCount     int
	ipVersion    int
	hasBucket    bool
}

// NewParquetWriter creates a new Parquet writer.
func NewParquetWriter(w io.Writer, cfg *config.Config) (*ParquetWriter, error) {
	return NewParquetWriterWithIPVersion(w, cfg, ipVersionAny)
}

// NewParquetWriterWithIPVersion creates a Parquet writer scoped to a specific IP version.
// ipVersion should be 0 (mixed), 4, or 6.
func NewParquetWriterWithIPVersion(
	w io.Writer,
	cfg *config.Config,
	ipVersion int,
) (*ParquetWriter, error) {
	// Build schema from config
	schema, err := buildSchema(cfg, ipVersion)
	if err != nil {
		return nil, fmt.Errorf("building Parquet schema: %w", err)
	}

	// Get compression codec
	codec, err := getCompressionCodec(cfg.Output.Parquet.Compression)
	if err != nil {
		return nil, fmt.Errorf("getting compression codec: %w", err)
	}

	// Build writer options: schema, compression, and optional sorting metadata
	opts := []parquet.WriterOption{
		schema,
		parquet.Compression(codec),
	}
	if sortOpt := determineSortingColumns(cfg); sortOpt != nil {
		opts = append(opts, sortOpt)
	}

	// Create Parquet writer with options
	parquetWriter := parquet.NewGenericWriter[map[string]any](w, opts...)

	return &ParquetWriter{
		writer:       parquetWriter,
		config:       cfg,
		schema:       schema,
		rowGroupSize: cfg.Output.Parquet.RowGroupSize,
		ipVersion:    ipVersion,
		hasBucket:    hasNetworkBucketColumn(cfg),
	}, nil
}

// WriteRow writes a single row with network prefix and column data.
// If a network_bucket column is configured, this may write multiple rows
// (one per bucket the network spans).
func (w *ParquetWriter) WriteRow(prefix netip.Prefix, data []mmdbtype.DataType) error {
	if w.hasBucket {
		return w.writeRowsWithBucketing(prefix, data)
	}
	return w.writeSingleRow(prefix, netip.Prefix{}, data)
}

// hasNetworkBucketColumn returns true if a network_bucket column is configured.
func hasNetworkBucketColumn(cfg *config.Config) bool {
	for _, col := range cfg.Network.Columns {
		if col.Type == NetworkColumnBucket {
			return true
		}
	}
	return false
}

// getBucketSize returns the bucket prefix length for the given IP version.
func (w *ParquetWriter) getBucketSize(isIPv6 bool) int {
	if isIPv6 {
		return w.config.Output.Parquet.IPv6BucketSize
	}
	return w.config.Output.Parquet.IPv4BucketSize
}

// writeRowsWithBucketing writes one row per bucket that the network spans.
func (w *ParquetWriter) writeRowsWithBucketing(
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
func (w *ParquetWriter) writeSingleRow(
	prefix netip.Prefix,
	bucket netip.Prefix,
	data []mmdbtype.DataType,
) error {
	// Build row with network columns + data columns
	row := map[string]any{}

	// Add network column values
	for _, netCol := range w.config.Network.Columns {
		value, err := w.generateNetworkColumnValue(prefix, bucket, netCol.Type)
		if err != nil {
			return fmt.Errorf("generating network column '%s': %w", netCol.Name, err)
		}
		row[string(netCol.Name)] = value
	}

	// Add data column values (with type conversion)
	for i, col := range w.config.Columns {
		value := data[i]
		converted, err := convertToParquetType(value, col.Type)
		if err != nil {
			return fmt.Errorf("converting column '%s': %w", col.Name, err)
		}
		row[string(col.Name)] = converted
	}

	// Write the row
	if _, err := w.writer.Write([]map[string]any{row}); err != nil {
		return fmt.Errorf("writing Parquet row: %w", err)
	}

	w.rowCount++

	// Flush row group if we've reached the size limit
	if w.rowCount >= w.rowGroupSize {
		if err := w.writer.Flush(); err != nil {
			return fmt.Errorf("flushing row group: %w", err)
		}
		w.rowCount = 0
	}

	return nil
}

// Flush ensures all buffered data is written.
func (w *ParquetWriter) Flush() error {
	if err := w.writer.Close(); err != nil {
		return fmt.Errorf("closing Parquet writer: %w", err)
	}
	return nil
}

// generateNetworkColumnValue generates the value for a network column.
// bucket is only used for NetworkColumnBucket; for other column types it is ignored.
func (w *ParquetWriter) generateNetworkColumnValue(
	prefix netip.Prefix,
	bucket netip.Prefix,
	colType string,
) (any, error) {
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
			if w.ipVersion == ipVersion6 {
				return nil, errors.New("encountered IPv4 address in IPv6-specific writer")
			}
			return int64(network.IPv4ToUint32(addr)), nil
		}
		if w.ipVersion == ipVersion4 {
			return nil, errors.New(
				"start_int column type only supports IPv4 in IPv4-only Parquet files; configure output.ipv4_file and output.ipv6_file to emit IPv6 integer columns",
			)
		}
		if w.ipVersion == ipVersion6 {
			return ipv6IntBytes(addr), nil
		}
		return nil, errors.New(
			"start_int column type only supports IPv4 unless you configure output.ipv4_file and output.ipv6_file",
		)

	case NetworkColumnEndInt:
		endIP := netipx.PrefixLastIP(prefix)
		if endIP.Is4() {
			if w.ipVersion == ipVersion6 {
				return nil, errors.New("encountered IPv4 address in IPv6-specific writer")
			}
			return int64(network.IPv4ToUint32(endIP)), nil
		}
		if w.ipVersion == ipVersion4 {
			return nil, errors.New(
				"end_int column type only supports IPv4 in IPv4-only Parquet files; configure output.ipv4_file and output.ipv6_file to emit IPv6 integer columns",
			)
		}
		if w.ipVersion == ipVersion6 {
			return ipv6IntBytes(endIP), nil
		}
		return nil, errors.New(
			"end_int column type only supports IPv4 unless you configure output.ipv4_file and output.ipv6_file",
		)

	case NetworkColumnBucket:
		if !bucket.IsValid() {
			return nil, errors.New("invalid bucket but network_bucket column requested")
		}
		bucketAddr := bucket.Addr()
		if bucketAddr.Is4() {
			// IPv4: int64 (same as start_int)
			return int64(network.IPv4ToUint32(bucketAddr)), nil
		}
		// IPv6: hex string by default, int64 when explicitly configured
		if w.config.Output.Parquet.IPv6BucketType != config.IPv6BucketTypeInt {
			return fmt.Sprintf("%x", bucketAddr.As16()), nil
		}
		val, err := network.IPv6BucketToInt64(bucketAddr)
		if err != nil {
			return nil, fmt.Errorf("converting IPv6 bucket to int64: %w", err)
		}
		return val, nil

	default:
		return nil, fmt.Errorf("unknown network column type: %s", colType)
	}
}

// buildSchema builds a Parquet schema from the config.
func buildSchema(cfg *config.Config, ipVersion int) (*parquet.Schema, error) {
	fields := make(parquet.Group)

	// Add network columns
	for _, netCol := range cfg.Network.Columns {
		node, err := buildNetworkNode(netCol, ipVersion, cfg)
		if err != nil {
			return nil, fmt.Errorf(
				"building node for network column '%s': %w",
				netCol.Name,
				err,
			)
		}
		fields[string(netCol.Name)] = node
	}

	// Add data columns
	for _, col := range cfg.Columns {
		node, err := buildDataNode(col)
		if err != nil {
			return nil, fmt.Errorf("building node for column '%s': %w", col.Name, err)
		}
		fields[string(col.Name)] = node
	}

	schema := parquet.NewSchema("mmdb", fields)
	return schema, nil
}

// buildNetworkNode builds a Parquet node for a network column.
func buildNetworkNode(
	col config.NetworkColumn,
	ipVersion int,
	cfg *config.Config,
) (parquet.Node, error) {
	switch col.Type {
	case NetworkColumnCIDR, NetworkColumnStartIP, NetworkColumnEndIP:
		// String columns
		return parquet.Optional(parquet.String()), nil

	case NetworkColumnStartInt, NetworkColumnEndInt:
		if ipVersion == ipVersion6 {
			return parquet.Optional(parquet.Leaf(parquet.FixedLenByteArrayType(16))), nil
		}
		return parquet.Optional(parquet.Int(64)), nil

	case NetworkColumnBucket:
		// IPv6 bucket: string (hex) by default, int64 when explicitly configured
		if ipVersion == ipVersion6 &&
			cfg.Output.Parquet.IPv6BucketType != config.IPv6BucketTypeInt {
			return parquet.Optional(parquet.String()), nil
		}
		return parquet.Optional(parquet.Int(64)), nil

	default:
		return nil, fmt.Errorf("unknown network column type: %s", col.Type)
	}
}

func ipv6IntBytes(addr netip.Addr) []byte {
	b := addr.As16()
	out := make([]byte, 16)
	copy(out, b[:])
	return out
}

// buildDataNode builds a Parquet node for a data column.
func buildDataNode(col config.Column) (parquet.Node, error) {
	// Default to string if no type specified
	if col.Type == "" || col.Type == "string" {
		return parquet.Optional(parquet.String()), nil
	}

	switch col.Type {
	case "int64":
		return parquet.Optional(parquet.Int(64)), nil

	case "float64":
		return parquet.Optional(parquet.Leaf(parquet.DoubleType)), nil

	case "bool":
		return parquet.Optional(parquet.Leaf(parquet.BooleanType)), nil

	case "binary":
		return parquet.Optional(parquet.Leaf(parquet.ByteArrayType)), nil

	default:
		return nil, fmt.Errorf("unknown column type: %s", col.Type)
	}
}

// convertToParquetType converts a value to the appropriate Parquet type.
func convertToParquetType(value any, typeHint string) (any, error) {
	if value == nil {
		return nil, nil
	}

	// If no type hint, return as-is (will be string)
	if typeHint == "" || typeHint == "string" {
		return convertToString(value)
	}

	switch typeHint {
	case "int64":
		switch v := value.(type) {
		case mmdbtype.Int32:
			return int64(v), nil
		case mmdbtype.Uint16:
			return int64(v), nil
		case mmdbtype.Uint32:
			return int64(v), nil
		case mmdbtype.Uint64:
			if v > 9223372036854775807 {
				return nil, fmt.Errorf("uint64 value %d overflows int64", v)
			}
			//nolint:gosec // Overflow checked above
			return int64(v), nil
		case *mmdbtype.Uint128:
			i := (*big.Int)(v)
			if !i.IsInt64() {
				return nil, fmt.Errorf("uint128 value %s overflows int64", i.String())
			}
			return i.Int64(), nil
		default:
			return nil, fmt.Errorf("cannot convert %T to int64", value)
		}

	case "float64":
		switch v := value.(type) {
		case mmdbtype.Float32:
			return float64(v), nil
		case mmdbtype.Float64:
			return float64(v), nil
		case mmdbtype.Int32:
			return float64(v), nil
		case mmdbtype.Uint16:
			return float64(v), nil
		case mmdbtype.Uint32:
			return float64(v), nil
		case mmdbtype.Uint64:
			return float64(v), nil
		case *mmdbtype.Uint128:
			i := (*big.Int)(v)
			f, _ := i.Float64()
			return f, nil
		default:
			return nil, fmt.Errorf("cannot convert %T to float64", value)
		}

	case "bool":
		if v, ok := value.(mmdbtype.Bool); ok {
			return bool(v), nil
		}
		return nil, fmt.Errorf("cannot convert %T to bool", value)

	case "binary":
		if v, ok := value.(mmdbtype.Bytes); ok {
			return []byte(v), nil
		}
		return nil, fmt.Errorf("cannot convert %T to binary", value)

	default:
		return nil, fmt.Errorf("unknown type hint: %s", typeHint)
	}
}

// getCompressionCodec returns the compression codec for the given name.
func getCompressionCodec(name string) (compress.Codec, error) {
	switch name {
	case "none":
		return &parquet.Uncompressed, nil
	case "snappy":
		return &parquet.Snappy, nil
	case "gzip":
		return &parquet.Gzip, nil
	case "lz4":
		return &parquet.Lz4Raw, nil
	case "zstd":
		return &parquet.Zstd, nil
	default:
		return nil, fmt.Errorf("unknown compression codec: %s", name)
	}
}

// determineSortingColumns returns a sorting writer config if a start_int column
// is configured. MMDB data is naturally sorted by network prefix, so we declare
// this sort order in the Parquet metadata to help query engines optimize lookups.
func determineSortingColumns(cfg *config.Config) parquet.WriterOption {
	for _, col := range cfg.Network.Columns {
		if col.Type == NetworkColumnStartInt {
			return parquet.SortingWriterConfig(
				parquet.SortingColumns(parquet.Ascending(string(col.Name))),
			)
		}
	}
	return nil
}
