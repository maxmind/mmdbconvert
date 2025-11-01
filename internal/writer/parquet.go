package writer

import (
	"errors"
	"fmt"
	"io"
	"net/netip"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/compress"

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
		return nil, fmt.Errorf("failed to build Parquet schema: %w", err)
	}

	// Get compression codec
	codec, err := getCompressionCodec(cfg.Output.Parquet.Compression)
	if err != nil {
		return nil, fmt.Errorf("failed to get compression codec: %w", err)
	}

	// Create Parquet writer with options
	parquetWriter := parquet.NewGenericWriter[map[string]any](
		w,
		schema,
		parquet.Compression(codec),
	)

	return &ParquetWriter{
		writer:       parquetWriter,
		config:       cfg,
		schema:       schema,
		rowGroupSize: cfg.Output.Parquet.RowGroupSize,
		ipVersion:    ipVersion,
	}, nil
}

// WriteRow writes a single row with network prefix and column data.
func (w *ParquetWriter) WriteRow(prefix netip.Prefix, data map[string]any) error {
	// Build row with network columns + data columns
	row := map[string]any{}

	// Add network column values
	for _, netCol := range w.config.Network.Columns {
		value, err := w.generateNetworkColumnValue(prefix, netCol.Type)
		if err != nil {
			return fmt.Errorf("failed to generate network column '%s': %w", netCol.Name, err)
		}
		row[netCol.Name] = value
	}

	// Add data column values (with type conversion)
	for _, col := range w.config.Columns {
		value := data[col.Name]
		converted, err := convertToParquetType(value, col.Type)
		if err != nil {
			return fmt.Errorf("failed to convert column '%s': %w", col.Name, err)
		}
		row[col.Name] = converted
	}

	// Write the row
	if _, err := w.writer.Write([]map[string]any{row}); err != nil {
		return fmt.Errorf("failed to write Parquet row: %w", err)
	}

	w.rowCount++

	// Flush row group if we've reached the size limit
	if w.rowCount >= w.rowGroupSize {
		if err := w.writer.Flush(); err != nil {
			return fmt.Errorf("failed to flush row group: %w", err)
		}
		w.rowCount = 0
	}

	return nil
}

// Flush ensures all buffered data is written.
func (w *ParquetWriter) Flush() error {
	if err := w.writer.Close(); err != nil {
		return fmt.Errorf("failed to close Parquet writer: %w", err)
	}
	return nil
}

// generateNetworkColumnValue generates the value for a network column.
func (w *ParquetWriter) generateNetworkColumnValue(
	prefix netip.Prefix,
	colType string,
) (any, error) {
	addr := prefix.Addr()

	switch colType {
	case NetworkColumnCIDR:
		return prefix.String(), nil

	case NetworkColumnStartIP:
		return addr.String(), nil

	case NetworkColumnEndIP:
		endIP := network.CalculateEndIP(prefix)
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
		endIP := network.CalculateEndIP(prefix)
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

	default:
		return nil, fmt.Errorf("unknown network column type: %s", colType)
	}
}

// buildSchema builds a Parquet schema from the config.
func buildSchema(cfg *config.Config, ipVersion int) (*parquet.Schema, error) {
	fields := make(parquet.Group)

	// Add network columns
	for _, netCol := range cfg.Network.Columns {
		node, err := buildNetworkNode(netCol, ipVersion)
		if err != nil {
			return nil, fmt.Errorf(
				"failed to build node for network column '%s': %w",
				netCol.Name,
				err,
			)
		}
		fields[netCol.Name] = node
	}

	// Add data columns
	for _, col := range cfg.Columns {
		node, err := buildDataNode(col)
		if err != nil {
			return nil, fmt.Errorf("failed to build node for column '%s': %w", col.Name, err)
		}
		fields[col.Name] = node
	}

	schema := parquet.NewSchema("mmdb", fields)
	return schema, nil
}

// buildNetworkNode builds a Parquet node for a network column.
func buildNetworkNode(col config.NetworkColumn, ipVersion int) (parquet.Node, error) {
	switch col.Type {
	case NetworkColumnCIDR, NetworkColumnStartIP, NetworkColumnEndIP:
		// String columns
		return parquet.Optional(parquet.String()), nil

	case NetworkColumnStartInt, NetworkColumnEndInt:
		if ipVersion == ipVersion6 {
			return parquet.Optional(parquet.Leaf(parquet.FixedLenByteArrayType(16))), nil
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
//
//nolint:gocyclo // Type conversion inherently requires many cases
func convertToParquetType(value any, typeHint string) (any, error) {
	if value == nil {
		return nil, nil
	}

	// If no type hint, return as-is (will be string)
	if typeHint == "" || typeHint == "string" {
		return convertToString(value), nil
	}

	switch typeHint {
	case "int64":
		switch v := value.(type) {
		case int:
			return int64(v), nil
		case int8:
			return int64(v), nil
		case int16:
			return int64(v), nil
		case int32:
			return int64(v), nil
		case int64:
			return v, nil
		case uint:
			if v > 9223372036854775807 {
				return nil, fmt.Errorf("uint value %d overflows int64", v)
			}
			return int64(v), nil
		case uint8:
			return int64(v), nil
		case uint16:
			return int64(v), nil
		case uint32:
			return int64(v), nil
		case uint64:
			if v > 9223372036854775807 {
				return nil, fmt.Errorf("uint64 value %d overflows int64", v)
			}
			return int64(v), nil
		default:
			return nil, fmt.Errorf("cannot convert %T to int64", value)
		}

	case "float64":
		switch v := value.(type) {
		case float32:
			return float64(v), nil
		case float64:
			return v, nil
		case int:
			return float64(v), nil
		case int8:
			return float64(v), nil
		case int16:
			return float64(v), nil
		case int32:
			return float64(v), nil
		case int64:
			return float64(v), nil
		case uint:
			return float64(v), nil
		case uint8:
			return float64(v), nil
		case uint16:
			return float64(v), nil
		case uint32:
			return float64(v), nil
		case uint64:
			return float64(v), nil
		default:
			return nil, fmt.Errorf("cannot convert %T to float64", value)
		}

	case "bool":
		if v, ok := value.(bool); ok {
			return v, nil
		}
		return nil, fmt.Errorf("cannot convert %T to bool", value)

	case "binary":
		if v, ok := value.([]byte); ok {
			return v, nil
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
