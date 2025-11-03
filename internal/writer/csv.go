// Package writer provides output writers for CSV and Parquet formats.
package writer

import (
	"encoding/csv"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"net/netip"
	"strconv"

	"github.com/maxmind/mmdbwriter/mmdbtype"
	"go4.org/netipx"

	"github.com/maxmind/mmdbconvert/internal/config"
	"github.com/maxmind/mmdbconvert/internal/network"
)

// Network column type constants.
const (
	NetworkColumnCIDR     = "cidr"
	NetworkColumnStartIP  = "start_ip"
	NetworkColumnEndIP    = "end_ip"
	NetworkColumnStartInt = "start_int"
	NetworkColumnEndInt   = "end_int"
)

// CSVWriter writes merged MMDB data to CSV format.
type CSVWriter struct {
	writer        *csv.Writer
	config        *config.Config
	headerWritten bool
	headerEnabled bool
	rangeCapable  bool
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

	return &CSVWriter{
		writer:        csvWriter,
		config:        cfg,
		headerEnabled: headerEnabled,
		headerWritten: !headerEnabled,
		rangeCapable:  rangeCapable,
	}
}

// WriteRow writes a single row with network prefix and column data.
func (w *CSVWriter) WriteRow(prefix netip.Prefix, data map[string]any) error {
	if err := w.ensureHeader(); err != nil {
		return err
	}

	// Build row with network columns + data columns
	row := make([]string, 0, len(w.config.Network.Columns)+len(w.config.Columns))

	// Add network column values
	for _, netCol := range w.config.Network.Columns {
		value, err := w.generateNetworkColumnValue(prefix, netCol.Type)
		if err != nil {
			return fmt.Errorf("generating network column '%s': %w", netCol.Name, err)
		}
		row = append(row, value)
	}

	// Add data column values (in config order)
	for _, col := range w.config.Columns {
		value := data[col.Name]
		strValue, err := convertToString(value)
		if err != nil {
			return fmt.Errorf("converting column '%s' to string: %w", col.Name, err)
		}
		row = append(row, strValue)
	}

	// Write the row
	if err := w.writer.Write(row); err != nil {
		return fmt.Errorf("writing CSV row: %w", err)
	}

	return nil
}

// Flush ensures all buffered data is written.
func (w *CSVWriter) Flush() error {
	w.writer.Flush()
	if err := w.writer.Error(); err != nil {
		return fmt.Errorf("CSV flush error: %w", err)
	}
	return nil
}

// WriteRange implements merger.RangeRowWriter, emitting a single row when the
// configured network columns support ranges, or falling back to prefix output
// otherwise.
func (w *CSVWriter) WriteRange(start, end netip.Addr, data map[string]any) error {
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

	for _, col := range w.config.Columns {
		value := data[col.Name]
		strValue, err := convertToString(value)
		if err != nil {
			return fmt.Errorf("converting column '%s' to string: %w", col.Name, err)
		}
		row = append(row, strValue)
	}

	if err := w.writer.Write(row); err != nil {
		return fmt.Errorf("writing CSV row: %w", err)
	}

	return nil
}

// writeHeader writes the CSV header row.
func (w *CSVWriter) writeHeader() error {
	header := make([]string, 0, len(w.config.Network.Columns)+len(w.config.Columns))

	// Add network column names
	for _, netCol := range w.config.Network.Columns {
		header = append(header, netCol.Name)
	}

	// Add data column names
	for _, col := range w.config.Columns {
		header = append(header, col.Name)
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
func (w *CSVWriter) generateNetworkColumnValue(
	prefix netip.Prefix,
	colType string,
) (string, error) {
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
			return strconv.FormatUint(uint64(network.IPv4ToUint32(addr)), 10), nil
		}
		return formatIPv6AsInt(addr), nil

	case NetworkColumnEndInt:
		endIP := network.CalculateEndIP(prefix)
		if endIP.Is4() {
			return strconv.FormatUint(uint64(network.IPv4ToUint32(endIP)), 10), nil
		}
		return formatIPv6AsInt(endIP), nil

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
		return formatIPv6AsInt(start), nil
	case NetworkColumnEndInt:
		if end.Is4() {
			return strconv.FormatUint(uint64(network.IPv4ToUint32(end)), 10), nil
		}
		return formatIPv6AsInt(end), nil
	default:
		return "", fmt.Errorf("unsupported network column type '%s' for range output", colType)
	}
}

// formatIPv6AsInt formats an IPv6 address as a decimal integer string.
func formatIPv6AsInt(addr netip.Addr) string {
	var i big.Int
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
