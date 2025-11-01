// Package writer provides output writers for CSV and Parquet formats.
package writer

import (
	"encoding/csv"
	"encoding/hex"
	"fmt"
	"io"
	"math/big"
	"net/netip"
	"strconv"

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

	return &CSVWriter{
		writer:        csvWriter,
		config:        cfg,
		headerEnabled: headerEnabled,
		headerWritten: !headerEnabled,
	}
}

// WriteRow writes a single row with network prefix and column data.
func (w *CSVWriter) WriteRow(prefix netip.Prefix, data map[string]any) error {
	// Write header on first row
	if w.headerEnabled && !w.headerWritten {
		if err := w.writeHeader(); err != nil {
			return fmt.Errorf("failed to write CSV header: %w", err)
		}
		w.headerWritten = true
	}

	// Build row with network columns + data columns
	row := make([]string, 0, len(w.config.Network.Columns)+len(w.config.Columns))

	// Add network column values
	for _, netCol := range w.config.Network.Columns {
		value, err := w.generateNetworkColumnValue(prefix, netCol.Type)
		if err != nil {
			return fmt.Errorf("failed to generate network column '%s': %w", netCol.Name, err)
		}
		row = append(row, value)
	}

	// Add data column values (in config order)
	for _, col := range w.config.Columns {
		value := data[col.Name]
		strValue := convertToString(value)
		row = append(row, strValue)
	}

	// Write the row
	if err := w.writer.Write(row); err != nil {
		return fmt.Errorf("failed to write CSV row: %w", err)
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
		return fmt.Errorf("failed to write header: %w", err)
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

// formatIPv6AsInt formats an IPv6 address as a decimal integer string.
func formatIPv6AsInt(addr netip.Addr) string {
	var i big.Int
	b := addr.As16()
	i.SetBytes(b[:])
	return i.String()
}

// convertToString converts a value to its CSV string representation.
func convertToString(value any) string {
	if value == nil {
		return ""
	}

	switch v := value.(type) {
	case string:
		return v
	case int, int8, int16, int32, int64:
		return fmt.Sprintf("%d", v)
	case uint, uint8, uint16, uint32, uint64:
		return fmt.Sprintf("%d", v)
	case float32, float64:
		return fmt.Sprintf("%g", v)
	case bool:
		return strconv.FormatBool(v)
	case []byte:
		// Binary data - encode as hex
		return hex.EncodeToString(v)
	default:
		// Fallback: use fmt.Sprintf
		return fmt.Sprintf("%v", v)
	}
}
