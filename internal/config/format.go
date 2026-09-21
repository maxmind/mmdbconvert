package config

import (
	"errors"
	"fmt"
	"slices"

	"github.com/pelletier/go-toml/v2"
)

// Bound the per-value allocation required by fixed-decimal formatting.
const maxFloatPrecision = 1000

// ColumnFormat selects a CSV formatter. Pointers distinguish omitted options
// from a precision of zero or an explicitly empty boolean label.
type ColumnFormat struct {
	Precision *int    `toml:"precision"`
	True      *string `toml:"true"`
	False     *string `toml:"false"`
}

func (f *ColumnFormat) validate(outputFormat string) error {
	if f == nil {
		return nil
	}
	if outputFormat != OutputFormatCSV {
		return fmt.Errorf("format is supported only for CSV output, got '%s'", outputFormat)
	}
	switch {
	case f.Precision != nil && (f.True != nil || f.False != nil):
		return errors.New("precision cannot be combined with true or false labels")
	case f.Precision != nil:
		if *f.Precision < 0 || *f.Precision > maxFloatPrecision {
			return fmt.Errorf(
				"format requires a precision from 0 through %d",
				maxFloatPrecision,
			)
		}
	case f.True != nil || f.False != nil:
		if f.True == nil || f.False == nil {
			return errors.New("format requires both true and false labels")
		}
	default:
		return errors.New("format requires precision or both true and false labels")
	}
	return nil
}

// validateFormatKeys rejects unknown options only within column formats.
func validateFormatKeys(data []byte) error {
	var raw struct {
		Columns []struct {
			Name   string         `toml:"name"`
			Format map[string]any `toml:"format"`
		} `toml:"columns"`
	}
	if err := toml.Unmarshal(data, &raw); err != nil {
		return fmt.Errorf("decoding format options: %w", err)
	}
	for _, col := range raw.Columns {
		for key := range col.Format {
			if !slices.Contains([]string{"precision", "true", "false"}, key) {
				return fmt.Errorf("column '%s': unknown format option %q", col.Name, key)
			}
		}
	}
	return nil
}
