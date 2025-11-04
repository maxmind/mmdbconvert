package writer

import (
	"io"
	"net/netip"
	"testing"

	"github.com/maxmind/mmdbwriter/mmdbtype"

	"github.com/maxmind/mmdbconvert/internal/config"
)

// BenchmarkCSVWriteRow benchmarks writing a complete CSV row (end-to-end writer operation).
func BenchmarkCSVWriteRow(b *testing.B) {
	includeHeader := true
	cfg := &config.Config{
		Network: config.NetworkConfig{
			Columns: []config.NetworkColumn{
				{Name: "network", Type: "cidr"},
			},
		},
		Columns: []config.Column{
			{Name: "country", Database: "db", Path: config.Path{"country", "iso_code"}},
			{Name: "city", Database: "db", Path: config.Path{"city", "name"}},
			{Name: "latitude", Database: "db", Path: config.Path{"location", "latitude"}},
			{Name: "longitude", Database: "db", Path: config.Path{"location", "longitude"}},
		},
		Output: config.OutputConfig{
			CSV: config.CSVConfig{
				Delimiter:     ",",
				IncludeHeader: &includeHeader,
			},
		},
	}

	writer := NewCSVWriter(io.Discard, cfg)

	prefix := netip.MustParsePrefix("1.0.0.0/24")

	data := mmdbtype.Map{
		"country":   mmdbtype.String("US"),
		"city":      mmdbtype.String("New York"),
		"latitude":  mmdbtype.Float64(40.7128),
		"longitude": mmdbtype.Float64(-74.0060),
	}

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		err := writer.WriteRow(prefix, data)
		if err != nil {
			b.Fatal(err)
		}
	}
}
