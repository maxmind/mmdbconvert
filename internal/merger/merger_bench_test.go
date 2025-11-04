package merger

import (
	"net/netip"
	"testing"

	"github.com/maxmind/mmdbwriter/mmdbtype"
	"github.com/stretchr/testify/require"

	"github.com/maxmind/mmdbconvert/internal/config"
	"github.com/maxmind/mmdbconvert/internal/mmdb"
)

// discardWriter is a RowWriter that discards all data (for benchmarking).
type discardWriter struct{}

func (d *discardWriter) WriteRow(_ netip.Prefix, _ mmdbtype.Map) error {
	return nil
}

// BenchmarkMergerFullMerge benchmarks a complete end-to-end merge operation.
// This is the most important benchmark as it shows overall performance.
func BenchmarkMergerFullMerge(b *testing.B) {
	dbPath := "../../testdata/MaxMind-DB/test-data/GeoIP2-City-Test.mmdb"

	databases := map[string]string{
		"city": dbPath,
	}

	cfg := &config.Config{
		Network: config.NetworkConfig{
			Columns: []config.NetworkColumn{
				{Name: "network", Type: "cidr"},
			},
		},
		Columns: []config.Column{
			{Name: "country", Database: "city", Path: config.Path{"country", "iso_code"}},
			{Name: "city", Database: "city", Path: config.Path{"city", "names", "en"}},
			{Name: "latitude", Database: "city", Path: config.Path{"location", "latitude"}},
			{Name: "longitude", Database: "city", Path: config.Path{"location", "longitude"}},
		},
		Output: config.OutputConfig{},
	}

	b.ReportAllocs()

	for b.Loop() {
		b.StopTimer()
		readers, err := mmdb.OpenDatabases(databases)
		require.NoError(b, err)

		writer := &discardWriter{}
		merger, err := NewMerger(readers, cfg, writer)
		require.NoError(b, err)

		b.StartTimer()
		err = merger.Merge()
		b.StopTimer()

		require.NoError(b, err)
		readers.Close()
	}
}
