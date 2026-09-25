package mmdbconvert

import (
	"bytes"
	"encoding/csv"
	"os"
	"path/filepath"
	"testing"

	"github.com/pelletier/go-toml/v2"
	"github.com/stretchr/testify/require"

	"github.com/maxmind/mmdbconvert/internal/config"
)

func TestRun_CityCSVParity(t *testing.T) {
	const fixtureDir = "testdata/geoip-csv"
	cfg, err := config.LoadConfig(filepath.Join(fixtureDir, "city.toml"))
	require.NoError(t, err)
	dir := t.TempDir()
	cfg.Output.IPv4File = filepath.Join(dir, "ipv4.csv")
	cfg.Output.IPv6File = filepath.Join(dir, "ipv6.csv")
	data, err := toml.Marshal(cfg)
	require.NoError(t, err)
	configPath := filepath.Join(t.TempDir(), "city.toml")
	require.NoError(t, os.WriteFile(configPath, data, 0o600))
	require.NoError(t, Run(Options{ConfigPath: configPath}))

	for _, tt := range []struct {
		version       string
		path          string
		referenceRows int
		actualRows    int
		differences   map[string]int
	}{
		{"4", cfg.Output.IPv4File, 18, 17, map[string]int{"geoname_id": 3, "is_anonymous_proxy": 16, "is_satellite_provider": 17}},
		{"6", cfg.Output.IPv6File, 230, 230, map[string]int{"geoname_id": 229, "is_anonymous_proxy": 230, "is_satellite_provider": 230}},
	} {
		t.Run("IPv"+tt.version, func(t *testing.T) {
			name := "GeoIP2-City-Blocks-IPv" + tt.version + ".csv"
			actual, readErr := os.ReadFile(filepath.Clean(tt.path))
			require.NoError(t, readErr)
			golden, readErr := os.ReadFile(
				filepath.Clean(filepath.Join(fixtureDir, "expected", name)),
			)
			require.NoError(t, readErr)
			// Pin every output byte, including order, quoting, and precision.
			require.Equal(t, string(golden), string(actual))
			reference, readErr := os.ReadFile(
				filepath.Clean(filepath.Join(fixtureDir, "reference", name)),
			)
			require.NoError(t, readErr)
			referenceRecords, readErr := csv.NewReader(bytes.NewReader(reference)).ReadAll()
			require.NoError(t, readErr)
			actualRecords, readErr := csv.NewReader(bytes.NewReader(actual)).ReadAll()
			require.NoError(t, readErr)
			require.Len(t, referenceRecords, tt.referenceRows+1)
			require.Len(t, actualRecords, tt.actualRows+1)
			require.Equal(t, referenceRecords[0], actualRecords[0])
			assertCityDifferences(t, referenceRecords, actualRecords, tt.differences)
		})
	}
}

func assertCityDifferences(t *testing.T, reference, actual [][]string, want map[string]int) {
	t.Helper()
	byNetwork := make(map[string][]string, len(actual)-1)
	actualOrder := make([]string, 0, len(actual)-1)
	for _, row := range actual[1:] {
		require.NotContains(t, byNetwork, row[0])
		byNetwork[row[0]] = row
		actualOrder = append(actualOrder, row[0])
	}
	differences := map[string]int{}
	referenceOrder := make([]string, 0, len(reference)-1)
	for _, row := range reference[1:] {
		got, exists := byNetwork[row[0]]
		if !exists {
			// Only the continent-only record has no projected values.
			require.Equal(
				t,
				[]string{"2.3.3.0/24", "6255148", "", "", "0", "0", "", "", "", "", ""},
				row,
			)
			continue
		}
		delete(byNetwork, row[0])
		referenceOrder = append(referenceOrder, row[0])
		for i, value := range row {
			if got[i] == value {
				continue
			}
			column := reference[0][i]
			switch column {
			case "geoname_id":
				require.Empty(t, got[i], row[0])
				require.NotEmpty(t, value, row[0])
			case "is_anonymous_proxy", "is_satellite_provider":
				require.Equal(t, "0", value, row[0])
				require.Empty(t, got[i], row[0])
			default:
				t.Fatalf(
					"unexpected difference for %s column %s: reference=%q actual=%q",
					row[0],
					column,
					value,
					got[i],
				)
			}
			differences[column]++
		}
	}
	require.Empty(t, byNetwork, "unexpected networks in converted output")
	require.Equal(t, referenceOrder, actualOrder, "network order")
	require.Equal(t, want, differences)
}
