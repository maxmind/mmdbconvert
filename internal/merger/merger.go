// Package merger implements streaming network merge algorithm using nested iteration.
//
// The merger processes networks from multiple MMDB databases, resolving overlaps
// by selecting the smallest network at each point. Adjacent networks with identical
// data are automatically merged for compact output. The streaming accumulator ensures
// O(1) memory usage regardless of database size.
package merger

import (
	"errors"
	"fmt"
	"net/netip"
	"strings"

	"github.com/oschwald/maxminddb-golang/v2"

	"github.com/maxmind/mmdbconvert/internal/config"
	"github.com/maxmind/mmdbconvert/internal/mmdb"
	"github.com/maxmind/mmdbconvert/internal/network"
)

// Merger handles merging multiple MMDB databases into a single output stream.
type Merger struct {
	readers     *mmdb.Readers
	config      *config.Config
	acc         *Accumulator
	readersList []*mmdb.Reader // Ordered list of readers for iteration
	dbNamesList []string       // Corresponding database names
}

// NewMerger creates a new merger instance.
func NewMerger(readers *mmdb.Readers, cfg *config.Config, writer RowWriter) *Merger {
	return &Merger{
		readers: readers,
		config:  cfg,
		acc:     NewAccumulator(writer),
	}
}

// Merge performs the streaming merge of all databases.
// It uses nested NetworksWithin iteration to find the smallest overlapping
// networks across all databases, then extracts data and streams to accumulator.
func (m *Merger) Merge() error {
	// Get the list of unique databases referenced in the config
	dbNames := m.getUniqueDatabaseNames()

	if len(dbNames) == 0 {
		return errors.New("no databases configured")
	}

	// Get readers for all databases
	readers := make([]*mmdb.Reader, 0, len(dbNames))
	dbNamesList := make([]string, 0, len(dbNames))
	for _, name := range dbNames {
		reader, ok := m.readers.Get(name)
		if !ok {
			return fmt.Errorf("database '%s' not found", name)
		}
		readers = append(readers, reader)
		dbNamesList = append(dbNamesList, name)
	}

	if err := validateIPVersions(readers, dbNamesList); err != nil {
		return err
	}

	// Store readers and names in the merger for easy access
	m.readersList = readers
	m.dbNamesList = dbNamesList

	// Start iteration with the first database
	firstReader := readers[0]

	// Iterate all networks in the first database
	for result := range firstReader.Networks(maxminddb.IncludeNetworksWithoutData()) {
		if err := result.Err(); err != nil {
			return fmt.Errorf("error iterating first database: %w", err)
		}

		prefix := result.Prefix()

		// If there's only one database, extract and process directly
		if len(readers) == 1 {
			if err := m.extractAndProcess(prefix); err != nil {
				return err
			}
			continue
		}

		// Process this network through remaining databases starting at index 1
		if err := m.processNetwork(prefix, 1); err != nil {
			return err
		}
	}

	// Flush any remaining accumulated data
	if err := m.acc.Flush(); err != nil {
		return fmt.Errorf("failed to flush accumulator: %w", err)
	}

	return nil
}

// processNetwork recursively processes a network through databases.
// It finds the smallest overlapping networks across all remaining databases.
// At the deepest level (no more databases), it extracts data and feeds to accumulator.
func (m *Merger) processNetwork(currentNetwork netip.Prefix, dbIndex int) error {
	// Base case: processed all databases - extract data and feed to accumulator
	if dbIndex >= len(m.readersList) {
		return m.extractAndProcess(currentNetwork)
	}

	// Get current database reader
	currentReader := m.readersList[dbIndex]

	// Use NetworksWithin to get all networks in the current database that overlap with
	// currentNetwork
	// We must use IncludeNetworksWithoutData to see the complete network structure
	iteratedAny := false
	for result := range currentReader.NetworksWithin(currentNetwork, maxminddb.IncludeNetworksWithoutData()) {
		if err := result.Err(); err != nil {
			return fmt.Errorf("error iterating database within %s: %w", currentNetwork, err)
		}

		iteratedAny = true
		nextNetwork := result.Prefix()

		// Use the smallest (most specific) of the two networks
		smallest := network.SmallestNetwork(currentNetwork, nextNetwork)

		// Recursively process with the next database
		if err := m.processNetwork(smallest, dbIndex+1); err != nil {
			return err
		}
	}

	// If the current database had no networks within currentNetwork,
	// we still need to process currentNetwork with remaining databases
	if !iteratedAny {
		return m.processNetwork(currentNetwork, dbIndex+1)
	}

	return nil
}

// extractAndProcess extracts data for all columns from all databases for the given network,
// then feeds it to the accumulator.
func (m *Merger) extractAndProcess(prefix netip.Prefix) error {
	data := map[string]any{}

	// Extract values for all columns from all databases
	for _, column := range m.config.Columns {
		// Find the reader for this column's database
		reader, ok := m.readers.Get(column.Database)
		if !ok {
			return fmt.Errorf(
				"database '%s' not found for column '%s'",
				column.Database,
				column.Name,
			)
		}

		// Extract the value (this is the single decode per column)
		value, err := mmdb.ExtractValue(reader, prefix, column.Path.Segments(), column.Type)
		if err != nil {
			return fmt.Errorf(
				"failed to extract column '%s' for network %s: %w",
				column.Name,
				prefix,
				err,
			)
		}

		// Store in data map (nil values are valid - they represent missing data)
		data[column.Name] = value
	}

	// Feed to accumulator
	return m.acc.Process(prefix, data)
}

// getUniqueDatabaseNames returns the list of unique database names used in columns.
func (m *Merger) getUniqueDatabaseNames() []string {
	seen := map[string]bool{}
	var names []string

	for _, column := range m.config.Columns {
		if !seen[column.Database] {
			seen[column.Database] = true
			names = append(names, column.Database)
		}
	}

	return names
}

func validateIPVersions(readers []*mmdb.Reader, names []string) error {
	var (
		ipv4Only     []string
		ipv6Capable  []string
		unsupportedV []string
	)

	for idx, reader := range readers {
		version := reader.Metadata().IPVersion
		switch version {
		case 4:
			ipv4Only = append(ipv4Only, names[idx])
		case 6:
			ipv6Capable = append(ipv6Capable, names[idx])
		default:
			unsupportedV = append(
				unsupportedV,
				fmt.Sprintf("%s (ip_version=%d)", names[idx], version),
			)
		}
	}

	if len(unsupportedV) > 0 {
		return fmt.Errorf(
			"unsupported ip_version values reported: %s",
			strings.Join(unsupportedV, ", "),
		)
	}

	if len(ipv4Only) > 0 && len(ipv6Capable) > 0 {
		return fmt.Errorf(
			"configured databases mix IPv4-only (%s) and IPv6-capable (%s) files; run separate conversions per IP version or supply homogeneous databases",
			strings.Join(ipv4Only, ", "),
			strings.Join(ipv6Capable, ", "),
		)
	}

	return nil
}
