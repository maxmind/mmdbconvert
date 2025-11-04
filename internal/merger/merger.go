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

	"github.com/maxmind/mmdbwriter/mmdbtype"
	"github.com/oschwald/maxminddb-golang/v2"

	"github.com/maxmind/mmdbconvert/internal/config"
	"github.com/maxmind/mmdbconvert/internal/mmdb"
	"github.com/maxmind/mmdbconvert/internal/network"
)

// columnExtractor caches the reader and path segments for a column to avoid
// per-row lookups and allocations.
type columnExtractor struct {
	reader   *mmdb.Reader    // Pre-resolved reader for this column
	path     []any           // Cached path segments (avoids per-row slice allocation)
	name     mmdbtype.String // Column name for error messages and map key
	database string          // Database name for error messages
}

// Merger handles merging multiple MMDB databases into a single output stream.
type Merger struct {
	readers     *mmdb.Readers
	config      *config.Config
	acc         *Accumulator
	readersList []*mmdb.Reader    // Ordered list of readers for iteration
	dbNamesList []string          // Corresponding database names
	extractors  []columnExtractor // Pre-built extractors for each column
	unmarshaler *mmdbtype.Unmarshaler
}

// NewMerger creates a new merger instance.
func NewMerger(readers *mmdb.Readers, cfg *config.Config, writer RowWriter) *Merger {
	includeEmptyRows := false
	if cfg.Output.IncludeEmptyRows != nil {
		includeEmptyRows = *cfg.Output.IncludeEmptyRows
	}

	// Pre-build column extractors to avoid per-row lookups and allocations
	extractors := make([]columnExtractor, len(cfg.Columns))
	for i, column := range cfg.Columns {
		reader, ok := readers.Get(column.Database)
		if !ok {
			// This shouldn't happen if validation passed, but handle gracefully
			// The error will be caught during Merge() when we try to use it
			extractors[i] = columnExtractor{
				reader:   nil,
				path:     nil,
				name:     column.Name,
				database: column.Database,
			}
			continue
		}

		// Normalize path segments once to avoid per-row normalization allocation
		// This converts int64 to int and validates segment types
		pathSegments, err := mmdb.NormalizeSegments(column.Path)
		if err != nil {
			// This shouldn't happen if validation passed, but handle gracefully
			extractors[i] = columnExtractor{
				reader:   nil,
				path:     nil,
				name:     column.Name,
				database: column.Database,
			}
			continue
		}

		extractors[i] = columnExtractor{
			reader:   reader,
			path:     pathSegments,
			name:     column.Name,
			database: column.Database,
		}
	}

	return &Merger{
		readers:     readers,
		config:      cfg,
		acc:         NewAccumulator(writer, includeEmptyRows),
		extractors:  extractors,
		unmarshaler: mmdbtype.NewUnmarshaler(),
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
			return fmt.Errorf("iterating first database: %w", err)
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
		return fmt.Errorf("flushing accumulator: %w", err)
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
			return fmt.Errorf("iterating database within %s: %w", currentNetwork, err)
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
	// Pre-allocate map capacity to avoid dynamic growth
	data := make(mmdbtype.Map, len(m.extractors))

	// Extract values for all columns using cached extractors
	for _, extractor := range m.extractors {
		// Check if reader was resolved during initialization
		if extractor.reader == nil {
			return fmt.Errorf(
				"database '%s' not found for column '%s'",
				extractor.database,
				extractor.name,
			)
		}

		// Extract the value using the cached reader and normalized path
		// Path segments are pre-normalized in NewMerger to avoid per-row allocation
		value, err := mmdb.ExtractValueNormalized(
			extractor.reader,
			prefix,
			extractor.path,
			m.unmarshaler,
		)
		if err != nil {
			return fmt.Errorf(
				"extracting column '%s' for network %s: %w",
				extractor.name,
				prefix,
				err,
			)
		}

		// Only add non-nil values to reduce allocations and simplify empty detection
		if value != nil {
			data[extractor.name] = value
		}
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
