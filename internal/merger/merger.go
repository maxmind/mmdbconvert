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
	"sync"

	"github.com/maxmind/mmdbwriter/mmdbtype"
	"github.com/oschwald/maxminddb-golang/v2"

	"github.com/maxmind/mmdbconvert/internal/config"
	"github.com/maxmind/mmdbconvert/internal/mmdb"
	"github.com/maxmind/mmdbconvert/internal/network"
)

// slicePool manages reusable data slices to reduce allocations.
type slicePool struct {
	pool sync.Pool
	size int
}

// newSlicePool creates a pool for slices of the given size.
func newSlicePool(size int) *slicePool {
	return &slicePool{
		pool: sync.Pool{
			New: func() any {
				return make([]mmdbtype.DataType, size)
			},
		},
		size: size,
	}
}

// Get retrieves a cleared slice from the pool.
func (p *slicePool) Get() []mmdbtype.DataType {
	s := p.pool.Get().([]mmdbtype.DataType)
	clear(s)
	return s
}

// Put returns a slice to the pool.
func (p *slicePool) Put(s []mmdbtype.DataType) {
	if len(s) == p.size {
		//nolint:staticcheck // SA6002: slices are reference types, no allocation here
		p.pool.Put(s)
	}
}

// columnExtractor caches the reader and path segments for a column to avoid
// per-row lookups and allocations.
type columnExtractor struct {
	reader   *mmdb.Reader    // Pre-resolved reader for this column
	path     []any           // Cached path segments (avoids per-row slice allocation)
	name     mmdbtype.String // Column name for error messages
	database string          // Database name for error messages
	dbIndex  int             // Index in readersList for O(1) Result lookup
	colIndex int             // Index in config.Columns for slice ordering
}

// Merger handles merging multiple MMDB databases into a single output stream.
type Merger struct {
	readers       *mmdb.Readers
	config        *config.Config
	acc           *Accumulator
	readersList   []*mmdb.Reader    // Ordered list of readers for iteration
	dbNamesList   []string          // Corresponding database names
	extractors    []columnExtractor // Pre-built extractors for each column
	unmarshalers  []*mmdbtype.Unmarshaler
	slicePool     *slicePool          // Pool for reusable data slices
	workingSlice  []mmdbtype.DataType // Reusable working slice (cleared each iteration)
	resultsBuffer []maxminddb.Result  // Pre-allocated buffer for recursion (eliminates slices.Concat allocations)
}

// NewMerger creates a new merger instance.
// Returns an error if database readers are missing or path normalization fails.
func NewMerger(readers *mmdb.Readers, cfg *config.Config, writer RowWriter) (*Merger, error) {
	includeEmptyRows := false
	if cfg.Output.IncludeEmptyRows != nil {
		includeEmptyRows = *cfg.Output.IncludeEmptyRows
	}

	// Create slice pool for reusable data slices
	slicePool := newSlicePool(len(cfg.Columns))

	// Create Merger instance with pool
	m := &Merger{
		readers:      readers,
		config:       cfg,
		acc:          NewAccumulator(writer, includeEmptyRows, slicePool),
		slicePool:    slicePool,
		workingSlice: make([]mmdbtype.DataType, len(cfg.Columns)),
	}

	// Build ordered list of unique database names
	// This determines the order for readersList and dbIndex values
	dbNamesList := m.getUniqueDatabaseNames()
	if len(dbNamesList) == 0 {
		return nil, errors.New("no databases configured")
	}
	m.dbNamesList = dbNamesList

	// Build readersList in the same order
	readersList := make([]*mmdb.Reader, 0, len(dbNamesList))
	for _, dbName := range dbNamesList {
		reader, ok := readers.Get(dbName)
		if !ok {
			return nil, fmt.Errorf("database '%s' not found", dbName)
		}
		readersList = append(readersList, reader)
	}
	m.readersList = readersList

	// Pre-allocate results buffer for recursion (eliminates slices.Concat allocations)
	m.resultsBuffer = make([]maxminddb.Result, len(readersList))

	// Validate IP versions before building extractors
	if err := validateIPVersions(readersList, dbNamesList); err != nil {
		return nil, err
	}

	// Pre-build column extractors with dbIndex values
	extractors := make([]columnExtractor, len(cfg.Columns))
	for i, column := range cfg.Columns {
		reader, ok := readers.Get(column.Database)
		if !ok {
			return nil, fmt.Errorf(
				"database '%s' not found for column '%s'",
				column.Database,
				column.Name,
			)
		}

		// Normalize path segments once to avoid per-row normalization allocation
		// This converts int64 to int and validates segment types
		pathSegments, err := mmdb.NormalizeSegments(column.Path)
		if err != nil {
			return nil, fmt.Errorf(
				"normalizing path for column '%s': %w",
				column.Name,
				err,
			)
		}

		// Find database index for O(1) lookup in extractAndProcess
		dbIdx := -1
		for j, name := range dbNamesList {
			if name == column.Database {
				dbIdx = j
				break
			}
		}

		extractors[i] = columnExtractor{
			reader:   reader,
			path:     pathSegments,
			name:     column.Name,
			database: column.Database,
			dbIndex:  dbIdx,
			colIndex: i,
		}
	}
	m.extractors = extractors

	// Create per-database unmarshaler to avoid cross-database cache contamination.
	// When cfg.DisableCache is false (default), use NewUnmarshaler() which provides caching.
	// When cfg.DisableCache is true, use zero-value unmarshalers which have no cache.
	m.unmarshalers = make([]*mmdbtype.Unmarshaler, len(readersList))
	for i := range readersList {
		if cfg.DisableCache {
			m.unmarshalers[i] = &mmdbtype.Unmarshaler{}
		} else {
			m.unmarshalers[i] = mmdbtype.NewUnmarshaler()
		}
	}

	return m, nil
}

// normalizePrefix applies IPv6 prefix normalization if configured.
// IPv4 prefixes and IPv6 prefixes already meeting the minimum are unchanged.
func (m *Merger) normalizePrefix(prefix netip.Prefix) netip.Prefix {
	if m.config.Output.IPv6MinPrefix != nil {
		return network.NormalizeIPv6Prefix(prefix, *m.config.Output.IPv6MinPrefix)
	}
	return prefix
}

// Merge performs the streaming merge of all databases.
// It uses nested NetworksWithin iteration to find the smallest overlapping
// networks across all databases, then extracts data and streams to accumulator.
func (m *Merger) Merge() error {
	// readersList and dbNamesList are already built in NewMerger()
	firstReader := m.readersList[0]

	// Track last normalized prefix to skip redundant iterations.
	// When IPv6 normalization is enabled (e.g., ipv6_min_prefix=64), the first
	// database might contain millions of /128 addresses that all normalize to
	// the same /64. We only need to process each unique normalized prefix once.
	var lastNormalized netip.Prefix

	// Iterate all networks in the first database
	for result := range firstReader.Networks(maxminddb.IncludeNetworksWithoutData()) {
		if err := result.Err(); err != nil {
			return fmt.Errorf("iterating first database: %w", err)
		}

		prefix := result.Prefix()
		// Apply IPv6 prefix normalization if configured
		prefix = m.normalizePrefix(prefix)

		// Skip if this normalizes to the same prefix we just processed.
		if lastNormalized.IsValid() && prefix == lastNormalized {
			continue
		}
		lastNormalized = prefix

		// If there's only one database, extract and process directly
		if len(m.readersList) == 1 {
			m.resultsBuffer[0] = result // Use buffer even for single database
			if err := m.extractAndProcess(m.resultsBuffer[:1], prefix); err != nil {
				return err
			}
			continue
		}

		// PUSH first result into buffer
		m.resultsBuffer[0] = result

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

// processNetwork recursively processes a network through remaining databases.
// It uses the pre-allocated resultsBuffer with depth tracking to avoid allocations.
//
// Invariants:
// - resultsBuffer[0:dbIndex] contains Results from previous databases.
// - effectivePrefix is the smallest network across all databases so far.
// - With IncludeNetworksWithoutData, we always get at least one Result per database.
func (m *Merger) processNetwork(
	effectivePrefix netip.Prefix,
	dbIndex int,
) error {
	// Base case: processed all databases - extract data
	if dbIndex >= len(m.readersList) {
		// Pass slice view of resultsBuffer containing all accumulated results
		return m.extractAndProcess(m.resultsBuffer[:dbIndex], effectivePrefix)
	}

	currentReader := m.readersList[dbIndex]

	// Iterate networks within effectivePrefix in this database
	// With IncludeNetworksWithoutData, this ALWAYS yields at least one Result
	for result := range currentReader.NetworksWithin(effectivePrefix, maxminddb.IncludeNetworksWithoutData()) {
		if err := result.Err(); err != nil {
			return fmt.Errorf("iterating database within %s: %w", effectivePrefix, err)
		}

		nextNetwork := result.Prefix()
		// Apply IPv6 prefix normalization if configured
		nextNetwork = m.normalizePrefix(nextNetwork)

		// Determine smallest (most specific) network
		smallest := network.SmallestNetwork(effectivePrefix, nextNetwork)

		// PUSH: Store result in pre-allocated buffer at current depth
		m.resultsBuffer[dbIndex] = result

		// Recurse with the smallest prefix
		// NOTE: smallest may be smaller than result.Prefix() - that's OK!
		// The result contains data for a broader network that covers smallest.
		if err := m.processNetwork(smallest, dbIndex+1); err != nil {
			return err
		}

		// POP: Not needed - next iteration or return will naturally overwrite
	}

	return nil
}

// extractAndProcess extracts data for all columns using precomputed Results,
// then feeds the result to the accumulator.
//
// Key optimization: Decode each database's full record once, then extract all
// columns from the cached record. This reduces decoder allocations from
// O(columns) to O(databases) per network.
//
// This function performs NO database lookups - all Results come from the slice.
// Invariants:
// - results[i] corresponds to readersList[i].
// - effectivePrefix is the actual network being processed (may be smaller than result.Prefix()).
func (m *Merger) extractAndProcess(
	results []maxminddb.Result,
	effectivePrefix netip.Prefix,
) error {
	// Step 1: Decode full records once per database
	// This replaces N decoder invocations (one per column) with M invocations (one per database)
	// For typical configs: N=50+, M=1-3, so this is a ~16-50x reduction in decoder calls
	decodedRecords := make([]mmdbtype.Map, len(results))
	for i, result := range results {
		unmarshaler := m.unmarshalers[i]
		if unmarshaler == nil {
			return fmt.Errorf(
				"unmarshaler for database %d (%s) is nil (this is a bug)",
				i,
				m.dbNamesList[i],
			)
		}

		// Decode the full record (empty path means decode entire record)
		if err := result.Decode(unmarshaler); err != nil {
			return fmt.Errorf("decoding database %d (%s): %w", i, m.dbNamesList[i], err)
		}

		// Get the decoded value and type-assert to Map
		value := unmarshaler.Result()
		unmarshaler.Clear()

		if record, ok := value.(mmdbtype.Map); ok {
			decodedRecords[i] = record
		}
		// If not a Map, leave decodedRecords[i] as nil (no data for this database)
	}
	// Step 2: Extract column values into reusable working slice
	// Clear the working slice before reuse
	clear(m.workingSlice)

	for _, extractor := range m.extractors {
		// Check if reader was resolved during initialization
		if extractor.reader == nil {
			return fmt.Errorf(
				"database '%s' not found for column '%s'",
				extractor.database,
				extractor.name,
			)
		}

		// Get cached decoded record for this database
		if extractor.dbIndex < 0 || extractor.dbIndex >= len(decodedRecords) {
			// Database index out of bounds - skip column
			continue
		}

		record := decodedRecords[extractor.dbIndex]
		if record == nil {
			continue // No data in this database for this network
		}

		// Walk the path in the cached record to extract the value
		value, err := walkPath(record, extractor.path)
		if err != nil {
			return fmt.Errorf(
				"decoding path for column '%s': %w",
				extractor.name,
				err,
			)
		}

		// Store value at column index (nil values are OK - they indicate missing data)
		if value != nil {
			m.workingSlice[extractor.colIndex] = value
		}
	}

	// Use the effectivePrefix parameter - NOT derived from results!
	// The accumulator will copy this slice to a pooled slice if data changes
	return m.acc.Process(effectivePrefix, m.workingSlice)
}

// walkPath navigates through a nested mmdbtype.Map/Slice structure using the given path.
// Returns nil if the path doesn't exist.
func walkPath(root mmdbtype.Map, path []any) (mmdbtype.DataType, error) {
	if len(path) == 0 {
		// Empty path means return the entire record
		return root, nil
	}

	var current mmdbtype.DataType = root

	for i, segment := range path {
		switch key := segment.(type) {
		case string:
			// Navigate through a map
			m, ok := current.(mmdbtype.Map)
			if !ok {
				return nil, fmt.Errorf(
					"navigating path %s segment %q: expected map but found %T",
					describeWalkPath(path[:i]),
					key,
					current,
				)
			}
			val, exists := m[mmdbtype.String(key)]
			if !exists {
				return nil, nil
			}
			current = val

		case int:
			// Navigate through a slice
			s, ok := current.(mmdbtype.Slice)
			if !ok {
				return nil, fmt.Errorf(
					"navigating path %s segment %d: expected slice but found %T",
					describeWalkPath(path[:i]),
					key,
					current,
				)
			}
			idx := key
			if idx < 0 {
				idx = len(s) + idx
			}
			if idx < 0 || idx >= len(s) {
				return nil, nil
			}
			current = s[idx]

		default:
			// Invalid path segment type
			return nil, fmt.Errorf(
				"navigating path %s: unsupported segment type %T",
				describeWalkPath(path[:i]),
				segment,
			)
		}
	}

	return current, nil
}

func describeWalkPath(path []any) string {
	if len(path) == 0 {
		return "[]"
	}

	var b strings.Builder
	b.WriteByte('[')
	for i, seg := range path {
		if i > 0 {
			b.WriteByte(' ')
		}
		switch v := seg.(type) {
		case string:
			b.WriteString(v)
		case int:
			fmt.Fprintf(&b, "%d", v)
		default:
			fmt.Fprintf(&b, "%v", v)
		}
	}
	b.WriteByte(']')
	return b.String()
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
