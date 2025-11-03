// Package mmdb provides MMDB database reading and data extraction functionality.
package mmdb

import (
	"fmt"
	"iter"
	"net/netip"

	"github.com/oschwald/maxminddb-golang/v2"
)

// Reader wraps a maxminddb.Reader with additional functionality.
type Reader struct {
	reader *maxminddb.Reader
	path   string
}

// Open opens an MMDB database file.
func Open(path string) (*Reader, error) {
	reader, err := maxminddb.Open(path)
	if err != nil {
		return nil, fmt.Errorf("opening MMDB file '%s': %w", path, err)
	}

	return &Reader{
		reader: reader,
		path:   path,
	}, nil
}

// Close closes the MMDB database.
func (r *Reader) Close() error {
	if err := r.reader.Close(); err != nil {
		return fmt.Errorf("closing MMDB reader: %w", err)
	}
	return nil
}

// Networks returns an iterator over all networks in the database.
func (r *Reader) Networks(options ...maxminddb.NetworksOption) iter.Seq[maxminddb.Result] {
	return r.reader.Networks(options...)
}

// NetworksWithin returns an iterator over networks within the specified prefix.
func (r *Reader) NetworksWithin(
	prefix netip.Prefix,
	options ...maxminddb.NetworksOption,
) iter.Seq[maxminddb.Result] {
	return r.reader.NetworksWithin(prefix, options...)
}

// Lookup looks up data for an IP address.
func (r *Reader) Lookup(ip netip.Addr) maxminddb.Result {
	return r.reader.Lookup(ip)
}

// Path returns the file path of the database.
func (r *Reader) Path() string {
	return r.path
}

// Metadata returns metadata about the database.
func (r *Reader) Metadata() maxminddb.Metadata {
	return r.reader.Metadata
}

// Readers manages multiple MMDB database readers.
type Readers struct {
	readers map[string]*Reader // database name -> reader
}

// OpenDatabases opens multiple MMDB databases.
func OpenDatabases(databases map[string]string) (*Readers, error) {
	readers := map[string]*Reader{}

	for name, path := range databases {
		reader, err := Open(path)
		if err != nil {
			// Close any already opened readers
			for _, r := range readers {
				r.Close()
			}
			return nil, err
		}
		readers[name] = reader
	}

	return &Readers{readers: readers}, nil
}

// Get returns the reader for a database by name.
func (rs *Readers) Get(name string) (*Reader, bool) {
	reader, ok := rs.readers[name]
	return reader, ok
}

// Close closes all database readers.
func (rs *Readers) Close() error {
	var firstErr error
	for _, reader := range rs.readers {
		if err := reader.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}
