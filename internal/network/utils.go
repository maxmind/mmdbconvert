// Package network provides IP address and network utilities for MMDB processing.
package network

import (
	"encoding/binary"
	"errors"
	"fmt"
	"net/netip"

	"go4.org/netipx"
)

// IPv4ToUint32 converts an IPv4 address to uint32.
func IPv4ToUint32(addr netip.Addr) uint32 {
	if !addr.Is4() {
		panic("IPv4ToUint32 called with non-IPv4 address")
	}
	bytes := addr.As4()
	return binary.BigEndian.Uint32(bytes[:])
}

// IPv6BucketToInt64 converts the first bucketSize bits of an IPv6 address to int64.
//
// NOTE: The address must already be masked to the appropriate bucket (i.e., if
// you have a bucket size of /16, you must provide 2001:: as opposed to
// something like 2001:abcd::).
//
// Only the first bucketSize bits are kept, which produces smaller integers
// that may be stored more efficiently in analytics platforms like BigQuery.
//
// In BigQuery, you can compute the same value using:
//
//	CAST(CONCAT('0x', SUBSTR(
//	  TO_HEX(NET.IP_TRUNC(NET.IP_FROM_STRING(ip), bucket_size)),
//	  1,
//	  CAST(CEILING(bucket_size / 4) AS INT64)
//	)) AS INT64)
//
// where bucket_size is the prefix length used for bucketing.
func IPv6BucketToInt64(addr netip.Addr, bucketSize int) (int64, error) {
	if !addr.Is6() {
		return 0, errors.New("IPv6BucketToInt64 called with non-IPv6 address")
	}
	bytes := addr.As16()
	val := binary.BigEndian.Uint64(bytes[:8])
	// Right-shift to keep only bucketSize bits
	shift := 64 - bucketSize
	//nolint:gosec // bucketSize is validated to be <= 60, so result fits in positive int64
	return int64(val >> shift), nil
}

// IsAdjacent checks if two IP addresses are consecutive (no gap between them).
func IsAdjacent(endIP, startIP netip.Addr) bool {
	if endIP.Is4() != startIP.Is4() {
		return false
	}
	return endIP.Next() == startIP
}

// SmallestNetwork returns the smaller (more specific) of two overlapping network prefixes.
func SmallestNetwork(a, b netip.Prefix) netip.Prefix {
	// The network with more bits (longer prefix length) is more specific
	if a.Bits() >= b.Bits() {
		return a
	}
	return b
}

// SplitPrefix splits a prefix into multiple prefixes of the desired size.
// If the prefix is already the requested size or smaller, it is returned as-is.
func SplitPrefix(prefix netip.Prefix, prefixSize int) ([]netip.Prefix, error) {
	if !prefix.IsValid() {
		return nil, fmt.Errorf("invalid prefix: %s", prefix)
	}

	bits := prefix.Bits()

	// If the prefix is equal or smaller than our desired size, return only it.
	if bits >= prefixSize {
		return []netip.Prefix{prefix}, nil
	}

	// We need to split the prefix into multiple prefixes.

	// We can end up with an enormous number of networks if the prefix is too
	// large.
	if prefixSize-bits > 10 {
		return nil, fmt.Errorf(
			"splitting %s to /%d would create too many prefixes (max 1024)",
			prefix,
			prefixSize,
		)
	}

	nPrefixes := 1 << (prefixSize - bits) // 2**(prefixSize-bits)
	prefixes := make([]netip.Prefix, 0, nPrefixes)

	startIP := prefix.Addr()
	lastIP := netipx.PrefixLastIP(prefix)

	// The IsValid() check prevents an infinite loop when the prefix covers the
	// end of the IP address space (e.g., 255.255.255.0/24 for IPv4). After
	// processing the last sub-prefix, Next() returns an invalid (zero) address.
	// Without IsValid(), the loop would continue forever because Compare()
	// returns -1 for invalid vs valid addresses, making the condition
	// `startIP.Compare(lastIP) <= 0` remain true.
	for startIP.Compare(lastIP) <= 0 && startIP.IsValid() {
		curPrefix := netip.PrefixFrom(startIP, prefixSize)
		prefixes = append(prefixes, curPrefix)
		startIP = netipx.PrefixLastIP(curPrefix).Next()
	}

	return prefixes, nil
}
