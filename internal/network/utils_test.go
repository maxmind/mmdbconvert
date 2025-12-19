package network

import (
	"fmt"
	"net/netip"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIPv4ToUint32(t *testing.T) {
	tests := []struct {
		name     string
		ip       string
		expected uint32
	}{
		{
			name:     "zero address",
			ip:       "0.0.0.0",
			expected: 0,
		},
		{
			name:     "simple address",
			ip:       "192.168.1.1",
			expected: 3232235777,
		},
		{
			name:     "max address",
			ip:       "255.255.255.255",
			expected: 4294967295,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ip := netip.MustParseAddr(tt.ip)
			result := IPv4ToUint32(ip)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestIsAdjacent(t *testing.T) {
	tests := []struct {
		name     string
		endIP    string
		startIP  string
		expected bool
	}{
		{
			name:     "IPv4 adjacent",
			endIP:    "192.168.1.1",
			startIP:  "192.168.1.2",
			expected: true,
		},
		{
			name:     "IPv4 not adjacent",
			endIP:    "192.168.1.1",
			startIP:  "192.168.1.3",
			expected: false,
		},
		{
			name:     "IPv4 same address",
			endIP:    "192.168.1.1",
			startIP:  "192.168.1.1",
			expected: false,
		},
		{
			name:     "IPv6 adjacent",
			endIP:    "2001:db8::1",
			startIP:  "2001:db8::2",
			expected: true,
		},
		{
			name:     "IPv6 not adjacent",
			endIP:    "2001:db8::1",
			startIP:  "2001:db8::3",
			expected: false,
		},
		{
			name:     "IPv4 and IPv6 mix",
			endIP:    "192.168.1.1",
			startIP:  "2001:db8::1",
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			endIP := netip.MustParseAddr(tt.endIP)
			startIP := netip.MustParseAddr(tt.startIP)
			result := IsAdjacent(endIP, startIP)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestSmallestNetwork(t *testing.T) {
	tests := []struct {
		name     string
		a        string
		b        string
		expected string
	}{
		{
			name:     "/24 is smaller than /16",
			a:        "10.0.0.0/16",
			b:        "10.0.1.0/24",
			expected: "10.0.1.0/24",
		},
		{
			name:     "/32 is smallest",
			a:        "10.0.0.0/24",
			b:        "10.0.0.1/32",
			expected: "10.0.0.1/32",
		},
		{
			name:     "equal prefixes",
			a:        "10.0.0.0/24",
			b:        "10.0.1.0/24",
			expected: "10.0.0.0/24",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := netip.MustParsePrefix(tt.a)
			b := netip.MustParsePrefix(tt.b)
			result := SmallestNetwork(a, b)
			expected := netip.MustParsePrefix(tt.expected)
			assert.Equal(t, expected, result)
		})
	}
}

func TestSplitPrefix(t *testing.T) {
	tests := []struct {
		name     string
		prefix   netip.Prefix
		size     int
		prefixes []netip.Prefix
	}{
		{
			name:   "prefix is smaller than the size, IPv4",
			prefix: netip.MustParsePrefix("1.0.0.0/24"),
			size:   16,
			prefixes: []netip.Prefix{
				netip.MustParsePrefix("1.0.0.0/24"),
			},
		},
		{
			name:   "prefix is smaller than the size, IPv6",
			prefix: netip.MustParsePrefix("dead::/64"),
			size:   48,
			prefixes: []netip.Prefix{
				netip.MustParsePrefix("dead::/64"),
			},
		},
		{
			name:   "prefix size is equal to the size, IPv4",
			prefix: netip.MustParsePrefix("1.0.0.0/16"),
			size:   16,
			prefixes: []netip.Prefix{
				netip.MustParsePrefix("1.0.0.0/16"),
			},
		},
		{
			name:   "prefix size is equal to the size, IPv6",
			prefix: netip.MustParsePrefix("dead::/48"),
			size:   48,
			prefixes: []netip.Prefix{
				netip.MustParsePrefix("dead::/48"),
			},
		},
		{
			name:   "prefix gets split, IPv4",
			prefix: netip.MustParsePrefix("1.0.0.0/15"),
			size:   16,
			prefixes: []netip.Prefix{
				netip.MustParsePrefix("1.0.0.0/16"),
				netip.MustParsePrefix("1.1.0.0/16"),
			},
		},
		{
			name:   "prefix gets split, another test, IPv4",
			prefix: netip.MustParsePrefix("1.2.0.0/23"),
			size:   24,
			prefixes: []netip.Prefix{
				netip.MustParsePrefix("1.2.0.0/24"),
				netip.MustParsePrefix("1.2.1.0/24"),
			},
		},
		{
			name:   "prefix gets split, IPv6",
			prefix: netip.MustParsePrefix("dead::/47"),
			size:   48,
			prefixes: []netip.Prefix{
				netip.MustParsePrefix("dead::/48"),
				netip.MustParsePrefix("dead:0:1::/48"),
			},
		},
		{
			name:   "prefix gets split, another test, IPv6",
			prefix: netip.MustParsePrefix("deae::/15"),
			size:   16,
			prefixes: []netip.Prefix{
				netip.MustParsePrefix("deae::/16"),
				netip.MustParsePrefix("deaf::/16"),
			},
		},
		{
			name:   "prefix gets split, larger prefix, IPv4",
			prefix: netip.MustParsePrefix("1.0.0.0/8"),
			size:   16,
			prefixes: func() []netip.Prefix {
				n := 256 // 2**8
				prefixes := make([]netip.Prefix, 0, n)
				for i := range n {
					prefixStr := fmt.Sprintf("1.%d.0.0/16", i)
					prefix := netip.MustParsePrefix(prefixStr)
					prefixes = append(prefixes, prefix)
				}
				return prefixes
			}(),
		},
		{
			name:   "prefix gets split, larger prefix, IPv6",
			prefix: netip.MustParsePrefix("dead::/24"),
			size:   32,
			prefixes: func() []netip.Prefix {
				n := 256 // 2**8
				prefixes := make([]netip.Prefix, 0, n)
				for i := range n {
					prefixStr := fmt.Sprintf("dead:%x::/32", i)
					prefix := netip.MustParsePrefix(prefixStr)
					prefixes = append(prefixes, prefix)
				}
				return prefixes
			}(),
		},
		// Edge cases: prefixes at the end of IP address space.
		// These test the IsValid() check that prevents infinite loops.
		{
			name:   "prefix at end of IPv4 space, split",
			prefix: netip.MustParsePrefix("255.255.255.0/24"),
			size:   25,
			prefixes: []netip.Prefix{
				netip.MustParsePrefix("255.255.255.0/25"),
				netip.MustParsePrefix("255.255.255.128/25"),
			},
		},
		{
			name:   "prefix at end of IPv4 space, no split needed",
			prefix: netip.MustParsePrefix("255.255.255.128/25"),
			size:   25,
			prefixes: []netip.Prefix{
				netip.MustParsePrefix("255.255.255.128/25"),
			},
		},
		{
			name:   "prefix at end of IPv6 space, split",
			prefix: netip.MustParsePrefix("ffff:ffff:ffff:ffff:ffff:ffff:ffff:ff00/120"),
			size:   121,
			prefixes: []netip.Prefix{
				netip.MustParsePrefix("ffff:ffff:ffff:ffff:ffff:ffff:ffff:ff00/121"),
				netip.MustParsePrefix("ffff:ffff:ffff:ffff:ffff:ffff:ffff:ff80/121"),
			},
		},
		{
			name:   "prefix at end of IPv6 space, no split needed",
			prefix: netip.MustParsePrefix("ffff:ffff:ffff:ffff:ffff:ffff:ffff:ff80/121"),
			size:   121,
			prefixes: []netip.Prefix{
				netip.MustParsePrefix("ffff:ffff:ffff:ffff:ffff:ffff:ffff:ff80/121"),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			prefixes, err := SplitPrefix(tt.prefix, tt.size)
			require.NoError(t, err)

			assert.Equal(t, tt.prefixes, prefixes)
		})
	}
}

func TestSplitPrefix_TooLarge(t *testing.T) {
	// Splitting more than 10 bits should fail.
	prefix := netip.MustParsePrefix("1.0.0.0/4")
	_, err := SplitPrefix(prefix, 16) // 12 bits difference
	require.Error(t, err)
	assert.Contains(t, err.Error(), "too many prefixes")
}

func TestSplitPrefix_InvalidPrefix(t *testing.T) {
	// Invalid (zero) prefix should return an error.
	var zeroPrefix netip.Prefix
	_, err := SplitPrefix(zeroPrefix, 16)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid prefix")
}
