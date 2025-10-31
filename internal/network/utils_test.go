package network

import (
	"net/netip"
	"testing"

	"github.com/stretchr/testify/assert"
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

func TestIPv6ToBytes(t *testing.T) {
	ip := netip.MustParseAddr("2001:db8::1")
	bytes := IPv6ToBytes(ip)
	assert.Equal(t, [16]byte{0x20, 0x01, 0x0d, 0xb8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1}, bytes)
}

func TestIPv4ToPaddedIPv6(t *testing.T) {
	ip := netip.MustParseAddr("192.168.1.1")
	bytes := IPv4ToPaddedIPv6(ip)

	// Should be ::ffff:192.168.1.1
	expected := [16]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xff, 0xff, 192, 168, 1, 1}
	assert.Equal(t, expected, bytes)
}

func TestCalculateEndIP(t *testing.T) {
	tests := []struct {
		name     string
		prefix   string
		expected string
	}{
		{
			name:     "IPv4 /32",
			prefix:   "192.168.1.1/32",
			expected: "192.168.1.1",
		},
		{
			name:     "IPv4 /24",
			prefix:   "192.168.1.0/24",
			expected: "192.168.1.255",
		},
		{
			name:     "IPv4 /16",
			prefix:   "10.0.0.0/16",
			expected: "10.0.255.255",
		},
		{
			name:     "IPv4 /23",
			prefix:   "10.0.0.0/23",
			expected: "10.0.1.255",
		},
		{
			name:     "IPv6 /128",
			prefix:   "2001:db8::1/128",
			expected: "2001:db8::1",
		},
		{
			name:     "IPv6 /64",
			prefix:   "2001:db8::/64",
			expected: "2001:db8::ffff:ffff:ffff:ffff",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			prefix := netip.MustParsePrefix(tt.prefix)
			result := CalculateEndIP(prefix)
			expected := netip.MustParseAddr(tt.expected)
			assert.Equal(t, expected, result)
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
