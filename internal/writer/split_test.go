package writer

import (
	"errors"
	"net/netip"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type recordWriter struct {
	rows   []netip.Prefix
	data   []map[string]any
	flushE error
}

func (r *recordWriter) WriteRow(prefix netip.Prefix, data map[string]any) error {
	r.rows = append(r.rows, prefix)
	r.data = append(r.data, data)
	return nil
}

func (r *recordWriter) Flush() error {
	return r.flushE
}

func TestSplitRowWriter_RoutesByIPVersion(t *testing.T) {
	v4 := &recordWriter{}
	v6 := &recordWriter{}

	split := NewSplitRowWriter(v4, v6)

	v4Prefix := netip.MustParsePrefix("10.0.0.0/24")
	v6Prefix := netip.MustParsePrefix("2001:db8::/32")

	require.NoError(t, split.WriteRow(v4Prefix, map[string]any{"col": "ipv4"}))
	require.NoError(t, split.WriteRow(v6Prefix, map[string]any{"col": "ipv6"}))

	require.Len(t, v4.rows, 1)
	assert.Equal(t, v4Prefix, v4.rows[0])
	require.Len(t, v6.rows, 1)
	assert.Equal(t, v6Prefix, v6.rows[0])
}

func TestSplitRowWriter_ErrorsWhenWriterMissing(t *testing.T) {
	v6 := &recordWriter{}
	split := NewSplitRowWriter(nil, v6)

	err := split.WriteRow(netip.MustParsePrefix("10.0.0.0/24"), nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "IPv4 writer")

	split = NewSplitRowWriter(&recordWriter{}, nil)
	err = split.WriteRow(netip.MustParsePrefix("2001:db8::/48"), nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "IPv6 writer")
}

func TestSplitRowWriter_FlushPropagatesErrors(t *testing.T) {
	v4 := &recordWriter{}
	v6 := &recordWriter{flushE: errors.New("flush failure")}

	split := NewSplitRowWriter(v4, v6)

	err := split.Flush()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "flush failure")
}
