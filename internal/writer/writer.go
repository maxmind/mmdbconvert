// Package writer provides output writers for CSV and Parquet formats.
package writer

import "github.com/maxmind/mmdbconvert/internal/config"

// Network column type constants.
const (
	NetworkColumnCIDR     = "cidr"
	NetworkColumnStartIP  = "start_ip"
	NetworkColumnEndIP    = "end_ip"
	NetworkColumnStartInt = "start_int"
	NetworkColumnEndInt   = "end_int"
	NetworkColumnBucket   = "network_bucket"
)

// hasNetworkBucketColumn returns true if a network_bucket column is configured.
func hasNetworkBucketColumn(cfg *config.Config) bool {
	for _, col := range cfg.Network.Columns {
		if col.Type == NetworkColumnBucket {
			return true
		}
	}
	return false
}
