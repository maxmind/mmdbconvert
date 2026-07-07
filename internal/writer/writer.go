// Package writer provides output writers for CSV and Parquet formats.
package writer

import "github.com/maxmind/mmdbconvert/internal/config"

// hasNetworkBucketColumn returns true if a network_bucket column is configured.
func hasNetworkBucketColumn(cfg *config.Config) bool {
	for _, col := range cfg.Network.Columns {
		if col.Type == config.NetworkColumnTypeBucket {
			return true
		}
	}
	return false
}
