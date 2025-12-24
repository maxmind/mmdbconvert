# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Parquet sorting column metadata for query optimization. When start_int columns
  are configured, mmdbconvert now writes sorting metadata to the Parquet file
  declaring that rows are sorted by start_int in ascending order. This enables
  query engines like DuckDB, Spark, and Trino to use the sort order for
  potential optimizations like binary search.
- New `network_bucket` network column type for CSV and Parquet output, enabling
  efficient IP lookups in BigQuery and other analytics platforms. When a network
  spans multiple buckets, rows are duplicated with different bucket values while
  preserving original network info. For IPv4, the bucket is an integer. For
  IPv6, the bucket is either a hex string (e.g.,
  "200f0000000000000000000000000000") or an integer depending on
  `ipv6_bucket_type`. Requires split output files (`ipv4_file` and `ipv6_file`).
- New CSV and Parquet options `ipv4_bucket_size` and `ipv6_bucket_size` to
  configure bucket prefix lengths (default: 16).
- New CSV and Parquet option `ipv6_bucket_type` to configure the IPv6 network
  bucket column format (default: string).

## [0.1.0] - 2025-11-07

### Added

- Initial release of mmdbconvert
- Merge multiple MaxMind MMDB databases into single output
- CSV output format with configurable delimiter
- Parquet output format with query optimization
- Non-overlapping network generation from overlapping MMDB databases
- Adjacent network merging for compact output
- Flexible column mapping with JSON pointer paths
- IPv4 and IPv6 support
- Network column types: CIDR, start_ip, end_ip, start_int, end_int
- Data column type hints for Parquet: string, int64, float64, bool, binary
- Streaming architecture with O(1) memory usage for CSV and Parquet
- Parquet compression options: snappy, gzip, lz4, zstd
- Configurable row group size for Parquet
- Progress reporting with --quiet flag
- Comprehensive configuration validation
- Error handling for missing files and invalid configurations

### Documentation

- Complete README with usage examples
- Configuration reference (docs/config.md)
- Parquet query optimization guide (docs/parquet-queries.md)
- Example configuration files for common use cases
- Package-level godoc comments

### Testing

- Integration tests for CSV and Parquet output
- Edge case handling (missing paths, nil values)
- Multi-database merge validation
- Performance testing (< 10ms for small databases, 7MB memory)

[unreleased]: https://github.com/maxmind/mmdbconvert/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/maxmind/mmdbconvert/releases/tag/v0.1.0
