# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Parquet sorting column metadata for query optimization. When start_int
  columns are configured, mmdbconvert now writes sorting metadata to the
  Parquet file declaring that rows are sorted by start_int in ascending order.
  This enables query engines like DuckDB, Spark, and Trino to use the sort
  order for potential optimizations like binary search.

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
