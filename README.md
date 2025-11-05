# mmdbconvert

A command-line tool to merge multiple MaxMind MMDB databases and export to CSV,
Parquet, or MMDB format.

[![License: Apache 2.0](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Go Version](https://img.shields.io/badge/Go-1.25%2B-00ADD8?logo=go)](https://golang.org)

## Features

- ✅ **Merge multiple MMDB databases** - Combine GeoIP2 databases (e.g.,
  Enterprise + Anonymous IP)
- ✅ **Non-overlapping networks** - Automatically resolves overlapping networks
  to smallest blocks
- ✅ **Adjacent network merging** - Combines adjacent networks with identical
  data for compact output
- ✅ **Multiple output formats** - Export to CSV, Parquet, or MMDB format
- ✅ **Query-optimized Parquet** - Integer columns enable 10-100x faster IP
  lookups
- ✅ **Type-preserving MMDB output** - Perfect type preservation for merged
  databases
- ✅ **Flexible column mapping** - Extract any fields from MMDB databases using
  JSON paths
- ✅ **IPv4 and IPv6 support** - Handle both IP versions seamlessly
- ✅ **Type hints for Parquet** - Native int64, float64, bool types for
  efficient storage

## Installation

### From Source

```bash
go install github.com/maxmind/mmdbconvert/cmd/mmdbconvert@latest
```

### Build Locally

```bash
git clone https://github.com/maxmind/mmdbconvert.git
cd mmdbconvert
go build -o mmdbconvert ./cmd/mmdbconvert
```

## Quick Start

### 1. Create a Configuration File

Create `config.toml`:

```toml
[output]
format = "csv"
file = "output.csv"

[[databases]]
name = "city"
path = "/path/to/GeoIP2-City.mmdb"

[[columns]]
name = "country_code"
database = "city"
path = ["country", "iso_code"]

[[columns]]
name = "city_name"
database = "city"
path = ["city", "names", "en"]
```

### 2. Run the Tool

```bash
mmdbconvert config.toml
```

### 3. View the Output

```bash
head output.csv
```

```
network,country_code,city_name
1.0.0.0/24,AU,Sydney
1.0.1.0/24,CN,Beijing
1.0.4.0/22,AU,Melbourne
```

> **Note:** The `network` column appears automatically because no
> `[[network.columns]]` sections were defined. By default, CSV output includes a
> CIDR column named `network`, while Parquet output includes `start_int` and
> `end_int` integer columns for faster IP lookups. You can customize network
> columns in the configuration.

## Usage

```bash
# Basic usage
mmdbconvert config.toml

# Explicit config flag
mmdbconvert --config config.toml

# Suppress progress output
mmdbconvert --config config.toml --quiet

# Disable unmarshaler caching to reduce memory usage (several times slower)
mmdbconvert --config config.toml --disable-cache

# Show version
mmdbconvert --version

# Show help
mmdbconvert --help
```

## Configuration

See [docs/config.md](docs/config.md) for complete configuration reference.

### CSV Output Example

```toml
[output]
format = "csv"
file = "geo.csv"

[output.csv]
delimiter = ","  # or "\t" for tab-delimited

[[network.columns]]
name = "network"
type = "cidr"

[[databases]]
name = "city"
path = "GeoIP2-City.mmdb"

[[columns]]
name = "country"
database = "city"
path = ["country", "iso_code"]
```

### Parquet Output Example

```toml
[output]
format = "parquet"
file = "geo.parquet"

[output.parquet]
compression = "snappy"
row_group_size = 500000

# Integer columns for fast queries
[[network.columns]]
name = "start_int"
type = "start_int"

[[network.columns]]
name = "end_int"
type = "end_int"

[[databases]]
name = "city"
path = "GeoIP2-City.mmdb"

[[columns]]
name = "country"
database = "city"
path = ["country", "iso_code"]
type = "string"

[[columns]]
name = "latitude"
database = "city"
path = ["location", "latitude"]
type = "float64"
```

### MMDB Output Example

```toml
[output]
format = "mmdb"
file = "merged.mmdb"

[output.mmdb]
database_type = "GeoIP2-City"
description = { en = "Merged GeoIP Database" }
record_size = 28

[[databases]]
name = "city"
path = "GeoIP2-City.mmdb"

# Use output_path to create nested structure
[[columns]]
name = "country_code"
database = "city"
path = ["country", "iso_code"]
output_path = ["country", "iso_code"]

[[columns]]
name = "city_name"
database = "city"
path = ["city", "names", "en"]
output_path = ["city", "names", "en"]

[[columns]]
name = "latitude"
database = "city"
path = ["location", "latitude"]
output_path = ["location", "latitude"]

[[columns]]
name = "longitude"
database = "city"
path = ["location", "longitude"]
output_path = ["location", "longitude"]
```

**MMDB output features:**

- Perfect type preservation from source databases
- Support for nested structures via `output_path`
- Compatible with all MMDB readers (libmaxminddb, etc.)
- Configurable record size (24, 28, or 32 bits)

## Querying Parquet Files

Parquet files generated with integer columns (`start_int`, `end_int`) support
**extremely fast IP lookups** (10-100x faster than string comparisons).

### DuckDB Example

```sql
-- Lookup IP address 203.0.113.100 (integer: 3405803876)
SELECT * FROM read_parquet('geo.parquet')
WHERE start_int <= 3405803876 AND end_int >= 3405803876;
```

**See [docs/parquet-queries.md](docs/parquet-queries.md) for comprehensive query
examples and performance optimization guide.**

## Examples

### Merging Multiple Databases

Combine GeoIP2 Enterprise with Anonymous IP data:

```toml
[output]
format = "parquet"
file = "merged.parquet"

[[network.columns]]
name = "start_int"
type = "start_int"

[[network.columns]]
name = "end_int"
type = "end_int"

[[databases]]
name = "enterprise"
path = "GeoIP2-Enterprise.mmdb"

[[databases]]
name = "anonymous"
path = "GeoIP2-Anonymous-IP.mmdb"

# Columns from Enterprise database
[[columns]]
name = "country_code"
database = "enterprise"
path = ["country", "iso_code"]

[[columns]]
name = "city_name"
database = "enterprise"
path = ["city", "names", "en"]

[[columns]]
name = "latitude"
database = "enterprise"
path = ["location", "latitude"]
type = "float64"

[[columns]]
name = "longitude"
database = "enterprise"
path = ["location", "longitude"]
type = "float64"

# Columns from Anonymous IP database
[[columns]]
name = "is_anonymous"
database = "anonymous"
path = ["is_anonymous"]
type = "bool"

[[columns]]
name = "is_anonymous_vpn"
database = "anonymous"
path = ["is_anonymous_vpn"]
type = "bool"
```

### All Network Column Types

```toml
[[network.columns]]
name = "network"
type = "cidr"          # e.g., "203.0.113.0/24"

[[network.columns]]
name = "start_ip"
type = "start_ip"      # e.g., "203.0.113.0"

[[network.columns]]
name = "end_ip"
type = "end_ip"        # e.g., "203.0.113.255"

[[network.columns]]
name = "start_int"
type = "start_int"     # e.g., 3405803776 (IPv4 only)

[[network.columns]]
name = "end_int"
type = "end_int"       # e.g., 3405804031 (IPv4 only)
```

**Default network columns:** If you don't define any `[[network.columns]]`,
mmdbconvert automatically provides sensible defaults based on output format:

- **CSV**: Single `network` column (CIDR format) for human readability
- **Parquet**: `start_int` and `end_int` columns for 10-100x faster IP queries

**Note:** `start_int` and `end_int` only work with IPv4 addresses unless you
split your output into separate IPv4/IPv6 files via `output.ipv4_file` and
`output.ipv6_file`. For single-file outputs that include IPv6 data, use string
columns (`start_ip`, `end_ip`, `cidr`).

### Data Type Hints

Parquet supports native types for efficient storage and queries:

```toml
[[columns]]
name = "population"
database = "city"
path = ["city", "population"]
type = "int64"          # Integer values

[[columns]]
name = "accuracy_radius"
database = "city"
path = ["location", "accuracy_radius"]
type = "int64"

[[columns]]
name = "latitude"
database = "city"
path = ["location", "latitude"]
type = "float64"        # Floating-point values

[[columns]]
name = "is_satellite"
database = "city"
path = ["traits", "is_satellite_provider"]
type = "bool"           # Boolean values
```

## Use Cases

### Merging Enterprise + Anonymous IP

Merging GeoIP2 Enterprise with GeoIP2 Anonymous IP to enrich traffic logs. The
merged database provides:

- Geographic location data (country, city, coordinates)
- Anonymous IP detection (VPN, proxy, hosting provider)
- Single query-optimized Parquet file for fast lookups
- Non-overlapping networks for accurate IP matching

### Creating Custom MMDB Databases

Merge multiple MMDB databases into a single custom database with perfect type
preservation:

- Combine multiple data sources (GeoIP, ISP, ASN, etc.)
- Create application-specific databases with only needed fields
- Maintain exact data types from source databases
- Deploy merged databases with existing MMDB readers
- No performance overhead compared to original databases

### Analytics Pipelines

Export MMDB databases to Parquet for use in analytics pipelines:

- **DuckDB:** Fast local queries on laptop/server
- **Apache Spark:** Distributed processing of billions of logs
- **Trino/Presto:** Query data in S3 without downloading
- **BigQuery:** Load Parquet files for SQL analysis

### Data Warehouse Integration

Convert MMDB databases to CSV/Parquet for loading into data warehouses:

- Snowflake
- Redshift
- BigQuery
- Databricks

## Architecture

### Streaming Network Merge

mmdbconvert uses a streaming accumulator algorithm:

1. **Nested iteration** through all databases using `NetworksWithin()`
2. **Smallest network selection** - Always chooses most specific network block
3. **Data extraction** from all databases for each network
4. **Adjacent network merging** - Combines networks with identical data

### Non-Overlapping Networks

When databases have overlapping networks, mmdbconvert automatically splits them
into non-overlapping blocks:

**Example:**

```
Database A: 10.0.0.0/16
Database B: 10.0.1.0/24

Output:
  10.0.0.0/24   (only in A)
  10.0.1.0/24   (in both A and B)
  10.0.2.0/23   (only in A)
  10.0.4.0/22   (only in A)
  10.0.8.0/21   (only in A)
  ... etc
```

This ensures accurate IP lookups with no ambiguity.

## Documentation

- [Configuration Reference](docs/config.md) - Complete config file documentation
- [Parquet Query Guide](docs/parquet-queries.md) - Optimizing IP lookup queries

## Requirements

- Go 1.25 or later
- MaxMind MMDB database files (GeoIP2, GeoLite2, etc.)

## License

Copyright 2025 MaxMind, Inc.

This project is licensed under either of:

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or
  http://www.apache.org/licenses/LICENSE-2.0)
- MIT License ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option.

## Contributing

Contributions are welcome! Please:

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Make your changes with tests
4. Run linters: `golangci-lint run`
5. Run tests: `go test ./...`
6. Commit your changes (`git commit -m 'Add amazing feature'`)
7. Push to the branch (`git push origin feature/amazing-feature`)
8. Open a Pull Request

## Acknowledgments

Built with:

- [go-toml](https://github.com/pelletier/go-toml) - TOML configuration parsing
- [maxminddb-golang](https://github.com/oschwald/maxminddb-golang) - MMDB
  database reading
- [mmdbwriter](https://github.com/maxmind/mmdbwriter) - MMDB database writing
- [parquet-go](https://github.com/parquet-go/parquet-go) - Parquet file writing

## Support

- **Issues:** [GitHub Issues](https://github.com/maxmind/mmdbconvert/issues)
- **Documentation:** [docs/](docs/)
- **MaxMind Support:**
  [https://support.maxmind.com](https://support.maxmind.com)
