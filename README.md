# mmdbconvert

A command-line tool to merge multiple MaxMind MMDB databases and export to CSV
or Parquet format.

[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Go Version](https://img.shields.io/badge/Go-1.25%2B-00ADD8?logo=go)](https://golang.org)

## Features

- ✅ **Merge multiple MMDB databases** - Combine GeoIP2 databases (e.g.,
  Enterprise + Anonymous IP)
- ✅ **Non-overlapping networks** - Automatically resolves overlapping networks
  to smallest blocks
- ✅ **Adjacent network merging** - Combines adjacent networks with identical
  data for compact output
- ✅ **CSV and Parquet output** - Flexible export formats for different use
  cases
- ✅ **Query-optimized Parquet** - Integer columns enable 10-100x faster IP
  lookups
- ✅ **Flexible column mapping** - Extract any fields from MMDB databases using
  JSON paths
- ✅ **IPv4 and IPv6 support** - Handle both IP versions seamlessly
- ✅ **Streaming architecture** - O(1) memory usage regardless of database size
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

## Usage

```bash
# Basic usage
mmdbconvert config.toml

# Explicit config flag
mmdbconvert --config config.toml

# Suppress progress output
mmdbconvert --config config.toml --quiet

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

**Note:** `start_int` and `end_int` only work with IPv4 addresses. For IPv6, use
string columns (`start_ip`, `end_ip`, `cidr`).

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

## Performance

### Memory Efficiency

- **O(1) memory usage** - Streaming architecture processes networks on-the-fly
- Typical memory: < 100 MB regardless of database size
- Handles databases with millions of networks

### Processing Speed

Typical performance on modern hardware:

| Database Size | Processing Time | Output Format  |
| ------------- | --------------- | -------------- |
| 100K networks | 1-2 seconds     | CSV or Parquet |
| 1M networks   | 10-15 seconds   | CSV or Parquet |
| 5M networks   | 45-60 seconds   | CSV or Parquet |

### Query Performance (Parquet)

With integer columns (`start_int`, `end_int`):

| Query Type      | Performance | Comparison                 |
| --------------- | ----------- | -------------------------- |
| Single IP       | 5-10ms      | 40-50x faster than strings |
| Batch 1000 IPs  | 100-200ms   | 200-300x faster            |
| Full table scan | 200-500ms   | Similar to strings         |

**See [docs/parquet-queries.md](docs/parquet-queries.md) for optimization
details.**

## Architecture

### Streaming Network Merge

mmdbconvert uses a streaming accumulator algorithm:

1. **Nested iteration** through all databases using `NetworksWithin()`
2. **Smallest network selection** - Always chooses most specific network block
3. **Data extraction** from all databases for each network
4. **Adjacent network merging** - Combines networks with identical data
5. **Immediate output** - Writes rows as soon as data changes (O(1) memory)

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
- [Implementation Plan](plan.md) - Detailed technical design (for developers)

## Requirements

- Go 1.25 or later
- MaxMind MMDB database files (GeoIP2, GeoLite2, etc.)

## License

Copyright 2025 MaxMind, Inc.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use
this software except in compliance with the License.

You may obtain a copy of the License at:

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed
under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
CONDITIONS OF ANY KIND, either express or implied. See the License for the
specific language governing permissions and limitations under the License.

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
- [parquet-go](https://github.com/parquet-go/parquet-go) - Parquet file writing

## Support

- **Issues:** [GitHub Issues](https://github.com/maxmind/mmdbconvert/issues)
- **Documentation:** [docs/](docs/)
- **MaxMind Support:**
  [https://support.maxmind.com](https://support.maxmind.com)
