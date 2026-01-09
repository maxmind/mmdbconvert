# Configuration File Reference

`mmdbconvert` uses a TOML configuration file to define how MMDB databases should
be merged and exported. This document describes all available configuration
options.

## Quick Start

Here's a minimal example that reads from one database and outputs to CSV:

```toml
[output]
format = "csv"
file = "output.csv"

[[databases]]
name = "geo"
path = "/path/to/GeoIP2-City.mmdb"

[[columns]]
name = "country_code"
database = "geo"
path = ["country", "iso_code"]
```

## Configuration Sections

### General Settings

Top-level configuration options that affect overall behavior:

```toml
disable_cache = false  # Disable MMDB unmarshaler caching (default: false)
```

**Performance Options:**

- `disable_cache` - Controls whether to disable MMDB unmarshaler caching. When
  `false` (default), uses cached unmarshalers for better performance. When
  `true`, disables the unmarshaler cache to reduce memory usage at the expense
  of performance (several times slower). For large databases with many columns,
  disabling cache can significantly reduce memory consumption but will make
  processing take several times longer. Can be overridden at runtime with the
  `--disable-cache` command-line flag.

### Output Settings

The `[output]` section defines where and how data should be written.

```toml
[output]
format = "csv"    # Output format: "csv", "parquet", or "mmdb"
file = "output.csv"  # Output file path (use this for a combined file)
# ipv4_file = "output_ipv4.csv"  # Optional IPv4-only file (set both ipv4_file and ipv6_file, omit file)
# ipv6_file = "output_ipv6.csv"  # Optional IPv6-only file (set both ipv4_file and ipv6_file, omit file)
include_empty_rows = false  # Include rows with no MMDB data (default: false)
ipv6_min_prefix = 64  # Optional: Minimum IPv6 prefix length - truncates more specific prefixes (default: nil = no normalization)
```

**Data Filtering:**

- `include_empty_rows` - Controls whether rows with no MMDB data are written to
  the output. When `false` (default), rows where all data columns are empty/null
  are skipped. When `true`, all network ranges are included even if they have no
  associated data. Network columns (CIDR, start_ip, etc.) are always present and
  don't affect this filtering.

**IPv6 Prefix Normalization:**

- `ipv6_min_prefix` - Optional minimum prefix length for IPv6 networks. When
  set, any IPv6 prefix more specific (larger prefix length) than this value will
  be truncated to this prefix length. For example, with `ipv6_min_prefix = 64`:
  - `2001:db8::1/128` becomes `2001:db8::/64`
  - `2001:db8::/48` remains unchanged (already /48)
  - IPv4 prefixes are never affected

  When omitted (default), no normalization is applied and prefixes are output as-is.
  Valid range: 0-128.

#### CSV Options

When `format = "csv"`, you can specify CSV-specific options:

```toml
[output.csv]
delimiter = ","           # Field delimiter (default: ",")
include_header = true     # Include column headers (default: true)
ipv4_bucket_size = 16     # Bucket prefix length for IPv4 (default: 16)
ipv6_bucket_size = 16     # Bucket prefix length for IPv6 (default: 16)
ipv6_bucket_type = "string"  # IPv6 bucket value type: "string" or "int" (default: "string")
```

| Option             | Description                                                                | Default  |
| ------------------ | -------------------------------------------------------------------------- | -------- |
| `delimiter`        | Field delimiter character                                                  | ","      |
| `include_header`   | Include column headers in output                                           | true     |
| `ipv4_bucket_size` | Prefix length for IPv4 buckets (1-32, when `network_bucket` column used)   | 16       |
| `ipv6_bucket_size` | Prefix length for IPv6 buckets (1-60, when `network_bucket` column used)   | 16       |
| `ipv6_bucket_type` | IPv6 bucket value type: "string" (hex) or "int" (first 60 bits as integer) | "string" |

#### Parquet Options

When `format = "parquet"`, you can specify Parquet-specific options:

```toml
[output.parquet]
compression = "snappy"    # Compression: "none", "snappy", "gzip", "lz4", "zstd" (default: "snappy")
row_group_size = 500000   # Rows per row group (default: 500000)
ipv4_bucket_size = 16     # Bucket prefix length for IPv4 (default: 16)
ipv6_bucket_size = 16     # Bucket prefix length for IPv6 (default: 16)
ipv6_bucket_type = "string"  # IPv6 bucket value type: "string" or "int" (default: "string")
```

| Option             | Description                                                                | Default  |
| ------------------ | -------------------------------------------------------------------------- | -------- |
| `compression`      | Compression codec: "none", "snappy", "gzip", "lz4", "zstd"                 | "snappy" |
| `row_group_size`   | Number of rows per row group                                               | 500000   |
| `ipv4_bucket_size` | Prefix length for IPv4 buckets (1-32, when `network_bucket` column used)   | 16       |
| `ipv6_bucket_size` | Prefix length for IPv6 buckets (1-60, when `network_bucket` column used)   | 16       |
| `ipv6_bucket_type` | IPv6 bucket value type: "string" (hex) or "int" (first 60 bits as integer) | "string" |

#### MMDB Options

When `format = "mmdb"`, you can specify MMDB-specific options:

```toml
[output.mmdb]
database_type = "GeoIP2-City"  # Database type (required)
description = { en = "Custom Database", de = "Benutzerdefinierte Datenbank" }  # Descriptions by language
languages = ["en", "de"]  # List of languages (auto-populated from description if omitted)
record_size = 28  # Record size: 24, 28, or 32 (default: 28)
include_reserved_networks = false  # Include reserved networks (default: false)
```

**Notes:**

- `database_type` is required for MMDB output
- `languages` is auto-populated from `description` keys if not specified
- Split IPv4/IPv6 files are not supported for MMDB output (must use single
  `file`)
- Network columns are not used for MMDB output (data is written by prefix)
- Type hints are not allowed for MMDB output (types are preserved from source
  databases)

#### Splitting IPv4 and IPv6 Output

Set `output.ipv4_file` and `output.ipv6_file` to write IPv4 and IPv6 rows to
separate files. When these fields are present, omit `output.file`. This works
for both CSV and Parquet outputs:

```toml
[output]
format = "parquet"
ipv4_file = "merged_ipv4.parquet"
ipv6_file = "merged_ipv6.parquet"
```

When splitting output, both `ipv4_file` and `ipv6_file` must be configured.

#### IPv6 Bucket Type Options

IPv6 buckets can be stored as either hex strings (default) or int64 values:

**String type (default):**

- Format: 32-character hex string (e.g., "20010db8000000000000000000000000")
- Storage: 32 bytes per value

**Int type (`ipv6_bucket_type = "int"`):**

- Format: First 60 bits of the bucket address as int64
- Storage: 8 bytes per value (4x smaller than string)

We use 60 bits (not 64) because 60-bit values always fit in a positive int64,
which simplifies queries by avoiding two's complement handling.

**When to use each type:**

- Use **string** (default) for databases where hex string representations are
  simpler to work with.
- Use **int** for reduced storage cost at the price of more complicated queries.

We do not provide a `bytes` type for the IPv6 bucket. Primarily this is because
there so far has not been a need. For example, BigQuery cannot cluster on
`bytes`, so it is not helpful there.

### Network Columns

Network columns define how IP network information is output. These columns
always appear first in the output, in the order defined.

```toml
[[network.columns]]
name = "network"    # Column name
type = "cidr"       # Output type
```

**Available types:**

| Type             | Description                                                                                                                                                        |
| ---------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `cidr`           | CIDR notation (e.g., "203.0.113.0/24")                                                                                                                             |
| `start_ip`       | Starting IP address (e.g., "203.0.113.0")                                                                                                                          |
| `end_ip`         | Ending IP address (e.g., "203.0.113.255")                                                                                                                          |
| `start_int`      | Starting IP as integer                                                                                                                                             |
| `end_int`        | Ending IP as integer                                                                                                                                               |
| `network_bucket` | Bucket for efficient lookups. IPv4: integer. IPv6: hex string (default) or integer (with `ipv6_bucket_type = "int"`). Requires split files (CSV and Parquet only). |

**Default behavior:** If no `[[network.columns]]` sections are defined:

- **CSV output**: A single CIDR column named `network` is generated
- **Parquet output**: Two integer columns `start_int` and `end_int` are
  generated for query-optimized IP lookups using predicate pushdown
- **MMDB output**: No network columns (data is written by prefix)

You can override these defaults by explicitly defining your own
`[[network.columns]]` sections.

> **Note:** Integer network columns (`start_int`, `end_int`) only work with IPv4
> when writing to a single Parquet file. To use these columns with IPv6 data,
> configure `output.ipv4_file` and `output.ipv6_file` so the rows are split by
> IP family, or switch to the string-based columns (`start_ip`, `end_ip`,
> `cidr`).

**Example with multiple network columns:**

```toml
[[network.columns]]
name = "network"
type = "cidr"

[[network.columns]]
name = "start_ip"
type = "start_ip"

[[network.columns]]
name = "end_ip"
type = "end_ip"
```

### Databases

The `[[databases]]` section defines MMDB databases to read from. You can specify
multiple databases.

```toml
[[databases]]
name = "enterprise"                              # Identifier used in column definitions
path = "/var/lib/GeoIP/GeoIP2-Enterprise.mmdb"  # Path to MMDB file

[[databases]]
name = "anonymous"
path = "/var/lib/GeoIP/GeoIP2-Anonymous-IP.mmdb"
```

The `name` field is used to reference the database in column definitions.

### Data Columns

Data columns map fields from MMDB databases to output columns. These appear
after network columns, in the order defined.

```toml
[[columns]]
name = "country_code"        # Output column name
database = "enterprise"      # Database to read from (must match a database name)
path = ["country", "iso_code"]   # Path segments to the field in source database
output_path = ["country", "iso_code"]  # Optional: path for MMDB output (defaults to [name])
```

**Field descriptions:**

- `name` - Column name for CSV/Parquet output
- `database` - Database to read from (must match a database name)
- `path` - Path to field in source MMDB database
- `output_path` - (Optional) Path for nested structure in MMDB output. If not
  specified, defaults to a flat structure using `[name]` as the path. Only
  relevant for MMDB output format.

#### Path Syntax

Paths are defined as TOML arrays. Each element represents one traversal step:

- Strings access map keys (e.g., `"country"`, `"names"`)
- Integers access array indices (supports negative indices)
- Strings are used verbatim, so keys may include `/` without escaping
- **Empty array** (`path = []`) means "copy entire record" - extracts all data
  from the MMDB record as a map

**Examples:**

```toml
# Simple field
path = ["country", "iso_code"]

# Nested object
path = ["country", "names", "en"]

# Array access
path = ["subdivisions", 0, "names", "en"]

# Deep nesting
path = ["location", "latitude"]

# Copy entire record
path = []
```

#### Copying Entire Records

Use `path = []` to copy all data from an MMDB record. This is useful when
merging entire databases:

```toml
[[columns]]
name = "all_enterprise_data"
database = "enterprise"
path = []  # Copy entire record from Enterprise database
```

**For MMDB output**, control where the data is placed using `output_path`:

- `output_path = []` - Merge all fields into root of output MMDB
- `output_path = ["some", "path"]` - Place all fields nested at specified path
- If `output_path` is not specified, defaults to `[name]` (single-level nesting)

**Map merging behavior:**

When multiple columns target the same path with maps, they are merged
recursively:

- Non-conflicting keys are combined
- Nested maps are merged recursively
- Conflicting keys (same key, different non-map values) cause an error

```toml
# Example: Merge Enterprise data at root + Anonymous IP data under traits
[[columns]]
name = "enterprise_all"
database = "enterprise"
path = []
output_path = []  # Merge into root

[[columns]]
name = "anonymous_all"
database = "anonymous"
path = []
output_path = ["traits"]  # Nest under traits
```

**For CSV/Parquet output**, the entire map is JSON-encoded as a string, just
like other complex values.

#### Data Types

- **Scalar values** are output based on type:
  - Strings and numbers are output as-is
  - Booleans are output as `1` (true) or `0` (false) in CSV format
- **Complex values** (objects, arrays) are automatically JSON-encoded
- **Missing data** results in an empty value (empty string for CSV, null for
  Parquet)

**Example with complex type:**

```toml
[[columns]]
name = "all_city_names"
database = "geo"
path = ["city", "names"]  # Outputs: {"en":"London","de":"Londres","es":"Londres"}
```

## Complete Examples

### Example 1: Client Use Case (GeoIP Enterprise + Anonymous IP)

```toml
[output]
format = "csv"
file = "enterprise-anonymous-merged.csv"

[[network.columns]]
name = "network"
type = "cidr"

[[databases]]
name = "enterprise"
path = "/var/lib/GeoIP/GeoIP2-Enterprise.mmdb"

[[databases]]
name = "anonymous"
path = "/var/lib/GeoIP/GeoIP2-Anonymous-IP.mmdb"

# GeoIP Enterprise fields
[[columns]]
name = "country_iso"
database = "enterprise"
path = ["country", "iso_code"]

[[columns]]
name = "country_name"
database = "enterprise"
path = ["country", "names", "en"]

[[columns]]
name = "subdivision_iso"
database = "enterprise"
path = ["subdivisions", 0, "iso_code"]

[[columns]]
name = "subdivision_name"
database = "enterprise"
path = ["subdivisions", 0, "names", "en"]

[[columns]]
name = "city_name"
database = "enterprise"
path = ["city", "names", "en"]

[[columns]]
name = "latitude"
database = "enterprise"
path = ["location", "latitude"]

[[columns]]
name = "longitude"
database = "enterprise"
path = ["location", "longitude"]

[[columns]]
name = "accuracy_radius"
database = "enterprise"
path = ["location", "accuracy_radius"]

# GeoIP Anonymous IP fields
[[columns]]
name = "is_anonymous"
database = "anonymous"
path = ["is_anonymous"]

[[columns]]
name = "is_anonymous_vpn"
database = "anonymous"
path = ["is_anonymous_vpn"]

[[columns]]
name = "is_hosting_provider"
database = "anonymous"
path = ["is_hosting_provider"]

[[columns]]
name = "is_public_proxy"
database = "anonymous"
path = ["is_public_proxy"]

[[columns]]
name = "is_tor_exit_node"
database = "anonymous"
path = ["is_tor_exit_node"]

[[columns]]
name = "is_residential_proxy"
database = "anonymous"
path = ["is_residential_proxy"]
```

### Example 2: Parquet with IP Ranges

```toml
[output]
format = "parquet"
file = "geo-data.parquet"

[output.parquet]
compression = "zstd"

[[network.columns]]
name = "network_cidr"
type = "cidr"

[[network.columns]]
name = "start_ip"
type = "start_ip"

[[network.columns]]
name = "end_ip"
type = "end_ip"

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

[[columns]]
name = "city"
database = "city"
path = ["city", "names", "en"]
```

### Example 3: Single Database with Complex Fields

```toml
[output]
format = "csv"
file = "geo-full.csv"

[[databases]]
name = "enterprise"
path = "GeoIP2-Enterprise.mmdb"

[[columns]]
name = "country_code"
database = "enterprise"
path = ["country", "iso_code"]

# This will output all localized names as JSON
[[columns]]
name = "country_names_json"
database = "enterprise"
path = ["country", "names"]

# Extract specific locales
[[columns]]
name = "country_name_en"
database = "enterprise"
path = ["country", "names", "en"]

[[columns]]
name = "country_name_de"
database = "enterprise"
path = ["country", "names", "de"]
```

### Example 4: MMDB Output with Flat Structure

```toml
[output]
format = "mmdb"
file = "merged.mmdb"

[output.mmdb]
database_type = "GeoIP2-City"
description = { en = "Merged GeoIP Database" }

[[databases]]
name = "city"
path = "GeoIP2-City.mmdb"

# Flat structure: each column becomes a top-level field
[[columns]]
name = "country_code"
database = "city"
path = ["country", "iso_code"]

[[columns]]
name = "city_name"
database = "city"
path = ["city", "names", "en"]

[[columns]]
name = "latitude"
database = "city"
path = ["location", "latitude"]

[[columns]]
name = "longitude"
database = "city"
path = ["location", "longitude"]
```

### Example 5: MMDB Output with Nested Structure

```toml
[output]
format = "mmdb"
file = "nested.mmdb"

[output.mmdb]
database_type = "GeoIP2-City"
description = { en = "Nested Structure Example", de = "Beispiel für verschachtelte Strukturen" }
record_size = 28
include_reserved_networks = false

[[databases]]
name = "city"
path = "GeoIP2-City.mmdb"

# Nested structure: use output_path to create hierarchical data
[[columns]]
name = "country_code"
database = "city"
path = ["country", "iso_code"]
output_path = ["country", "iso_code"]  # Creates nested {"country": {"iso_code": "US"}}

[[columns]]
name = "city_name"
database = "city"
path = ["city", "names", "en"]
output_path = ["city", "names", "en"]  # Creates nested {"city": {"names": {"en": "New York"}}}

[[columns]]
name = "latitude"
database = "city"
path = ["location", "latitude"]
output_path = ["location", "latitude"]  # Creates nested {"location": {"latitude": 40.7128}}

[[columns]]
name = "longitude"
database = "city"
path = ["location", "longitude"]
output_path = ["location", "longitude"]  # Creates nested {"location": {"longitude": -74.0060}}
```

**Notes on MMDB output:**

- Types are preserved from source databases (uint16, float32, etc.)
- `output_path` determines the structure in the output MMDB
- Without `output_path`, fields use a flat structure with `name` as the key
- Multiple columns can share parent paths to build nested structures

### Example 6: Copying Entire Databases with path = []

```toml
[output]
format = "mmdb"
file = "enterprise-with-anonymous.mmdb"

[output.mmdb]
database_type = "GeoIP2-Enterprise"
description = { en = "Enterprise + Anonymous IP Merged" }
record_size = 28

[[databases]]
name = "enterprise"
path = "GeoIP2-Enterprise.mmdb"

[[databases]]
name = "anonymous"
path = "GeoIP2-Anonymous-IP.mmdb"

# Copy all Enterprise fields to root of output MMDB
[[columns]]
name = "enterprise_all"
database = "enterprise"
path = []           # Copy entire record
output_path = []    # Merge into root

# Copy all Anonymous IP fields nested under traits
[[columns]]
name = "anonymous_all"
database = "anonymous"
path = []           # Copy entire record
output_path = ["traits"]  # Place under traits map
```

This configuration creates a merged MMDB where:

- All Enterprise database fields appear at the root level (country, city,
  location, etc.)
- All Anonymous IP fields are nested under `traits` (e.g.,
  `traits.is_anonymous`, `traits.is_anonymous_vpn`)
- If field names conflict at the same level, the tool exits with a clear error
  message
- Nested maps are merged recursively, so multiple columns can contribute to the
  same parent map

**Resulting structure:**

```json
{
  "country": {"iso_code": "US", "names": {...}},
  "city": {"names": {...}},
  "location": {"latitude": 37.751, "longitude": -97.822},
  "traits": {
    "is_anonymous": true,
    "is_anonymous_vpn": false,
    "is_hosting_provider": false
  }
}
```

## Network Merging Behavior

When multiple databases contain overlapping networks, `mmdbconvert` creates the
smallest possible non-overlapping network blocks. For each output network:

1. The tool determines which input databases have data for that network
2. For each column, data is retrieved from the specified database only
3. If the specified database has no data for that network, the column value is
   empty/null

This means each column independently specifies its data source, giving you
complete control over the output.

## Error Handling

- **Missing database files**: Tool exits with an error
- **Invalid paths**: Empty/null value in output
- **Invalid TOML syntax**: Tool exits with parse error
- **Duplicate column names**: Tool exits with an error
