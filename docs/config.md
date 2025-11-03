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

### Output Settings

The `[output]` section defines where and how data should be written.

```toml
[output]
format = "csv"    # Output format: "csv" or "parquet"
file = "output.csv"  # Output file path (use this for a combined file)
# ipv4_file = "output_ipv4.csv"  # Optional IPv4-only file (set both ipv4_file and ipv6_file, omit file)
# ipv6_file = "output_ipv6.csv"  # Optional IPv6-only file (set both ipv4_file and ipv6_file, omit file)
include_empty_rows = false  # Include rows with no MMDB data (default: false)
```

**Data Filtering:**

- `include_empty_rows` - Controls whether rows with no MMDB data are written to
  the output. When `false` (default), rows where all data columns are empty/null
  are skipped. When `true`, all network ranges are included even if they have no
  associated data. Network columns (CIDR, start_ip, etc.) are always present and
  don't affect this filtering.

#### CSV Options

When `format = "csv"`, you can specify CSV-specific options:

```toml
[output.csv]
delimiter = ","           # Field delimiter (default: ",")
include_header = true     # Include column headers (default: true)
```

#### Parquet Options

When `format = "parquet"`, you can specify Parquet-specific options:

```toml
[output.parquet]
compression = "snappy"  # Compression: "none", "snappy", "gzip", "lz4", "zstd" (default: "snappy")
```

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

### Network Columns

Network columns define how IP network information is output. These columns
always appear first in the output, in the order defined.

```toml
[[network.columns]]
name = "network"    # Column name
type = "cidr"       # Output type
```

**Available types:**

- `cidr` - CIDR notation (e.g., "203.0.113.0/24")
- `start_ip` - Starting IP address (e.g., "203.0.113.0")
- `end_ip` - Ending IP address (e.g., "203.0.113.255")
- `start_int` - Starting IP as integer
- `end_int` - Ending IP as integer

**Default behavior:** If no `[[network.columns]]` sections are defined, a single
CIDR column named "network" is output.

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
path = ["country", "iso_code"]   # Path segments to the field
```

#### Path Syntax

Paths are defined as TOML arrays. Each element represents one traversal step:

- Strings access map keys (e.g., `"country"`, `"names"`)
- Integers access array indices (supports negative indices)
- Strings are used verbatim, so keys may include `/` without escaping

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
```

#### Data Types

- **Scalar values** (string, number, boolean) are output as-is
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
