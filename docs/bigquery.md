# BigQuery with Network Bucketing

BigQuery performs full table scans for range queries like
`WHERE start_int <= ip AND end_int >= ip`. Use a `network_bucket` column to
enable efficient lookups.

**Note:** The BigQuery table must be clustered on the `network_bucket` column
for efficient querying.

**Important:** The bucket size in your queries must match the configured bucket
size. The examples below use the default `/16` bucket size. If you configured a
different `ipv4_bucket_size` or `ipv6_bucket_size`, adjust the second argument
to `NET.IP_TRUNC()` accordingly.

## IPv4 Lookup

For IPv4, the bucket is int64. Use `NET.IP_TRUNC()` to get the bucket and
`NET.IPV4_TO_INT64()` to convert to the integer type:

```sql
-- Using default ipv4_bucket_size = 16
SELECT *
FROM `project.dataset.geoip_v4`
WHERE network_bucket = NET.IPV4_TO_INT64(NET.IP_TRUNC(NET.IP_FROM_STRING('203.0.113.100'), 16))
AND NET.IPV4_TO_INT64(NET.IP_FROM_STRING('203.0.113.100')) BETWEEN start_int AND end_int;
```

## IPv6 Lookup

The query depends on your `ipv6_bucket_type` configuration.

**Note:** For IPv6 files, `start_int` and `end_int` columns are stored as
16-byte binary values, not integers. The comparison with `NET.IP_FROM_STRING()`
works because it also returns BYTES.

**Using default `ipv6_bucket_type = "string"` (hex string):**

```sql
-- Using default ipv6_bucket_size = 16
SELECT *
FROM `project.dataset.geoip_v6`
WHERE network_bucket = TO_HEX(NET.IP_TRUNC(NET.IP_FROM_STRING('2001:db8::1'), 16))
AND NET.IP_FROM_STRING('2001:db8::1') BETWEEN start_int AND end_int;
```

**Using `ipv6_bucket_type = "int"` (60-bit int64):**

```sql
-- Using default ipv6_bucket_size = 16
SELECT *
FROM `project.dataset.geoip_v6`
WHERE network_bucket = CAST(CONCAT('0x', SUBSTR(
    TO_HEX(NET.IP_TRUNC(NET.IP_FROM_STRING('2001:db8::1'), 16)), 1, 15
  )) AS INT64)
AND NET.IP_FROM_STRING('2001:db8::1') BETWEEN start_int AND end_int;
```

The int type expression extracts the first 60 bits (15 hex chars) of the
truncated IPv6 address as an integer.

## Why Bucketing Helps

Without bucketing, BigQuery must scan every row to check the range condition.
With bucketing:

1. BigQuery first filters by exact match on `network_bucket`
2. Only matching bucket rows are checked for the range condition
3. Result: Query scans only rows in the matching bucket instead of the entire
   table
