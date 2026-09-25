# City CSV compatibility fixtures

These fixtures pair the public City CSV example with its historical MMDB input.
Tests read only committed files and do not download anything.

- `reference/`: unchanged blocks CSVs from the
  [City CSV example ZIP](https://dev.maxmind.com/examples/GeoIP2-City-CSV_Example.zip),
  retrieved September 21, 2026. The ZIP entries are dated December 27, 2023.
- `GeoIP2-City-Test.mmdb`: copied from
  [MaxMind-DB revision 655ed62823f969e4d2b76a9ff81123fe10f38004](https://github.com/maxmind/MaxMind-DB/blob/655ed62823f969e4d2b76a9ff81123fe10f38004/test-data/GeoIP2-City-Test.mmdb).
  This is the most recent City fixture change preceding the ZIP entry date.
- `city.toml`: conversion configuration, using fixed precision for coordinates
  and explicit labels for booleans. Paths are relative to the repository root.
- `expected/`: complete mmdbconvert output after formatting changes.

The MMDB is pinned separately from the MaxMind-DB submodule to keep the
comparison stable when other fixtures change.

## SHA-256 provenance

| Artifact                  | SHA-256                                                            |
| ------------------------- | ------------------------------------------------------------------ |
| Downloaded ZIP            | `957012607fbcde1d59bd20ca807ad0c672ff96d4b87c21e2d7b8cfa5bb226e31` |
| `GeoIP2-City-Test.mmdb`   | `d165914a64c42cfb39ddd45e991c3aa89531992c9508acec3e7dab8940bfc602` |
| Reference IPv4 blocks CSV | `62fec088296420aedf382e62a53112708b4ecfa56606cf63e2cf4757b9430306` |
| Reference IPv6 blocks CSV | `7b58747e8c0ecf33bf6b710e48ae927f23ca7c301832201b1cc4926a5954f0a6` |

To recover the MMDB from a clone with the historical commit available:

```sh
git -C testdata/MaxMind-DB show \
  655ed62823f969e4d2b76a9ff81123fe10f38004:test-data/GeoIP2-City-Test.mmdb \
  > testdata/geoip-csv/GeoIP2-City-Test.mmdb
```

## Compare with the City sample

Run from the repository root:

```sh
mkdir -p build/geoip-csv
go run ./cmd/mmdbconvert --quiet testdata/geoip-csv/city.toml
go test -run '^TestRun_CityCSVParity$' .

for version in 4 6; do
  name="GeoIP2-City-Blocks-IPv${version}.csv"
  cmp "testdata/geoip-csv/expected/$name" "build/geoip-csv/$name"
  diff -u --label "reference/$name" --label "expected/$name" \
    "testdata/geoip-csv/reference/$name" "build/geoip-csv/$name"
done
```

`cmp` succeeds; `diff` exits with status 1 for the differences checked by
`assertCityDifferences` in [geoip_csv_test.go](../../geoip_csv_test.go). When
changing expected output, update those assertions too.
