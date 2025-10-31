# mmdbconvert - AI Assistant Context

## Project Overview

`mmdbconvert` is a Go command-line tool that merges multiple MaxMind MMDB
(MaxMind Database) files and exports the merged data to CSV or Parquet format.
The primary use case is merging GeoIP databases (e.g., GeoIP2-Enterprise +
GeoIP2-Anonymous-IP) while maintaining non-overlapping network blocks and
allowing flexible column mapping.

**Key Features:**

- Merge data from multiple MMDB databases
- Output to CSV or Parquet format
- Flexible column mapping via TOML configuration
- Query-optimized Parquet output with integer columns for efficient IP lookups
- Streaming architecture for memory-efficient processing
- Optional type hints for Parquet native types (int64, float64, bool, etc.)

## Project Structure

```
mmdbconvert/
├── cmd/
│   └── mmdbconvert/
│       └── main.go              # CLI entry point
├── internal/
│   ├── config/                  # TOML configuration parsing & validation
│   ├── mmdb/                    # MMDB database reading & data extraction
│   ├── network/                 # IP/CIDR utilities
│   └── writer/                  # CSV and Parquet writers
├── examples/                    # Example configuration files
├── testdata/                    # Test MMDB files
├── docs/
│   ├── config.md                # Configuration file reference
│   └── parquet-queries.md       # Parquet query optimization guide
├── plan.md                      # Detailed implementation plan
├── README.md
├── LICENSE
├── .gitignore
├── .precious.toml               # Precious (linter runner) configuration
├── .prettierrc.json             # Prettier configuration for markdown
├── go.mod
└── go.sum
```

## Dependencies

- `github.com/pelletier/go-toml/v2` - TOML configuration parsing
- `github.com/oschwald/maxminddb-golang/v2` - MMDB database reading
- `github.com/parquet-go/parquet-go` - Parquet file writing

## Building

```bash
# Build the binary
go build -o mmdbconvert ./cmd/mmdbconvert

# Install to $GOPATH/bin
go install ./cmd/mmdbconvert
```

## Running

```bash
# Run with config file
./mmdbconvert config.toml

# Or with explicit flag
./mmdbconvert --config config.toml

# Suppress progress output
./mmdbconvert --config config.toml --quiet
```

## Testing

```bash
# Run all tests
go test ./...

# Run tests with coverage
go test -cover ./...

# Run tests with race detection
go test -race ./...

# Run tests verbosely
go test -v ./...

# Run benchmarks
go test -bench ./...
```

## Code Quality & Formatting

### Linting with golangci-lint

```bash
# Run all linters
golangci-lint run

# Run with auto-fix where possible
golangci-lint run --fix
```

**Note:** This project uses `golangci-lint` for Go code quality checks. Ensure
it's installed:

```bash
# Install golangci-lint
go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest
```

### Formatting

```bash
# Format Go code (standard)
gofmt -w .

# Or use goimports for import organization
goimports -w .
```

**Note:** There is no `golangci-lint fmt` command. Use `gofmt` or `goimports`
for formatting.

### Using Precious (Unified Linter Runner)

This project uses [Precious](https://github.com/houseabsolute/precious) to run
linters consistently:

```bash
# Run all configured linters (includes prettier for markdown)
precious tidy

# Check without making changes (CI mode)
precious lint

# Format specific files
precious tidy docs/config.md plan.md
```

**Precious Configuration:** See `.precious.toml` for configured linters:

- `prettier-markdown` - Formats markdown files with 80-character line wrap

### Pre-commit Checklist

Before committing changes:

1. **Format code:**

   ```bash
   gofmt -w .
   ```

2. **Run linters:**

   ```bash
   golangci-lint run
   ```

3. **Format markdown:**

   ```bash
   precious tidy
   ```

4. **Run tests:**
   ```bash
   go test ./...
   ```

## Development Workflow

### Adding a New Feature

1. Read the implementation plan in `plan.md`
2. Implement changes following the project structure
3. Add tests for new functionality
4. Run linters and tests
5. Update documentation if needed
6. Format code and markdown

### Key Implementation Details

**Streaming Architecture:**

- Never collect all networks in memory
- Use streaming accumulator pattern
- Write rows immediately when data changes
- Ensures O(1) memory regardless of database size

**Network Iteration:**

- Uses `maxminddb.Networks()` and `NetworksWithin()`
- Nested iteration pattern for multiple databases
- Always uses smallest overlapping network
- Adjacent networks with identical data are merged

**Error Handling:**

- Fail-fast approach for data extraction and type conversion errors
- Exit immediately on JSON encoding failures or type mismatches
- Ensures data quality over partial completion

**Type System:**

- CSV: All values as strings
- Parquet: Optional type hints (string, int64, float64, bool, binary)
- Type mismatches cause immediate failure

## Configuration

Configuration is via TOML file. See:

- `docs/config.md` - Full configuration reference
- `examples/` - Example configuration files

## Documentation

- `README.md` - User-facing documentation
- `docs/config.md` - Configuration file reference
- `docs/parquet-queries.md` - How to query generated Parquet files efficiently
- `plan.md` - Detailed implementation plan (development reference)
- `CLAUDE.md` - This file (AI assistant context)

## Common Tasks

### Add a new network column type

1. Update config validation in `internal/config/`
2. Update network column generation in `internal/network/`
3. Update CSV writer in `internal/writer/csv.go`
4. Update Parquet writer in `internal/writer/parquet.go`
5. Add tests
6. Update `docs/config.md`

### Add a new Parquet type hint

1. Update config validation in `internal/config/`
2. Update type conversion in `internal/mmdb/extractor.go`
3. Update Parquet schema generation in `internal/writer/parquet.go`
4. Add tests
5. Update `docs/config.md` with type conversion rules

### Debug network merging issues

1. Check `internal/mmdb/merger.go` - Streaming accumulator logic
2. Verify `rangeToCIDRs()` function - Converts ranges to valid CIDRs
3. Test with small MMDB files in `testdata/`
4. Enable verbose logging to see accumulator flush points

## Performance Considerations

- **Memory:** O(1) - Streaming accumulator ensures constant memory
- **Processing Time:** O(N log N) where N = total networks across all databases
- **Parquet Query Performance:** 10-100x faster with integer columns (see
  `docs/parquet-queries.md`)

## Function Organization

**Function Ordering**: Functions should be ordered by their call hierarchy
within files:

- Main/exported functions first
- Helper functions ordered by when they are first called
- Utility functions at the bottom

For example, in a handler file:

```go
// Handler function (main entry point)
func (h *Handler) SomeHandler(ctx context.Context, r *website.RequestContext) error {
    // calls helperA, then helperB
}

// helperA appears first (called first in Handler)
func helperA() {}

// helperB appears second (called second in Handler)
func helperB() {}

// utility functions at bottom
func utilityFunction() {}
```

This ordering makes code easier to read and understand the flow of execution.

### Function Organization Best Practices

**Call Hierarchy Ordering**: When adding new functions, place them in the order
they are called:

- This makes the code flow easier to follow
- Reduces mental overhead when reading the code
- Makes debugging and maintenance more intuitive

**Example of Good Ordering**:

```go
// Main handler function
func (h *Handler) ProcessPayment(ctx context.Context, r *website.RequestContext) error {
    address := getCustomerAddress(ctx, customerID)  // Called first
    form := createForm(person, address)             // Called second
    return processForm(form)                        // Called third
}

// Helper functions in call order
func getCustomerAddress(ctx context.Context, customerID string) *Address { /* ... */ }
func createForm(person *Person, address *Address) *Form { /* ... */ }
func processForm(form *Form) error { /* ... */ }
```

## Go errors

- Errors should be wrapped, e.g., `fmt.Errorf("looking up %s: %w", ip, err)`.
- When wrapping errors, do not using words like "failed", "error", etc. You
  should instead say what the program was doing.

## Go Testing Best Practices

- **Unit tests:** Every package has `*_test.go` files
- **Integration tests:** End-to-end tests with small test MMDB files
- **Test data:** Use existing MaxMind test databases from MaxMind repos
- **Coverage goal:** >80% overall, 100% for network merging and config
  validation

### Use Testify for Assertions

Use the testify library for test assertions:

- **assert**: Use when the test can continue even if the assertion fails
- **require**: Use when the test must stop if the assertion fails

```go
import (
    "github.com/stretchr/testify/assert"
    "github.com/stretchr/testify/require"
)

func TestMyFunction(t *testing.T) {
    result, err := MyFunction("input")
    require.NoError(t, err) // Must pass for test to continue
    assert.Equal(t, "expected", result) // Can fail but test continues
}
```

### Prefer Table-Driven Tests

Use table-driven tests for Go code to test multiple scenarios efficiently:

```go
func TestMyFunction(t *testing.T) {
    tests := []struct {
        name     string
        input    string
        expected string
        wantErr  bool
    }{
        {
            name:     "valid input",
            input:    "test",
            expected: "result",
            wantErr:  false,
        },
        {
            name:    "invalid input",
            input:   "",
            wantErr: true,
        },
    }

    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            result, err := MyFunction(tt.input)
            if tt.wantErr {
                require.Error(t, err)
                return
            }
            require.NoError(t, err)
            assert.Equal(t, tt.expected, result)
        })
    }
}
```

## Contributing

When making changes:

1. Follow the implementation plan in `plan.md`
2. Maintain streaming architecture (no collecting all rows in memory)
3. Add comprehensive tests
4. Update documentation
5. Run linters and formatters before committing

## Troubleshooting

### Build fails

- Ensure Go 1.25+ is installed
- Run `go mod tidy` to sync dependencies

### Tests fail

- Check that test MMDB files are present in `testdata/`
- Verify MMDB file paths in test configurations

### Linter errors

- Run `golangci-lint run` to see all issues
- Use `golangci-lint run --fix` for auto-fixable issues
- Check `.golangci.yml` for linter configuration

### Precious not found

- Install: `brew install precious` (macOS) or see
  [Precious releases](https://github.com/houseabsolute/precious/releases)
