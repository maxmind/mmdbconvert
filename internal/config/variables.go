package config

import (
	"errors"
	"fmt"
	"maps"
	"regexp"
	"slices"
	"strings"
)

var variableNamePattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// ValidateVariableName checks that name is a valid path parameter identifier.
func ValidateVariableName(name string) error {
	if !variableNamePattern.MatchString(name) {
		return fmt.Errorf("invalid variable name %q (expected [A-Za-z_][A-Za-z0-9_]*)", name)
	}
	return nil
}

func resolvePaths(cfg *Config, variables map[string]string) error {
	// Sort names so errors are deterministic, including for Go API callers.
	names := slices.Sorted(maps.Keys(variables))
	for _, name := range names {
		if err := ValidateVariableName(name); err != nil {
			return err
		}
	}

	// Only filesystem paths participate. In particular, columns.path and
	// columns.output_path are MMDB field paths and must remain literal.
	paths := []struct {
		field string
		value *string
	}{
		{"output.file", &cfg.Output.File},
		{"output.ipv4_file", &cfg.Output.IPv4File},
		{"output.ipv6_file", &cfg.Output.IPv6File},
	}
	used := make(map[string]bool, len(variables))
	for _, path := range paths {
		value, err := expandPath(*path.value, variables, used)
		if err != nil {
			return fmt.Errorf("%s: %w", path.field, err)
		}
		*path.value = value
	}
	for i := range cfg.Databases {
		db := &cfg.Databases[i]
		value, err := expandPath(db.Path, variables, used)
		if err != nil {
			return fmt.Errorf("databases[%d].path (name %q): %w", i, db.Name, err)
		}
		db.Path = value
	}
	for _, name := range names {
		if !used[name] {
			return fmt.Errorf("unused variable %q", name)
		}
	}
	return nil
}

// expandPath substitutes ${name} once, treating supplied values as literal text.
// $${ escapes a placeholder opener; other dollar signs are left alone.
func expandPath(path string, variables map[string]string, used map[string]bool) (string, error) {
	var result strings.Builder
	for path != "" {
		switch {
		case strings.HasPrefix(path, "$${"):
			result.WriteString("${")
			path = path[3:]
		case strings.HasPrefix(path, "${"):
			end := strings.IndexByte(path, '}')
			if end == -1 {
				return "", errors.New("unterminated variable placeholder")
			}
			name := path[2:end]
			if err := ValidateVariableName(name); err != nil {
				return "", err
			}
			value, ok := variables[name]
			if !ok {
				return "", fmt.Errorf("undefined variable %q", name)
			}
			result.WriteString(value)
			used[name] = true
			path = path[end+1:]
		default:
			result.WriteByte(path[0])
			path = path[1:]
		}
	}
	return result.String(), nil
}
