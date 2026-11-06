// Package config provides size parsing utilities for CLI flags.
package config

import (
	"fmt"
	"strconv"
	"unicode"
)

// ParseSize parses human-readable size strings (e.g., "512MB", "1GB", "1024KB")
// and returns the value in bytes as int64.
// Supports units: B, KB/K, MB/M, GB/G, TB/T.
func ParseSize(s string) (int64, error) {
	if s == "" {
		return 0, nil
	}

	var numStr, unit string
	for _, r := range s {
		if unicode.IsDigit(r) || r == '.' {
			numStr += string(r)
		} else if unicode.IsLetter(r) {
			unit += string(unicode.ToUpper(r))
		}
	}

	if numStr == "" {
		return 0, fmt.Errorf("invalid size format: %s", s)
	}

	num, err := strconv.ParseFloat(numStr, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid size value: %s (%w)", numStr, err)
	}

	switch unit {
	case "B":
		return int64(num), nil
	case "KB", "K":
		return int64(num * 1024), nil
	case "MB", "M":
		return int64(num * 1024 * 1024), nil
	case "GB", "G":
		return int64(num * 1024 * 1024 * 1024), nil
	case "TB", "T":
		return int64(num * 1024 * 1024 * 1024 * 1024), nil
	case "":
		return int64(num), nil
	default:
		return 0, fmt.Errorf("unknown size unit: %s", unit)
	}
}
