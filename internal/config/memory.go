// Package config provides memory parsing utilities for CLI flags.
package config

// ParseMemoryLimit parses human-readable memory strings (e.g., "512MB", "1GB", "1024KB")
// and returns the value in bytes. Returns 0 if input is empty.
func ParseMemoryLimit(s string) (int64, error) {
	return ParseSize(s)
}
