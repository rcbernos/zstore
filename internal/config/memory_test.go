package config

import "testing"

func TestParseMemoryLimit(t *testing.T) {
	tests := []struct {
		input    string
		expected int64
		hasError bool
	}{
		{"", 0, false},
		{"512", 512, false},
		{"1B", 1, false},
		{"512B", 512, false},
		{"1KB", 1024, false},
		{"1K", 1024, false},
		{"1MB", 1024 * 1024, false},
		{"1M", 1024 * 1024, false},
		{"1GB", 1024 * 1024 * 1024, false},
		{"1G", 1024 * 1024 * 1024, false},
		{"1TB", 1024 * 1024 * 1024 * 1024, false},
		{"1T", 1024 * 1024 * 1024 * 1024, false},
		{"512MB", 512 * 1024 * 1024, false},
		{"2.5GB", 2*1024*1024*1024 + 512*1024*1024, false},
		{"16MB", 16 * 1024 * 1024, false},
		{"invalid", 0, true},
		{"512XY", 0, true},
	}

	for _, tt := range tests {
		result, err := ParseMemoryLimit(tt.input)
		if tt.hasError {
			if err == nil {
				t.Errorf("ParseMemoryLimit(%q) expected error, got none", tt.input)
			}
		} else {
			if err != nil {
				t.Errorf("ParseMemoryLimit(%q) unexpected error: %v", tt.input, err)
			}
			if result != tt.expected {
				t.Errorf("ParseMemoryLimit(%q) = %d, want %d", tt.input, result, tt.expected)
			}
		}
	}
}
