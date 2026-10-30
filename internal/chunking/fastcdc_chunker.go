// Package chunking - FastCDC content-defined chunking implementation
//
// This file implements FastCDC (Fast Content-Defined Chunking) using the Gear
// rolling hash algorithm. FastCDC provides content-defined chunk boundaries,
// enabling deduplication of identical content across files.
package chunking

import (
	"errors"
	"fmt"
	"io"
)

// GearHash implements the Gear rolling hash from the FastCDC paper.
// It implements the hash.Hash32 interface for compatibility.
// It uses a fixed table of pseudo-random values indexed by byte values
// and maintains a rolling window of bytes.
type GearHash struct {
	gear   [256]uint32
	ghash  uint32
	window []byte
	pos    int
	size   int
}

// NewGearHash creates a new Gear hash instance with the specified window size.
func NewGearHash(windowSize int) *GearHash {
	gh := &GearHash{
		window: make([]byte, windowSize),
		size:   windowSize,
	}
	// Initialize Gear table with deterministic pseudo-random values.
	// These values are derived using the same approach as the FastCDC paper:
	// a fixed mapping of byte values to 32-bit unsigned integers.
	for i := 0; i < 256; i++ {
		gh.gear[i] = randomGearValue(i)
	}
	return gh
}

// Write adds bytes to the rolling hash.
func (gh *GearHash) Write(p []byte) (int, error) {
	for _, b := range p {
		gh.ghash = (gh.ghash << 1) + gh.gear[b]
		gh.window[gh.pos] = b
		gh.pos = (gh.pos + 1) % gh.size
	}
	return len(p), nil
}

// Sum32 returns the current hash value as uint32.
func (gh *GearHash) Sum32() uint32 {
	return gh.ghash
}

// Sum implements the hash.Hash32 interface.
func (gh *GearHash) Sum(b []byte) []byte {
	h := gh.ghash
	sum := make([]byte, len(b), len(b)+4)
	copy(sum, b)
	sum = append(sum, byte(h), byte(h>>8), byte(h>>16), byte(h>>24))
	return sum
}

// Reset clears the hash state (implements hash.Hash32).
func (gh *GearHash) Reset() {
	gh.ghash = 0
	gh.pos = 0
}

// Size implements the hash.Hash32 interface.
func (gh *GearHash) Size() int {
	return 4
}

// BlockSize implements the hash.Hash32 interface.
func (gh *GearHash) BlockSize() int {
	return 1
}

// randomGearValue generates deterministic pseudo-random Gear values using a
// xorshift32-based formula, matching the FastCDC paper's approach of using a
// fixed mapping table of byte values to random-looking 32-bit integers.
func randomGearValue(i int) uint32 {
	x := uint32(i)
	x ^= x << 13
	x ^= x >> 17
	x ^= x << 5
	return x | 1 // Ensure odd for better distribution
}

// FastCDCChunker implements content-defined chunking using FastCDC algorithm.
// It uses the Gear rolling hash to find chunk boundaries based on content,
// enabling deduplication of identical content across files.
type FastCDCChunker struct {
	config       FastCDCConfig
	currIndex    int
	closed       bool
	gearHash     *GearHash
	minSize      int
	maxSize      int
	mask         uint32
	level        int         // Chunking level (1, 2, or 3)
	skipFactor   int         // Number of positions to skip between boundary checks (level-based)
	buffer       []byte
	bufferSize   int
}

// NewFastCDCChunker creates a new FastCDCChunker with the given configuration.
// It validates the configuration and applies defaults for any zero or negative values.
func NewFastCDCChunker(config FastCDCConfig) (*FastCDCChunker, error) {
	// Apply defaults
	if config.TargetSize <= 0 {
		config.TargetSize = DefaultFastCDCTargetSize
	}
	if config.MinSize <= 0 {
		config.MinSize = DefaultFastCDCMinSize
	}
	if config.MaxSize <= 0 {
		config.MaxSize = DefaultFastCDCMaxSize
	}
	if config.Level == 0 {
		config.Level = DefaultFastCDCLevel
	}
	if config.WindowSize == 0 {
		config.WindowSize = DefaultFastCDCWindowSize
	}

	// Calculate mask from target size only (level does NOT affect mask per paper)
	// mask = (1 << k) - 1 where k = floor(log2(target_size))
	k := 0
	for (1 << k) < config.TargetSize {
		k++
	}
	config.Mask = (1 << uint(k)) - 1

	// Calculate skip factor based on level (per FastCDC paper):
	// Level 1: skipFactor = 1 (check every position after minSize)
	// Level 2: skipFactor = 2 (check every 2nd position - ~2x speed)
	// Level 3: skipFactor = 4 (check every 4th position - ~4x speed for small files)
	var skipFactor int
	switch config.Level {
	case 1:
		skipFactor = 1
	case 2:
		skipFactor = 2
	case 3:
		skipFactor = 4
	default:
		return nil, errors.New("invalid level: must be 1, 2, or 3")
	}

	// Validate configuration
	if config.MinSize > config.TargetSize {
		return nil, errors.New("min size must be <= target size")
	}
	if config.MaxSize < config.TargetSize {
		return nil, errors.New("max size must be >= target size")
	}
	if config.MinSize > config.MaxSize {
		return nil, errors.New("min size must be <= max size")
	}

	// Ensure window size is reasonable (32 to 256 bytes per paper)
	if config.WindowSize < 32 || config.WindowSize > 256 {
		config.WindowSize = DefaultFastCDCWindowSize
	}

	// Create buffer for buffer (max possible chunk size)
	return &FastCDCChunker{
		config:       config,
		currIndex:    0,
		closed:       false,
		gearHash:     NewGearHash(config.WindowSize),
		minSize:      config.MinSize,
		maxSize:      config.MaxSize,
		mask:         config.Mask,
		level:        config.Level,
		skipFactor:   skipFactor,
		buffer:       make([]byte, 0, config.MaxSize),
		bufferSize:   0,
	}, nil
}
// NextChunk reads the next content-defined chunk from the reader.
// It uses the Gear rolling hash to determine chunk boundaries based on the mask.
// Boundary checking follows FastCDC paper's level-based approach:
//   - Level 1: Check every position after minSize
//   - Level 2: Check every 2nd position after minSize
//   - Level 3: Check every 4th position after minSize
func (c *FastCDCChunker) NextChunk(r io.Reader) (*Chunk, error) {
	if c.closed {
		return nil, ErrChunkerClosed
	}

	c.buffer = c.buffer[:0]
	c.bufferSize = 0
	c.gearHash.Reset()

	readBuf := make([]byte, 4096)
	checkPos := 0 // Position within current chunk for boundary checking

	for {
		n, err := r.Read(readBuf)
		if n > 0 {
			c.buffer = append(c.buffer, readBuf[:n]...)
			c.bufferSize += n

			// Update rolling hash for all bytes
			c.gearHash.Write(readBuf[:n])

			// Check for chunk boundary using level-based skip factor
			if c.bufferSize >= c.minSize && c.isBoundary(checkPos) {
				chunk := &Chunk{
					Index: c.currIndex,
					Data:  make([]byte, len(c.buffer)),
				}
				copy(chunk.Data, c.buffer)
				c.currIndex++
				return chunk, nil
			}

			// Enforce maximum size
			if c.bufferSize >= c.maxSize {
				chunk := &Chunk{
					Index: c.currIndex,
					Data:  make([]byte, len(c.buffer)),
				}
				copy(chunk.Data, c.buffer)
				c.currIndex++
				return chunk, nil
			}

			checkPos += n
		}

		if err == io.EOF {
			if len(c.buffer) > 0 {
				chunk := &Chunk{
					Index: c.currIndex,
					Data:  make([]byte, len(c.buffer)),
				}
				copy(chunk.Data, c.buffer)
				c.currIndex++
				return chunk, nil
			}
			return nil, io.EOF
		}
		if err != nil {
			return nil, fmt.Errorf("error reading chunk: %w", err)
		}
	}
}

// isBoundary checks if the current position is a chunk boundary.
// Uses level-based skip factor: only checks boundary when checkPos is
// a multiple of skipFactor (after minSize requirement is met).
func (c *FastCDCChunker) isBoundary(checkPos int) bool {
	// Must have at least minSize before checking boundary
	if c.bufferSize < c.minSize {
		return false
	}

	// Apply level-based skip factor per FastCDC paper
	if checkPos%c.skipFactor != 0 {
		return false
	}

	// Check Gear hash against mask
	hash := c.gearHash.Sum32()
	return (hash & c.mask) == 0
}

// Reset returns the FastCDCChunker to its initial state.
func (c *FastCDCChunker) Reset() {
	c.currIndex = 0
	c.closed = false
	c.buffer = c.buffer[:0]
	c.bufferSize = 0
	if c.gearHash != nil {
		c.gearHash.Reset()
	}
}

// Close marks the chunker as closed.
func (c *FastCDCChunker) Close() {
	c.closed = true
}

// TotalChunksEstimate returns the estimated number of chunks for a file of the given size.
// For FastCDC, this is approximate since chunk sizes are content-defined.
func (c *FastCDCChunker) TotalChunksEstimate(size int64) int {
	if size <= 0 {
		return 0
	}
	// Estimate based on target size (actual will vary based on content)
	return int((size + int64(c.config.TargetSize) - 1) / int64(c.config.TargetSize))
}

// SetTotalChunks returns ErrUnsupported for FastCDC since it uses content-defined chunking.
func (c *FastCDCChunker) SetTotalChunks(total int) error {
	return ErrUnsupported
}
