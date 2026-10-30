// Package chunking provides abstraction for dividing files into chunks for
// chunked erasure coding storage.
//
// This package enables memory-efficient processing of large files by allowing
// them to be split into manageable chunks that can be erasure-coded and
// distributed independently.
//
// Key Concepts:
//   - Chunk: A portion of a file with a specific index
//   - Chunker: Abstraction for different chunking strategies
//
// The chunking layer sits between file upload/download operations and
// erasure coding, allowing for streaming processing without loading entire
// files into memory.
//
// Usage:
//
//	chunker := chunking.NewFixedChunker(chunkSize)
//	for {
//	    chunk, err := chunker.NextChunk(reader)
//	    if err == io.EOF {
//	        break
//	    }
//	    // Process chunk (shard, upload, etc.)
//	}
package chunking

import (
	"errors"
	"hash"
	"io"
)

// Chunk represents a single chunk of data from a file being chunked.
// It contains the chunk's index in the sequence and the actual data bytes.
type Chunk struct {
	// Index is the zero-based position of this chunk in the file sequence.
	// Chunk 0 is the first chunk, chunk 1 is the second, etc.
	Index int

	// Data contains the actual bytes of this chunk.
	// The size may vary for the last chunk if the file size is not
	// evenly divisible by the chunk size.
	Data []byte
}

// Chunker defines the interface for chunking strategies that divide files
// into manageable portions for erasure coding and distributed storage.
//
// Implementations must be safe for concurrent use if needed, or must be
// called sequentially by the caller.
type Chunker interface {
	// NextChunk reads the next chunk from the provided io.Reader.
	// It returns a Chunk with the next portion of data and its index.
	//
	// On successful reads, returns the chunk and a nil error.
	// When EOF is reached, returns io.EOF as the error.
	// Other errors indicate failure conditions.
	//
	// The caller should not modify the returned Chunk or its Data slice
	// as they may be reused internally by the implementation.
	NextChunk(r io.Reader) (*Chunk, error)

	// Reset returns the chunker to its initial state, ready to process
	// a new file. After Reset, the next call to NextChunk will start
	// with index 0 again.
	Reset()

	// TotalChunksEstimate returns the estimated number of chunks that a file
	// of the given size (in bytes) would be split into using this chunker's
	// current configuration. This is a pure calculation and does not require
	// reading from a reader, making it suitable for pre-allocation of
	// metadata structures before file processing begins.
	//
	// For files of zero bytes, the estimate is 0.
	TotalChunksEstimate(size int64) int

	// SetTotalChunks reconfigures the target number of chunks for chunkers
	// that support chunk-count-based splitting (such as EqualSplitChunker).
	// It recalculates internal chunk boundaries accordingly.
	//
	// For chunkers that do not support chunk-count configuration (such as
	// FixedChunker), this method returns ErrUnsupported.
	//
	// After calling SetTotalChunks, Reset should be called before processing
	// a new file.
	SetTotalChunks(total int) error
}

// Sentinel errors for chunking operations
var (
	// ErrInvalidChunkSize is returned when an invalid chunk size is specified.
	ErrInvalidChunkSize = errors.New("invalid chunk size: must be greater than 0")

	// ErrChunkerClosed is returned when operations are attempted on a closed chunker.
	ErrChunkerClosed = errors.New("chunker is closed")

	// ErrPartialRead is returned when a partial read occurs unexpectedly.
	ErrPartialRead = errors.New("unexpected partial read")

	// ErrUnsupported is returned when an operation is not supported by the
	// given chunker implementation.
	ErrUnsupported = errors.New("operation not supported by this chunker")
)

// FastCDC configuration constants
const (
	// DefaultFastCDCTargetSize is the target average chunk size (8KB)
	DefaultFastCDCTargetSize = 8 * 1024
	// DefaultFastCDCMinSize is the minimum chunk size (2KB)
	DefaultFastCDCMinSize = 2 * 1024
	// DefaultFastCDCMaxSize is the maximum chunk size (16KB, as recommended by paper)
	DefaultFastCDCMaxSize = 16 * 1024
	// DefaultFastCDCLevel is the default chunking level (1 = primary boundaries)
	DefaultFastCDCLevel = 1
	// DefaultFastCDCWindowSize is the default rolling hash window size (64 bytes)
	DefaultFastCDCWindowSize = 64
)

// FastCDCConfig holds configuration for FastCDC chunking
type FastCDCConfig struct {
	// TargetSize is the target average chunk size in bytes
	// Must be >= MinSize and <= MaxSize
	TargetSize int

	// MinSize is the minimum chunk size in bytes
	// Must be > 0 and <= TargetSize
	MinSize int

	// MaxSize is the maximum chunk size in bytes
	// Must be >= TargetSize
	MaxSize int

	// Mask is the bit mask for boundary detection
	// Lower bits = smaller chunks, more boundaries
	// Per FastCDC paper: mask = (1 << k) - 1 where k = floor(log2(TargetSize))
	// Level does NOT affect mask - only skip factor varies by level
	Mask uint32

	// Level is the FastCDC chunking level (1, 2, or 3)
	// Level 1: Primary boundaries (skip=1, smallest chunks, most granular)
	// Level 2: Aggregated boundaries (skip=2, ~2x larger chunks, faster)
	// Level 3: Coarse boundaries (skip=4, ~4x larger chunks, fastest for re-chunking)
	Level int

	// WindowSize is the rolling hash window size in bytes
	// Default is 64 for 8KB target, should be proportional to target size
	WindowSize int

	// HashFunc is the hash function to use (default: Gear Hash)
	HashFunc func() hash.Hash32
}
