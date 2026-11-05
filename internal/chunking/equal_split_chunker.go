package chunking

import (
	"errors"
	"fmt"
	"io"
)

// Sentinel errors specific to EqualSplitChunker construction.
var (
	// ErrInvalidMaxChunkSize is returned when an invalid max chunk size is
	// specified (i.e. less than or equal to zero).
	ErrInvalidMaxChunkSize = errors.New("invalid max chunk size: must be greater than 0")

	// ErrInvalidFileSize is returned when a negative file size is specified.
	ErrInvalidFileSize = errors.New("invalid file size: must be non-negative")
)

// EqualSplitChunker implements Equal-Split Chunking (ESC), a secondary chunking
// strategy that divides files into equally-sized chunks with a maximum byte size.
//
// Unlike FixedChunker which creates chunks of max size with a smaller remainder,
// EqualSplitChunker distributes bytes as evenly as possible across all chunks,
// ensuring no two chunks differ by more than 1 byte.
//
// The algorithm:
// 1. Sets a maximum chunk size in bytes
// 2. Determines the minimum number of chunks needed so all chunks fit under max size
// 3. Distributes bytes evenly across chunks (max 1 byte difference between chunks)
//
// This produces balanced chunk sizes unlike Fixed-Length Chunking which can have
// one very small remainder chunk.
//
// Example with maxChunkSize=4MB, fileSize=10MB:
//
//	chunker, err := NewEqualSplitChunker(4*1024*1024, 10*1024*1024)
//	if err != nil {
//	    log.Fatal(err)
//	}
//	for {
//	    chunk, err := chunker.NextChunk(reader)
//	    if err == io.EOF {
//	        break
//	    }
//	    if err != nil {
//	        log.Fatal(err)
//	    }
//	    // Process chunk (shard, upload, etc.)
//	}
type EqualSplitChunker struct {
	// maxChunkSize is the maximum allowed size for each chunk.
	maxChunkSize int64

	// numChunks is the calculated number of chunks (derived from fileSize and maxChunkSize).
	numChunks int

	// chunkIndex is the zero-based index of the next chunk to be returned.
	chunkIndex int

	// chunkSizes holds the pre-calculated byte size of each chunk to emit.
	// The slice is ordered by index: chunkSizes[0] corresponds to Index 0,
	// chunkSizes[1] to Index 1, and so on. All sizes differ by at most 1 byte.
	chunkSizes []int64

	// closed indicates whether Close has been called, preventing further
	// operations on this chunker.
	closed bool
}

// NewEqualSplitChunker creates a new EqualSplitChunker that splits a file
// into chunks with a maximum size, distributing bytes as evenly as possible.
//
// maxChunkSize is the maximum allowed size for each chunk in bytes.
// fileSize must be non-negative.
//
// The algorithm:
// 1. Calculates the minimum number of chunks needed: ceil(fileSize / maxChunkSize)
// 2. Distributes bytes evenly: all chunks differ by at most 1 byte
// 3. Empty files produce zero chunks
//
// Returns ErrInvalidMaxChunkSize if maxChunkSize is less than or equal to 0.
// Returns ErrInvalidFileSize if fileSize is negative.
func NewEqualSplitChunker(maxChunkSize int64, fileSize int64) (*EqualSplitChunker, error) {
	if maxChunkSize <= 0 {
		return nil, ErrInvalidMaxChunkSize
	}
	if fileSize < 0 {
		return nil, ErrInvalidFileSize
	}

	sizes := calculateEqualChunkSizes(maxChunkSize, fileSize)
	numChunks := len(sizes)

	return &EqualSplitChunker{
		maxChunkSize: maxChunkSize,
		numChunks:    numChunks,
		chunkIndex:   0,
		chunkSizes:   sizes,
		closed:       false,
	}, nil
}

// calculateEqualChunkSizes divides fileSize into chunks where:
// 1. The number of chunks is the minimum needed to fit all data under maxChunkSize
// 2. All chunks differ by at most 1 byte (bytes distributed as evenly as possible)
//
// This produces balanced chunk sizes, unlike Fixed-Length Chunking which can have
// one very small remainder chunk.
func calculateEqualChunkSizes(maxChunkSize int64, fileSize int64) []int64 {
	if fileSize == 0 {
		return []int64{}
	}

	// Calculate minimum number of chunks needed
	numChunks := int((fileSize + maxChunkSize - 1) / maxChunkSize)

	sizes := make([]int64, 0, numChunks)
	base := fileSize / int64(numChunks)
	remainder := fileSize % int64(numChunks)

	// First 'remainder' chunks get base + 1, rest get base
	// This ensures all chunks differ by at most 1 byte
	for i := 0; i < numChunks; i++ {
		size := base
		if i < int(remainder) {
			size++
		}
		sizes = append(sizes, size)
	}

	return sizes
}

// NextChunk reads the next chunk from the provided io.Reader.
//
// It reads exactly the pre-calculated number of bytes for the current chunk
// (using io.ReadFull internally) and returns them as a Chunk with the
// current chunk index. The index increments on each successful call so
// that subsequent chunks continue the sequence.
//
// When all pre-calculated chunks have been returned, the next call returns
// io.EOF. If the underlying reader returns fewer bytes than expected
// (io.ErrUnexpectedEOF), the partial data is returned as the final chunk,
// mirroring the behaviour of FixedChunker.
//
// Returns ErrChunkerClosed if the chunker has been closed via Close.
func (c *EqualSplitChunker) NextChunk(r io.Reader) (*Chunk, error) {
	if c.closed {
		return nil, ErrChunkerClosed
	}

	if c.chunkIndex >= len(c.chunkSizes) {
		return nil, io.EOF
	}

	size := c.chunkSizes[c.chunkIndex]
	buf := make([]byte, size)

	n, err := io.ReadFull(r, buf)
	if err != nil {
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			// Return whatever partial data was read, if any.
			if n > 0 {
				chunk := &Chunk{
					Index: c.chunkIndex,
					Data:  buf[:n],
				}
				c.chunkIndex++
				return chunk, nil
			}
			// No data at all — treat as EOF.
			return nil, io.EOF
		}
		return nil, fmt.Errorf("error reading chunk: %w", err)
	}

	chunk := &Chunk{
		Index: c.chunkIndex,
		Data:  buf,
	}
	c.chunkIndex++

	return chunk, nil
}

// TotalChunksEstimate returns the number of chunks that a file of the given
// size would be split into using the current maxChunkSize configuration.
//
// This recomputes the chunk-size calculation for the provided size, so the
// result may differ from numChunks if the size differs from the file
// size the chunker was originally configured with.
//
// For files of zero bytes, the estimate is 0.
func (c *EqualSplitChunker) TotalChunksEstimate(size int64) int {
	return len(calculateEqualChunkSizes(c.maxChunkSize, size))
}

// SetTotalChunks interprets the 'total' parameter as a new max chunk size
// (in bytes) for the previously-configured file size. This maintains backward
// compatibility with the Chunker interface while adapting to the new ESC semantics.
//
// Returns ErrInvalidMaxChunkSize if total is less than or equal to 0.
func (c *EqualSplitChunker) SetTotalChunks(total int) error {
	if total <= 0 {
		return ErrInvalidMaxChunkSize
	}
	// Since we don't store original file size, estimate it from existing chunks
	if len(c.chunkSizes) > 0 {
		var totalSize int64
		for _, s := range c.chunkSizes {
			totalSize += s
		}
		newSizes := calculateEqualChunkSizes(int64(total), totalSize)
		c.maxChunkSize = int64(total)
		c.numChunks = len(newSizes)
		c.chunkSizes = newSizes
		c.chunkIndex = 0
		c.closed = false
	}
	return nil
}

// Reset returns the EqualSplitChunker to its initial state, ready to process
// a new file. After Reset, the next call to NextChunk will start with
// index 0 again and re-emit the pre-calculated chunk sizes from the
// beginning.
//
// The chunk size calculation (and thus maxChunkSize) is not re-evaluated;
// only the read position is rewound.
func (c *EqualSplitChunker) Reset() {
	c.chunkIndex = 0
	c.closed = false
}

// Close marks the chunker as closed, preventing further NextChunk calls.
// This is useful for releasing resources (such as internal buffers) when the
// chunker is no longer needed.
func (c *EqualSplitChunker) Close() {
	c.closed = true
}
