package chunking

import (
	"errors"
	"fmt"
	"io"
)

// Sentinel errors specific to EqualSplitChunker construction.
var (
	// ErrInvalidChunkCount is returned when an invalid total chunk count is
	// specified (i.e. less than or equal to zero).
	ErrInvalidChunkCount = errors.New("invalid chunk count: must be greater than 0")

	// ErrInvalidFileSize is returned when a negative file size is specified.
	ErrInvalidFileSize = errors.New("invalid file size: must be non-negative")
)

// EqualSplitChunker implements a secondary chunking strategy that divides a
// file into a specified number of roughly equal-sized chunks.
//
// Unlike FixedChunker which accepts a chunk size, EqualSplitChunker accepts
// the total number of desired chunks and the file size, pre-calculating the
// chunk boundaries at construction time. This is useful when the number of
// output chunks needs to be controlled — for example, to match the number of
// erasure-coding shards or downstream processing workers.
//
// The first (fileSize % totalChunks) chunks will be one byte larger than the
// remaining chunks to distribute the remainder as evenly as possible. Any
// zero-size chunks that would result when totalChunks exceeds fileSize are
// omitted, so the actual number of chunks returned may be fewer than
// totalChunks.
//
// Example:
//
//	chunker, err := NewEqualSplitChunker(4, 10*1024*1024) // 4 chunks of 10MB file
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
	// totalChunks is the original number of chunks requested by the caller.
	// It may differ from len(chunkSizes) when zero-size chunks are omitted.
	totalChunks int

	// chunkIndex is the zero-based index of the next chunk to be returned.
	chunkIndex int

	// chunkSizes holds the pre-calculated byte size of each chunk to emit.
	// The slice is ordered by index: chunkSizes[0] corresponds to Index 0,
	// chunkSizes[1] to Index 1, and so on.
	chunkSizes []int64

	// closed indicates whether Close has been called, preventing further
	// operations on this chunker.
	closed bool
}

// NewEqualSplitChunker creates a new EqualSplitChunker that splits a file of
// the given size into the specified number of roughly equal-sized chunks.
//
// totalChunks must be positive. fileSize must be non-negative.
//
// The chunk sizes are calculated upfront: the first (fileSize % totalChunks)
// chunks will be one byte larger than the remaining chunks. Chunks with a
// size of zero (which occur when totalChunks > fileSize) are silently
// omitted, so the number of chunks actually returned may be fewer than
// totalChunks.
//
// Returns ErrInvalidChunkCount if totalChunks is less than or equal to 0.
// Returns ErrInvalidFileSize if fileSize is negative.
func NewEqualSplitChunker(totalChunks int, fileSize int64) (*EqualSplitChunker, error) {
	if totalChunks <= 0 {
		return nil, ErrInvalidChunkCount
	}
	if fileSize < 0 {
		return nil, ErrInvalidFileSize
	}

	sizes := calculateEqualChunkSizes(totalChunks, fileSize)

	return &EqualSplitChunker{
		totalChunks: totalChunks,
		chunkIndex:  0,
		chunkSizes:  sizes,
		closed:      false,
	}, nil
}

// calculateEqualChunkSizes divides fileSize into at most totalChunks
// portions as evenly as possible. The first (fileSize % totalChunks) chunks
// are one byte larger than the rest. Chunks with a size of zero are omitted.
func calculateEqualChunkSizes(totalChunks int, fileSize int64) []int64 {
	if fileSize == 0 {
		return []int64{}
	}

	sizes := make([]int64, 0, totalChunks)
	base := fileSize / int64(totalChunks)
	remainder := fileSize % int64(totalChunks)

	for i := 0; i < totalChunks; i++ {
		size := base
		if i < int(remainder) {
			size++
		}
		if size > 0 {
			sizes = append(sizes, size)
		}
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
// size would be split into using the current totalChunks configuration.
//
// This recomputes the chunk-size calculation for the provided size, so the
// result may differ from len(chunkSizes) if the size differs from the file
// size the chunker was originally configured with.
//
// For files of zero bytes, the estimate is 0.
func (c *EqualSplitChunker) TotalChunksEstimate(size int64) int {
	return len(calculateEqualChunkSizes(c.totalChunks, size))
}

// SetTotalChunks reconfigures the target number of chunks and recalculates
// the chunk boundaries accordingly. The chunkIndex is reset to 0 and the
// chunker is marked as not closed.
//
// Returns ErrInvalidChunkCount if total is less than or equal to 0.
//
// Note: SetTotalChunks does not take a fileSize parameter — it only
// reconfigures the number of chunks. If the file size also needs updating,
// construct a new EqualSplitChunker via NewEqualSplitChunker.
func (c *EqualSplitChunker) SetTotalChunks(total int) error {
	if total <= 0 {
		return ErrInvalidChunkCount
	}
	c.totalChunks = total
	c.chunkIndex = 0
	c.closed = false
	// Note: chunkSizes is not recalculated here because we don't have a
	// fileSize. Callers should construct a new EqualSplitChunker with
	// NewEqualSplitChunker if the file size also changes.
	return nil
}

// Reset returns the EqualSplitChunker to its initial state, ready to process
// a new file. After Reset, the next call to NextChunk will start with
// index 0 again and re-emit the pre-calculated chunk sizes from the
// beginning.
//
// The chunk size calculation (and thus totalChunks) is not re-evaluated;
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
