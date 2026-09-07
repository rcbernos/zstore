package chunking

import (
	"fmt"
	"io"
)

// FixedChunker implements a chunking strategy that divides files into
// equal-sized chunks of a specified size.
//
// This is the primary chunking strategy for Phase 1, providing predictable
// chunk sizes that simplify storage and retrieval operations.
//
// Example:
//
//	chunker := NewFixedChunker(1024 * 1024) // 1MB chunks
//	for {
//	    chunk, err := chunker.NextChunk(reader)
//	    if err == io.EOF {
//	        break
//	    }
//	    // Process chunk (shard, upload, etc.)
//	}
type FixedChunker struct {
	chunkSize int
	currIndex int
	closed    bool
}

// NewFixedChunker creates a new FixedChunker with the specified chunk size.
// The chunk size determines the maximum number of bytes in each chunk.
// The final chunk may be smaller if the file size is not evenly divisible.
//
// Returns ErrInvalidChunkSize if chunkSize is less than or equal to 0.
func NewFixedChunker(chunkSize int) (*FixedChunker, error) {
	if chunkSize <= 0 {
		return nil, ErrInvalidChunkSize
	}

	return &FixedChunker{
		chunkSize: chunkSize,
		currIndex: 0,
		closed:    false,
	}, nil
}

// NextChunk reads the next chunk from the provided io.Reader.
// It reads up to chunkSize bytes and returns them as a Chunk with the
// current index, then increments the index for the next call.
//
// Returns io.EOF when no more data is available.
// Returns an error for other failure conditions.
func (c *FixedChunker) NextChunk(r io.Reader) (*Chunk, error) {
	if c.closed {
		return nil, ErrChunkerClosed
	}

	// Allocate buffer for the chunk
	buf := make([]byte, c.chunkSize)

	// Read up to chunkSize bytes
	n, err := io.ReadFull(r, buf)
	if err != nil {
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			// End of data reached - return what we have if non-empty
			if n > 0 {
				chunk := &Chunk{
					Index: c.currIndex,
					Data:  buf[:n],
				}
				c.currIndex++
				return chunk, nil
			}
			// No more data
			return nil, io.EOF
		}
		return nil, fmt.Errorf("error reading chunk: %w", err)
	}

	chunk := &Chunk{
		Index: c.currIndex,
		Data:  buf,
	}
	c.currIndex++

	return chunk, nil
}

// Reset returns the FixedChunker to its initial state, ready to process
// a new file. After Reset, the next call to NextChunk will start with
// index 0 again.
func (c *FixedChunker) Reset() {
	c.currIndex = 0
	c.closed = false
}

// Close marks the chunker as closed, preventing further operations.
// This is useful for releasing resources when the chunker is no longer needed.
func (c *FixedChunker) Close() {
	c.closed = true
}

// TotalChunksEstimate returns the number of chunks that a file of the given
// size would be split into using this chunker's chunk size.
//
// For files of zero bytes, the estimate is 0. For non-zero files, the
// estimate is the ceiling of size / chunkSize, matching the behaviour of
// NextChunk which returns a final (potentially partial) chunk.
func (c *FixedChunker) TotalChunksEstimate(size int64) int {
	if size <= 0 {
		return 0
	}
	cs := int64(c.chunkSize)
	return int((size + cs - 1) / cs)
}

// SetTotalChunks is not supported by FixedChunker, which is configured with a
// chunk size rather than a target chunk count. It returns ErrUnsupported.
func (c *FixedChunker) SetTotalChunks(total int) error {
	return ErrUnsupported
}
