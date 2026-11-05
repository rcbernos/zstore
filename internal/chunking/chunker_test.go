package chunking

import (
	"bytes"
	"io"
	"testing"
)

// Helper function to create a reader from data
func newReader(data []byte) io.Reader {
	return bytes.NewReader(data)
}

// Helper function to create test data of a specific size
func makeTestData(size int64) []byte {
	data := make([]byte, size)
	for i := range data {
		data[i] = byte(i % 256)
	}
	return data
}

// Tests for FixedChunker

func TestFixedChunker_NChunks(t *testing.T) {
	// Test 1MB file split into 256KB chunks
	chunkSize := int64(256 * 1024) // 256KB
	fileSize := int64(1024 * 1024) // 1MB

	chunker, err := NewFixedChunker(int(chunkSize))
	if err != nil {
		t.Fatalf("Failed to create FixedChunker: %v", err)
	}

	expectedChunks := int64(4)

	reader := newReader(makeTestData(fileSize))
	chunkCount := 0

	for {
		chunk, err := chunker.NextChunk(reader)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		if chunk == nil {
			t.Fatal("Expected non-nil chunk")
		}

		if chunk.Index != chunkCount {
			t.Errorf("Expected chunk index %d, got %d", chunkCount, chunk.Index)
		}

		chunkCount++
	}

	if int64(chunkCount) != expectedChunks {
		t.Errorf("Expected %d chunks, got %d", expectedChunks, chunkCount)
	}
}

func TestFixedChunker_PartialLastChunk(t *testing.T) {
	// Test file size not evenly divisible by chunk size
	chunkSize := 10
	fileSize := int64(32) // 32 bytes, should give 3 full chunks + 1 partial (2 bytes)

	chunker, err := NewFixedChunker(chunkSize)
	if err != nil {
		t.Fatalf("Failed to create FixedChunker: %v", err)
	}

	reader := newReader(makeTestData(fileSize))

	var lastChunkSize int
	actualChunks := 0

	for {
		chunk, err := chunker.NextChunk(reader)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		actualChunks++

		// Track last chunk size
		lastChunkSize = len(chunk.Data)
	}

	// File size 32 with chunk size 10 should give 4 chunks (10 + 10 + 10 + 2)
	if actualChunks != 4 {
		t.Errorf("Expected 4 chunks for 32 bytes with 10 byte chunks, got %d", actualChunks)
	}

	// Last chunk should be 2 bytes
	if lastChunkSize != 2 {
		t.Errorf("Expected last chunk to be 2 bytes, got %d", lastChunkSize)
	}
}

func TestFixedChunker_EmptyFile(t *testing.T) {
	chunkSize := 1024

	chunker, err := NewFixedChunker(chunkSize)
	if err != nil {
		t.Fatalf("Failed to create FixedChunker: %v", err)
	}

	reader := newReader([]byte{})

	chunk, err := chunker.NextChunk(reader)
	if err != io.EOF {
		t.Errorf("Expected io.EOF for empty file, got err=%v, chunk=%v", err, chunk)
	}
}

func TestFixedChunker_InvalidChunkSize(t *testing.T) {
	testCases := []struct {
		name      string
		chunkSize int
	}{
		{"zero chunk size", 0},
		{"negative chunk size", -10},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := NewFixedChunker(tc.chunkSize)
			if err != ErrInvalidChunkSize {
				t.Errorf("Expected ErrInvalidChunkSize, got %v", err)
			}
		})
	}
}

func TestFixedChunker_ClosedChunker(t *testing.T) {
	chunkSize := 1024

	chunker, err := NewFixedChunker(chunkSize)
	if err != nil {
		t.Fatalf("Failed to create FixedChunker: %v", err)
	}

	chunker.Close()

	reader := newReader(makeTestData(100))
	_, err = chunker.NextChunk(reader)
	if err != ErrChunkerClosed {
		t.Errorf("Expected ErrChunkerClosed, got %v", err)
	}
}

func TestFixedChunker_Reset(t *testing.T) {
	chunkSize := 10
	fileSize := int64(25)

	chunker, err := NewFixedChunker(chunkSize)
	if err != nil {
		t.Fatalf("Failed to create FixedChunker: %v", err)
	}

	reader := newReader(makeTestData(fileSize))

	count := 0
	for {
		_, err := chunker.NextChunk(reader)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		count++
	}

	chunker.Reset()

	reader = newReader(makeTestData(fileSize))
	count2 := 0
	for {
		_, err := chunker.NextChunk(reader)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("Unexpected error after reset: %v", err)
		}
		count2++
	}

	if count != count2 {
		t.Errorf("After reset, expected same chunk count %d, got %d", count, count2)
	}
}

func TestFixedChunker_TotalChunksEstimate(t *testing.T) {
	testCases := []struct {
		name           string
		fileSize       int64
		chunkSize      int
		expectedChunks int
	}{
		{"empty file", 0, 1024, 0},
		{"exact multiple", 1024, 256, 4},
		{"not evenly divisible", 100, 30, 4},
		{"single chunk", 100, 100, 1},
		{"larger than file", 100, 1000, 1},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			chunker, err := NewFixedChunker(tc.chunkSize)
			if err != nil {
				t.Fatalf("Failed to create FixedChunker: %v", err)
			}

			estimate := chunker.TotalChunksEstimate(tc.fileSize)
			if estimate != tc.expectedChunks {
				t.Errorf("Expected %d chunks for size %d with chunk size %d, got %d",
					tc.expectedChunks, tc.fileSize, tc.chunkSize, estimate)
			}
		})
	}
}

func TestFixedChunker_SetTotalChunks(t *testing.T) {
	chunker, err := NewFixedChunker(1024)
	if err != nil {
		t.Fatalf("Failed to create FixedChunker: %v", err)
	}

	err = chunker.SetTotalChunks(4)
	if err != ErrUnsupported {
		t.Errorf("Expected ErrUnsupported, got %v", err)
	}
}

// Tests for EqualSplitChunker (ESC - Equal Split Chunking with max chunk size)

func TestEqualSplitChunker_MaxChunkSize(t *testing.T) {
	// Test file size under max chunk size -> should produce 1 chunk
	fileSize := int64(1000)
	maxChunkSize := int64(1024)

	chunker, err := NewEqualSplitChunker(maxChunkSize, fileSize)
	if err != nil {
		t.Fatalf("Failed to create EqualSplitChunker: %v", err)
	}

	reader := newReader(makeTestData(fileSize))

	chunk, err := chunker.NextChunk(reader)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if chunk.Index != 0 {
		t.Errorf("Expected chunk index 0, got %d", chunk.Index)
	}

	if int64(len(chunk.Data)) != fileSize {
		t.Errorf("Expected chunk size %d, got %d", fileSize, len(chunk.Data))
	}

	_, err = chunker.NextChunk(reader)
	if err != io.EOF {
		t.Errorf("Expected io.EOF after single chunk, got %v", err)
	}
}

func TestEqualSplitChunker_DivideEvenly(t *testing.T) {
	// Test 10MB file with max 4MB chunks -> should produce 3 chunks (4MB, 4MB, 2MB)
	// but with equal distribution: 4MB, 3MB, 3MB (as evenly as possible)
	fileSize := int64(10 * 1024 * 1024)    // 10MB
	maxChunkSize := int64(4 * 1024 * 1024) // 4MB max

	chunker, err := NewEqualSplitChunker(maxChunkSize, fileSize)
	if err != nil {
		t.Fatalf("Failed to create EqualSplitChunker: %v", err)
	}

	// Test 10MB file with max 4MB chunks -> ceil(10MB/4MB) = 3 chunks
	// 10MB / 3 = 3495253 base bytes, 10MB % 3 = 1 remainder
	expectedSizes := []int64{3495254, 3495253, 3495253}

	reader := newReader(makeTestData(fileSize))

	for i := 0; i < len(expectedSizes); i++ {
		chunk, err := chunker.NextChunk(reader)
		if err != nil {
			t.Fatalf("Unexpected error on chunk %d: %v", i, err)
		}

		if chunk.Index != i {
			t.Errorf("Expected chunk index %d, got %d", i, chunk.Index)
		}

		if int64(len(chunk.Data)) != expectedSizes[i] {
			t.Errorf("Chunk %d: expected size %d, got %d", i, expectedSizes[i], len(chunk.Data))
		}
	}

	_, err = chunker.NextChunk(reader)
	if err != io.EOF {
		t.Errorf("Expected io.EOF after all chunks, got %v", err)
	}
}

func TestEqualSplitChunker_NotEvenlyDivisible(t *testing.T) {
	// Test file size not evenly divisible by max chunk size
	// 10 bytes with max 4MB -> ceil(10/4) = 1 chunk = 10 bytes
	// For ESC: need to distribute 10 bytes as evenly as possible
	fileSize := int64(10)
	maxChunkSize := int64(4 * 1024 * 1024) // 4MB max (much larger than file)

	chunker, err := NewEqualSplitChunker(maxChunkSize, fileSize)
	if err != nil {
		t.Fatalf("Failed to create EqualSplitChunker: %v", err)
	}

	reader := newReader(makeTestData(fileSize))

	// With max 4MB, 10 bytes fits in 1 chunk
	chunk, err := chunker.NextChunk(reader)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if int64(len(chunk.Data)) != fileSize {
		t.Errorf("Expected chunk size %d, got %d", fileSize, len(chunk.Data))
	}

	_, err = chunker.NextChunk(reader)
	if err != io.EOF {
		t.Errorf("Expected io.EOF after single chunk, got %v", err)
	}
}

func TestEqualSplitChunker_SingleChunk(t *testing.T) {
	// File smaller than max chunk size
	fileSize := int64(1000)
	maxChunkSize := int64(2048)

	chunker, err := NewEqualSplitChunker(maxChunkSize, fileSize)
	if err != nil {
		t.Fatalf("Failed to create EqualSplitChunker: %v", err)
	}

	reader := newReader(makeTestData(fileSize))

	chunk, err := chunker.NextChunk(reader)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if chunk.Index != 0 {
		t.Errorf("Expected chunk index 0, got %d", chunk.Index)
	}

	if int64(len(chunk.Data)) != fileSize {
		t.Errorf("Expected chunk size %d, got %d", fileSize, len(chunk.Data))
	}

	_, err = chunker.NextChunk(reader)
	if err != io.EOF {
		t.Errorf("Expected io.EOF after single chunk, got %v", err)
	}
}

func TestEqualSplitChunker_EmptyFile(t *testing.T) {
	fileSize := int64(0)
	maxChunkSize := int64(100)

	chunker, err := NewEqualSplitChunker(maxChunkSize, fileSize)
	if err != nil {
		t.Fatalf("Failed to create EqualSplitChunker: %v", err)
	}

	reader := newReader([]byte{})

	chunk, err := chunker.NextChunk(reader)
	if err != io.EOF {
		t.Errorf("Expected io.EOF for empty file, got err=%v, chunk=%v", err, chunk)
	}

	if len(chunker.chunkSizes) != 0 {
		t.Errorf("Expected empty chunkSizes for zero file size, got %d elements", len(chunker.chunkSizes))
	}
}

func TestEqualSplitChunker_InvalidMaxChunkSize(t *testing.T) {
	testCases := []struct {
		name         string
		maxChunkSize int64
		fileSize     int64
	}{
		{"zero chunk size", 0, 100},
		{"negative chunk size", -1, 100},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := NewEqualSplitChunker(tc.maxChunkSize, tc.fileSize)
			if err != ErrInvalidMaxChunkSize {
				t.Errorf("Expected ErrInvalidMaxChunkSize, got %v", err)
			}
		})
	}
}

func TestEqualSplitChunker_InvalidFileSize(t *testing.T) {
	_, err := NewEqualSplitChunker(100, -1)
	if err != ErrInvalidFileSize {
		t.Errorf("Expected ErrInvalidFileSize, got %v", err)
	}
}

func TestEqualSplitChunker_ClosedChunker(t *testing.T) {
	chunker, err := NewEqualSplitChunker(100, 1000)
	if err != nil {
		t.Fatalf("Failed to create EqualSplitChunker: %v", err)
	}

	chunker.Close()

	reader := newReader(makeTestData(1000))
	_, err = chunker.NextChunk(reader)
	if err != ErrChunkerClosed {
		t.Errorf("Expected ErrChunkerClosed, got %v", err)
	}
}

func TestEqualSplitChunker_Reset(t *testing.T) {
	fileSize := int64(100)
	maxChunkSize := int64(50)

	chunker, err := NewEqualSplitChunker(maxChunkSize, fileSize)
	if err != nil {
		t.Fatalf("Failed to create EqualSplitChunker: %v", err)
	}

	reader := newReader(makeTestData(fileSize))

	count := 0
	for {
		_, err := chunker.NextChunk(reader)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		count++
	}

	chunker.Reset()

	reader = newReader(makeTestData(fileSize))
	count2 := 0
	for {
		_, err := chunker.NextChunk(reader)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("Unexpected error after reset: %v", err)
		}
		count2++
	}

	if count != count2 {
		t.Errorf("After reset, expected same chunk count %d, got %d", count, count2)
	}
}

func TestEqualSplitChunker_Close(t *testing.T) {
	chunker, err := NewEqualSplitChunker(100, 1000)
	if err != nil {
		t.Fatalf("Failed to create EqualSplitChunker: %v", err)
	}

	chunker.Close()

	reader := newReader(makeTestData(1000))
	_, err = chunker.NextChunk(reader)
	if err != ErrChunkerClosed {
		t.Errorf("Expected ErrChunkerClosed, got %v", err)
	}
}

func TestEqualSplitChunker_TotalChunksEstimate(t *testing.T) {
	testCases := []struct {
		name           string
		maxChunkSize   int64
		fileSize       int64
		testFileSize   int64
		expectedChunks int
	}{
		{"empty file", 100, 0, 0, 0},
		{"exact multiple", 100, 100, 200, 2},
		{"not evenly divisible", 100, 10, 25, 1},
		{"large file", 1024 * 1024, 10 * 1024 * 1024, 10 * 1024 * 1024, 10},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			chunker, err := NewEqualSplitChunker(tc.maxChunkSize, tc.fileSize)
			if err != nil {
				t.Fatalf("Failed to create EqualSplitChunker: %v", err)
			}

			estimate := chunker.TotalChunksEstimate(tc.testFileSize)
			if estimate != tc.expectedChunks {
				t.Errorf("Expected %d chunks estimate, got %d", tc.expectedChunks, estimate)
			}
		})
	}
}

func TestEqualSplitChunker_SetTotalChunks(t *testing.T) {
	// File size is 100 bytes, originally with max chunk size 50 -> 2 chunks
	fileSize := int64(100)
	maxChunkSize := int64(50)

	chunker, err := NewEqualSplitChunker(maxChunkSize, fileSize)
	if err != nil {
		t.Fatalf("Failed to create EqualSplitChunker: %v", err)
	}

	// Change max chunk size to 25 -> should have 4 chunks (100/25 = 4)
	err = chunker.SetTotalChunks(25)
	if err != nil {
		t.Fatalf("Unexpected error from SetTotalChunks: %v", err)
	}

	// Verify the chunk sizes are recalculated
	// 100 / 4 = 25, 100 % 4 = 0, so all chunks should be 25 bytes
	if len(chunker.chunkSizes) != 4 {
		t.Errorf("Expected 4 chunks, got %d", len(chunker.chunkSizes))
	}

	for i, size := range chunker.chunkSizes {
		if size != 25 {
			t.Errorf("Chunk %d: expected size 25, got %d", i, size)
		}
	}

	if chunker.chunkIndex != 0 {
		t.Errorf("Expected chunkIndex to be 0 after SetTotalChunks, got %d", chunker.chunkIndex)
	}

	if chunker.closed {
		t.Error("Expected closed to be false after SetTotalChunks")
	}
}

func TestEqualSplitChunker_SetTotalChunks_Invalid(t *testing.T) {
	chunker, err := NewEqualSplitChunker(100, 1000)
	if err != nil {
		t.Fatalf("Failed to create EqualSplitChunker: %v", err)
	}

	testCases := []struct {
		name            string
		newMaxChunkSize int
	}{
		{"zero total", 0},
		{"negative total", -5},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := chunker.SetTotalChunks(tc.newMaxChunkSize)
			if err != ErrInvalidMaxChunkSize {
				t.Errorf("Expected ErrInvalidMaxChunkSize, got %v", err)
			}
		})
	}
}

// Edge case tests

func TestChunker_VerySmallFiles(t *testing.T) {
	// Test very small files (1-2 bytes)
	t.Run("FixedChunker tiny file", func(t *testing.T) {
		chunker, err := NewFixedChunker(1024)
		if err != nil {
			t.Fatalf("Failed to create FixedChunker: %v", err)
		}

		for size := int64(1); size <= 2; size++ {
			chunker.Reset()
			reader := newReader(makeTestData(size))

			chunk, err := chunker.NextChunk(reader)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			if int64(len(chunk.Data)) != size {
				t.Errorf("Expected chunk size %d, got %d", size, len(chunk.Data))
			}
		}
	})

	t.Run("EqualSplitChunker tiny file", func(t *testing.T) {
		for size := int64(1); size <= 2; size++ {
			chunker, err := NewEqualSplitChunker(size, size)
			if err != nil {
				t.Fatalf("Failed to create EqualSplitChunker: %v", err)
			}

			reader := newReader(makeTestData(size))

			chunk, err := chunker.NextChunk(reader)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			if int64(len(chunk.Data)) != size {
				t.Errorf("Expected chunk size %d, got %d", size, len(chunk.Data))
			}
		}
	})
}

func TestChunker_LargeChunkSizes(t *testing.T) {
	// Test very large chunk sizes relative to file size
	t.Run("FixedChunker large chunk size", func(t *testing.T) {
		chunkSize := int64(10000)
		fileSize := int64(100)

		chunker, err := NewFixedChunker(int(chunkSize))
		if err != nil {
			t.Fatalf("Failed to create FixedChunker: %v", err)
		}

		reader := newReader(makeTestData(fileSize))

		chunk, err := chunker.NextChunk(reader)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		if int64(len(chunk.Data)) != fileSize {
			t.Errorf("Expected chunk size %d, got %d", fileSize, len(chunk.Data))
		}
	})
}

func TestChunker_ChunkSizeLargerThanFile(t *testing.T) {
	// Test chunk size larger than file size (should still work)
	chunkSize := 1000
	fileSize := 100

	t.Run("FixedChunker", func(t *testing.T) {
		chunker, err := NewFixedChunker(chunkSize)
		if err != nil {
			t.Fatalf("Failed to create FixedChunker: %v", err)
		}

		reader := newReader(makeTestData(int64(fileSize)))

		chunk, err := chunker.NextChunk(reader)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		if int64(len(chunk.Data)) != int64(fileSize) {
			t.Errorf("Expected chunk size %d, got %d", fileSize, len(chunk.Data))
		}

		_, err = chunker.NextChunk(reader)
		if err != io.EOF {
			t.Errorf("Expected io.EOF, got %v", err)
		}
	})
}

func TestChunker_DataIntegrity(t *testing.T) {
	// Verify that chunk data is correctly read and preserved
	fileSize := int64(100)

	t.Run("FixedChunker data integrity", func(t *testing.T) {
		chunkSize := 10

		chunker, err := NewFixedChunker(chunkSize)
		if err != nil {
			t.Fatalf("Failed to create FixedChunker: %v", err)
		}

		expectedData := makeTestData(fileSize)
		var receivedData []byte

		reader := newReader(expectedData)

		for {
			chunk, err := chunker.NextChunk(reader)
			if err == io.EOF {
				break
			}
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			receivedData = append(receivedData, chunk.Data...)
		}

		if !bytes.Equal(receivedData, expectedData) {
			t.Error("Data integrity check failed: received data doesn't match original")
		}
	})

	t.Run("EqualSplitChunker data integrity", func(t *testing.T) {
		maxChunkSize := int64(25)

		chunker, err := NewEqualSplitChunker(maxChunkSize, fileSize)
		if err != nil {
			t.Fatalf("Failed to create EqualSplitChunker: %v", err)
		}

		expectedData := makeTestData(fileSize)
		var receivedData []byte

		reader := newReader(expectedData)

		for {
			chunk, err := chunker.NextChunk(reader)
			if err == io.EOF {
				break
			}
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			receivedData = append(receivedData, chunk.Data...)
		}

		if !bytes.Equal(receivedData, expectedData) {
			t.Error("Data integrity check failed: received data doesn't match original")
		}
	})
}

// Interface compliance tests

func TestChunker_InterfaceCompliance(t *testing.T) {
	// Verify that both chunkers implement the Chunker interface
	var _ Chunker = (*FixedChunker)(nil)
	var _ Chunker = (*EqualSplitChunker)(nil)
}

func TestChunker_ChunkOrder(t *testing.T) {
	// Verify that chunks are returned in correct order
	fileSize := int64(50)
	chunkSize := 10

	t.Run("FixedChunker order", func(t *testing.T) {
		chunker, err := NewFixedChunker(chunkSize)
		if err != nil {
			t.Fatalf("Failed to create FixedChunker: %v", err)
		}

		reader := newReader(makeTestData(fileSize))

		expectedIndex := 0
		for {
			chunk, err := chunker.NextChunk(reader)
			if err == io.EOF {
				break
			}
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			if chunk.Index != expectedIndex {
				t.Errorf("Wrong order: expected index %d, got %d", expectedIndex, chunk.Index)
			}

			expectedIndex++
		}
	})

	t.Run("EqualSplitChunker order", func(t *testing.T) {
		maxChunkSize := int64(25)

		chunker, err := NewEqualSplitChunker(maxChunkSize, fileSize)
		if err != nil {
			t.Fatalf("Failed to create EqualSplitChunker: %v", err)
		}

		reader := newReader(makeTestData(fileSize))

		expectedIndex := 0
		for {
			chunk, err := chunker.NextChunk(reader)
			if err == io.EOF {
				break
			}
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			if chunk.Index != expectedIndex {
				t.Errorf("Wrong order: expected index %d, got %d", expectedIndex, chunk.Index)
			}

			expectedIndex++
		}
	})
}
