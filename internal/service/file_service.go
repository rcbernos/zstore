// Package service provides the core business logic for the erasure coding object storage system.
// This file implements the main FileService for erasure-coded file operations.
//
// FileService provides erasure-coded file operations with the following features:
// - Reed-Solomon erasure coding for fault tolerance
// - Multi-bucket shard distribution via placement strategies
// - Dynamic concurrent downloads with early termination
// - Shard integrity verification using CRC64 hashes
// - Fail-fast upload logic respecting parity shard limits
// - Metadata storage for reconstruction information
//
// Key Operations:
// - UploadFile: Shards file, distributes across buckets, stores metadata
// - UploadFileChunked: Optional chunked upload via the chunking layer for large files
// - DownloadFile: Retrieves shards, verifies integrity, reconstructs file
// - DeleteFile: Removes shards from all buckets and metadata
//
// Architecture:
// - Uses Placer interface for multi-bucket/multi-provider support
// - Integrates with MetadataRepository for shard location tracking
// - Implements dynamic concurrency control for optimal performance
// - Supports configurable Reed-Solomon parameters (data/parity shards)
// - Supports chunked uploads via the chunking layer (fixed / equal-split strategies)
package service

import (
	"bytes"
	"context"
	"fmt"
	"hash/crc64"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/zzenonn/zstore/internal/chunking"
	"github.com/zzenonn/zstore/internal/domain"
	"github.com/zzenonn/zstore/internal/errors"
	"github.com/zzenonn/zstore/internal/placement"
)

type MetadataRepository interface {
	CreateMetadata(ctx context.Context, metadata domain.ObjectMetadata) (domain.ObjectMetadata, error)
	GetMetadata(ctx context.Context, prefix, fileName string) (domain.ObjectMetadata, error)
	ListMetadataByPrefix(ctx context.Context, prefix string) ([]domain.ObjectMetadata, error)
	UpdateMetadata(ctx context.Context, metadata domain.ObjectMetadata) (domain.ObjectMetadata, error)
	DeleteMetadata(ctx context.Context, prefix, fileName string) error
}

type FileService struct {
	placer       placement.Placer
	metadataRepo MetadataRepository
	concurrency  int
}

// NewFileService creates a new FileService instance
func NewFileService(placer placement.Placer, metadataRepo MetadataRepository) *FileService {
	return &FileService{
		placer:       placer,
		metadataRepo: metadataRepo,
		concurrency:  1,
	}
}

// Chunk method identifiers persisted on ObjectMetadata.ChunkMethod describe how
// an object was (or was not) chunked during upload. They are intentionally kept
// in sync with the chunkMethod accepted by UploadFileChunked.
const (
	// ChunkMethodNone means the object was stored as a single erasure-coded unit
	// (the legacy, backward-compatible behaviour).
	ChunkMethodNone = "none"
	// ChunkMethodFixed means the object was split into fixed-size chunks before
	// each chunk was independently erasure-coded.
	ChunkMethodFixed = "fixed"
	// ChunkMethodEqualSplit means the object was split into N roughly equal-sized
	// chunks before each chunk was independently erasure-coded.
	ChunkMethodEqualSplit = "equal-split"
)

// UploadFile uploads a file across multiple cloud storage buckets.
//
// This is the legacy, non-chunked entry point retained for backward
// compatibility. It reads the whole file into memory and shards it as a single
// unit. For memory-efficient processing of large files prefer UploadFileChunked,
// which streams the file through the chunking layer.
//
// When chunkSize is 0 or chunkMethod is "none", the file is processed as a
// single erasure-coded unit (legacy behavior). Otherwise, the file is split
// into chunks and each chunk is independently erasure-coded.
func (s *FileService) UploadFile(ctx context.Context, key string, r io.Reader, quiet bool, dataShards, parityShards, concurrency int, chunkSize int64, chunkMethod string) error {
	return s.UploadFileChunked(ctx, key, r, quiet, dataShards, parityShards, concurrency, chunkSize, chunkMethod)
}

// UploadFileChunked uploads a file with optional chunked upload support.
//
// When chunkMethod is ChunkMethodNone the file is read in its entirety and
// processed as a single erasure-coded unit — identical to UploadFile. When
// chunkMethod is ChunkMethodFixed or ChunkMethodEqualSplit the file is first
// divided into chunks by the chunking layer and each chunk is independently
// erasure-coded and sharded across buckets, bounding peak memory usage to
// roughly one chunk at a time.
//
// chunkSize is interpreted according to chunkMethod:
//   - ChunkMethodFixed: chunk size in bytes (each chunk is at most this big).
//   - ChunkMethodEqualSplit: the target number of chunks (the file is divided
//     into that many roughly equal parts).
//   - ChunkMethodNone: chunkSize is ignored.
//
// For chunked uploads each chunk's shards are stored under keys of the form
// "<prefix>/<filename>/chunk_<N>/<shardHash>" and per-chunk metadata (with the
// populated shard locations) is recorded on ObjectMetadata.Chunks. The
// top-level ShardHashes field is left empty for chunked objects so the download
// path can distinguish chunked objects from legacy ones.
func (s *FileService) UploadFileChunked(ctx context.Context, key string, r io.Reader, quiet bool, dataShards, parityShards, concurrency int, chunkSize int64, chunkMethod string) error {
	start := time.Now()

	if chunkMethod == "" {
		chunkMethod = ChunkMethodNone
	}

	prefix := filepath.Dir(key)

	log.Debugf("Uploading %s (chunkMethod=%s)", key, chunkMethod)

	// Delete prefix contents if it exists from all buckets so stale shards from a
	// previous upload of the same key don't linger.
	deleteStart := time.Now()
	buckets := s.placer.ListBuckets()
	for _, bucketName := range buckets {
		if repo, err := s.placer.GetRepositoryForBucket(bucketName); err == nil {
			repo.DeletePrefix(ctx, key) // Ignore errors
		}
	}
	log.Debugf("Delete prefix took: %v", time.Since(deleteStart))

	// Build the object metadata using either the chunked or legacy code path.
	var metadata domain.ObjectMetadata
	switch chunkMethod {
	case ChunkMethodNone:
		m, uploadErr := s.uploadFileLegacy(ctx, key, r, quiet, dataShards, parityShards, concurrency)
		if uploadErr != nil {
			return uploadErr
		}
		m.Prefix = prefix
		m.FileName = filepath.Base(key)
		m.ChunkMethod = ChunkMethodNone
		metadata = m
	default:
		m, uploadErr := s.uploadFileChunked(ctx, key, r, quiet, dataShards, parityShards, concurrency, chunkSize, chunkMethod)
		if uploadErr != nil {
			return uploadErr
		}
		m.Prefix = prefix
		m.FileName = filepath.Base(key)
		metadata = m
	}

	// Store metadata
	metadataStart := time.Now()
	_, err := s.metadataRepo.CreateMetadata(ctx, metadata)
	log.Debugf("Metadata storage took: %v", time.Since(metadataStart))
	log.Debugf("Total upload took: %v", time.Since(start))
	return err
}

// uploadFileLegacy implements the non-chunked upload path: it reads the entire
// file into memory, erasure-codes it as a single unit, and uploads the resulting
// shards under the original "<key>/<shardHash>" key scheme.
func (s *FileService) uploadFileLegacy(ctx context.Context, key string, r io.Reader, quiet bool, dataShards, parityShards, concurrency int) (domain.ObjectMetadata, error) {
	// Read file data
	readStart := time.Now()
	data, err := io.ReadAll(r)
	if err != nil {
		return domain.ObjectMetadata{}, err
	}
	log.Debugf("File read took: %v", time.Since(readStart))

	// Check for empty file
	if len(data) == 0 {
		return domain.ObjectMetadata{}, errors.ErrEmptyFile
	}

	// Create shards using erasure coding
	shardStart := time.Now()
	metadata, shards, err := ShardFile(data, dataShards, parityShards)
	if err != nil {
		return domain.ObjectMetadata{}, err
	}
	log.Debugf("Sharding took: %v", time.Since(shardStart))

	// Upload shards in parallel
	uploadStart := time.Now()
	if err := s.uploadShards(ctx, key, shards, metadata.ShardHashes, quiet, concurrency, parityShards); err != nil {
		return domain.ObjectMetadata{}, err
	}
	log.Debugf("Shard uploads took: %v", time.Since(uploadStart))

	return metadata, nil
}

// uploadFileChunked orchestrates chunked upload: it walks the chunking layer to
// obtain successive chunks, erasure-codes each chunk independently, uploads the
// resulting shards (keyed by "<key>/chunk_<N>/<shardHash>"), and accumulates
// per-chunk metadata (with populated shard locations) onto the returned
// ObjectMetadata.Chunks slice.
func (s *FileService) uploadFileChunked(ctx context.Context, key string, r io.Reader, quiet bool, dataShards, parityShards, concurrency int, chunkSize int64, chunkMethod string) (domain.ObjectMetadata, error) {
	chunker, err := newChunker(chunkMethod, chunkSize, r)
	if err != nil {
		return domain.ObjectMetadata{}, err
	}

	var metadata domain.ObjectMetadata
	metadata.ChunkMethod = chunkMethod
	metadata.ParityShards = parityShards

	chunkUploadStart := time.Now()

	for {
		chunk, err := chunker.NextChunk(r)
		if err == io.EOF {
			break
		}
		if err != nil {
			return domain.ObjectMetadata{}, fmt.Errorf("failed to read next chunk: %w", err)
		}

		if len(chunk.Data) == 0 {
			continue
		}

		// Erasure-code this chunk independently
		shardStart := time.Now()
		chunkMeta, chunkShards, err := ShardFile(chunk.Data, dataShards, parityShards)
		if err != nil {
			return domain.ObjectMetadata{}, fmt.Errorf("failed to shard chunk %d: %w", chunk.Index, err)
		}
		log.Debugf("Chunk %d sharding took: %v", chunk.Index, time.Since(shardStart))

		// Upload this chunk's shards under "<key>/chunk_<N>/<hash>".
		// Key format: <prefix>/<filename>/chunk_<N>/<shardHash>
		shardKeyPrefix := fmt.Sprintf("%s/chunk_%d", key, chunk.Index)
		uploadStart := time.Now()
		if err := s.uploadShards(ctx, shardKeyPrefix, chunkShards, chunkMeta.ShardHashes, quiet, concurrency, parityShards); err != nil {
			log.Debugf("Chunk %d shard uploads took: %v", chunk.Index, time.Since(uploadStart))
			return domain.ObjectMetadata{}, fmt.Errorf("failed to upload chunk %d shards: %w", chunk.Index, err)
		}
		log.Debugf("Chunk %d shard uploads took: %v", chunk.Index, time.Since(uploadStart))

		// Accumulate chunk metadata
		metadata.Chunks = append(metadata.Chunks, domain.ChunkMetadata{
			ChunkIndex: chunk.Index,
			ChunkSize:  int64(len(chunk.Data)),
			Shards:     chunkMeta.ShardHashes,
		})
		metadata.OriginalSize += int64(len(chunk.Data))
		if metadata.ShardSize == 0 && len(chunkShards) > 0 {
			metadata.ShardSize = int64(len(chunkShards[0]))
		}
	}

	if len(metadata.Chunks) == 0 {
		return domain.ObjectMetadata{}, errors.ErrEmptyFile
	}

	metadata.TotalChunks = len(metadata.Chunks)
	// For the "fixed" strategy the configured byte size is the meaningful value;
	// for "equal-split" chunkSize holds a chunk count, so record the real byte
	// size of the first emitted chunk instead.
	if chunkMethod == ChunkMethodEqualSplit {
		metadata.ChunkSize = metadata.Chunks[0].ChunkSize
	} else {
		metadata.ChunkSize = chunkSize
	}

	log.Debugf("Chunked upload of %d chunks took: %v", metadata.TotalChunks, time.Since(chunkUploadStart))

	return metadata, nil
}

// newChunker constructs the appropriate Chunker implementation for the given
// chunking strategy. For ChunkMethodEqualSplit the reader must be seekable so
// the total file size can be determined up front.
//
// For ChunkMethodEqualSplit, chunkSize is interpreted as the target number of
// chunks. For ChunkMethodFixed it is the byte size of each chunk.
func newChunker(chunkMethod string, chunkSize int64, r io.Reader) (chunking.Chunker, error) {
	switch chunkMethod {
	case ChunkMethodFixed:
		if chunkSize <= 0 {
			return nil, fmt.Errorf("chunkSize must be greater than 0 for %q chunking", ChunkMethodFixed)
		}
		return chunking.NewFixedChunker(int(chunkSize))
	case ChunkMethodEqualSplit:
		total, err := readerSize(r)
		if err != nil {
			return nil, fmt.Errorf("%q chunking requires a seekable reader: %w", ChunkMethodEqualSplit, err)
		}
		if chunkSize <= 0 {
			return nil, fmt.Errorf("chunkSize (number of chunks) must be greater than 0 for %q chunking", ChunkMethodEqualSplit)
		}
		return chunking.NewEqualSplitChunker(int(chunkSize), total)
	default:
		return nil, fmt.Errorf("unsupported chunk method: %q", chunkMethod)
	}
}

// readerSize returns the total size in bytes of a seekable reader by seeking to
// the end and rewinding to the start. Readers that don't implement io.Seeker
// cannot be sized without being consumed, which prevents equal-split chunking
// from pre-calculating chunk boundaries.
func readerSize(r io.Reader) (int64, error) {
	seeker, ok := r.(io.Seeker)
	if !ok {
		return 0, fmt.Errorf("reader must implement io.Seeker")
	}
	end, err := seeker.Seek(0, io.SeekEnd)
	if err != nil {
		return 0, fmt.Errorf("seek to end failed: %w", err)
	}
	if _, err := seeker.Seek(0, io.SeekStart); err != nil {
		return 0, fmt.Errorf("rewind to start failed: %w", err)
	}
	return end, nil
}

// DownloadFile downloads a file from cloud storage to the provided writer.
// It supports both legacy (non-chunked) files and chunked files, automatically
// detecting the file type based on metadata.ChunkMethod.
// When chunked is true, it uses chunked download for chunked files if available.
// When chunked is false, it forces the legacy download path.
func (s *FileService) DownloadFile(ctx context.Context, key string, dest io.Writer, quiet bool, verifyIntegrity bool, chunked bool) error {
	// Get prefix and filename for metadata lookup
	prefix := filepath.Dir(key)
	fileName := filepath.Base(key)

	// Get metadata
	metadata, err := s.metadataRepo.GetMetadata(ctx, prefix, fileName)
	if err != nil {
		return err
	}

	log.Debugf("Object Metadata: %+v\n", metadata)

	// Check if file is chunked
	if chunked && metadata.ChunkMethod != ChunkMethodNone && len(metadata.Chunks) > 0 {
		return s.downloadFileChunked(ctx, key, dest, metadata, quiet, verifyIntegrity)
	}

	// Legacy download path for non-chunked files
	// Download shards to temporary files (retaining original shard indices)
	indexedShards, err := s.downloadShards(ctx, metadata.ShardHashes, metadata.ParityShards, quiet, verifyIntegrity)
	if err != nil {
		return err
	}

	// Cleanup temp files when done
	defer func() {
		for _, shard := range indexedShards {
			os.Remove(shard.Path)
		}
	}()

	// Reconstruct file from indexed temp files
	reconstructedData, err := ReconstructFileFromPaths(indexedShards, metadata)
	if err != nil {
		return err
	}

	// Write reconstructed data to destination (streaming to io.Writer)
	_, err = dest.Write(reconstructedData)
	return err
}

// downloadFileChunked handles downloading a chunked file.
// It iterates through each chunk, downloads the chunk's shards, reconstructs
// each chunk, and writes it incrementally to the destination.
func (s *FileService) downloadFileChunked(ctx context.Context, key string, dest io.Writer, metadata domain.ObjectMetadata, quiet bool, verifyIntegrity bool) error {
	// Download and reconstruct each chunk, writing incrementally to dest
	for _, chunk := range metadata.Chunks {
		// Create temp files for each shard in this chunk
		tempFilePaths := make([]string, len(chunk.Shards))
		tempFiles := make([]*os.File, len(chunk.Shards))
		var wg sync.WaitGroup
		var mu sync.Mutex
		var downloadErrors []error

		cancelCtx, cancel := context.WithCancel(context.Background())

		// Download each shard of the chunk
		for i, shardInfo := range chunk.Shards {
			wg.Add(1)
			go func(i int, shardInfo domain.ShardStorage) {
				defer wg.Done()

				// Select bucket and repository for this shard
				repo, err := s.placer.GetRepositoryForBucket(shardInfo.BucketName)
				if err != nil {
					mu.Lock()
					downloadErrors = append(downloadErrors, err)
					mu.Unlock()
					return
				}

				// Create temp file for this shard
				tempFile, err := os.CreateTemp("", fmt.Sprintf("chunk_shard_%d_*.tmp", i))
				if err != nil {
					mu.Lock()
					downloadErrors = append(downloadErrors, err)
					mu.Unlock()
					return
				}
				tempFilePath := tempFile.Name()

				// Download shard to temp file
				err = repo.Download(cancelCtx, shardInfo.Key, tempFile, quiet)
				tempFile.Close() // Close after download
				if err != nil {
					os.Remove(tempFilePath)
					mu.Lock()
					downloadErrors = append(downloadErrors, err)
					mu.Unlock()
					return
				}

				// Verify integrity if requested
				if verifyIntegrity {
					data, err := os.ReadFile(tempFilePath)
					if err != nil {
						os.Remove(tempFilePath)
						mu.Lock()
						downloadErrors = append(downloadErrors, err)
						mu.Unlock()
						return
					}
					if err := verifyFileIntegrity(data, shardInfo.Hash); err != nil {
						os.Remove(tempFilePath)
						mu.Lock()
						downloadErrors = append(downloadErrors, err)
						mu.Unlock()
						return
					}
				}

				// Store successful download
				mu.Lock()
				tempFilePaths[shardInfo.Index] = tempFilePath
				tempFiles[i] = tempFile
				mu.Unlock()
			}(i, shardInfo)
		}

		// Wait for all downloads to complete
		wg.Wait()
		cancel() // Cancel context after all downloads complete for this chunk

		// Check for errors
		mu.Lock()
		if len(downloadErrors) > 0 {
			for _, path := range tempFilePaths {
				if path != "" {
					os.Remove(path)
				}
			}
			mu.Unlock()
			return downloadErrors[0]
		}
		mu.Unlock()

		// Check if we have enough shards for reconstruction
		dataShards := len(chunk.Shards) - metadata.ParityShards
		if int(successfulShardsCount(tempFilePaths)) < dataShards {
			return errors.ErrInsufficientShards
		}

		// Reconstruct the chunk from downloaded shards
		chunkData, err := s.reconstructChunk(tempFilePaths, chunk, metadata.ParityShards)
		if err != nil {
			// Cleanup temp files
			for _, path := range tempFilePaths {
				if path != "" {
					os.Remove(path)
				}
			}
			return err
		}

		// Write chunk data to destination (streaming)
		if _, err := dest.Write(chunkData); err != nil {
			// Cleanup temp files
			for _, path := range tempFilePaths {
				if path != "" {
					os.Remove(path)
				}
			}
			return fmt.Errorf("failed to write chunk %d: %w", chunk.ChunkIndex, err)
		}

		// Cleanup temp files for this chunk
		for _, path := range tempFilePaths {
			if path != "" {
				os.Remove(path)
			}
		}
	}

	return nil
}

// successfulShardsCount returns the number of non-empty paths in the slice
func successfulShardsCount(paths []string) int {
	count := 0
	for _, p := range paths {
		if p != "" {
			count++
		}
	}
	return count
}

// reconstructChunk reconstructs a single chunk from downloaded shard files.
func (s *FileService) reconstructChunk(tempFilePaths []string, chunk domain.ChunkMetadata, parityShards int) ([]byte, error) {
	// Read all shard data into memory for reconstruction
	shards := make([][]byte, len(tempFilePaths))
	for i, path := range tempFilePaths {
		if path == "" {
			continue
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("failed to read shard %d: %w", i, err)
		}
		shards[i] = data
	}

	// Create metadata for this chunk
	chunkMeta := domain.ObjectMetadata{
		ParityShards: parityShards,
		OriginalSize: chunk.ChunkSize,
		ShardHashes:  chunk.Shards,
	}

	// Reconstruct
	return ReconstructFile(shards, chunkMeta)
}

// DeleteFile deletes a file from cloud storage
func (s *FileService) DeleteFile(ctx context.Context, key string) error {
	// Delete all shards using prefix from all buckets
	log.Debugf("Deleting Key %s", key)
	buckets := s.placer.ListBuckets()
	for _, bucketName := range buckets {
		repo, err := s.placer.GetRepositoryForBucket(bucketName)
		if err != nil {
			continue // Skip failed buckets
		}
		repo.DeletePrefix(ctx, key)
	}

	// Delete metadata
	prefix := filepath.Dir(key)
	fileName := filepath.Base(key)
	return s.metadataRepo.DeleteMetadata(ctx, prefix, fileName)
}

// uploadShards uploads erasure-coded shards in parallel with concurrency control.
//
// Each shard is stored under the key "<shardKeyPrefix>/<shardHash>" (where the
// hash is the CRC64 checksum recorded in the corresponding entry of
// storageSlots). On successful upload the storage location is written into the
// matching index-aligned entry of storageSlots, so storageSlots must have the
// same length as shards and carry the pre-computed shard hashes.
//
// This function implements the core shard upload strategy:
// 1. Creates goroutines for each shard upload (limited by semaphore)
// 2. Uses fail-fast logic - stops if too many uploads fail
// 3. Updates storageSlots with actual storage locations after successful uploads
func (s *FileService) uploadShards(ctx context.Context, shardKeyPrefix string, shards [][]byte, storageSlots []domain.ShardStorage, quiet bool, concurrency, parityShards int) error {
	// Setup channels for goroutine coordination
	var wg sync.WaitGroup
	errorCh := make(chan error, len(shards)) // Buffered to prevent goroutine blocking
	pathCh := make(chan struct {             // Channel for successful upload results
		index       int    // Shard index for metadata update
		storageType string // Storage backend type (e.g., "s3", "gcs")
		bucketName  string // Cloud storage bucket name
		key         string // Actual storage key where shard was stored
	}, len(shards))
	semaphore := make(chan struct{}, concurrency) // Limits concurrent uploads

	// Launch upload goroutines for each shard
	for i, shard := range shards {
		wg.Add(1)
		go func(i int, shard []byte) {
			defer wg.Done()
			semaphore <- struct{}{}        // Acquire semaphore slot
			defer func() { <-semaphore }() // Release semaphore slot

			// Generate shard key using original hash from the storage slot.
			// Format: "<shardKeyPrefix>/<shard-hash>"
			originalHash := storageSlots[i].Hash
			shardKey := fmt.Sprintf("%s/%s", shardKeyPrefix, originalHash)

			// Select bucket and repository for this shard using placement algorithm
			bucketName, repo, err := s.placer.Place(i)
			if err != nil {
				errorCh <- err
				return
			}

			// Upload shard to selected bucket
			path, err := repo.Upload(ctx, shardKey, bytes.NewReader(shard), quiet)
			if err != nil {
				errorCh <- err // Send error to main thread
				return
			}

			// Parse returned path to extract actual storage key
			// Expected format: "bucket/actual-key"
			parts := strings.SplitN(path, "/", 2)
			pathCh <- struct {
				index       int
				storageType string
				bucketName  string
				key         string
			}{
				index:       i,
				storageType: repo.GetStorageType(),
				bucketName:  bucketName,
				key:         parts[1], // Extract key part after bucket
			}
		}(i, shard)
	}

	// Wait for all uploads to complete
	wg.Wait()
	close(errorCh)
	close(pathCh)

	// Implement fail-fast error handling
	// Reed-Solomon can tolerate up to 'parityShards' failures
	// If more than parityShards fail, we cannot guarantee reconstruction
	errorCount := 0
	var uploadErr error
	for err := range errorCh {
		if err != nil {
			errorCount++
			if uploadErr == nil {
				uploadErr = err // Capture first error for reporting
			}
			// Fail fast if too many shards failed
			if errorCount > parityShards {
				return uploadErr
			}
		}
	}
	// If we had some failures but within tolerance, still return error
	if uploadErr != nil {
		return uploadErr
	}

	// Update storage slots with actual storage locations
	// This allows the download process to find shards later
	for result := range pathCh {
		storageSlots[result.index].Index = result.index
		storageSlots[result.index].StorageType = result.storageType
		storageSlots[result.index].BucketName = result.bucketName
		storageSlots[result.index].Key = result.key
	}

	return nil
}

// downloadShards downloads shards using dynamic concurrency strategy with temp files
func (s *FileService) downloadShards(ctx context.Context, shardHashes []domain.ShardStorage, parityShards int, quiet bool, verifyIntegrity bool) ([]IndexedShard, error) {
	// Dynamic Shard Downloading Strategy:
	// 1. Start with limited concurrent downloads (s.concurrency)
	// 2. When a shard completes, check if we need more shards
	// 3. If still needed, start downloading the next available shard
	// 4. Stop early once we have enough shards for reconstruction
	// This optimizes network usage and reduces unnecessary downloads

	tempFilePaths := make([]string, len(shardHashes))
	var wg sync.WaitGroup
	var mu sync.Mutex               // Protects shared state between goroutines
	successfulShards := 0           // Count of successfully downloaded shards
	nextShardIndex := s.concurrency // Index of next shard to download
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Calculate minimum shards needed for Reed-Solomon reconstruction
	// Formula: total_shards - parity_shards = minimum_data_shards_needed
	minShardsNeeded := len(shardHashes) - parityShards

	// Phase 1: Start initial batch of downloads (up to concurrency limit)
	// This prevents overwhelming the network with too many simultaneous requests
	for i := 0; i < s.concurrency && i < len(shardHashes); i++ {
		wg.Add(1)
		go s.downloadShard(ctx, &wg, &mu, tempFilePaths, shardHashes[i], i, quiet, &successfulShards, &nextShardIndex, minShardsNeeded, shardHashes, cancel, verifyIntegrity)
	}

	// Phase 2: Wait for all download goroutines to complete
	// This includes both initial downloads and any dynamically started ones
	wg.Wait()

	// Phase 3: Log final count of successful downloads
	// successfulShards is accurately maintained by downloadShard under mutex protection
	log.Debugf("%d shards downloaded successfully", successfulShards)

	// Phase 4: Ensure we have enough shards for Reed-Solomon reconstruction
	// If insufficient, return error rather than attempting reconstruction
	if successfulShards < minShardsNeeded {
		// Cleanup temp files on failure
		for _, path := range tempFilePaths {
			if path != "" {
				os.Remove(path)
			}
		}
		return nil, errors.ErrInsufficientShards
	}

	// Filter out empty paths and pair each downloaded path with its metadata shard index
	var successfulShardsList []IndexedShard
	for i, path := range tempFilePaths {
		if path != "" {
			// Retrieve explicit index from metadata record
			shardIdx := shardHashes[i].Index
			successfulShardsList = append(successfulShardsList, IndexedShard{
				Index: shardIdx,
				Path:  path,
			})
		}
	}

	return successfulShardsList, nil
}

// verifyFileIntegrity checks if the reconstructed file matches the expected CRC64 hash
func verifyFileIntegrity(data []byte, expectedHash string) error {
	table := crc64.MakeTable(crc64.ISO)
	fileHash := fmt.Sprintf("%016x", crc64.Checksum(data, table))

	if fileHash != expectedHash {
		log.Debugf("Integrity check failed: expected %s, got %s", expectedHash, fileHash)
		return errors.ErrFileIntegrityCheck
	}
	log.Debugf("File integrity check passed: %s", fileHash)
	return nil
}

// downloadShard downloads a single shard to temp file and manages dynamic concurrency
// This function implements the core logic for the dynamic downloading strategy:
// 1. Downloads the assigned shard to a temp file
// 2. Verifies shard integrity using CRC64 hash
// 3. Decides whether to start downloading additional shards
// 4. Handles early termination when enough shards are available
func (s *FileService) downloadShard(ctx context.Context, wg *sync.WaitGroup, mu *sync.Mutex, tempFilePaths []string, shardInfo domain.ShardStorage, i int, quiet bool, successfulShards *int, nextShardIndex *int, minShardsNeeded int, allShards []domain.ShardStorage, cancel context.CancelFunc, verifyIntegrity bool) {
	defer wg.Done()

	// Early termination check: stop if context was cancelled
	// This happens when we already have enough shards or an error occurred
	select {
	case <-ctx.Done():
		return
	default:
	}

	shardStart := time.Now()
	log.Debugf("[PERF] Starting shard %d download: bucket=%s, key=%s", i, shardInfo.BucketName, shardInfo.Key)

	// Step 1: Get repository for the shard's bucket
	repoStart := time.Now()
	repo, err := s.placer.GetRepositoryForBucket(shardInfo.BucketName)
	if err != nil {
		// Mark shard as failed and potentially start next download
		tempFilePaths[i] = ""
		s.maybeStartNext(wg, mu, tempFilePaths, successfulShards, nextShardIndex, minShardsNeeded, allShards, ctx, cancel, quiet, verifyIntegrity)
		return
	}
	log.Debugf("[PERF] Shard %d: Repository lookup took %v", i, time.Since(repoStart))

	// Step 2: Create temp file for this shard
	tempFileStart := time.Now()
	tempFile, err := os.CreateTemp("", fmt.Sprintf("shard_%d_*.tmp", i))
	if err != nil {
		tempFilePaths[i] = ""
		s.maybeStartNext(wg, mu, tempFilePaths, successfulShards, nextShardIndex, minShardsNeeded, allShards, ctx, cancel, quiet, verifyIntegrity)
		return
	}
	tempFilePath := tempFile.Name()
	log.Debugf("[PERF] Shard %d: Temp file creation took %v", i, time.Since(tempFileStart))

	// Step 3: Download directly to temp file using WriterAt interface
	downloadStart := time.Now()
	err = repo.Download(ctx, shardInfo.Key, tempFile, quiet)
	log.Debugf("[PERF] Shard %d: Download initiation took %v", i, time.Since(downloadStart))
	tempFile.Close()
	if err != nil {
		// Check if error is due to context cancellation (expected when we have enough shards)
		if ctx.Err() != nil {
			// Context was cancelled - this is expected, don't log as error
			os.Remove(tempFilePath)
			tempFilePaths[i] = ""
			return
		}
		// Mark shard as failed and potentially start next download
		log.Errorf("Shard %d download failed: %v", i, err)
		os.Remove(tempFilePath)
		tempFilePaths[i] = ""
		s.maybeStartNext(wg, mu, tempFilePaths, successfulShards, nextShardIndex, minShardsNeeded, allShards, ctx, cancel, quiet, verifyIntegrity)
		return
	}

	// Debug: Check file size after download
	if fileInfo, err := os.Stat(tempFilePath); err == nil {
		log.Debugf("[PERF] Shard %d: Downloaded file size: %d bytes", i, fileInfo.Size())
	} else {
		log.Errorf("Shard %d: Failed to stat temp file: %v", i, err)
	}

	// Copy temp file content for performance measurement
	copyStart := time.Now()
	shardData, err := os.ReadFile(tempFilePath)
	if err != nil {
		log.Errorf("Shard %d: Failed to read temp file: %v", i, err)
		os.Remove(tempFilePath)
		tempFilePaths[i] = ""
		s.maybeStartNext(wg, mu, tempFilePaths, successfulShards, nextShardIndex, minShardsNeeded, allShards, ctx, cancel, quiet, verifyIntegrity)
		return
	}
	log.Debugf("[PERF] Shard %d: Copied %d bytes in %v (%.2f MB/s)", i, len(shardData), time.Since(copyStart), float64(len(shardData))/1024/1024/time.Since(copyStart).Seconds())

	// Step 4: Verify shard integrity using CRC64 hash (optional)
	// This ensures downloaded data matches what was originally stored
	if verifyIntegrity {
		if err := verifyFileIntegrity(shardData, shardInfo.Hash); err != nil {
			log.Warnf("Shard %d failed integrity check", i)
			os.Remove(tempFilePath)
			tempFilePaths[i] = ""
			s.maybeStartNext(wg, mu, tempFilePaths, successfulShards, nextShardIndex, minShardsNeeded, allShards, ctx, cancel, quiet, verifyIntegrity)
			return
		}
	}

	// Step 5: Successfully downloaded shard
	// Update shared state under mutex protection
	mu.Lock()
	tempFilePaths[i] = tempFilePath
	*successfulShards++
	shardTotal := time.Since(shardStart)
	log.Debugf("[PERF] Shard %d: TOTAL time %v (%d/%d needed)", i, shardTotal, *successfulShards, minShardsNeeded)

	// Step 6: Early termination optimization
	// If we have enough shards for reconstruction, cancel remaining downloads
	// This prevents unnecessary network traffic and speeds up the process
	if *successfulShards >= minShardsNeeded {
		cancel() // Signal all other goroutines to stop
		mu.Unlock()
		return
	}
	mu.Unlock()

	// Step 7: Dynamic concurrency - start next download if needed
	// This maintains optimal network utilization by keeping downloads active
	s.maybeStartNext(wg, mu, tempFilePaths, successfulShards, nextShardIndex, minShardsNeeded, allShards, ctx, cancel, quiet, verifyIntegrity)
}

// maybeStartNext implements the dynamic concurrency control logic
// This function decides whether to start downloading the next available shard
// based on current progress and remaining needs. It's called after each
// shard completion (success or failure) to maintain optimal download flow.
func (s *FileService) maybeStartNext(wg *sync.WaitGroup, mu *sync.Mutex, tempFilePaths []string, successfulShards *int, nextShardIndex *int, minShardsNeeded int, allShards []domain.ShardStorage, ctx context.Context, cancel context.CancelFunc, quiet bool, verifyIntegrity bool) {
	mu.Lock()
	defer mu.Unlock()

	// Decision Logic: Start next download only if BOTH conditions are true:
	// 1. We still need more shards (*successfulShards < minShardsNeeded)
	// 2. There are more shards available to download (*nextShardIndex < len(allShards))
	//
	// This prevents:
	// - Starting unnecessary downloads when we have enough shards
	// - Attempting to download non-existent shards (index out of bounds)
	if *successfulShards < minShardsNeeded && *nextShardIndex < len(allShards) {
		// Atomically claim the next shard index to prevent race conditions
		currentIndex := *nextShardIndex
		*nextShardIndex++

		// Start new download goroutine for the claimed shard
		// This maintains the concurrency level as other downloads complete
		wg.Add(1)
		go s.downloadShard(ctx, wg, mu, tempFilePaths, allShards[currentIndex], currentIndex, quiet, successfulShards, nextShardIndex, minShardsNeeded, allShards, cancel, verifyIntegrity)
	}
	// If conditions not met, no new download is started, allowing
	// the system to naturally wind down as remaining downloads complete
}

// ListFiles lists all files stored under a given prefix
func (s *FileService) ListFiles(ctx context.Context, prefix string) ([]domain.ObjectMetadata, error) {
	return s.metadataRepo.ListMetadataByPrefix(ctx, prefix)
}

// SetConcurrency sets the concurrency limit for uploads
func (s *FileService) SetConcurrency(concurrency int) {
	s.concurrency = concurrency
}
