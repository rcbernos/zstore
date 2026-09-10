// Package service provides chunked file operations for memory-efficient processing
// of large files through the chunking layer.
package service

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/zzenonn/zstore/internal/domain"
)

// ChunkedUploadFile uploads a file with chunked support for memory-efficient processing.
// It splits the file into chunks using the specified chunking strategy and independently
// erasure-codes and uploads each chunk.
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
// <prefix>/<filename>/chunk_<N>/<shardHash> and per-chunk metadata is recorded
// on ObjectMetadata.Chunks.
func (s *FileService) ChunkedUploadFile(ctx context.Context, key string, r io.Reader, quiet bool, dataShards, parityShards, concurrency int, chunkSize int64, chunkMethod string) (domain.ObjectMetadata, error) {
	start := time.Now()

	if chunkMethod == "" {
		chunkMethod = ChunkMethodNone
	}

	prefix := filepath.Dir(key)

	log.Debugf("ChunkedUpload: %s (chunkMethod=%s)", key, chunkMethod)

	// Delete prefix contents from all buckets
	deleteStart := time.Now()
	buckets := s.placer.ListBuckets()
	for _, bucketName := range buckets {
		if repo, err := s.placer.GetRepositoryForBucket(bucketName); err == nil {
			repo.DeletePrefix(ctx, key)
		}
	}
	log.Debugf("Delete prefix took: %v", time.Since(deleteStart))

	var metadata domain.ObjectMetadata
	switch chunkMethod {
	case ChunkMethodNone:
		m, uploadErr := s.uploadFileLegacy(ctx, key, r, quiet, dataShards, parityShards, concurrency)
		if uploadErr != nil {
			return domain.ObjectMetadata{}, uploadErr
		}
		m.Prefix = prefix
		m.FileName = filepath.Base(key)
		m.ChunkMethod = ChunkMethodNone
		metadata = m
	default:
		m, uploadErr := s.uploadFileChunked(ctx, key, r, quiet, dataShards, parityShards, concurrency, chunkSize, chunkMethod)
		if uploadErr != nil {
			return domain.ObjectMetadata{}, uploadErr
		}
		m.Prefix = prefix
		m.FileName = filepath.Base(key)
		metadata = m
	}

	metadataStart := time.Now()
	_, err := s.metadataRepo.CreateMetadata(ctx, metadata)
	log.Debugf("Metadata storage took: %v", time.Since(metadataStart))
	log.Debugf("Total chunked upload took: %v", time.Since(start))
	return metadata, err
}

// ChunkedDownloadFile downloads a file that was uploaded with chunked support.
// It reconstructs the file by downloading and reassembling all chunks.
func (s *FileService) ChunkedDownloadFile(ctx context.Context, key string, dest io.Writer, quiet bool, verifyIntegrity bool) error {
	prefix := filepath.Dir(key)
	fileName := filepath.Base(key)

	metadata, err := s.metadataRepo.GetMetadata(ctx, prefix, fileName)
	if err != nil {
		return err
	}

	log.Debugf("Object Metadata: %+v\n", metadata)

	if metadata.ChunkMethod != ChunkMethodNone && len(metadata.Chunks) > 0 {
		return s.downloadFileChunked(ctx, key, dest, metadata, quiet, verifyIntegrity)
	}

	// Legacy path for non-chunked files
	indexedShards, err := s.downloadShards(ctx, metadata.ShardHashes, metadata.ParityShards, quiet, verifyIntegrity)
	if err != nil {
		return err
	}

	defer func() {
		for _, shard := range indexedShards {
			os.Remove(shard.Path)
		}
	}()

	reconstructedData, err := ReconstructFileFromPaths(indexedShards, metadata)
	if err != nil {
		return err
	}

	_, err = dest.Write(reconstructedData)
	return err
}
