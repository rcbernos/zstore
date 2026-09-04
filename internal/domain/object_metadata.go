package domain

import "errors"

// ShardStorage - storage information for a single shard
// Contains the index, hash, and storage location details for a shard.
type ShardStorage struct {
	// Index is the zero-based position of this shard in the erasure-coded set
	Index int `json:"index" dynamodbav:"index"`

	// Hash is the CRC64 checksum of the shard data, used for integrity verification
	Hash string `json:"hash" dynamodbav:"hash"`

	// StorageType indicates the storage backend (e.g., "s3", "gcs")
	StorageType string `json:"storage_type" dynamodbav:"storage_type"`

	// BucketName is the logical bucket name (not the physical cloud bucket name)
	BucketName string `json:"bucket_name" dynamodbav:"bucket_name"`

	// Key is the storage key within the bucket where the shard is stored
	Key string `json:"key" dynamodbav:"key"`
}

// ObjectMetadata - representation of an erasure coded object's metadata
// Contains all information needed to reconstruct a file from its stored shards.
type ObjectMetadata struct {
	// Legacy fields (for backward compatibility with non-chunked files)
	// Prefix is the directory path, used as the DynamoDB partition key
	Prefix string `json:"prefix" dynamodbav:"prefix"`

	// FileName is the filename, used as the DynamoDB sort key
	FileName string `json:"file_name" dynamodbav:"file_name"`

	// OriginalSize is the size of the original file in bytes (before chunking)
	OriginalSize int64 `json:"original_size" dynamodbav:"original_size"`

	// ShardSize is the size of each shard in bytes
	ShardSize int64 `json:"shard_size" dynamodbav:"shard_size"`

	// ParityShards is the number of parity shards in the erasure coding
	ParityShards int `json:"parity_shards" dynamodbav:"parity_shards"`

	// ShardHashes is the ordered array of shard storage info for legacy non-chunked files
	ShardHashes []ShardStorage `json:"shard_hashes" dynamodbav:"shard_hashes"`

	// New chunked fields (added in Phase 1 of chunking implementation)

	// ChunkSize is the target size for each chunk in bytes
	ChunkSize int64 `json:"chunk_size" dynamodbav:"chunk_size"`

	// TotalChunks is the total number of chunks the file was split into
	TotalChunks int `json:"total_chunks" dynamodbav:"total_chunks"`

	// Chunks contains metadata for each chunk's shards
	Chunks []ChunkMetadata `json:"chunks" dynamodbav:"chunks"`

	// ChunkMethod indicates the chunking strategy used ("none", "fixed", "equal-split")
	ChunkMethod string `json:"chunk_method" dynamodbav:"chunk_method"`
}

// ChunkMetadata stores shard locations for a single chunk
type ChunkMetadata struct {
	// ChunkIndex is the zero-based position of this chunk in the file
	ChunkIndex int `json:"chunk_index" dynamodbav:"chunk_index"`

	// ChunkSize is the size of this chunk in bytes
	ChunkSize int64 `json:"chunk_size" dynamodbav:"chunk_size"`

	// Shards contains the storage info for each shard in this chunk
	Shards []ShardStorage `json:"shards" dynamodbav:"shards"`
}

// ErrNoShardsAvailable is returned when no shards are available in metadata
var ErrNoShardsAvailable = errors.New("no shards available in metadata")
