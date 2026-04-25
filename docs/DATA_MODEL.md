# Data Model

All metadata for erasure-coded files is stored in a single DynamoDB table. This document describes the schema, struct mappings, key construction, and the migration system.

## DynamoDB Table: `object_metadata`

| Attribute | Type | Role |
|-----------|------|------|
| `prefix` | String (S) | Partition Key |
| `file_name` | String (S) | Sort Key |
| `original_size` | Number (N) | |
| `shard_size` | Number (N) | |
| `parity_shards` | Number (N) | |
| `shard_hashes` | List (L) | Array of maps |

Billing mode: on-demand (pay-per-request). No GSIs.

## ObjectMetadata

Defined in `internal/domain/object_metadata.go`. Represents one stored file.

```go
type ObjectMetadata struct {
    Prefix       string         `dynamodbav:"prefix"`
    FileName     string         `dynamodbav:"file_name"`
    OriginalSize int64          `dynamodbav:"original_size"`
    ShardSize    int64          `dynamodbav:"shard_size"`
    ParityShards int            `dynamodbav:"parity_shards"`
    ShardHashes  []ShardStorage `dynamodbav:"shard_hashes"`
}
```

### Fields

- **`Prefix`** (partition key) — The directory path portion of the object key. Derived from `filepath.Dir(key)`. For `zs://mybucket/photos/vacation/img.jpg`, the prefix is `mybucket/photos/vacation`.

- **`FileName`** (sort key) — The filename portion. Derived from `filepath.Base(key)`. For the example above: `img.jpg`.

- **`OriginalSize`** — Size of the original file in bytes before sharding. Used during reconstruction: Reed-Solomon `Join()` truncates the reassembled data to this length (since shards are padded to equal size).

- **`ShardSize`** — Size of each individual shard in bytes. All shards (data + parity) are the same size after Reed-Solomon encoding.

- **`ParityShards`** — Number of parity shards used during encoding. The number of data shards is derived at reconstruction time: `dataShards = len(ShardHashes) - ParityShards`. This means the system can tolerate up to `ParityShards` lost/corrupted shards.

- **`ShardHashes`** — Ordered array of `ShardStorage` entries, one per shard (data shards first, then parity shards). The array index corresponds to the shard index used during Reed-Solomon encoding/decoding.

## ShardStorage

Defined in `internal/domain/object_metadata.go`. Tracks where a single shard is stored.

```go
type ShardStorage struct {
    Hash        string `dynamodbav:"hash"`
    StorageType string `dynamodbav:"storage_type"`
    BucketName  string `dynamodbav:"bucket_name"`
    Key         string `dynamodbav:"key"`
}
```

### Fields

- **`Hash`** — CRC64/ISO checksum of the shard data, formatted as a 16-character lowercase hex string (e.g., `00a1b2c3d4e5f678`). Computed during upload using `crc64.MakeTable(crc64.ISO)`. Serves two purposes:
  1. Used as the shard's object key suffix in cloud storage
  2. Used for optional integrity verification during download (`--verify-integrity` flag)

- **`StorageType`** — The storage provider: `"s3"` or `"gcs"`. Set after successful upload based on which repository handled the shard.

- **`BucketName`** — The config key name of the bucket this shard was stored in (e.g., `"primary"`, `"secondary"`). This maps to the bucket registered in the placement system, not the actual cloud bucket name.

- **`Key`** — The actual object key in cloud storage where the shard lives. Format: `<original-key>/<crc64-hash>` with the bucket prefix stripped. For example, if the file key is `photos/img.jpg` and the shard hash is `00a1b2c3d4e5f678`, the key stored here is `photos/img.jpg/00a1b2c3d4e5f678`.

## Key Construction

Given a CLI command like:

```bash
./zstore upload photo.jpg zs://data/photos/photo.jpg
```

The following keys are derived:

| Value | Source | Example |
|-------|--------|---------|
| Full key | Parsed from `zs://` URL | `data/photos/photo.jpg` |
| Prefix (PK) | `filepath.Dir(key)` | `data/photos` |
| FileName (SK) | `filepath.Base(key)` | `photo.jpg` |
| Shard object key | `<key>/<crc64-hash>` | `data/photos/photo.jpg/00a1b2c3d4e5f678` |

## Example DynamoDB Item

For a file uploaded with 4 data shards and 2 parity shards across 2 buckets:

```json
{
  "prefix": {"S": "data/photos"},
  "file_name": {"S": "photo.jpg"},
  "original_size": {"N": "1048576"},
  "shard_size": {"N": "262144"},
  "parity_shards": {"N": "2"},
  "shard_hashes": {"L": [
    {"M": {
      "hash": {"S": "00a1b2c3d4e5f678"},
      "storage_type": {"S": "s3"},
      "bucket_name": {"S": "primary"},
      "key": {"S": "data/photos/photo.jpg/00a1b2c3d4e5f678"}
    }},
    {"M": {
      "hash": {"S": "11b2c3d4e5f67890"},
      "storage_type": {"S": "gcs"},
      "bucket_name": {"S": "secondary"},
      "key": {"S": "data/photos/photo.jpg/11b2c3d4e5f67890"}
    }}
  ]}
}
```

(6 entries total in `shard_hashes` — shown truncated)

## Access Patterns

| Operation | DynamoDB API | Key |
|-----------|-------------|-----|
| Get file metadata | `GetItem` | PK=prefix, SK=file_name |
| List files in directory | `Query` | PK=prefix (returns all files) |
| Store metadata | `PutItem` | Full item |
| Update metadata | `PutItem` (full replacement) | Full item |
| Delete metadata | `DeleteItem` | PK=prefix, SK=file_name |

## Migration System

Migrations are tracked using AWS Resource Groups Tagging API tags on DynamoDB table resources, not a separate migrations table.

### How it works

1. Each migration has a version string (format: `YYYYMMDDHHMMSS_description`) and a table name
2. On `zstore init`, the system checks for a `Migration` tag on the table with the version value
3. If the tag exists, the migration is skipped
4. If not, the migration's `Up()` is executed, then the table is tagged with `Migration=<version>` and `MigratedAt=<timestamp>`
5. On `zstore down`, migrations run in reverse order, and the tags are removed

### Adding a new migration

1. Create a new file in `internal/repository/migrate/` (e.g., `0002_add_index.go`)
2. Implement the `Migration` interface:
   - `Up(ctx, *dynamodb.Client) error`
   - `Down(ctx, *dynamodb.Client) error`
   - `Version() string` — e.g., `"20260101000000_add_index"`
   - `TableName() string`
3. Register it in the `migrations` slice in `internal/repository/db/migrate.go`

Migrations run in the order they appear in the `migrations` slice.
