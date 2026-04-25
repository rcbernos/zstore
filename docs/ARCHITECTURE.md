# Architecture

Zstore is a multi-provider erasure coding object storage system. It splits files into Reed-Solomon encoded shards, distributes them across multiple cloud storage backends (S3, GCS), and stores reconstruction metadata in DynamoDB. This provides fault tolerance (survive provider outages), load distribution, and cost optimization across providers.

## High-Level Diagram

```
                         ┌─────────────┐
                         │   CLI (cmd/) │
                         │  Cobra cmds  │
                         └──────┬───────┘
                                │
                    ┌───────────┴───────────┐
                    │                       │
             ┌──────┴──────┐        ┌───────┴───────┐
             │ FileService │        │RawFileService │
             │ (erasure-   │        │ (direct ops)  │
             │  coded ops) │        └───────┬───────┘
             └──┬───┬──────┘                │
                │   │                       │
    ┌───────────┘   └──────────┐    ┌───────┴────────┐
    │                          │    │  ObjectRepo    │
┌───┴──────────┐    ┌──────────┴──┐ │  Factory       │
│   Placer     │    │ Metadata    │ └───────┬────────┘
│ (round-robin │    │ Repository  │         │
│  placement)  │    │ (DynamoDB)  │         │
└───┬──────────┘    └──────┬──────┘         │
    │                      │                │
    │    ┌─────────────────┼────────────────┘
    │    │                 │
┌───┴────┴───┐      ┌─────┴──────┐
│ ObjectRepo │      │  DynamoDB  │
│ S3 / GCS   │      │            │
└────────────┘      └────────────┘
```

## Package Structure

### `cmd/`

CLI entrypoint. Contains `main.go` (Cobra root command, flag definitions, initialization wiring) and `file.go` (all file operation subcommands: upload, download, delete, list, and their raw variants). The `initConfig()` function in `main.go` is where all dependencies are wired together: config loading, client creation, repository registration, and service construction.

### `internal/config/`

Configuration loading via Viper. Reads from `config.yaml`, environment variables, and CLI flags with priority: CLI flags > env vars > config file > defaults. Manages AWS SDK config, GCS client initialization, DynamoDB region resolution, and bucket configuration parsing.

### `internal/domain/`

Core data model. Defines `ObjectMetadata` (the DynamoDB record representing an erasure-coded file) and `ShardStorage` (per-shard storage location info). These structs are the central data types passed between all layers. See [DATA_MODEL.md](DATA_MODEL.md) for field-by-field documentation.

### `internal/errors/`

Sentinel error definitions: `ErrInsufficientShards`, `ErrEmptyFile`, `ErrFileIntegrityCheck`, `ErrAWSRegionNotConfigured`, etc.

### `internal/integration/`

External service integrations. Currently contains AWS SSM Parameter Store client for fetching secrets.

### `internal/logging/`

Logrus logger initialization based on config log level.

### `internal/placement/`

Shard placement strategy. Defines the `Placer` interface (methods: `Place`, `RegisterBucket`, `GetRepositoryForBucket`, `ListBuckets`) and the `RoundRobinPlacer` implementation. The placer sits between the service layer and storage layer, abstracting which bucket a shard goes to. Round-robin distributes shards evenly: shard `i` goes to bucket `i % numBuckets`.

### `internal/repository/db/`

DynamoDB infrastructure. `db.go` creates the DynamoDB and Resource Groups Tagging API clients. `metadata_repository.go` implements CRUD operations for `ObjectMetadata` records (PutItem, GetItem, Query by prefix, DeleteItem). `migrate.go` runs the migration system using AWS Resource Groups Tagging API to track which migrations have been applied (tags on DynamoDB table resources).

### `internal/repository/migrate/`

Migration definitions. Each migration implements the `Migration` interface (`Up`, `Down`, `Version`, `TableName`). Currently contains `0001_create_object_metadata.go` which creates the `object_metadata` DynamoDB table.

### `internal/repository/objectstore/`

Storage backend implementations. Defines the `ObjectRepository` interface (`Upload`, `Download`, `Delete`, `DeletePrefix`, `GetBucketName`, `GetStorageType`). Contains `S3ObjectRepository` (uses AWS S3 Manager for multipart uploads/downloads) and `GCSObjectRepository` (uses Google Cloud Storage client). Also contains `ObjectRepositoryFactory` which creates the appropriate repository based on provider type, with S3 client caching by region.

### `internal/service/`

Business logic layer. Three files:

- **`file_service.go`** — `FileService` orchestrates erasure-coded operations. Handles upload (shard + distribute + store metadata), download (fetch metadata + download shards with dynamic concurrency + reconstruct), delete (remove shards from all buckets + delete metadata), and list.
- **`erasure_coding_service.go`** — Reed-Solomon encoding/decoding functions. `ShardFile` splits data into shards and computes CRC64 hashes. `ReconstructFile` and `ReconstructFileFromPaths` rebuild original data from available shards.
- **`raw_file_service.go`** — `RawFileService` provides direct upload/download/delete to a specific bucket without erasure coding or metadata, using the `ObjectRepositoryFactory`.

## Key Interfaces

### `ObjectRepository` (`internal/repository/objectstore/`)

Abstracts a single storage bucket. Implementations exist for S3 and GCS. Methods:

- `Upload(ctx, key, reader, quiet) (path, error)`
- `Download(ctx, key, dest WriterAt, quiet) error`
- `Delete(ctx, key) error`
- `DeletePrefix(ctx, prefix) error`
- `GetBucketName() string`
- `GetStorageType() string`

The `Download` method takes an `io.WriterAt` (not `io.Writer`) because the S3 download manager writes chunks at arbitrary offsets in parallel. This is critical for performance.

### `Placer` (`internal/placement/`)

Abstracts shard-to-bucket mapping. The `Place(shardIndex)` method returns which bucket and repository to use. `GetRepositoryForBucket(name)` is used during downloads when the bucket is already known from metadata.

### `MetadataRepository` (defined in `internal/service/file_service.go`)

Interface for metadata CRUD. Implemented by `db.MetadataRepository`. Methods: `CreateMetadata`, `GetMetadata`, `ListMetadataByPrefix`, `UpdateMetadata`, `DeleteMetadata`.

## Initialization Flow

`cmd/main.go` → `initConfig()`:

1. `config.LoadConfig()` — reads config file, env vars, CLI flags via Viper
2. `loadAWSConfig()` — creates AWS SDK config with DynamoDB region
3. `loadGCSClient()` — creates GCS client
4. `db.NewDatabase()` — creates DynamoDB + Tagging API clients
5. `initRepositories()` — creates `ObjectRepositoryFactory`, creates `RoundRobinPlacer`, iterates configured buckets and registers each with the placer via the factory
6. `db.NewMetadataRepository()` — creates metadata repository with DynamoDB client
7. `service.NewFileService(placer, metadataRepo)` — creates the erasure-coded file service
8. `service.NewRawFileService(factory)` — creates the raw file service
