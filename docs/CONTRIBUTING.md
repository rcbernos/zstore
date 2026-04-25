# Contributing

## Prerequisites

- **Go 1.24+** — see [INSTALL_GO.md](../INSTALL_GO.md) for Amazon Linux instructions
- **[Task](https://taskfile.dev/)** (go-task) — task runner used for build/test/lint
- **[golangci-lint](https://golangci-lint.run/)** — used by `task lint`
- **AWS account** — required for DynamoDB (metadata) and S3 buckets
- **GCP project** — optional, only needed if using GCS buckets

## Clone and Build

```bash
git clone https://github.com/zzenonn/zstore.git
cd zstore
go-task build    # Builds to bin/zstore (CGO_ENABLED=0, linux target)
```

Available tasks (defined in `Taskfile.yaml`):

| Task | What it does |
|------|-------------|
| `go-task build` | Compiles to `bin/zstore` (static binary, linux) |
| `go-task test` | Runs all tests (`go test -v ./...`) |
| `go-task lint` | Runs `golangci-lint run` |
| `go-task run` | Runs the built binary |

All tasks source `.env` before executing. You must create this file (see below).

## Environment Setup

### 1. Create `.env` file

Create a `.env` file in the project root:

```bash
export AWS_ACCESS_KEY_ID=your_access_key
export AWS_SECRET_ACCESS_KEY=your_secret_key
export AWS_REGION=us-east-1
export ZSTORE_CONFIG_PATH=./config.yaml

# Only if using GCS buckets:
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account.json
```

This file is sourced by `Taskfile.yaml` before every command. It is gitignored.

### 2. Create `config.yaml`

```yaml
log_level: debug
dynamodb_table: object_metadata
dynamodb_region: us-east-1

buckets:
  primary:
    bucket_name: your-s3-bucket
    platform: s3
    region: us-east-1
  # Optional: add a GCS bucket
  secondary:
    bucket_name: your-gcs-bucket
    platform: gcs
```

You need at least one bucket configured. The bucket must already exist in your cloud account.

### 3. Initialize DynamoDB

```bash
./bin/zstore init
```

This creates the `object_metadata` table. See [IAM_PERMISSIONS.md](IAM_PERMISSIONS.md) for required permissions.

## Running Tests

Tests are **integration tests** — they hit real AWS/GCS services. There are no mocks or unit tests currently.

```bash
# Run integration tests
go test ./tests/service/

# Run with verbose output
go test -v ./tests/service/

# Run benchmarks
go test -bench=. -run='^$' ./tests/service/

# Run specific benchmark
go test -bench=BenchmarkFileService_ErasureCoded_UploadFile -run='^$' ./tests/service/
```

Tests require:
- `ZSTORE_CONFIG_PATH` pointing to a config with real, accessible buckets
- AWS credentials with appropriate permissions
- The DynamoDB table must exist (`zstore init`)

Test files are in `tests/service/`:
- `file_service_integration_test.go` — upload/download/delete cycle with SHA256 verification across file sizes (1KB, 100KB, 1MB)
- `file_service_benchmark_test.go` — throughput benchmarks for erasure-coded and raw operations

## Project Structure

See [ARCHITECTURE.md](ARCHITECTURE.md) for detailed package descriptions. Key directories:

```
cmd/                              # CLI commands and wiring
internal/
  config/                         # Configuration loading
  domain/                         # Data model (ObjectMetadata, ShardStorage)
  errors/                         # Sentinel errors
  integration/                    # AWS SSM integration
  logging/                        # Logger setup
  placement/                      # Shard placement (Placer interface, RoundRobinPlacer)
  repository/
    db/                           # DynamoDB client, migrations, MetadataRepository
    migrate/                      # Migration definitions
    objectstore/                  # ObjectRepository interface, S3/GCS implementations, factory
  service/                        # FileService, RawFileService, erasure coding
tests/service/                    # Integration tests and benchmarks
docs/                             # Documentation
```

## Adding a New Storage Provider

To add support for a new cloud storage backend (e.g., Azure Blob Storage):

### 1. Implement `ObjectRepository`

Create a new file in `internal/repository/objectstore/` (e.g., `azure_object_repository.go`). Implement the interface:

```go
type ObjectRepository interface {
    Upload(ctx context.Context, key string, r io.Reader, quiet bool) (string, error)
    Download(ctx context.Context, key string, dest io.WriterAt, quiet bool) error
    Delete(ctx context.Context, key string) error
    DeletePrefix(ctx context.Context, prefix string) error
    GetBucketName() string
    GetStorageType() string
}
```

**Important:** `Download` takes `io.WriterAt`, not `io.Writer`. The S3 download manager writes chunks at parallel offsets for performance. Your implementation must support this interface. If the provider's SDK doesn't support parallel writes natively, read the full object and write at offset 0 (see the GCS implementation for this approach).

`Upload` must return `"<bucketName>/<key>"` — the upload path parsing in `file_service.go:uploadShards()` does `strings.SplitN(path, "/", 2)` to extract the key.

### 2. Add a `RepositoryType` constant

In `internal/repository/objectstore/object_store_factory.go`:

```go
const (
    S3Type    RepositoryType = "s3"
    GCSType   RepositoryType = "gcs"
    AzureType RepositoryType = "azure"  // Add this
)
```

### 3. Add a case to the factory

In `ObjectRepositoryFactory.CreateRepository()`, add a new case:

```go
case AzureType:
    repo := NewAzureObjectRepository(f.azureClient, config.Name)
    return &repo, nil
```

### 4. Update config parsing

In `internal/config/config.go`, users can now set `platform: azure` in their bucket config. No code changes needed — the platform string is passed through as-is to the factory. You may want to add client initialization in `LoadConfig()` similar to how `loadGCSClient()` works.

### 5. Add CLI support for raw operations

In `cmd/file.go`, the raw commands parse URL schemes. Add handling for your scheme (e.g., `az://`).

## Adding a New Migration

1. Create a new file in `internal/repository/migrate/` following the naming convention:

```go
// internal/repository/migrate/0002_add_some_index.go
package migrate

const (
    SomeIndexVersion = "20260424000000_add_some_index"
)

type AddSomeIndex struct{}

func (m *AddSomeIndex) Version() string   { return SomeIndexVersion }
func (m *AddSomeIndex) TableName() string { return ObjectMetadataTableName }

func (m *AddSomeIndex) Up(ctx context.Context, client *dynamodb.Client) error {
    // Create index, modify table, etc.
    return nil
}

func (m *AddSomeIndex) Down(ctx context.Context, client *dynamodb.Client) error {
    // Reverse the Up operation
    return nil
}
```

2. Register it in `internal/repository/db/migrate.go`:

```go
var migrations = []Migration{
    &migrate.CreateObjectMetadataTable{},
    &migrate.AddSomeIndex{},  // Add here — order matters
}
```

Migrations run in slice order. Rollbacks run in reverse order.

## Code Conventions

- Run `task lint` before submitting. The project uses `golangci-lint`.
- Follow existing patterns in the codebase for error wrapping (`fmt.Errorf("context: %w", err)`).
- Package-level comments exist on all major files — maintain this convention.
- Interfaces are defined where they're consumed (e.g., `MetadataRepository` is defined in `internal/service/file_service.go`, not in the `db` package).
