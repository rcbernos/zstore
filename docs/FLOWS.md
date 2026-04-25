# Operation Flows

This document walks through what happens during each zstore operation, referencing the source files involved.

## Upload (Erasure-Coded)

**Entry:** `cmd/file.go` → `uploadCmd` → `FileService.UploadFile()`

```
file.txt ──Read──▶ []byte ──ShardFile──▶ [shard0, shard1, ..., shardN+M]
                                              │
                                    uploadShards (parallel)
                                              │
                          ┌───────────────────┼───────────────────┐
                          ▼                   ▼                   ▼
                     Bucket A            Bucket B            Bucket A
                    (shard 0)           (shard 1)           (shard 2) ...
                                              │
                                    Store metadata ──▶ DynamoDB
```

**Steps** (`internal/service/file_service.go`):

1. **Read file** — `io.ReadAll(reader)` loads the entire file into memory. Empty files are rejected (`ErrEmptyFile`).

2. **Shard** — `ShardFile(data, dataShards, parityShards)` (`erasure_coding_service.go`):
   - Creates a Reed-Solomon encoder with the specified data/parity ratio
   - `enc.Split(data)` splits data into `dataShards` equal-sized pieces (padded if needed)
   - `enc.Encode(shards)` generates `parityShards` additional shards
   - Computes CRC64/ISO hash for each shard
   - Returns `ObjectMetadata` (with hashes but empty storage locations) and the shard byte arrays

3. **Set metadata keys** — `Prefix = filepath.Dir(key)`, `FileName = filepath.Base(key)`

4. **Cleanup** — Deletes any existing objects with the same key prefix from all registered buckets. This handles re-uploads/overwrites.

5. **Upload shards in parallel** — `uploadShards()`:
   - Creates a semaphore channel of size `concurrency` to limit parallel uploads
   - For each shard, launches a goroutine that:
     - Acquires a semaphore slot
     - Calls `placer.Place(shardIndex)` to get the target bucket (round-robin: `shardIndex % numBuckets`)
     - Constructs shard key: `<original-key>/<crc64-hash>`
     - Calls `repo.Upload()` on the selected repository
     - Sends result (storage type, bucket name, key) to a channel
   - Waits for all goroutines to complete
   - **Fail-fast**: if more than `parityShards` uploads fail, returns an error immediately (since reconstruction would be impossible)
   - Updates `metadata.ShardHashes[i]` with actual storage locations from successful uploads

6. **Store metadata** — `metadataRepo.CreateMetadata()` writes the `ObjectMetadata` to DynamoDB via `PutItem`

## Download (Erasure-Coded)

**Entry:** `cmd/file.go` → `downloadCmd` → `FileService.DownloadFile()`

```
DynamoDB ──GetMetadata──▶ ObjectMetadata
                               │
                     downloadShards (dynamic concurrency)
                               │
              ┌────────────────┼────────────────┐
              ▼                ▼                ▼
         temp_shard_0     temp_shard_1    temp_shard_2
              │                │                │
              └────────────────┼────────────────┘
                               │
                   ReconstructFileFromPaths
                               │
                               ▼
                         output file
```

**Steps** (`internal/service/file_service.go`):

1. **Fetch metadata** — `metadataRepo.GetMetadata(prefix, fileName)` retrieves the `ObjectMetadata` from DynamoDB.

2. **Calculate minimum shards needed** — `minShardsNeeded = len(shardHashes) - parityShards`. With defaults (4 data + 2 parity = 6 total), you need at least 4 shards.

3. **Download shards with dynamic concurrency** — `downloadShards()`:
   - Starts an initial batch of downloads (up to `concurrency` limit)
   - Each `downloadShard()` goroutine:
     - Looks up the repository for the shard's bucket via `placer.GetRepositoryForBucket()`
     - Creates a temp file (`os.CreateTemp`)
     - Downloads shard to temp file via `repo.Download()` (uses `WriterAt` interface)
     - Reads temp file back to verify size
     - Optionally verifies CRC64 integrity against `shardInfo.Hash`
     - On success: increments `successfulShards` counter under mutex
     - **Early termination**: if `successfulShards >= minShardsNeeded`, cancels context (stops all remaining downloads)
     - On completion (success or failure): calls `maybeStartNext()` to potentially start downloading the next shard
   - `maybeStartNext()` — under mutex, checks if more shards are needed AND more are available. If both true, launches a new goroutine for the next shard index. This maintains the concurrency level as downloads complete.

4. **Reconstruct** — `ReconstructFileFromPaths(tempFilePaths, metadata)` (`erasure_coding_service.go`):
   - Reads each temp file into a byte slice
   - Creates a sparse array of shards (nil entries for missing shards)
   - `enc.Reconstruct()` fills in missing shards using Reed-Solomon math
   - `enc.Join()` concatenates data shards and truncates to `OriginalSize`

5. **Write output** — `dest.WriteAt(reconstructedData, 0)` writes to the output file.

6. **Cleanup** — Deferred removal of all temp files.

## Delete (Erasure-Coded)

**Entry:** `cmd/file.go` → `deleteCmd` → `FileService.DeleteFile()`

1. **Delete shards** — Iterates all registered buckets via `placer.ListBuckets()`. For each bucket, calls `repo.DeletePrefix(key)` which lists and deletes all objects with that prefix. Errors on individual buckets are skipped (best-effort).

2. **Delete metadata** — `metadataRepo.DeleteMetadata(prefix, fileName)` removes the DynamoDB item.

## List

**Entry:** `cmd/file.go` → `listCmd` → `FileService.ListFiles()`

1. Calls `metadataRepo.ListMetadataByPrefix(prefix)` which runs a DynamoDB `Query` on the partition key.
2. Returns all `ObjectMetadata` entries matching the prefix.

## Raw Operations

Raw operations bypass erasure coding and metadata entirely. They work directly with a single cloud storage bucket.

**Entry:** `cmd/file.go` → `uploadRawCmd` / `downloadRawCmd` / `deleteRawCmd` → `RawFileService`

### Upload Raw

1. Parse URL scheme (`s3://` or `gs://`) to determine provider type and extract bucket + key
2. `RawFileService.UploadToRepository()` creates a repository via the factory (with region for S3)
3. Calls `repo.Upload()` directly

### Download Raw

1. Parse URL, create repository via factory
2. `repo.Download()` writes directly to the output file via `WriterAt`

### Delete Raw

1. Parse URL, create repository via factory
2. `repo.Delete()` removes the single object

## Concurrency Model

### Upload concurrency

Uses a **semaphore pattern** (buffered channel of size `concurrency`). All shard goroutines are launched immediately but block on acquiring a semaphore slot. A `sync.WaitGroup` coordinates completion.

### Download concurrency

Uses a **dynamic concurrency** model. Instead of launching all downloads at once:

1. Start `concurrency` initial downloads
2. As each completes, `maybeStartNext()` decides whether to start another
3. Shared state (`successfulShards`, `nextShardIndex`) is protected by `sync.Mutex`
4. `context.WithCancel` enables early termination when enough shards are available

This avoids downloading unnecessary shards — with 6 total shards and 4 needed, if the first 4 downloads succeed, the remaining 2 are cancelled immediately.
