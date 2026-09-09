package service

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"

	"github.com/zzenonn/zstore/internal/domain"
	"github.com/zzenonn/zstore/internal/placement"
)

// --- In-memory fakes for testing upload logic without cloud credentials ---

// memRepo is an in-memory ObjectRepository used to validate shard placement.
type memRepo struct {
	mu          sync.Mutex
	bucketName  string
	storageType string
	objects     map[string][]byte
}

func newMemRepo(bucketName, storageType string) *memRepo {
	return &memRepo{
		bucketName:  bucketName,
		storageType: storageType,
		objects:     make(map[string][]byte),
	}
}

func (r *memRepo) Upload(_ context.Context, key string, reader io.Reader, _ bool) (string, error) {
	data, err := io.ReadAll(reader)
	if err != nil {
		return "", err
	}
	r.mu.Lock()
	r.objects[key] = data
	r.mu.Unlock()
	return r.bucketName + "/" + key, nil
}

func (r *memRepo) Download(_ context.Context, key string, dest io.WriterAt, _ bool) error {
	r.mu.Lock()
	data := cloneBytes(r.objects[key])
	r.mu.Unlock()
	if data == nil {
		return fmt.Errorf("object not found: %s", key)
	}
	_, err := dest.WriteAt(data, 0)
	return err
}

func (r *memRepo) Delete(_ context.Context, key string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.objects, key)
	return nil
}

func (r *memRepo) DeletePrefix(_ context.Context, prefix string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	for k := range r.objects {
		if strings.HasPrefix(k, prefix) {
			delete(r.objects, k)
		}
	}
	return nil
}

func (r *memRepo) GetBucketName() string  { return r.bucketName }
func (r *memRepo) GetStorageType() string { return r.storageType }

func cloneBytes(b []byte) []byte {
	if b == nil {
		return nil
	}
	c := make([]byte, len(b))
	copy(c, b)
	return c
}

// memMetadataRepo is an in-memory MetadataRepository for testing.
type memMetadataRepo struct {
	mu      sync.Mutex
	records map[string]domain.ObjectMetadata // key: prefix + "/" + fileName
}

func newMemMetadataRepo() *memMetadataRepo {
	return &memMetadataRepo{records: make(map[string]domain.ObjectMetadata)}
}

func metaKey(prefix, fileName string) string { return prefix + "/" + fileName }

func (r *memMetadataRepo) CreateMetadata(_ context.Context, metadata domain.ObjectMetadata) (domain.ObjectMetadata, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.records[metaKey(metadata.Prefix, metadata.FileName)] = metadata
	return metadata, nil
}

func (r *memMetadataRepo) GetMetadata(_ context.Context, prefix, fileName string) (domain.ObjectMetadata, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	m, ok := r.records[metaKey(prefix, fileName)]
	if !ok {
		return domain.ObjectMetadata{}, fmt.Errorf("metadata not found: %s/%s", prefix, fileName)
	}
	return m, nil
}

func (r *memMetadataRepo) ListMetadataByPrefix(_ context.Context, prefix string) ([]domain.ObjectMetadata, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []domain.ObjectMetadata
	for k, m := range r.records {
		if strings.HasPrefix(k, prefix) {
			out = append(out, m)
		}
	}
	return out, nil
}

func (r *memMetadataRepo) UpdateMetadata(_ context.Context, metadata domain.ObjectMetadata) (domain.ObjectMetadata, error) {
	return r.CreateMetadata(context.Background(), metadata)
}

func (r *memMetadataRepo) DeleteMetadata(_ context.Context, prefix, fileName string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.records, metaKey(prefix, fileName))
	return nil
}

// bufferWriterAt is an io.WriterAt backed by a []byte. memRepo.Download writes
// the whole object at offset 0, so only a single zero-offset write needs support.
type bufferWriterAt struct {
	data []byte
}

func (w *bufferWriterAt) WriteAt(p []byte, off int64) (int, error) {
	if off != 0 {
		return 0, fmt.Errorf("unexpected offset %d", off)
	}
	w.data = append(w.data[:0], p...)
	return len(p), nil
}

func makeTestData(size int) []byte {
	data := make([]byte, size)
	for i := range data {
		data[i] = byte(i % 251) // 251 is prime, keeps the pattern interesting
	}
	return data
}

// newTestFileService builds a FileService backed by two in-memory repos and a
// real round-robin placer, returning the service and the placer (needed to drive
// downloads during reconstruction).
func newTestFileService(t *testing.T) (*FileService, placement.Placer) {
	t.Helper()
	repoA := newMemRepo("bucket-a", "s3")
	repoB := newMemRepo("bucket-b", "gcs")
	placer := placement.NewRoundRobinPlacer()
	if err := placer.RegisterBucket("primary", repoA); err != nil {
		t.Fatalf("register bucket a: %v", err)
	}
	if err := placer.RegisterBucket("secondary", repoB); err != nil {
		t.Fatalf("register bucket b: %v", err)
	}
	repo := newMemMetadataRepo()
	return NewFileService(placer, repo), placer
}

// reconstructObject rebuilds the original file bytes from the object metadata by
// downloading every stored shard and reconstructing them with Reed-Solomon. It
// works for both legacy (ShardHashes) and chunked (Chunks) objects, exercising
// the metadata produced by UploadFile / UploadFileChunked.
func reconstructObject(t *testing.T, ctx context.Context, meta domain.ObjectMetadata, placer placement.Placer, parityShards int) []byte {
	t.Helper()
	if len(meta.Chunks) > 0 {
		var out []byte
		for _, chunk := range meta.Chunks {
			shards := make([][]byte, len(chunk.Shards))
			for i, ss := range chunk.Shards {
				repo, err := placer.GetRepositoryForBucket(ss.BucketName)
				if err != nil {
					t.Fatalf("get repo for bucket %q: %v", ss.BucketName, err)
				}
				w := &bufferWriterAt{}
				if err := repo.Download(ctx, ss.Key, w, true); err != nil {
					t.Fatalf("download shard %d of chunk %d: %v", i, chunk.ChunkIndex, err)
				}
				shards[i] = w.data
			}
			chunkMeta := domain.ObjectMetadata{
				ParityShards: parityShards,
				OriginalSize: chunk.ChunkSize,
				ShardHashes:  chunk.Shards,
			}
			data, err := ReconstructFile(shards, chunkMeta)
			if err != nil {
				t.Fatalf("reconstruct chunk %d: %v", chunk.ChunkIndex, err)
			}
			out = append(out, data...)
		}
		return out
	}

	shards := make([][]byte, len(meta.ShardHashes))
	for i, ss := range meta.ShardHashes {
		repo, err := placer.GetRepositoryForBucket(ss.BucketName)
		if err != nil {
			t.Fatalf("get repo for bucket %q: %v", ss.BucketName, err)
		}
		w := &bufferWriterAt{}
		if err := repo.Download(ctx, ss.Key, w, true); err != nil {
			t.Fatalf("download shard %d: %v", i, err)
		}
		shards[i] = w.data
	}
	data, err := ReconstructFile(shards, meta)
	if err != nil {
		t.Fatalf("reconstruct legacy object: %v", err)
	}
	return data
}

// dirOf / baseOf mirror filepath.Dir / filepath.Base for the simple "dir/file"
// keys used in these tests, avoiding an extra dependency in the test helpers.
func dirOf(key string) string {
	if i := strings.LastIndex(key, "/"); i >= 0 {
		return key[:i]
	}
	return "."
}

func baseOf(key string) string {
	if i := strings.LastIndex(key, "/"); i >= 0 {
		return key[i+1:]
	}
	return key
}

func TestUploadFile_Legacy_RoundTrip(t *testing.T) {
	fileService, placer := newTestFileService(t)
	ctx := context.Background()

	data := makeTestData(2048)
	key := "unittest/legacy.bin"

	if err := fileService.UploadFile(ctx, key, bytes.NewReader(data), true, 4, 2, 3); err != nil {
		t.Fatalf("UploadFile failed: %v", err)
	}

	meta, err := fileService.metadataRepo.GetMetadata(ctx, dirOf(key), baseOf(key))
	if err != nil {
		t.Fatalf("GetMetadata failed: %v", err)
	}

	if meta.ChunkMethod != ChunkMethodNone {
		t.Errorf("expected ChunkMethod %q, got %q", ChunkMethodNone, meta.ChunkMethod)
	}
	if meta.OriginalSize != int64(len(data)) {
		t.Errorf("expected OriginalSize %d, got %d", len(data), meta.OriginalSize)
	}
	// Legacy path stores 4 data + 2 parity shards at the top level.
	if len(meta.ShardHashes) != 6 {
		t.Fatalf("expected 6 ShardHashes, got %d", len(meta.ShardHashes))
	}
	if len(meta.Chunks) != 0 {
		t.Errorf("expected empty Chunks for legacy upload, got %d", len(meta.Chunks))
	}
	for i, ss := range meta.ShardHashes {
		if ss.BucketName == "" || ss.Key == "" {
			t.Errorf("shard %d missing storage location: %+v", i, ss)
		}
	}

	got := reconstructObject(t, ctx, meta, placer, 2)
	if !bytes.Equal(got, data) {
		t.Fatalf("legacy round-trip mismatch: got %d bytes, want %d", len(got), len(data))
	}
}

func TestUploadFileChunked_Fixed_RoundTrip(t *testing.T) {
	fileService, placer := newTestFileService(t)
	ctx := context.Background()

	data := makeTestData(2048)
	key := "unittest/fixed.bin"
	chunkSize := int64(500) // -> 5 chunks: 500,500,500,500,248

	if err := fileService.UploadFileChunked(ctx, key, bytes.NewReader(data), true, 4, 2, 3, chunkSize, ChunkMethodFixed); err != nil {
		t.Fatalf("UploadFileChunked failed: %v", err)
	}

	meta, err := fileService.metadataRepo.GetMetadata(ctx, dirOf(key), baseOf(key))
	if err != nil {
		t.Fatalf("GetMetadata failed: %v", err)
	}

	if meta.ChunkMethod != ChunkMethodFixed {
		t.Errorf("expected ChunkMethod %q, got %q", ChunkMethodFixed, meta.ChunkMethod)
	}
	if meta.OriginalSize != int64(len(data)) {
		t.Errorf("expected OriginalSize %d, got %d", len(data), meta.OriginalSize)
	}
	if meta.TotalChunks != len(meta.Chunks) {
		t.Errorf("TotalChunks %d != len(Chunks) %d", meta.TotalChunks, len(meta.Chunks))
	}
	if len(meta.Chunks) != 5 {
		t.Fatalf("expected 5 chunks, got %d", len(meta.Chunks))
	}
	if len(meta.ShardHashes) != 0 {
		t.Errorf("expected empty top-level ShardHashes for chunked upload, got %d", len(meta.ShardHashes))
	}
	for _, c := range meta.Chunks {
		if len(c.Shards) != 6 { // 4 data + 2 parity
			t.Errorf("chunk %d: expected 6 shards, got %d", c.ChunkIndex, len(c.Shards))
		}
		for _, ss := range c.Shards {
			if ss.BucketName == "" || ss.Key == "" {
				t.Errorf("chunk %d shard missing storage loc: %+v", c.ChunkIndex, ss)
			}
			// Key format: <prefix>/<filename>/chunk_<N>/<shardHash>
			expectedKey := fmt.Sprintf("%s/chunk_%d/%s", key, c.ChunkIndex, ss.Hash)
			if ss.Key != expectedKey {
				t.Errorf("chunk %d shard key format: got %q, want %q", c.ChunkIndex, ss.Key, expectedKey)
			}
		}
	}

	got := reconstructObject(t, ctx, meta, placer, 2)
	if !bytes.Equal(got, data) {
		t.Fatalf("chunked round-trip mismatch: got %d bytes, want %d", len(got), len(data))
	}
}

func TestUploadFileChunked_EqualSplit_RoundTrip(t *testing.T) {
	fileService, placer := newTestFileService(t)
	ctx := context.Background()

	data := makeTestData(1024)
	key := "unittest/equal.bin"
	numChunks := int64(4) // chunkSize param reinterpreted as chunk count

	if err := fileService.UploadFileChunked(ctx, key, bytes.NewReader(data), true, 4, 2, 3, numChunks, ChunkMethodEqualSplit); err != nil {
		t.Fatalf("UploadFileChunked failed: %v", err)
	}

	meta, err := fileService.metadataRepo.GetMetadata(ctx, dirOf(key), baseOf(key))
	if err != nil {
		t.Fatalf("GetMetadata failed: %v", err)
	}

	if meta.ChunkMethod != ChunkMethodEqualSplit {
		t.Errorf("expected ChunkMethod %q, got %q", ChunkMethodEqualSplit, meta.ChunkMethod)
	}
	if meta.OriginalSize != int64(len(data)) {
		t.Errorf("expected OriginalSize %d, got %d", len(data), meta.OriginalSize)
	}
	if len(meta.Chunks) != 4 {
		t.Fatalf("expected 4 chunks, got %d", len(meta.Chunks))
	}
	if len(meta.ShardHashes) != 0 {
		t.Errorf("expected empty top-level ShardHashes for chunked upload, got %d", len(meta.ShardHashes))
	}
	// Equal-split of 1024 into 4 chunks -> each 256 bytes.
	if int64(meta.Chunks[0].ChunkSize) != 256 {
		t.Errorf("expected first chunk size 256, got %d", meta.Chunks[0].ChunkSize)
	}

	got := reconstructObject(t, ctx, meta, placer, 2)
	if !bytes.Equal(got, data) {
		t.Fatalf("equal-split round-trip mismatch: got %d bytes, want %d", len(got), len(data))
	}
}

func TestUploadFileChunked_EmptyFile(t *testing.T) {
	fileService, _ := newTestFileService(t)
	ctx := context.Background()
	err := fileService.UploadFileChunked(ctx, "unittest/empty.bin", bytes.NewReader(nil), true, 4, 2, 3, 500, ChunkMethodFixed)
	if err == nil {
		t.Fatal("expected error uploading empty file, got nil")
	}
}

func TestUploadFile_Legacy_EmptyFile(t *testing.T) {
	fileService, _ := newTestFileService(t)
	ctx := context.Background()
	err := fileService.UploadFile(ctx, "unittest/empty-legacy.bin", bytes.NewReader(nil), true, 4, 2, 3)
	if err == nil {
		t.Fatal("expected error uploading empty file via UploadFile, got nil")
	}
}

func TestUploadFileChunked_UnsupportedMethod(t *testing.T) {
	fileService, _ := newTestFileService(t)
	ctx := context.Background()
	err := fileService.UploadFileChunked(ctx, "unittest/bogus.bin", bytes.NewReader(makeTestData(64)), true, 4, 2, 3, 500, "bogus")
	if err == nil {
		t.Fatal("expected error for unsupported chunk method, got nil")
	}
}

func TestUploadFileChunked_FixedInvalidChunkSize(t *testing.T) {
	fileService, _ := newTestFileService(t)
	ctx := context.Background()
	err := fileService.UploadFileChunked(ctx, "unittest/bad-chunksize.bin", bytes.NewReader(makeTestData(64)), true, 4, 2, 3, 0, ChunkMethodFixed)
	if err == nil {
		t.Fatal("expected error for chunkSize<=0 with fixed chunking, got nil")
	}
}

// TestDownloadFile_Legacy tests downloading a non-chunked file using io.Writer
func TestDownloadFile_Legacy(t *testing.T) {
	fileService, _ := newTestFileService(t)
	ctx := context.Background()

	data := makeTestData(1024)
	key := "unittest/download-legacy.bin"

	// Upload file
	if err := fileService.UploadFile(ctx, key, bytes.NewReader(data), true, 4, 2, 3); err != nil {
		t.Fatalf("UploadFile failed: %v", err)
	}

	// Get metadata to verify storage
	meta, err := fileService.metadataRepo.GetMetadata(ctx, dirOf(key), baseOf(key))
	if err != nil {
		t.Fatalf("GetMetadata failed: %v", err)
	}

	// Download using io.Writer
	var buf bytes.Buffer
	if err := fileService.DownloadFile(ctx, key, &buf, true, true); err != nil {
		t.Fatalf("DownloadFile failed: %v", err)
	}

	// Verify downloaded data
	if !bytes.Equal(buf.Bytes(), data) {
		t.Fatalf("downloaded data mismatch: got %d bytes, want %d", len(buf.Bytes()), len(data))
	}

	// Verify metadata
	if meta.ChunkMethod != ChunkMethodNone {
		t.Errorf("expected ChunkMethod %q, got %q", ChunkMethodNone, meta.ChunkMethod)
	}
}

// TestDownloadFile_Chunked tests downloading a chunked file using io.Writer
func TestDownloadFile_Chunked(t *testing.T) {
	fileService, _ := newTestFileService(t)
	ctx := context.Background()

	data := makeTestData(2048)
	key := "unittest/download-chunked.bin"

	// Upload file with chunking
	if err := fileService.UploadFileChunked(ctx, key, bytes.NewReader(data), true, 4, 2, 3, 500, ChunkMethodFixed); err != nil {
		t.Fatalf("UploadFileChunked failed: %v", err)
	}

	// Get metadata
	meta, err := fileService.metadataRepo.GetMetadata(ctx, dirOf(key), baseOf(key))
	if err != nil {
		t.Fatalf("GetMetadata failed: %v", err)
	}

	// Download using io.Writer
	var buf bytes.Buffer
	if err := fileService.DownloadFile(ctx, key, &buf, true, true); err != nil {
		t.Fatalf("DownloadFile failed: %v", err)
	}

	// Verify downloaded data
	if !bytes.Equal(buf.Bytes(), data) {
		t.Fatalf("downloaded data mismatch: got %d bytes, want %d", len(buf.Bytes()), len(data))
	}

	// Verify metadata for chunked file
	if meta.ChunkMethod != ChunkMethodFixed {
		t.Errorf("expected ChunkMethod %q, got %q", ChunkMethodFixed, meta.ChunkMethod)
	}
	if len(meta.Chunks) == 0 {
		t.Error("expected Chunks to be populated for chunked file")
	}
}

// TestDownloadFile_NotFound tests downloading a non-existent file
func TestDownloadFile_NotFound(t *testing.T) {
	fileService, _ := newTestFileService(t)
	ctx := context.Background()

	var buf bytes.Buffer
	err := fileService.DownloadFile(ctx, "nonexistent/file.bin", &buf, true, true)
	if err == nil {
		t.Fatal("expected error for non-existent file, got nil")
	}
}
