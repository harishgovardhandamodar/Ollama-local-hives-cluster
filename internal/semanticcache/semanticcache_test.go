package semanticcache

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestFlatIndexInsertAndSearch(t *testing.T) {
	idx := NewFlatIndex()

	v1 := []float32{1.0, 0.0, 0.0}
	v2 := []float32{0.9, 0.1, 0.0}
	v3 := []float32{0.0, 1.0, 0.0}

	idx.Insert("a", "model", v1)
	idx.Insert("b", "model", v2)
	idx.Insert("c", "model", v3)

	if idx.Len() != 3 {
		t.Fatalf("expected 3 entries, got %d", idx.Len())
	}

	results := idx.Search([]float32{1.0, 0.0, 0.0}, 1, "model", nil)
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if results[0].ID != "a" {
		t.Fatalf("expected 'a', got %s", results[0].ID)
	}
	if results[0].Score < 0.99 {
		t.Fatalf("expected score > 0.99, got %f", results[0].Score)
	}
}

func TestFlatIndexSearchK(t *testing.T) {
	idx := NewFlatIndex()

	for i := 0; i < 10; i++ {
		vec := make([]float32, 3)
		vec[0] = float32(i)
		idx.Insert(string(rune('a'+i)), "model", vec)
	}

	query := []float32{5.0, 0.0, 0.0}
	results := idx.Search(query, 3, "model", nil)
	if len(results) != 3 {
		t.Fatalf("expected 3 results, got %d", len(results))
	}
}

func TestFlatIndexModelFilter(t *testing.T) {
	idx := NewFlatIndex()

	idx.Insert("a", "model-x", []float32{1.0, 0.0})
	idx.Insert("b", "model-x", []float32{0.9, 0.1})

	results := idx.Search([]float32{1.0, 0.0}, 10, "model-y", nil)
	if len(results) != 0 {
		t.Fatalf("expected 0 results for wrong model, got %d", len(results))
	}
}

func TestFlatIndexRemove(t *testing.T) {
	idx := NewFlatIndex()

	idx.Insert("a", "model", []float32{1.0, 0.0})
	idx.Insert("b", "model", []float32{0.0, 1.0})

	idx.Remove("a")

	if idx.Len() != 1 {
		t.Fatalf("expected 1 entry after remove, got %d", idx.Len())
	}

	results := idx.Search([]float32{1.0, 0.0}, 10, "model", nil)
	if len(results) != 1 {
		t.Fatalf("expected 1 result after remove (b still exists), got %d", len(results))
	}
	if results[0].ID != "b" {
		t.Fatalf("expected remaining entry to be 'b', got %s", results[0].ID)
	}
}

func TestLSHIndexInsertAndSearch(t *testing.T) {
	idx := NewLSHIndex(3)

	v1 := []float32{1.0, 0.0, 0.0}
	v2 := []float32{0.9, 0.1, 0.0}
	v3 := []float32{0.0, 1.0, 0.0}

	idx.Insert("a", "model", v1)
	idx.Insert("b", "model", v2)
	idx.Insert("c", "model", v3)

	if idx.Len() != 3 {
		t.Fatalf("expected 3 entries, got %d", idx.Len())
	}

	results := idx.Search([]float32{1.0, 0.0, 0.0}, 1, "model", nil)
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if results[0].ID != "a" {
		t.Fatalf("expected 'a', got %s", results[0].ID)
	}
}

func TestLSHIndexSearchK(t *testing.T) {
	idx := NewLSHIndex(3)

	for i := 0; i < 20; i++ {
		vec := make([]float32, 3)
		vec[0] = float32(i) / 20.0
		vec[1] = float32(i%5) / 5.0
		vec[2] = float32(i%3) / 3.0
		idx.Insert(string(rune('a'+i%26)), "model", vec)
	}

	query := []float32{0.5, 0.2, 0.1}
	results := idx.Search(query, 5, "model", nil)
	if len(results) > 5 {
		t.Fatalf("expected at most 5 results, got %d", len(results))
	}
}

func TestLSHIndexModelFilter(t *testing.T) {
	idx := NewLSHIndex(2)

	idx.Insert("a", "model-x", []float32{1.0, 0.0})
	idx.Insert("b", "model-x", []float32{0.9, 0.1})

	results := idx.Search([]float32{1.0, 0.0}, 10, "model-y", nil)
	if len(results) != 0 {
		t.Fatalf("expected 0 results for wrong model, got %d", len(results))
	}
}

func TestLSHIndexRemove(t *testing.T) {
	idx := NewLSHIndex(2)

	idx.Insert("a", "model", []float32{1.0, 0.0})
	idx.Insert("b", "model", []float32{0.0, 1.0})

	idx.Remove("a")

	if idx.Len() != 1 {
		t.Fatalf("expected 1 entry after remove, got %d", idx.Len())
	}
}

func TestLSHIndexHighDimensional(t *testing.T) {
	dim := 768
	idx := NewLSHIndex(dim)

	for i := 0; i < 50; i++ {
		vec := make([]float32, dim)
		for j := range vec {
			vec[j] = float32(i*dim+j) / float32(dim*50)
		}
		idx.Insert(fmt.Sprintf("v%d", i), "model", vec)
	}

	query := make([]float32, dim)
	for j := range query {
		query[j] = float32(j) / float32(dim)
	}

	results := idx.Search(query, 5, "model", nil)
	if len(results) == 0 {
		t.Fatal("expected at least 1 result")
	}
}

func TestEmbeddingCacheSetAndGet(t *testing.T) {
	cache := NewEmbeddingCache(100)

	emb := []float32{0.1, 0.2, 0.3}
	cache.Set("key1", emb)

	got, ok := cache.Get("key1")
	if !ok {
		t.Fatal("expected cache hit")
	}
	if len(got) != 3 {
		t.Fatalf("expected 3 values, got %d", len(got))
	}
	for i := range emb {
		if got[i] != emb[i] {
			t.Fatalf("mismatch at index %d", i)
		}
	}
}

func TestEmbeddingCacheMiss(t *testing.T) {
	cache := NewEmbeddingCache(100)

	_, ok := cache.Get("nonexistent")
	if ok {
		t.Fatal("expected cache miss")
	}
}

func TestEmbeddingCacheLRU(t *testing.T) {
	cache := NewEmbeddingCache(3)

	cache.Set("a", []float32{1.0})
	cache.Set("b", []float32{2.0})
	cache.Set("c", []float32{3.0})
	cache.Set("d", []float32{4.0})

	if cache.Len() != 3 {
		t.Fatalf("expected 3 entries, got %d", cache.Len())
	}

	_, ok := cache.Get("a")
	if ok {
		t.Fatal("expected 'a' to be evicted")
	}

	_, ok = cache.Get("d")
	if !ok {
		t.Fatal("expected 'd' to exist")
	}
}

func TestEmbeddingCacheDuplicate(t *testing.T) {
	cache := NewEmbeddingCache(10)

	cache.Set("key", []float32{1.0, 2.0})
	cache.Set("key", []float32{3.0, 4.0})

	got, ok := cache.Get("key")
	if !ok {
		t.Fatal("expected cache hit")
	}
	if got[0] != 1.0 {
		t.Fatal("expected original value to be kept")
	}
}

func TestEmbeddingCacheClear(t *testing.T) {
	cache := NewEmbeddingCache(10)

	cache.Set("a", []float32{1.0})
	cache.Set("b", []float32{2.0})

	cache.Clear()

	if cache.Len() != 0 {
		t.Fatalf("expected 0 entries after clear, got %d", cache.Len())
	}
}

func TestCacheWarmingFromFile(t *testing.T) {
	tmpDir := t.TempDir()
	warmupFile := filepath.Join(tmpDir, "warmup.jsonl")

	entries := []WarmupEntry{
		{Model: "llama3", Prompt: "What is Go?", Response: "Go is a language."},
		{Model: "llama3", Prompt: "What is Python?", Response: "Python is a language."},
		{Model: "mistral", Prompt: "What is Rust?", Response: "Rust is a systems language."},
	}

	file, err := os.Create(warmupFile)
	if err != nil {
		t.Fatalf("failed to create warmup file: %v", err)
	}
	for _, e := range entries {
		data, _ := json.Marshal(e)
		file.Write(append(data, '\n'))
	}
	file.Close()

	cfg := DefaultConfig()
	cfg.Enabled = true
	cfg.MaxEntries = 100
	cfg.WarmupFile = warmupFile
	cache := New(cfg, nil)
	defer cache.Stop()

	stats := cache.Stats()
	if stats.Entries != 3 {
		t.Fatalf("expected 3 entries after warmup, got %d", stats.Entries)
	}

	ctx := context.Background()
	entry, found := cache.Get(ctx, "llama3", "What is Go?", nil)
	if !found {
		t.Fatal("expected cache hit for warmed entry")
	}
	if entry.Response != "Go is a language." {
		t.Fatalf("unexpected response: %s", entry.Response)
	}
}

func TestCacheWarmingSkipsDuplicates(t *testing.T) {
	tmpDir := t.TempDir()
	warmupFile := filepath.Join(tmpDir, "warmup.jsonl")

	entries := []WarmupEntry{
		{Model: "llama3", Prompt: "What is Go?", Response: "Go is a language."},
		{Model: "llama3", Prompt: "What is Go?", Response: "Go is a programming language."},
	}

	file, _ := os.Create(warmupFile)
	for _, e := range entries {
		data, _ := json.Marshal(e)
		file.Write(append(data, '\n'))
	}
	file.Close()

	cfg := DefaultConfig()
	cfg.Enabled = true
	cfg.MaxEntries = 100
	cfg.WarmupFile = warmupFile
	cache := New(cfg, nil)
	defer cache.Stop()

	stats := cache.Stats()
	if stats.Entries != 1 {
		t.Fatalf("expected 1 entry after dedup, got %d", stats.Entries)
	}
}

func TestCacheWarmingInvalidFile(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Enabled = true
	cfg.WarmupFile = "/nonexistent/file.jsonl"
	cache := New(cfg, nil)
	defer cache.Stop()

	stats := cache.Stats()
	if stats.Entries != 0 {
		t.Fatalf("expected 0 entries for invalid file, got %d", stats.Entries)
	}
}

func TestPerModelEmbeddingConfig(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Enabled = true
	cfg.ModelEmbeddingMap = map[string]string{
		"llama3":   "nomic-embed-text",
		"mistral":  "bge-small",
		"codellama": "code-embeddings",
	}
	cache := New(cfg, nil)
	defer cache.Stop()

	model1 := cache.resolveEmbeddingModel("llama3")
	if model1 != "nomic-embed-text" {
		t.Fatalf("expected nomic-embed-text, got %s", model1)
	}

	model2 := cache.resolveEmbeddingModel("mistral")
	if model2 != "bge-small" {
		t.Fatalf("expected bge-small, got %s", model2)
	}

	model3 := cache.resolveEmbeddingModel("codellama")
	if model3 != "code-embeddings" {
		t.Fatalf("expected code-embeddings, got %s", model3)
	}

	model4 := cache.resolveEmbeddingModel("unknown")
	if model4 != "nomic-embed-text" {
		t.Fatalf("expected default model, got %s", model4)
	}
}

func TestCacheWithLSHIndex(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Enabled = true
	cfg.IndexType = "lsh"
	cache := New(cfg, nil)
	defer cache.Stop()

	entry := &CacheEntry{
		ID:          "test-1",
		Key:         "key",
		Model:       "model",
		Prompt:      "prompt",
		Response:    "response",
		Embedding:   []float32{1.0, 0.0, 0.0},
		CreatedAt:   time.Now(),
		TTLSeconds:  300,
	}
	cache.mu.Lock()
	cache.entries = append(cache.entries, entry)
	cache.embeddings[entry.ID] = entry.Embedding
	cache.index.Insert(entry.ID, entry.Model, entry.Embedding)
	cache.mu.Unlock()

	query := []float32{0.9, 0.1, 0.0}
	results := cache.index.Search(query, 1, "model", nil)
	if len(results) != 1 {
		t.Fatalf("expected 1 result from LSH index, got %d", len(results))
	}
}

func TestCacheWithFlatIndex(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Enabled = true
	cfg.IndexType = "flat"
	cache := New(cfg, nil)
	defer cache.Stop()

	entry := &CacheEntry{
		ID:          "test-1",
		Key:         "key",
		Model:       "model",
		Prompt:      "prompt",
		Response:    "response",
		Embedding:   []float32{1.0, 0.0, 0.0},
		CreatedAt:   time.Now(),
		TTLSeconds:  300,
	}
	cache.mu.Lock()
	cache.entries = append(cache.entries, entry)
	cache.embeddings[entry.ID] = entry.Embedding
	cache.index.Insert(entry.ID, entry.Model, entry.Embedding)
	cache.mu.Unlock()

	query := []float32{0.9, 0.1, 0.0}
	results := cache.index.Search(query, 1, "model", nil)
	if len(results) != 1 {
		t.Fatalf("expected 1 result from flat index, got %d", len(results))
	}
}

func TestVectorIndexInterface(t *testing.T) {
	var idx VectorIndex

	idx = NewFlatIndex()
	idx.Insert("a", "model", []float32{1.0, 0.0})
	if idx.Len() != 1 {
		t.Fatal("flat index: expected 1 entry")
	}
	idx.Remove("a")
	if idx.Len() != 0 {
		t.Fatal("flat index: expected 0 entries after remove")
	}

	idx = NewLSHIndex(2)
	idx.Insert("a", "model", []float32{1.0, 0.0})
	if idx.Len() != 1 {
		t.Fatal("lsh index: expected 1 entry")
	}
	idx.Remove("a")
	if idx.Len() != 0 {
		t.Fatal("lsh index: expected 0 entries after remove")
	}
}

func TestEmbeddingCacheConcurrency(t *testing.T) {
	cache := NewEmbeddingCache(100)

	done := make(chan struct{})
	for i := 0; i < 20; i++ {
		go func(idx int) {
			for j := 0; j < 50; j++ {
				key := fmt.Sprintf("key-%d-%d", idx, j)
				cache.Set(key, []float32{float32(idx), float32(j)})
				cache.Get(key)
			}
			done <- struct{}{}
		}(i)
	}

	for i := 0; i < 20; i++ {
		<-done
	}

	if cache.Len() > 100 {
		t.Fatalf("expected at most 100 entries, got %d", cache.Len())
	}
}

func TestLSHIndexDifferentDimensions(t *testing.T) {
	dims := []int{2, 10, 64, 256, 768}

	for _, dim := range dims {
		t.Run(fmt.Sprintf("dim-%d", dim), func(t *testing.T) {
			idx := NewLSHIndex(dim)

			for i := 0; i < 10; i++ {
				vec := make([]float32, dim)
				for j := range vec {
					vec[j] = float32(i*dim+j) / float32(dim*10)
				}
				idx.Insert(fmt.Sprintf("v%d", i), "model", vec)
			}

			query := make([]float32, dim)
			for j := range query {
				query[j] = float32(j) / float32(dim)
			}

			results := idx.Search(query, 3, "model", nil)
			if len(results) == 0 {
				t.Fatal("expected at least 1 result")
			}
		})
	}
}

func TestDefaultConfigNewFields(t *testing.T) {
	cfg := DefaultConfig()

	if cfg.IndexType != "lsh" {
		t.Fatalf("expected lsh index type, got %s", cfg.IndexType)
	}
	if cfg.EmbeddingCacheSize != 5000 {
		t.Fatalf("expected 5000 embedding cache size, got %d", cfg.EmbeddingCacheSize)
	}
	if cfg.ModelEmbeddingMap == nil {
		t.Fatal("expected non-nil model embedding map")
	}
}
