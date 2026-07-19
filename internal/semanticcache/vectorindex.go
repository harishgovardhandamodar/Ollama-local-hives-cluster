package semanticcache

import (
	"math"
	"math/rand"
	"sort"
	"sync"
)

type VectorIndex interface {
	Insert(id string, model string, vector []float32)
	Remove(id string)
	Search(query []float32, k int, model string, filter func(id string) bool) []IndexResult
	Len() int
}

type IndexResult struct {
	ID    string
	Score float64
}

type vectorEntry struct {
	id     string
	vector []float32
	model  string
}

type FlatIndex struct {
	mu      sync.RWMutex
	entries map[string]vectorEntry
}

func NewFlatIndex() *FlatIndex {
	return &FlatIndex{
		entries: make(map[string]vectorEntry),
	}
}

func (f *FlatIndex) Insert(id string, model string, vector []float32) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.entries[id] = vectorEntry{id: id, vector: vector, model: model}
}

func (f *FlatIndex) Remove(id string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.entries, id)
}

func (f *FlatIndex) Search(query []float32, k int, model string, filter func(id string) bool) []IndexResult {
	f.mu.RLock()
	defer f.mu.RUnlock()

	var results []IndexResult
	for _, entry := range f.entries {
		if len(entry.vector) != len(query) {
			continue
		}
		if entry.model != model {
			continue
		}
		if filter != nil && !filter(entry.id) {
			continue
		}
		score := cosineSimilarity(query, entry.vector)
		results = append(results, IndexResult{ID: entry.id, Score: score})
	}

	sort.Slice(results, func(i, j int) bool {
		return results[i].Score > results[j].Score
	})

	if k > 0 && len(results) > k {
		results = results[:k]
	}
	return results
}

func (f *FlatIndex) Len() int {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return len(f.entries)
}

const lshNumPlanes = 16
const lshNumBuckets = 256

type LSHIndex struct {
	mu      sync.RWMutex
	dim     int
	planes  [][]float32
	buckets [lshNumBuckets]map[string]vectorEntry
	entries map[string]vectorEntry
}

func NewLSHIndex(dim int) *LSHIndex {
	idx := &LSHIndex{
		dim:     dim,
		entries: make(map[string]vectorEntry),
	}
	idx.initPlanes()
	for i := range idx.buckets {
		idx.buckets[i] = make(map[string]vectorEntry)
	}
	return idx
}

func (l *LSHIndex) initPlanes() {
	l.planes = make([][]float32, lshNumPlanes)
	for i := range l.planes {
		plane := make([]float32, l.dim)
		for j := range plane {
			plane[j] = float32(rand.NormFloat64())
		}
		l.planes[i] = plane
	}
}

func (l *LSHIndex) hash(vector []float32) int {
	signature := 0
	for i, plane := range l.planes {
		dot := float32(0)
		for j := range plane {
			if j < len(vector) {
				dot += vector[j] * plane[j]
			}
		}
		if dot > 0 {
			signature |= 1 << uint(i)
		}
	}
	return signature % lshNumBuckets
}

func (l *LSHIndex) Insert(id string, model string, vector []float32) {
	l.mu.Lock()
	defer l.mu.Unlock()

	entry := vectorEntry{id: id, vector: vector, model: model}
	l.entries[id] = entry

	bucket := l.hash(vector)
	l.buckets[bucket][id] = entry
}

func (l *LSHIndex) Remove(id string) {
	l.mu.Lock()
	defer l.mu.Unlock()

	entry, ok := l.entries[id]
	if !ok {
		return
	}

	bucket := l.hash(entry.vector)
	delete(l.buckets[bucket], id)
	delete(l.entries, id)
}

func (l *LSHIndex) Search(query []float32, k int, model string, filter func(id string) bool) []IndexResult {
	l.mu.RLock()
	defer l.mu.RUnlock()

	candidates := make(map[string]bool)
	queryBucket := l.hash(query)

	for id := range l.buckets[queryBucket] {
		candidates[id] = true
	}
	for _, offset := range []int{-1, 0, 1} {
		bucket := (queryBucket + offset + lshNumBuckets) % lshNumBuckets
		for id := range l.buckets[bucket] {
			candidates[id] = true
		}
	}

	var results []IndexResult
	for id := range candidates {
		entry, ok := l.entries[id]
		if !ok {
			continue
		}
		if len(entry.vector) != len(query) {
			continue
		}
		if entry.model != model {
			continue
		}
		if filter != nil && !filter(entry.id) {
			continue
		}
		score := cosineSimilarity(query, entry.vector)
		results = append(results, IndexResult{ID: id, Score: score})
	}

	sort.Slice(results, func(i, j int) bool {
		return results[i].Score > results[j].Score
	})

	if k > 0 && len(results) > k {
		results = results[:k]
	}
	return results
}

func (l *LSHIndex) Len() int {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return len(l.entries)
}

func euclideanDistance(a, b []float32) float64 {
	if len(a) != len(b) {
		return math.MaxFloat64
	}
	var sum float64
	for i := range a {
		diff := float64(a[i] - b[i])
		sum += diff * diff
	}
	return math.Sqrt(sum)
}
