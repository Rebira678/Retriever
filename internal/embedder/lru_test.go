package embedder

import (
	"sync"
	"testing"
)

func TestLRU_Basic(t *testing.T) {
	lru := NewLRUCache[string, int](2)

	if _, found := lru.Get("a"); found {
		t.Errorf("Expected 'a' to be missing")
	}

	evicted := lru.Add("a", 1)
	if evicted {
		t.Errorf("Expected no eviction on first item")
	}

	if val, found := lru.Get("a"); !found || val != 1 {
		t.Errorf("Expected 'a' to be 1, got %v", val)
	}
}

func TestLRU_Eviction(t *testing.T) {
	lru := NewLRUCache[int, string](2)

	lru.Add(1, "one")
	lru.Add(2, "two")
	
	// Capacity is 2, adding 3 should evict 1
	evicted := lru.Add(3, "three")
	if !evicted {
		t.Errorf("Expected eviction when adding 3rd item to capacity 2 cache")
	}

	if _, found := lru.Get(1); found {
		t.Errorf("Expected 1 to be evicted")
	}

	if _, found := lru.Get(2); !found {
		t.Errorf("Expected 2 to be present")
	}
	if _, found := lru.Get(3); !found {
		t.Errorf("Expected 3 to be present")
	}
}

func TestLRU_UpdateBringsToFront(t *testing.T) {
	lru := NewLRUCache[int, string](2)

	lru.Add(1, "one")
	lru.Add(2, "two")

	lru.Get(1) // Brings 1 to front

	lru.Add(3, "three") // Evicts 2

	if _, found := lru.Get(2); found {
		t.Errorf("Expected 2 to be evicted, not 1")
	}

	if _, found := lru.Get(1); !found {
		t.Errorf("Expected 1 to survive eviction")
	}

	lru.Add(1, "ONE_UPDATED")
	if val, found := lru.Get(1); !found || val != "ONE_UPDATED" {
		t.Errorf("Expected 1 to be updated to ONE_UPDATED, got %v", val)
	}
}

func TestLRU_ZeroCapacity(t *testing.T) {
	lru := NewLRUCache[string, int](0)
	
	if lru.capacity != 1000 {
		t.Errorf("Expected fallback capacity of 1000, got %d", lru.capacity)
	}
}

// TestLRU_Concurrency proves thread-safety under extreme concurrent mutation.
// This is an expert-level test to ensure no race conditions or deadlocks occur.
func TestLRU_Concurrency(t *testing.T) {
	lru := NewLRUCache[int, int](100)
	var wg sync.WaitGroup
	
	workers := 1000
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func(val int) {
			defer wg.Done()
			lru.Add(val%200, val)
			lru.Get(val % 200)
		}(i)
	}
	wg.Wait()
	
	lru.mu.RLock()
	length := lru.evictList.Len()
	lru.mu.RUnlock()
	
	if length > 100 {
		t.Errorf("expected max length 100, got %d", length)
	}
}

// FuzzLRU explores edge cases with arbitrary string inputs via Go 1.18+ Fuzzing.
// This ensures no payload (e.g. malformed unicode, giant strings) can crash the cache.
func FuzzLRU(f *testing.F) {
	f.Add("hello", "world")
	f.Add("", "")
	f.Add("🔥", "emoji")
	
	f.Fuzz(func(t *testing.T, key string, val string) {
		lru := NewLRUCache[string, string](5)
		
		lru.Add(key, val)
		
		if got, found := lru.Get(key); !found || got != val {
			t.Errorf("Fuzz failure: inserted %q but got %q", val, got)
		}
	})
}
