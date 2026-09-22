package embedder

import (
	"container/list"
	"sync"
)

// LRUCache is a generic, thread-safe Least Recently Used cache.
// Bounding the cache is a senior-level requirement to prevent memory leaks (OOM)
// under sustained high-load production environments.
type LRUCache[K comparable, V any] struct {
	capacity  int
	items     map[K]*list.Element
	evictList *list.List
	mu        sync.RWMutex
}

type entry[K comparable, V any] struct {
	key   K
	value V
}

// NewLRUCache creates a new bounded LRU cache.
func NewLRUCache[K comparable, V any](capacity int) *LRUCache[K, V] {
	if capacity <= 0 {
		capacity = 1000 // Safe default to prevent unbounded growth
	}
	return &LRUCache[K, V]{
		capacity:  capacity,
		items:     make(map[K]*list.Element),
		evictList: list.New(),
	}
}

// Get looks up a key's value from the cache.
func (c *LRUCache[K, V]) Get(key K) (V, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if ent, ok := c.items[key]; ok {
		c.evictList.MoveToFront(ent)
		return ent.Value.(*entry[K, V]).value, true
	}
	var zero V
	return zero, false
}

// Add adds a value to the cache. Returns true if an eviction occurred.
func (c *LRUCache[K, V]) Add(key K, value V) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Update existing item
	if ent, ok := c.items[key]; ok {
		c.evictList.MoveToFront(ent)
		ent.Value.(*entry[K, V]).value = value
		return false
	}

	// Add new item
	ent := &entry[K, V]{key, value}
	element := c.evictList.PushFront(ent)
	c.items[key] = element

	// Evict oldest if capacity exceeded
	evict := c.evictList.Len() > c.capacity
	if evict {
		c.removeOldest()
	}
	return evict
}

// removeOldest removes the oldest item from the cache.
// Requires the lock to be held.
func (c *LRUCache[K, V]) removeOldest() {
	ent := c.evictList.Back()
	if ent != nil {
		c.evictList.Remove(ent)
		kv := ent.Value.(*entry[K, V])
		delete(c.items, kv.key)
	}
}
