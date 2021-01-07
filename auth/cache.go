package auth

import (
	"math/rand"
	"sync"
)

// Cache represents a random replacement cache.
type Cache struct {
	size  int                 // Maximum size where this cache can potentially grow upto.
	keys  []string            // A slice of keys for choosing a random key for eviction.
	cache map[string]struct{} // Set of keys for fast lookup.
	lock  sync.RWMutex        // Protects both cache and keys from concurrent access.
}

// Creates a new cache that can potentially grow upto the provided size.
func NewCache(size int) *Cache {
	return &Cache{
		size:  size,
		cache: make(map[string]struct{}, size),
		keys:  make([]string, 0, size),
	}
}

// Contains returns true if the provided key is present
// in the cache and false otherwise.
func (c *Cache) Contains(key string) (ok bool) {
	c.lock.RLock()
	_, ok = c.cache[key]
	c.lock.RUnlock()
	return ok
}

// Put adds a key to the cache. Eviction occurs when memory is
// over filled or greater than the specified size in the cache.
// Keys are evicted randomly from the cache.
func (c *Cache) Put(key string) {
	c.lock.Lock()
	if _, ok := c.cache[key]; ok {
		c.lock.Unlock()
		return
	}
	if len(c.cache) >= c.size {
		index := rand.Intn(len(c.keys))
		delete(c.cache, c.keys[index])
		c.keys[index] = key
	} else {
		c.keys = append(c.keys, key)
	}
	c.cache[key] = struct{}{}
	c.lock.Unlock()
}

// Len returns the number of keys in the cache.
func (c *Cache) Len() int {
	c.lock.RLock()
	length := len(c.cache)
	c.lock.RUnlock()
	return length
}
