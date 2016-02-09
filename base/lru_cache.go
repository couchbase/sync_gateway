package base

import (
	"container/list"
	"errors"
	"sync"
)

// An LRU cache of document revision bodies, together with their channel access.
type LRUCache struct {
	cache      map[string]*list.Element // Fast lookup of list element by key
	lruList    *list.List               // List ordered by most recent access (Front is newest)
	capacity   int                      // Max number of entries to cache
	lruLock    sync.Mutex               // For thread-safety
	loaderFunc LRUCacheLoaderFunc
}

type LRUCacheLoaderFunc func(key string) (value interface{}, err error)

// The cache payload data. Stored as the Value of a list Element.
type lruCacheValue struct {
	key          string
	value        interface{}
	err          error      // Error from loaderFunc if it failed
	lruValueLock sync.Mutex // Synchronizes access to this struct
}

// Creates an LRU cache with the given capacity and an optional loader function.
func NewLRUCache(capacity int) (*LRUCache, error) {

	if capacity <= 0 {
		return nil, errors.New("LRU cache capacity must be non-zero")
	}

	return &LRUCache{
		cache:    map[string]*list.Element{},
		lruList:  list.New(),
		capacity: capacity}, nil
}

// Looks up an entry from the cache.
func (lc *LRUCache) Get(key string) (result interface{}, found bool) {
	lc.lruLock.Lock()
	defer lc.lruLock.Unlock()
	if elem, ok := lc.cache[key]; ok {
		lc.lruList.MoveToFront(elem)
		return elem.Value.(*lruCacheValue).value, true
	}
	return result, false
}

// Adds an entry to the cache.
func (lc *LRUCache) Put(key string, value interface{}) {

	// If already present, move to front
	if elem := lc.cache[key]; elem != nil {
		lc.lruList.MoveToFront(elem)
		value = elem.Value.(*lruCacheValue)
		return
	}

	// Not found - add as new
	cacheValue := &lruCacheValue{
		key:   key,
		value: value,
	}
	lc.cache[key] = lc.lruList.PushFront(cacheValue)

	// Purge oldest if over capacity
	for len(lc.cache) > lc.capacity {
		lc.purgeOldest_()
	}
}

func (lc *LRUCache) purgeOldest_() {
	value := lc.lruList.Remove(lc.lruList.Back()).(*lruCacheValue)
	delete(lc.cache, value.key)
}

func (lc *LRUCache) Count() int {
	return len(lc.cache)
}
