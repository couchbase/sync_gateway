package db

import (
	"container/list"
	"sync"

	"github.com/couchbase/sync_gateway/base"
)

// Number of recently-accessed doc revisions to cache in RAM
const KDefaultRevisionCacheCapacity = 5000

// An LRU cache of document revision bodies, together with their channel access.
type RevisionCache struct {
	cache      map[IDAndRev]*list.Element // Fast lookup of list element by doc/rev ID
	lruList    *list.List                 // List ordered by most recent access (Front is newest)
	capacity   int                        // Max number of revisions to cache
	loaderFunc RevisionCacheLoaderFunc
	lock       sync.Mutex // For thread-safety
}

type RevisionCacheLoaderFunc func(id IDAndRev) (body Body, history Body, channels base.Set, err error)

// The cache payload data. Stored as the Value of a list Element.
type revCacheValue struct {
	key      IDAndRev   // doc/rev IDs
	body     Body       // Revision body (a pristine shallow copy)
	history  Body       // Rev history encoded like a "_revisions" property
	channels base.Set   // Set of channels that have access
	err      error      // Error from loaderFunc if it failed
	lock     sync.Mutex // Synchronizes access to this struct
}

// Creates a revision cache with the given capacity and an optional loader function.
func NewRevisionCache(capacity int, loaderFunc RevisionCacheLoaderFunc) *RevisionCache {

	if capacity == 0 {
		capacity = KDefaultRevisionCacheCapacity
	}

	return &RevisionCache{
		cache:      map[IDAndRev]*list.Element{},
		lruList:    list.New(),
		capacity:   capacity,
		loaderFunc: loaderFunc,
	}
}

// Looks up a revision from the cache.
// Returns the body of the revision, its history, and the set of channels it's in.
// If the cache has a loaderFunction, it will be called if the revision isn't in the cache;
// any error returned by the loaderFunction will be returned from Get.
func (rc *RevisionCache) Get(docid, revid string) (Body, Body, base.Set, error) {
	value := rc.getValue(docid, revid, rc.loaderFunc != nil)
	if value == nil {
		return nil, nil, nil, nil
	}
	body, history, channels, err := value.load(rc.loaderFunc)
	if err != nil {
		rc.removeValue(value) // don't keep failed loads in the cache
	}
	return body, history, channels, err
}

// Adds a revision to the cache.
func (rc *RevisionCache) Put(body Body, history Body, channels base.Set) {
	if history == nil {
		panic("Missing history for RevisionCache.Put")
	}
	value := rc.getValue(body["_id"].(string), body["_rev"].(string), true)
	value.store(body, history, channels)
}

func (rc *RevisionCache) getValue(docid, revid string, create bool) (value *revCacheValue) {
	if docid == "" || revid == "" {
		panic("RevisionCache: invalid empty doc/rev id")
	}
	key := IDAndRev{DocID: docid, RevID: revid}
	rc.lock.Lock()
	defer rc.lock.Unlock()
	if elem := rc.cache[key]; elem != nil {
		rc.lruList.MoveToFront(elem)
		value = elem.Value.(*revCacheValue)
	} else if create {
		value = &revCacheValue{key: key}
		rc.cache[key] = rc.lruList.PushFront(value)
		for len(rc.cache) > rc.capacity {
			rc.purgeOldest_()
		}
	}
	return
}

func (rc *RevisionCache) removeValue(value *revCacheValue) {
	rc.lock.Lock()
	if element := rc.cache[value.key]; element != nil && element.Value == value {
		rc.lruList.Remove(element)
		delete(rc.cache, value.key)
	}
	rc.lock.Unlock()
}

func (rc *RevisionCache) purgeOldest_() {
	value := rc.lruList.Remove(rc.lruList.Back()).(*revCacheValue)
	delete(rc.cache, value.key)
}

// Gets the body etc. out of a revCacheValue. If they aren't present already, the loader func
// will be called. This is synchronized so that the loader will only be called once even if
// multiple goroutines try to load at the same time.
func (value *revCacheValue) load(loaderFunc RevisionCacheLoaderFunc) (Body, Body, base.Set, error) {
	value.lock.Lock()
	defer value.lock.Unlock()
	if value.body == nil && value.err == nil {
		base.StatsExpvars.Add("revisionCache_misses", 1)
		if loaderFunc != nil {
			value.body, value.history, value.channels, value.err = loaderFunc(value.key)
		}
	} else {
		base.StatsExpvars.Add("revisionCache_hits", 1)
	}
	body := value.body
	if body != nil {
		body = body.ShallowCopy() // Never let the caller mutate the stored body
	}
	return body, value.history, value.channels, value.err
}

// Stores a body etc. into a revCacheValue if there isn't one already.
func (value *revCacheValue) store(body Body, history Body, channels base.Set) {
	value.lock.Lock()
	if value.body == nil {
		value.body = body.ShallowCopy() // Don't store a body the caller might later mutate
		value.history = history
		value.channels = channels
		value.err = nil
		dbExpvars.Add("revisionCache_adds", 1)
	}
	value.lock.Unlock()
}
