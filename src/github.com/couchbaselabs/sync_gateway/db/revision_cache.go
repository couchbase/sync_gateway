package db

import (
	"container/list"
	"sync"

	"github.com/couchbaselabs/sync_gateway/base"
)

// An LRU cache of document revision bodies, together with their channel access.
type RevisionCache struct {
	cache    map[IDAndRev]*list.Element // Fast lookup of list element by doc/rev ID
	lruList  *list.List                 // List ordered by most recent access (Front is newest)
	capacity int                        // Max number of revisions to cache
	lock     sync.Mutex                 // For thread-safety
}

// The cache payload data. Stored as the Value of a list Element.
type revCacheValue struct {
	body     Body     // Revision body (a pristine shallow copy)
	history  Body     // Rev history encoded like a "_revisions" property
	channels base.Set // Set of channels that have access
}

// Creates a revision cache with the given capacity.
func NewRevisionCache(capacity int) *RevisionCache {
	return &RevisionCache{
		cache:    map[IDAndRev]*list.Element{},
		lruList:  list.New(),
		capacity: capacity,
	}
}

// Looks up a revision from the cache.
// Returns the body of the revision, its history, and the set of channels it's in.
func (rc *RevisionCache) Get(docid, revid string) (Body, Body, base.Set) {
	if revid == "" {
		return nil, nil, nil // I can only get specific revisions
	}
	key := IDAndRev{DocID: docid, RevID: revid}
	rc.lock.Lock()
	elem := rc.cache[key]
	if elem != nil {
		rc.lruList.MoveToFront(elem)
	}
	rc.lock.Unlock()

	if elem == nil {
		return nil, nil, nil
	}

	value := elem.Value.(revCacheValue)
	return value.body.ShallowCopy(), value.history, value.channels
}

// Adds a revision to the cache.
func (rc *RevisionCache) Put(body Body, history Body, channels base.Set) {
	body = body.ShallowCopy()
	if history == nil {
		panic("Missing history for RevisionCache.Put")
	}
	value := revCacheValue{body: body, history: history, channels: channels}
	key := value.key()

	rc.lock.Lock()
	defer rc.lock.Unlock()
	elem := rc.lruList.PushFront(value)
	if rc.cache[key] == nil {
		rc.cache[key] = elem
		for len(rc.cache) > rc.capacity {
			rc.purgeOldest_()
		}
	}
}

func (value revCacheValue) key() IDAndRev {
	return IDAndRev{DocID: value.body["_id"].(string), RevID: value.body["_rev"].(string)}
}

func (rc *RevisionCache) purgeOldest_() {
	value := rc.lruList.Remove(rc.lruList.Back()).(revCacheValue)
	delete(rc.cache, value.key())
}
