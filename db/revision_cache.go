package db

import (
	"container/list"
	"sync"

	"github.com/couchbase/sync_gateway/base"
)

// Number of recently-accessed doc revisions to cache in RAM
var KDefaultRevisionCacheCapacity uint32 = 5000

// An LRU cache of document revision bodies, together with their channel access.
type RevisionCache struct {
	cache      map[IDAndRev]*list.Element // Fast lookup of list element by doc/rev ID
	lruList    *list.List                 // List ordered by most recent access (Front is newest)
	capacity   uint32                     // Max number of revisions to cache
	loaderFunc RevisionCacheLoaderFunc
	lock       sync.Mutex // For thread-safety
}

type RevisionCacheLoaderFunc func(id IDAndRev) (body Body, history Revisions, channels base.Set, err error)

// The cache payload data. Stored as the Value of a list Element.
type revCacheValue struct {
	key      IDAndRev   // doc/rev IDs
	body     Body       // Revision body (a pristine shallow copy)
	history  Revisions  // Rev history encoded like a "_revisions" property
	channels base.Set   // Set of channels that have access
	err      error      // Error from loaderFunc if it failed
	lock     sync.Mutex // Synchronizes access to this struct
}

// Creates a revision cache with the given capacity and an optional loader function.
func NewRevisionCache(capacity uint32, loaderFunc RevisionCacheLoaderFunc) *RevisionCache {

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
func (rc *RevisionCache) Get(docid, revid string) (Body, Revisions, base.Set, error) {
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

// Attempts to retrieve the active revision for a document from the cache.  Requires retrieval
// of the document from the bucket to guarantee the current active revision, but does minimal unmarshalling
// of the retrieved document to get the current rev from _sync metadata.  If active rev is already in the
// rev cache, will use it.  Otherwise will add to the rev cache using the raw document obtained in the
// initial retrieval.
func (rc *RevisionCache) GetActive(docid string, context *DatabaseContext) (body Body, history Revisions, channels base.Set, currentRev string, err error) {

	// Look up active rev for doc
	bucketDoc, getErr := context.GetDocument(docid, DocUnmarshalSync)
	if getErr != nil {
		return nil, nil, nil, "", getErr
	}
	if bucketDoc == nil {
		return nil, nil, nil, "", nil
	}

	currentRev = bucketDoc.CurrentRev

	// Retrieve from or add to rev cache
	value := rc.getValue(docid, currentRev, true)
	body, history, channels, err = value.loadForDoc(bucketDoc, context)
	if err != nil {
		rc.removeValue(value) // don't keep failed loads in the cache
	}
	return body, history, channels, currentRev, err
}

// Adds a revision to the cache.
func (rc *RevisionCache) Put(docid string, revid string, body Body, history Revisions, channels base.Set) {
	if history == nil {
		panic("Missing history for RevisionCache.Put")
	}
	value := rc.getValue(docid, revid, true)
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
		for len(rc.cache) > int(rc.capacity) {
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
func (value *revCacheValue) load(loaderFunc RevisionCacheLoaderFunc) (Body, Revisions, base.Set, error) {
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

// Retrieves the body etc. out of a revCacheValue.  If they aren't already present, loads into the cache value using
// the provided document.
func (value *revCacheValue) loadForDoc(doc *document, context *DatabaseContext) (Body, Revisions, base.Set, error) {
	value.lock.Lock()
	defer value.lock.Unlock()
	if value.body == nil && value.err == nil {
		base.StatsExpvars.Add("revisionCache_misses", 1)
		value.body, value.history, value.channels, value.err = context.revCacheLoaderForDocument(doc, value.key.RevID)
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
func (value *revCacheValue) store(body Body, history Revisions, channels base.Set) {
	value.lock.Lock()
	if value.body == nil {
		value.body = body.ShallowCopy()      // Don't store a body the caller might later mutate
		value.body[BodyId] = value.key.DocID // Rev cache includes id and rev in the body.  Ensure they are set in case callers aren't passing
		value.body[BodyRev] = value.key.RevID
		value.history = history
		value.channels = channels
		value.err = nil
		dbExpvars.Add("revisionCache_adds", 1)
	}
	value.lock.Unlock()
}
