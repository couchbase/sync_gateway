package db

import (
	"container/list"
	"errors"
	"expvar"
	"sync"
	"time"

	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/base"
)

// Number of recently-accessed doc revisions to cache in RAM
var KDefaultRevisionCacheCapacity uint32 = 5000
var KDefaultNumCacheShards uint16 = 8

type ShardedLRURevisionCache struct {
	caches    []*LRURevisionCache
	numShards uint16
}

// Creates a sharded revision cache with the given capacity and an optional loader function.
func NewShardedLRURevisionCache(capacity uint32, loaderFunc RevisionCacheLoaderFunc, statsCache *expvar.Map) *ShardedLRURevisionCache {

	numShards := KDefaultNumCacheShards
	if capacity == 0 {
		capacity = KDefaultRevisionCacheCapacity
	}

	caches := make([]*LRURevisionCache, numShards)
	perCacheCapacity := uint32(capacity/uint32(numShards)) + 1
	for i := 0; i < int(numShards); i++ {
		caches[i] = NewLRURevisionCache(perCacheCapacity, loaderFunc, statsCache)
	}

	return &ShardedLRURevisionCache{
		caches:    caches,
		numShards: numShards,
	}
}

func (sc *ShardedLRURevisionCache) getShard(docID string) *LRURevisionCache {
	return sc.caches[sgbucket.VBHash(docID, sc.numShards)]
}

func (sc *ShardedLRURevisionCache) Get(docID, revID string, copyType BodyCopyType) (docRev DocumentRevision, err error) {
	return sc.getShard(docID).Get(docID, revID, copyType)
}

func (sc *ShardedLRURevisionCache) Peek(docID, revID string) (docRev DocumentRevision, found bool) {
	return sc.getShard(docID).Peek(docID, revID)
}

func (sc *ShardedLRURevisionCache) UpdateDelta(docID, revID string, toDelta *RevisionDelta) {
	sc.getShard(docID).UpdateDelta(docID, revID, toDelta)
}

func (sc *ShardedLRURevisionCache) GetActive(docID string, context *DatabaseContext, copyType BodyCopyType) (docRev DocumentRevision, err error) {
	return sc.getShard(docID).GetActive(docID, context, copyType)
}

func (sc *ShardedLRURevisionCache) Put(docID string, docRev DocumentRevision) {
	sc.getShard(docID).Put(docID, docRev)
}

// An LRU cache of document revision bodies, together with their channel access.
type LRURevisionCache struct {
	cache       map[IDAndRev]*list.Element // Fast lookup of list element by doc/rev ID
	lruList     *list.List                 // List ordered by most recent access (Front is newest)
	capacity    uint32                     // Max number of revisions to cache
	loaderFunc  RevisionCacheLoaderFunc    // Function which does actual loading of something from rev cache
	lock        sync.Mutex                 // For thread-safety
	cacheHits   *expvar.Int
	cacheMisses *expvar.Int
}

// Callback function signature for loading something from the rev cache
type RevisionCacheLoaderFunc func(id IDAndRev) (body Body, history Revisions, channels base.Set, attachments AttachmentsMeta, expiry *time.Time, err error)

// The cache payload data. Stored as the Value of a list Element.
type revCacheValue struct {
	key         IDAndRev        // doc/rev IDs
	body        Body            // Revision body (a pristine shallow copy)
	history     Revisions       // Rev history encoded like a "_revisions" property
	channels    base.Set        // Set of channels that have access
	expiry      *time.Time      // Document expiry
	attachments AttachmentsMeta // Document _attachments property
	delta       *RevisionDelta  // Available delta *from* this revision
	err         error           // Error from loaderFunc if it failed
	lock        sync.RWMutex    // Synchronizes access to this struct
}

// Creates a revision cache with the given capacity and an optional loader function.
func NewLRURevisionCache(capacity uint32, loaderFunc RevisionCacheLoaderFunc, statsCache *expvar.Map) *LRURevisionCache {

	if capacity == 0 {
		capacity = KDefaultRevisionCacheCapacity
	}

	return &LRURevisionCache{
		cache:       map[IDAndRev]*list.Element{},
		lruList:     list.New(),
		capacity:    capacity,
		loaderFunc:  loaderFunc,
		cacheHits:   statsCache.Get(base.StatKeyRevisionCacheHits).(*expvar.Int),
		cacheMisses: statsCache.Get(base.StatKeyRevisionCacheMisses).(*expvar.Int),
	}
}

// Looks up a revision from the cache.
// Returns the body of the revision, its history, and the set of channels it's in.
// If the cache has a loaderFunction, it will be called if the revision isn't in the cache;
// any error returned by the loaderFunction will be returned from Get.
func (rc *LRURevisionCache) Get(docID, revID string, copyType BodyCopyType) (DocumentRevision, error) {
	return rc.getFromCache(docID, revID, copyType, rc.loaderFunc != nil)
}

// Looks up a revision from the cache only.  Will not fall back to loader function if not
// present in the cache.
func (rc *LRURevisionCache) Peek(docID, revID string) (docRev DocumentRevision, found bool) {
	docRev, err := rc.getFromCache(docID, revID, BodyShallowCopy, false)
	if err != nil {
		return DocumentRevision{}, false
	}
	return docRev, docRev.Body != nil
}

// Attempt to update the delta on a revision cache entry.  If the entry is no longer resident in the cache,
// fails silently
func (rc *LRURevisionCache) UpdateDelta(docID, revID string, toDelta *RevisionDelta) {
	value := rc.getValue(docID, revID, false)
	if value != nil {
		value.updateDelta(toDelta)
	}
}

func (rc *LRURevisionCache) getFromCache(docID, revID string, copyType BodyCopyType, loadOnCacheMiss bool) (DocumentRevision, error) {
	value := rc.getValue(docID, revID, loadOnCacheMiss)
	if value == nil {
		return DocumentRevision{}, nil
	}
	docRev, statEvent, err := value.load(rc.loaderFunc, copyType)
	rc.statsRecorderFunc(statEvent)

	if err != nil {
		rc.removeValue(value) // don't keep failed loads in the cache
	}
	return docRev, err
}

// Attempts to retrieve the active revision for a document from the cache.  Requires retrieval
// of the document from the bucket to guarantee the current active revision, but does minimal unmarshalling
// of the retrieved document to get the current rev from _sync metadata.  If active rev is already in the
// rev cache, will use it.  Otherwise will add to the rev cache using the raw document obtained in the
// initial retrieval.
func (rc *LRURevisionCache) GetActive(docID string, context *DatabaseContext, copyType BodyCopyType) (docRev DocumentRevision, err error) {

	// Look up active rev for doc
	bucketDoc, getErr := context.GetDocument(docID, DocUnmarshalSync)
	if getErr != nil {
		return DocumentRevision{}, getErr
	}
	if bucketDoc == nil {
		return DocumentRevision{}, nil
	}

	// Retrieve from or add to rev cache
	value := rc.getValue(docID, bucketDoc.CurrentRev, true)
	docRev, statEvent, err := value.loadForDoc(bucketDoc, context, copyType)
	rc.statsRecorderFunc(statEvent)

	if err != nil {
		rc.removeValue(value) // don't keep failed loads in the cache
	}
	return docRev, err
}

func (rc *LRURevisionCache) statsRecorderFunc(cacheHit bool) {

	if cacheHit {
		rc.cacheHits.Add(1)
	} else {
		rc.cacheMisses.Add(1)
	}
}

// Adds a revision to the cache.
func (rc *LRURevisionCache) Put(docID string, docRev DocumentRevision) {
	if docRev.History == nil {
		panic("Missing history for RevisionCache.Put")
	}
	value := rc.getValue(docID, docRev.RevID, true)
	value.store(docRev)
}

func (rc *LRURevisionCache) getValue(docID, revID string, create bool) (value *revCacheValue) {
	if docID == "" || revID == "" {
		panic("RevisionCache: invalid empty doc/rev id")
	}
	key := IDAndRev{DocID: docID, RevID: revID}
	rc.lock.Lock()
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
	rc.lock.Unlock()
	return
}

func (rc *LRURevisionCache) removeValue(value *revCacheValue) {
	rc.lock.Lock()
	if element := rc.cache[value.key]; element != nil && element.Value == value {
		rc.lruList.Remove(element)
		delete(rc.cache, value.key)
	}
	rc.lock.Unlock()
}

func (rc *LRURevisionCache) purgeOldest_() {
	value := rc.lruList.Remove(rc.lruList.Back()).(*revCacheValue)
	delete(rc.cache, value.key)
}

// Gets the body etc. out of a revCacheValue. If they aren't present already, the loader func
// will be called. This is synchronized so that the loader will only be called once even if
// multiple goroutines try to load at the same time.
func (value *revCacheValue) load(loaderFunc RevisionCacheLoaderFunc, copyType BodyCopyType) (docRev DocumentRevision, cacheHit bool, err error) {

	// Attempt to read cached value
	value.lock.RLock()
	if value.body != nil || value.err != nil {
		docRev, err = value._asDocumentRevision(copyType)
		value.lock.RUnlock()
		return docRev, true, err
	}
	value.lock.RUnlock()

	// Cached value not found - attempt to load if loaderFunc provided
	if loaderFunc == nil {
		return DocumentRevision{}, false, errors.New("No loader function defined for revision cache")
	}
	value.lock.Lock()
	// Check if the value was loaded while we waited for the lock - if so, return.
	if value.body != nil || value.err != nil {
		cacheHit = true
	} else {
		cacheHit = false
		value.body, value.history, value.channels, value.attachments, value.expiry, value.err = loaderFunc(value.key)
	}

	docRev, err = value._asDocumentRevision(copyType)
	value.lock.Unlock()
	return docRev, cacheHit, err
}

// _asDocumentRevision copies the rev cache value into a DocumentRevision.  Requires callers hold at least the read lock on value.lock
func (value *revCacheValue) _asDocumentRevision(copyType BodyCopyType) (DocumentRevision, error) {
	return DocumentRevision{
		RevID:       value.key.RevID,
		Body:        value.body.Copy(copyType), // Never let the caller mutate the stored body
		History:     value.history,
		Channels:    value.channels,
		Expiry:      value.expiry,
		Attachments: value.attachments.ShallowCopy(), // Avoid caller mutating the stored attachments
		Delta:       value.delta,
	}, value.err
}

// Retrieves the body etc. out of a revCacheValue.  If they aren't already present, loads into the cache value using
// the provided document.
func (value *revCacheValue) loadForDoc(doc *document, context *DatabaseContext, copyType BodyCopyType) (docRev DocumentRevision, cacheHit bool, err error) {

	value.lock.RLock()
	if value.body != nil || value.err != nil {
		docRev, err = value._asDocumentRevision(copyType)
		value.lock.RUnlock()
		return docRev, true, err
	}
	value.lock.RUnlock()

	value.lock.Lock()
	// Check if the value was loaded while we waited for the lock - if so, return.
	if value.body != nil || value.err != nil {
		cacheHit = true
	} else {
		cacheHit = false
		value.body, value.history, value.channels, value.attachments, value.expiry, value.err = context.revCacheLoaderForDocument(doc, value.key.RevID)
	}

	docRev, err = value._asDocumentRevision(copyType)
	value.lock.Unlock()
	return docRev, cacheHit, err
}

// Stores a body etc. into a revCacheValue if there isn't one already.
func (value *revCacheValue) store(docRev DocumentRevision) {
	value.lock.Lock()
	if value.body == nil {
		value.body = docRev.Body.ShallowCopy() // Don't store a body the caller might later mutate
		value.body[BodyId] = value.key.DocID   // Rev cache includes id and rev in the body.  Ensure they are set in case callers aren't passing
		value.body[BodyRev] = value.key.RevID
		value.history = docRev.History
		value.channels = docRev.Channels
		value.expiry = docRev.Expiry
		value.attachments = docRev.Attachments.ShallowCopy() // Don't store attachments the caller might later mutate
		value.err = nil
	}
	value.lock.Unlock()
}

func (value *revCacheValue) updateDelta(toDelta *RevisionDelta) {
	value.lock.Lock()
	value.delta = toDelta
	value.lock.Unlock()
}
