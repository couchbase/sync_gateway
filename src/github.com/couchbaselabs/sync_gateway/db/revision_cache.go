package db

import (
	"container/list"
	"sync"
)

// An LRU cache of document revision bodies.
type RevisionCache struct {
	cache    map[IDAndRev]*list.Element
	ordered  *list.List
	capacity int
	lock     sync.Mutex
}

func NewRevisionCache(capacity int) *RevisionCache {
	return &RevisionCache{
		cache:    map[IDAndRev]*list.Element{},
		ordered:  list.New(),
		capacity: capacity,
	}
}

func (rc *RevisionCache) Get(docid, revid string, withRevisions bool) Body {
	key := IDAndRev{DocID: docid, RevID: revid}
	rc.lock.Lock()
	elem := rc.cache[key]
	if elem != nil {
		rc.ordered.MoveToFront(elem)
	}
	rc.lock.Unlock()

	if elem == nil {
		return nil
	}

	body := elem.Value.(Body).ShallowCopy()
	if !withRevisions {
		delete(body, "_revisions")
	} else if body["_revisions"] == nil {
		return nil
	}
	return body
}

func (rc *RevisionCache) Put(body Body, revisions interface{}) {
	key := rc.keyForBody(body)
	body = body.ShallowCopy()
	if revisions != nil {
		body["_revisions"] = revisions
	}

	rc.lock.Lock()
	elem := rc.ordered.PushFront(body)
	rc.cache[key] = elem
	for len(rc.cache) > rc.capacity {
		rc.purgeOldest()
	}
	rc.lock.Unlock()
}

func (rc *RevisionCache) keyForBody(body Body) IDAndRev {
	return IDAndRev{DocID: body["_id"].(string), RevID: body["_rev"].(string)}
}

func (rc *RevisionCache) purgeOldest() {
	body := rc.ordered.Remove(rc.ordered.Back()).(Body)
	key := rc.keyForBody(body)
	delete(rc.cache, key)
}
