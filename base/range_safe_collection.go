/*
Copyright 2019-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package base

import (
	"sync"
)

// RangeSafeCollection is a concurrency-safe collection comprising an AppendOnlyList and a
// map for key-based retrieval of list elements.  It has the following characteristics
//  - concurrency-safe
//  - snapshot-based iteration, Range doesn't block Append
//  - key-based access to entries
type RangeSafeCollection struct {
	valueMap    map[string]*AppendOnlyListElement // map from key to collection elements
	valueList   AppendOnlyList                    // List of collection elements
	valueLock   sync.RWMutex                      // Mutex for valueMap interactions.
	iterateLock sync.RWMutex                      // Mutex for list iteration.  Write lock required for any non-append list mutations.
}

// NewRangeSafeCollection creates and initializes a new RangeSafeCollection
func NewRangeSafeCollection() *RangeSafeCollection {
	ism := &RangeSafeCollection{}
	ism.Init()
	return ism
}

// Init initializes or resets a RangeSafeCollection
func (r *RangeSafeCollection) Init() {
	r.iterateLock.Lock()
	r.valueLock.Lock()

	r.valueMap = make(map[string](*AppendOnlyListElement), 0)
	r.valueList.Init()

	r.valueLock.Unlock()
	r.iterateLock.Unlock()
}

// Remove removes an entry from the collection, and returns the new length of the collection.
// Obtains both the iterate and value lock, so will block on active Range operations.
func (r *RangeSafeCollection) Remove(key string) (length int) {
	r.iterateLock.Lock()
	r.valueLock.Lock()

	if item, ok := r.valueMap[key]; ok {
		r.valueList.Remove(item)
		delete(r.valueMap, key)
	}

	length = len(r.valueMap)

	r.valueLock.Unlock()
	r.iterateLock.Unlock()
	return length

}

// Remove removes a set of list elements from the collection, and returns the new length of the collection.
// Obtains both the iterate and value lock, so will block on active Range operations.
func (r *RangeSafeCollection) RemoveElements(elements []*AppendOnlyListElement) (length int) {
	r.iterateLock.Lock()
	r.valueLock.Lock()

	for _, item := range elements {
		key := item.key
		r.valueList.Remove(item)
		delete(r.valueMap, key)
	}
	length = len(r.valueMap)

	r.valueLock.Unlock()
	r.iterateLock.Unlock()
	return length
}

// Get performs map-style retrieval from the collection.
func (r *RangeSafeCollection) Get(key string) (value interface{}, ok bool) {

	r.valueLock.RLock()

	item, ok := r.valueMap[key]
	if ok {
		value = item.Value
	}

	r.valueLock.RUnlock()
	return value, ok

}

// GetOrInsert returns the value of the specified key if already present in the collection - otherwise
// will append the element to the collection.  Does not block on active Range operations.
func (r *RangeSafeCollection) GetOrInsert(key string, value interface{}) (actual interface{}, created bool, length int) {

	r.valueLock.Lock()

	item, ok := r.valueMap[key]
	if ok {
		length = len(r.valueMap)
		r.valueLock.Unlock()
		return item.Value, false, length
	}
	item = r.valueList.PushBack(key, value)
	r.valueMap[key] = item
	length = len(r.valueMap)

	r.valueLock.Unlock()
	return value, true, length

}

// Length returns the length of the collection.
func (r *RangeSafeCollection) Length() int {
	r.valueLock.RLock()
	length := len(r.valueMap)
	r.valueLock.RUnlock()
	return length
}

// Range iterates over the set up to the last element at the time range was called, invoking
// the callback function with the element value.
func (r *RangeSafeCollection) Range(f func(value interface{}) bool) {

	wrapperFunc := func(item *AppendOnlyListElement) bool {
		return f(item.Value)
	}
	r.RangeElements(wrapperFunc)
}

// RangeElements iterates over the set up to the last element at the time range was called, invoking
// the callback function with the element.  Exposed to facilitate subsequent invocation of
// RemoveElements.
// Range function invocations (f) occur while holding a read lock for r.iterateLock - calls to r.Remove/r.RemoveElements
// or nested range invocations within f will deadlock.
func (r *RangeSafeCollection) RangeElements(f func(item *AppendOnlyListElement) bool) {

	r.iterateLock.RLock()

	r.valueLock.RLock()
	lastElem := r.valueList.Back()
	elem := r.valueList.Front()
	r.valueLock.RUnlock()

	// Empty list handling
	if elem == nil || lastElem == nil {
		r.iterateLock.RUnlock()
		return
	}

	for {
		shouldContinue := f(elem)
		if !shouldContinue {
			break
		}
		if elem == lastElem {
			break
		}
		elem = elem.Next()
	}

	r.iterateLock.RUnlock()
}

// AppendOnlyListElement is an element of an AppendOnlyList.
type AppendOnlyListElement struct {
	// Next and previous pointers in the doubly-linked list of elements.
	// To simplify the implementation, internally a list l is implemented
	// as a ring, such that &l.root is both the next element of the last
	// list element (l.Back()) and the previous element of the first list
	// element (l.Front()).
	next, prev *AppendOnlyListElement

	// The list to which this element belongs.
	list *AppendOnlyList

	// The key for this element in the map.  Used when retrieving element from list
	key string

	// The value stored with this element.
	Value interface{}
}

// Next returns the next list element or nil.
func (e *AppendOnlyListElement) Next() *AppendOnlyListElement {
	if p := e.next; e.list != nil && p != &e.list.root {
		return p
	}
	return nil
}

// Prev returns the previous list element or nil.
func (e *AppendOnlyListElement) Prev() *AppendOnlyListElement {
	if p := e.prev; e.list != nil && p != &e.list.root {
		return p
	}
	return nil
}

// AppendOnlyList is a doubly-linked list that only supports adding elements to the list
// using PushBack.  Modelled on container/list, but modified to support
// concurrent iteration and PushBack calls.
type AppendOnlyList struct {
	root AppendOnlyListElement // sentinel list element, only &root, root.prev, and root.next are used
}

// Init initializes or clears list l.
func (l *AppendOnlyList) Init() *AppendOnlyList {
	l.root.next = &l.root
	l.root.prev = &l.root
	return l
}

// Front returns the first element of list l or nil if the list is empty.
func (l *AppendOnlyList) Front() *AppendOnlyListElement {
	if l.root.next == &l.root {
		return nil
	}
	return l.root.next
}

// Back returns the last element of list l or nil if the list is empty.
func (l *AppendOnlyList) Back() *AppendOnlyListElement {
	if l.root.prev == &l.root {
		return nil
	}
	return l.root.prev
}

// lazyInit lazily initializes a zero List value.
func (l *AppendOnlyList) lazyInit() {
	if l.root.next == nil {
		l.Init()
	}
}

// insert inserts e after at, and returns e.
func (l *AppendOnlyList) append(e *AppendOnlyListElement) *AppendOnlyListElement {
	lastElem := l.root.prev
	lastElem.next = e
	e.prev = lastElem
	e.next = &l.root
	l.root.prev = e
	e.list = l
	return e
}

// insertValue is a convenience wrapper for insert(&Element{Value: v}, at).
func (l *AppendOnlyList) appendValue(k string, v interface{}) *AppendOnlyListElement {
	return l.append(&AppendOnlyListElement{key: k, Value: v})
}

// remove removes e from its list, decrements l.len, and returns e.
func (l *AppendOnlyList) remove(e *AppendOnlyListElement) *AppendOnlyListElement {
	e.prev.next = e.next
	e.next.prev = e.prev
	e.next = nil // avoid memory leaks
	e.prev = nil // avoid memory leaks
	e.list = nil
	return e
}

// Remove removes e from l if e is an element of list l.
// It returns the element value e.Value.
// The element must not be nil.
func (l *AppendOnlyList) Remove(e *AppendOnlyListElement) interface{} {
	if e.list == l {
		// if e.list == l, l must have been initialized when e was inserted
		// in l or l == nil (e is a zero Element) and l.remove will crash
		l.remove(e)
	}
	return e.Value
}

// PushBack inserts a new element e with value v at the back of list l and returns e.
func (l *AppendOnlyList) PushBack(key string, v interface{}) *AppendOnlyListElement {
	l.lazyInit()
	return l.appendValue(key, v)
}
