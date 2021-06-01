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
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRangeSafeCollection(t *testing.T) {

	rsc := NewRangeSafeCollection()
	assert.Equal(t, rsc.Length(), 0)

	// Insert item
	actual, created, length := rsc.GetOrInsert("key1", "value1")
	assert.Equal(t, "value1", actual.(string))
	assert.True(t, created)
	assert.Equal(t, 1, length)

	// Attempt to re-insert the same key
	actual, created, length = rsc.GetOrInsert("key1", "value2")
	assert.Equal(t, "value1", actual.(string))
	assert.False(t, created)
	assert.Equal(t, 1, length)

	// Insert a second item
	actual, created, length = rsc.GetOrInsert("key2", "value2")
	assert.Equal(t, "value2", actual.(string))
	assert.True(t, created)
	assert.Equal(t, 2, length)

	// Get an item
	value, ok := rsc.Get("key2")
	assert.Equal(t, "value2", value.(string))
	assert.True(t, ok)

	// Attempt to get a non-existent item
	value, ok = rsc.Get("key3")
	assert.False(t, ok)

	// Remove an item
	length = rsc.Remove("key1")
	assert.Equal(t, 1, length)

	// Attempt to remove a non-existent item
	length = rsc.Remove("key1")
	assert.Equal(t, 1, length)

	// Remove the last item
	length = rsc.Remove("key2")
	assert.Equal(t, 0, length)

	// Attempt to remove from empty set
	length = rsc.Remove("key3")
	assert.Equal(t, 0, length)

}

func TestRangeSafeCollectionRange(t *testing.T) {

	rsc := NewRangeSafeCollection()
	assert.Equal(t, rsc.Length(), 0)

	// Add  elements to the set
	for i := 0; i < 10; i++ {
		rsc.GetOrInsert(fmt.Sprintf("key_%d", i), fmt.Sprintf("value_%d", i))
	}

	// Simple range
	count := 0
	countFunc := func(value interface{}) bool {
		count++
		return true
	}

	rsc.Range(countFunc)
	assert.Equal(t, 10, count)

	// Range with concurrent insert
	count = 0
	appendFunc := func(value interface{}) bool {
		count++
		rsc.GetOrInsert(fmt.Sprintf("key2_%d", count), fmt.Sprintf("value2_%d", count))
		return true
	}

	rsc.Range(appendFunc)
	assert.Equal(t, 10, count)
	assert.Equal(t, 20, rsc.Length())

	// Range elements, terminate range after 5
	elementSet := make([]*AppendOnlyListElement, 0)
	count = 0
	firstFiveFunc := func(item *AppendOnlyListElement) bool {
		count++
		if count > 5 {
			return false
		}
		elementSet = append(elementSet, item)
		return true
	}
	rsc.RangeElements(firstFiveFunc)

	// Remove elements
	postRemoveLen := rsc.RemoveElements(elementSet)
	assert.Equal(t, 15, postRemoveLen)

	// Verify locking for concurrent range, remove
	var wg sync.WaitGroup
	wg.Add(2)
	// Range a few times
	go func() {
		count := 0
		countFunc := func(value interface{}) bool {
			count++
			return true
		}
		rsc.Range(countFunc)
		rsc.Range(countFunc)
		rsc.Range(countFunc)
		wg.Done()
	}()

	// Remove a few times
	go func() {
		rsc.Remove("key2_1")
		rsc.Remove("key2_2")
		rsc.Remove("key2_3")
		wg.Done()
	}()

	wg.Wait()
	assert.Equal(t, 12, rsc.Length())

}
