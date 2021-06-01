/*
Copyright 2016-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package base

import (
	"fmt"
	"testing"

	goassert "github.com/couchbaselabs/go.assert"
)

type testStruct struct {
	x int
	y int
}

func keyForTest(i int) string {
	return fmt.Sprintf("key-%d", i)
}

func valueForTest(i int) *testStruct {
	return &testStruct{
		x: i,
		y: i * i,
	}
}

func verifyValue(t *testing.T, value *testStruct, i int) {
	goassert.True(t, value != nil)
	goassert.Equals(t, value.x, i)
	goassert.Equals(t, value.y, i*i)
}

func TestLRUCache(t *testing.T) {
	ids := make([]string, 20)
	for i := 0; i < 20; i++ {
		ids[i] = fmt.Sprintf("%d", i)
	}

	cache, err := NewLRUCache(10)
	goassert.True(t, err == nil)
	for i := 0; i < 10; i++ {
		key := keyForTest(i)
		value := valueForTest(i)
		cache.Put(key, value)
	}

	for i := 0; i < 10; i++ {
		value, _ := cache.Get(keyForTest(i))
		testValue, ok := value.(*testStruct)
		goassert.True(t, ok)
		verifyValue(t, testValue, i)
	}

	for i := 10; i < 13; i++ {
		key := keyForTest(i)
		value := valueForTest(i)
		cache.Put(key, value)
	}

	for i := 0; i < 3; i++ {
		value, _ := cache.Get(keyForTest(i))
		goassert.True(t, value == nil)
	}
	for i := 3; i < 13; i++ {
		value, _ := cache.Get(keyForTest(i))
		testValue, ok := value.(*testStruct)
		goassert.True(t, ok)
		verifyValue(t, testValue, i)
	}
}
