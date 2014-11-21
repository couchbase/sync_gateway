package db

import (
	"fmt"
	"testing"
}

func EventManagerTest(t *testing.T) {
	ids := make([]string, 20)
	for i := 0; i < 20; i++ {
		ids[i] = fmt.Sprintf("%d", i)
	}

	revForTest := func(i int) (Body, Body, base.Set) {
		body := Body{
			"_id":  ids[i],
			"_rev": "x",
		}
		history := Body{"start": i}
		return body, history, nil
	}
	verify := func(body Body, history Body, channels base.Set, i int) {
		if body == nil {
			t.Fatalf("nil body at #%d", i)
		}
		assert.True(t, body != nil)
		assert.Equals(t, body["_id"], ids[i])
		assert.True(t, history != nil)
		assert.Equals(t, history["start"], i)
		assert.DeepEquals(t, channels, base.Set(nil))
	}

	cache := NewRevisionCache(10, nil)
	for i := 0; i < 10; i++ {
		body, history, channels := revForTest(i)
		cache.Put(body, history, channels)
	}

	for i := 0; i < 10; i++ {
		body, history, channels, _ := cache.Get(ids[i], "x")
		verify(body, history, channels, i)
	}

	for i := 10; i < 13; i++ {
		body, history, channels := revForTest(i)
		cache.Put(body, history, channels)
	}

	for i := 0; i < 3; i++ {
		body, _, _, _ := cache.Get(ids[i], "x")
		assert.True(t, body == nil)
	}
	for i := 3; i < 13; i++ {
		body, history, channels, _ := cache.Get(ids[i], "x")
		verify(body, history, channels, i)
	}
}