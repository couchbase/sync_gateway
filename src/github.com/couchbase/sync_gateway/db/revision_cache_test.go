package db

import (
	"fmt"
	"testing"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbaselabs/go.assert"
)

func TestRevisionCache(t *testing.T) {
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

func TestLoaderFunction(t *testing.T) {
	var callsToLoader = 0
	loader := func(id IDAndRev) (body Body, history Body, channels base.Set, err error) {
		callsToLoader++
		if id.DocID[0] != 'J' {
			err = base.HTTPErrorf(404, "missing")
		} else {
			body = Body{
				"_id":  id.DocID,
				"_rev": id.RevID,
			}
			history = Body{"start": 1}
			channels = base.SetOf("*")
		}
		return
	}
	cache := NewRevisionCache(10, loader)

	body, history, channels, err := cache.Get("Jens", "1")
	assert.Equals(t, body["_id"], "Jens")
	assert.True(t, history != nil)
	assert.True(t, channels != nil)
	assert.Equals(t, err, error(nil))
	assert.Equals(t, callsToLoader, 1)

	body, history, channels, err = cache.Get("Peter", "1")
	assert.DeepEquals(t, body, Body(nil))
	assert.DeepEquals(t, err, base.HTTPErrorf(404, "missing"))
	assert.Equals(t, callsToLoader, 2)

	body, history, channels, err = cache.Get("Jens", "1")
	assert.Equals(t, body["_id"], "Jens")
	assert.True(t, history != nil)
	assert.True(t, channels != nil)
	assert.Equals(t, err, error(nil))
	assert.Equals(t, callsToLoader, 2)

	body, history, channels, err = cache.Get("Peter", "1")
	assert.DeepEquals(t, body, Body(nil))
	assert.DeepEquals(t, err, base.HTTPErrorf(404, "missing"))
	assert.Equals(t, callsToLoader, 3)
}
