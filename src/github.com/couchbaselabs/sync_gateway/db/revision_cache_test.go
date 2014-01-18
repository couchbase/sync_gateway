package db

import (
	"fmt"
	"testing"

	"github.com/couchbaselabs/go.assert"
)

func TestRevisionCache(t *testing.T) {
	ids := make([]string, 20)
	for i := 0; i < 20; i++ {
		ids[i] = fmt.Sprintf("%d", i)
	}

	cache := NewRevisionCache(10)
	for i := 0; i < 10; i++ {
		body := Body{
			"_id":  ids[i],
			"_rev": "x",
		}
		cache.Put(body, nil)
	}

	for i := 0; i < 10; i++ {
		body := cache.Get(ids[i], "x", false)
		assert.True(t, body != nil)
		assert.Equals(t, body["_id"], ids[i])
	}

	for i := 10; i < 13; i++ {
		body := Body{
			"_id":  ids[i],
			"_rev": "x",
		}
		cache.Put(body, nil)
	}

	for i := 0; i < 3; i++ {
		body := cache.Get(ids[i], "x", false)
		assert.True(t, body == nil)
	}
	for i := 3; i < 13; i++ {
		body := cache.Get(ids[i], "x", false)
		assert.True(t, body != nil)
		assert.Equals(t, body["_id"], ids[i])
	}
}
