package auth

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/couchbase/sync_gateway/base"
)

func TestCache(t *testing.T) {
	defer base.SetUpTestLogging(base.LevelInfo, base.KeyAuth)()
	cache := NewCache(2)

	k1 := key()
	cache.Put(k1)
	assert.Equal(t, 1, cache.Len())
	assert.True(t, cache.Contains(k1))

	k2 := key()
	cache.Put(k2)
	assert.Equal(t, 2, cache.Len())
	assert.True(t, cache.Contains(k1) && cache.Contains(k2))

	k3 := key()
	cache.Put(k3)
	assert.Equal(t, 2, cache.Len())
	assert.True(t, cache.Contains(k1) && cache.Contains(k3) || cache.Contains(k2) && cache.Contains(k3))

	k4 := key()
	cache.Put(k4)
	assert.Equal(t, 2, cache.Len())
	assert.True(t, cache.Contains(k1) && cache.Contains(k4) || cache.Contains(k2) && cache.Contains(k4) || cache.Contains(k3) && cache.Contains(k4))
}

func key() (key string) {
	uniq := time.Now().UnixNano()
	key = fmt.Sprintf("k%d", uniq)
	return key
}

func TestCacheRace(t *testing.T) {
	defer base.SetUpTestLogging(base.LevelInfo, base.KeyAuth)()
	var wg sync.WaitGroup
	cache := NewCache(5)

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(cache *Cache, i int) {
			defer wg.Done()
			key := fmt.Sprintf("k%d", i)
			cache.Put(key)
			base.Infof(base.KeyAuth, "Goroutine%v Put key: %v", i, key)
		}(cache, i)

		wg.Add(1)
		go func(cache *Cache, i int) {
			defer wg.Done()
			key := fmt.Sprintf("k%d", i)
			ok := cache.Contains(key)
			base.Infof(base.KeyAuth, "Goroutine%v Contains: %v, key: %v", i, ok, key)
		}(cache, i)
	}
	wg.Wait()
	assert.Equal(t, 5, cache.Len())

	cache.lock.RLock()
	for key, value := range cache.cache {
		base.Infof(base.KeyAuth, "Item: key: %v, value %v", key, value)
	}
	cache.lock.RUnlock()
}

func BenchmarkCachePut(b *testing.B) {
	b.ReportAllocs()
	cache := NewCache(kMaxCacheSize)
	for i := 0; i < b.N; i++ {
		cache.Put(key())
	}
}

func BenchmarkParallelCachePut(b *testing.B) {
	b.ReportAllocs()
	cache := NewCache(kMaxCacheSize)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			cache.Put(key())
		}
	})
}

func BenchmarkCachePutOverflow(b *testing.B) {
	b.ReportAllocs()
	b.StopTimer()
	cache := NewCache(kMaxCacheSize)
	for i := 0; i < kMaxCacheSize; i++ {
		cache.Put(key())
	}
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		cache.Put(key())
	}
}

func BenchmarkParallelCachePutOverflow(b *testing.B) {
	b.ReportAllocs()
	b.StopTimer()
	cache := NewCache(kMaxCacheSize)
	for i := 0; i < kMaxCacheSize; i++ {
		cache.Put(key())
	}
	b.StartTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			cache.Put(key())
		}
	})
}
