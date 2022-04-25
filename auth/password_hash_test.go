/*
Copyright 2018-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package auth

import (
	"fmt"
	"math"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/couchbase/sync_gateway/base"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/bcrypt"
)

// BenchmarkBcryptCostTime will output the time it takes to hash a password with each bcrypt cost value
func BenchmarkBcryptCostTimes(b *testing.B) {
	// Little value in running this regularly. Might be useful for one-off informational purposes
	b.Skip("Benchmark disabled")

	minCostToTest := bcrypt.DefaultCost
	maxCostToTest := bcrypt.DefaultCost + 5

	for i := minCostToTest; i < maxCostToTest; i++ {
		b.Run(fmt.Sprintf("cost%d", i), func(bn *testing.B) {
			bn.N = 1
			_, err := bcrypt.GenerateFromPassword([]byte("hunter2"), i)
			assert.NoError(bn, err)
		})
	}
}

// TestBcryptDefaultCostTime will ensure that the default bcrypt cost takes at least a 'reasonable' amount of time
// If this test fails, it suggests maybe we need to think about increasing the default cost...
func TestBcryptDefaultCostTime(t *testing.T) {
	// Modest 2.2GHz macbook i7 takes ~80ms at cost 10
	// Assume server CPUs are ~2x faster
	minimumDuration := 40 * time.Millisecond

	startTime := time.Now()
	_, err := bcrypt.GenerateFromPassword([]byte("hunter2"), DefaultBcryptCost)
	duration := time.Since(startTime)

	t.Logf("bcrypt.GenerateFromPassword with cost %d took: %v", DefaultBcryptCost, duration)
	assert.NoError(t, err)
	assert.True(t, minimumDuration < duration)
}

func TestSetBcryptCost(t *testing.T) {
	bucket := base.GetTestBucket(t)
	defer bucket.Close()

	auth := NewAuthenticator(bucket, nil, DefaultAuthenticatorOptions())

	err := auth.SetBcryptCost(DefaultBcryptCost - 1) // below minimum allowed value
	assert.Equal(t, ErrInvalidBcryptCost, errors.Cause(err))
	assert.Equal(t, DefaultBcryptCost, auth.BcryptCost)
	assert.False(t, auth.bcryptCostChanged)

	err = auth.SetBcryptCost(0) // use default value
	assert.NoError(t, err)
	assert.Equal(t, DefaultBcryptCost, auth.BcryptCost)
	assert.False(t, auth.bcryptCostChanged) // Not explicitly changed

	err = auth.SetBcryptCost(DefaultBcryptCost + 1) // use increased value
	assert.NoError(t, err)
	assert.Equal(t, DefaultBcryptCost+1, auth.BcryptCost)
	assert.True(t, auth.bcryptCostChanged)

	err = auth.SetBcryptCost(DefaultBcryptCost) // back to explicit default value, check changed is still true
	assert.NoError(t, err)
	assert.Equal(t, DefaultBcryptCost, auth.BcryptCost)
	assert.True(t, auth.bcryptCostChanged)
}

// NoReplKeyCache represents a key-only cache that doesn't support eviction.
// When the cache fills up, the entire cache is cleared and starts
// building it again from an empty cache to make room for new items.
type NoReplKeyCache struct {
	size  int                 // Maximum size where this cache can potentially grow upto.
	cache map[string]struct{} // Set of keys for fast lookup.
	lock  sync.RWMutex        // Protects both cache and keys from concurrent access.
}

// Returns a new no replacement key-only cache that can
// potentially grow upto the provided size.
func NewNoReplKeyCache(size int) *NoReplKeyCache {
	return &NoReplKeyCache{
		size:  size,
		cache: make(map[string]struct{}),
	}
}

// Contains returns true if the provided key is present
// in the cache and false otherwise.
func (c *NoReplKeyCache) Contains(key string) (ok bool) {
	c.lock.RLock()
	_, ok = c.cache[key]
	c.lock.RUnlock()
	return ok
}

// Put adds a key to the cache. No eviction occurs when memory is
// over filled or greater than the specified size in the cache.
// The entire cache is cleared and starts building it again from
// an empty cache instead.
func (c *NoReplKeyCache) Put(key string) {
	c.lock.Lock()
	if _, ok := c.cache[key]; ok {
		c.lock.Unlock()
		return
	}
	if len(c.cache) >= c.size {
		c.cache = map[string]struct{}{}
	}
	c.cache[key] = struct{}{}
	c.lock.Unlock()
}

// Len returns the number of keys in the cache.
func (c *NoReplKeyCache) Len() int {
	c.lock.RLock()
	length := len(c.cache)
	c.lock.RUnlock()
	return length
}

// Purge deletes all items from the cache.
func (c *NoReplKeyCache) Purge() {
	c.lock.Lock()
	c.cache = map[string]struct{}{}
	c.lock.Unlock()
}

// cacheType represents a specific key-only cache implementation type.
type cacheType int

const (
	// randReplKeyCache represents a key-only cache implementation
	// that supports random eviction of keys.
	randReplKeyCache cacheType = iota

	// noReplKeyCache represents a key-only cache implementation
	// that doesn't supports eviction of keys.
	noReplKeyCache
)

func TestCache(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelInfo, base.KeyAuth)
	var tests = []struct {
		name      string
		cache     Cache
		cacheType cacheType
	}{
		{"RandReplKeyCache", NewRandReplKeyCache(2), randReplKeyCache},
		{"NoReplKeyCache", NewNoReplKeyCache(2), noReplKeyCache},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cache := tt.cache
			defer func() {
				cache.Purge()
				assert.Equal(t, 0, cache.Len())
			}()

			k1 := "k1"
			cache.Put(k1)
			assert.Equal(t, 1, cache.Len())
			assert.True(t, cache.Contains(k1))

			k2 := "k2"
			cache.Put(k2)
			assert.Equal(t, 2, cache.Len())
			assert.True(t, cache.Contains(k1) && cache.Contains(k2))

			k3 := "k3"
			cache.Put(k3)
			if tt.cacheType == randReplKeyCache {
				assert.Equal(t, 2, cache.Len())
				assert.True(t, cache.Contains(k1) && cache.Contains(k3) || cache.Contains(k2) && cache.Contains(k3))
			} else if tt.cacheType == noReplKeyCache {
				assert.Equal(t, 1, cache.Len())
				assert.True(t, cache.Contains(k3) && !cache.Contains(k1) && !cache.Contains(k2))
			}

			k4 := "k4"
			cache.Put(k4)
			assert.Equal(t, 2, cache.Len())
			if tt.cacheType == randReplKeyCache {
				assert.True(t, cache.Contains(k1) && cache.Contains(k4) || cache.Contains(k2) && cache.Contains(k4) || cache.Contains(k3) && cache.Contains(k4))
			} else if tt.cacheType == noReplKeyCache {
				assert.True(t, cache.Contains(k3) && cache.Contains(k4) && !cache.Contains(k1) && !cache.Contains(k2))
			}
		})
	}
}

func TestCacheRace(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelInfo, base.KeyAuth)
	const maxCacheSize = 5
	var tests = []struct {
		name      string
		cache     Cache
		cacheType cacheType
	}{
		{"RandReplKeyCache", NewRandReplKeyCache(maxCacheSize), randReplKeyCache},
		{"NoReplKeyCache", NewNoReplKeyCache(maxCacheSize), noReplKeyCache},
	}
	ctx := base.TestCtx(t)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var wg sync.WaitGroup
			cache := tt.cache
			defer func() {
				cache.Purge()
				assert.Equal(t, 0, cache.Len())
			}()

			for i := 0; i < 10; i++ {
				wg.Add(1)
				go func(cache Cache, i int) {
					defer wg.Done()
					key := fmt.Sprintf("k%d", i)
					cache.Put(key)
					base.InfofCtx(ctx, base.KeyAuth, "Goroutine%v Put key: %v", i, key)
				}(cache, i)

				wg.Add(1)
				go func(cache Cache, i int) {
					defer wg.Done()
					key := fmt.Sprintf("k%d", i)
					ok := cache.Contains(key)
					base.InfofCtx(ctx, base.KeyAuth, "Goroutine%v Contains: %v, key: %v", i, ok, key)
				}(cache, i)
			}
			wg.Wait()
			assert.Equal(t, 5, cache.Len())
		})
	}
}

func BenchmarkPutAndOverflow(b *testing.B) {
	const maxCacheSize = 100

	setupCache := func(cacheType cacheType, warmupCache bool) (cache Cache) {
		if cacheType == randReplKeyCache {
			cache = NewRandReplKeyCache(maxCacheSize)
		} else if cacheType == noReplKeyCache {
			cache = NewNoReplKeyCache(maxCacheSize)
		}
		if warmupCache {
			for i := 0; i < maxCacheSize; i++ {
				cache.Put(randKey(math.MaxUint32))
			}
		}
		return cache
	}

	benchmarks := []struct {
		name        string
		cacheType   cacheType
		warmupCache bool
		runParallel bool
	}{
		{"FillRandReplKeyCache", randReplKeyCache, false, false},
		{"FillNoReplKeyCache", noReplKeyCache, false, false},

		{"RefillRandReplKeyCache", randReplKeyCache, true, false},
		{"RefillNoReplKeyCache", noReplKeyCache, true, false},

		{"ParallelFillRandReplKeyCache", randReplKeyCache, false, true},
		{"ParallelFillNoReplKeyCache", noReplKeyCache, false, true},

		{"ParallelRefillRandReplKeyCache", randReplKeyCache, true, true},
		{"ParallelRefillNoReplKeyCache", noReplKeyCache, true, true},
	}
	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			cache := setupCache(bm.cacheType, bm.warmupCache)
			b.ResetTimer()
			if bm.runParallel {
				b.RunParallel(func(pb *testing.PB) {
					b.ReportAllocs()
					for pb.Next() {
						cache.Put(randKey(math.MaxUint32))
					}
				})
			} else {
				b.Run(bm.name, func(b *testing.B) {
					b.ReportAllocs()
					for i := 0; i < b.N; i++ {
						cache.Put(randKey(math.MaxUint32))
					}
				})
			}
		})
	}
}

type cacheStat struct {
	cacheName   string
	cacheHits   int
	cacheMisses int
}

func showCacheStat(b *testing.B, stats []cacheStat) {
	for idx, stat := range stats {
		b.Logf("Iteration: %d, Cache Name: %v, Cache Hits: %d, Cache Misses: %d\n",
			idx, stat.cacheName, stat.cacheHits, stat.cacheMisses)
	}
}

func BenchmarkContains(b *testing.B) {
	const maxCacheSize = kMaxCacheSize
	benchmarks := []struct {
		name  string
		cache Cache
	}{
		{"RandReplKeyCache", NewRandReplKeyCache(maxCacheSize)},
		{"NoReplKeyCache", NewNoReplKeyCache(maxCacheSize)},
	}

	stats := make([]cacheStat, 0)
	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			cacheHits, cacheMisses := 0, 0
			b.ReportAllocs()
			cache := bm.cache
			cache.Put(randKey(maxCacheSize))

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if cache.Contains(randKey(maxCacheSize)) {
					cacheHits++
				} else {
					cacheMisses++
				}
			}
			b.StopTimer()
			stat := cacheStat{cacheName: bm.name, cacheHits: cacheHits, cacheMisses: cacheMisses}
			stats = append(stats, stat)
			b.StartTimer()
		})
	}
	showCacheStat(b, stats)
}

func randKey(limit int) string {
	return fmt.Sprintf("foo%d", rand.Intn(limit))
}

// bcryptHash returns the bcrypt hash of the given password.
func bcryptHash(b *testing.B, password []byte) []byte {
	hash, err := bcrypt.GenerateFromPassword(password, DefaultBcryptCost)
	require.NoError(b, err)
	return hash
}

// hashAndPassword represents the password hash pair.
type hashAndPassword struct {
	hash     []byte // bcrypt hashed password
	password []byte // plaintext equivalent
}

// generateTestData generates the test data and warm up the cache if required.
func generateTestData(b *testing.B, cache Cache, warmCache bool, key string, numPasswords int) []hashAndPassword {
	hashAndPasswords := make([]hashAndPassword, numPasswords)
	for i := 0; i < numPasswords; i++ {
		password := []byte(fmt.Sprintf("%s%d", key, i))
		hashAndPassword := hashAndPassword{
			hash:     bcryptHash(b, password),
			password: password,
		}
		hashAndPasswords[i] = hashAndPassword
		if warmCache {
			key := authKey(hashAndPassword.hash, hashAndPassword.password)
			cache.Put(key)
		}
	}
	return hashAndPasswords
}

func BenchmarkCompareHashAndPassword100PercentCacheHit(b *testing.B) {
	const maxCacheSize = 100
	benchmarks := []struct {
		name  string
		cache Cache
	}{
		{"RandReplKeyCache", NewRandReplKeyCache(maxCacheSize)},
		{"NoReplKeyCache", NewNoReplKeyCache(maxCacheSize)},
	}
	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			b.ReportAllocs()
			cache := bm.cache
			defer cache.Purge()
			testData := generateTestData(b, cache, true, "foo", maxCacheSize)

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				hashAndPassword := testData[i%maxCacheSize]
				_ = compareHashAndPassword(cache, hashAndPassword.hash, hashAndPassword.password)
			}
		})
	}
}

func BenchmarkCompareHashAndPasswordCacheMissAndFill(b *testing.B) {
	const maxCacheSize = 100
	benchmarks := []struct {
		name  string
		cache Cache
	}{
		{"RandReplKeyCache", NewRandReplKeyCache(maxCacheSize)},
		{"NoReplKeyCache", NewNoReplKeyCache(maxCacheSize)},
	}
	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			b.ReportAllocs()
			cache := bm.cache
			defer cache.Purge()
			testData := generateTestData(b, cache, false, "bar", maxCacheSize)

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				hashAndPassword := testData[i%maxCacheSize]
				_ = compareHashAndPassword(cache, hashAndPassword.hash, hashAndPassword.password)
			}
		})
	}
}

func BenchmarkCompareHashAndPassword100PercentIncorrect(b *testing.B) {
	const maxCacheSize = 100
	benchmarks := []struct {
		name  string
		cache Cache
	}{
		{"RandReplKeyCache", NewRandReplKeyCache(maxCacheSize)},
		{"NoReplKeyCache", NewNoReplKeyCache(maxCacheSize)},
	}
	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			b.ReportAllocs()
			cache := bm.cache
			defer cache.Purge()
			hash := bcryptHash(b, []byte("foo"))

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = compareHashAndPassword(cache, hash, []byte("baz"))
			}
		})
	}
}

func BenchmarkPutAndContains(b *testing.B) {
	const (
		maxCacheSize = kMaxCacheSize
		numKeys      = 2 * maxCacheSize
	)
	benchmarks := []struct {
		name  string
		cache Cache
	}{
		{"RandReplKeyCache", NewRandReplKeyCache(maxCacheSize)},
		{"NoReplKeyCache", NewNoReplKeyCache(maxCacheSize)},
	}

	stats := make([]cacheStat, 0)
	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			cacheHits, cacheMisses := 0, 0
			b.ReportAllocs()
			cache := bm.cache
			defer cache.Purge()
			for i := 0; i < maxCacheSize; i++ {
				key := randKey(maxCacheSize)
				cache.Put(key)
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				key := randKey(numKeys)
				if !cache.Contains(key) {
					cache.Put(key)
					cacheMisses++
				} else {
					cacheHits++
				}
			}
			b.StopTimer()
			stat := cacheStat{cacheName: bm.name, cacheHits: cacheHits, cacheMisses: cacheMisses}
			stats = append(stats, stat)
			b.StartTimer()
		})
	}
	showCacheStat(b, stats)
}
