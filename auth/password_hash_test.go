package auth

import (
	"fmt"
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
	_, err := bcrypt.GenerateFromPassword([]byte("hunter2"), bcryptDefaultCost)
	duration := time.Since(startTime)

	t.Logf("bcrypt.GenerateFromPassword with cost %d took: %v", bcryptDefaultCost, duration)
	assert.NoError(t, err)
	assert.True(t, minimumDuration < duration)
}

func TestSetBcryptCost(t *testing.T) {
	err := SetBcryptCost(bcryptDefaultCost - 1) // below minimum allowed value
	assert.Equal(t, ErrInvalidBcryptCost, errors.Cause(err))
	assert.Equal(t, bcryptDefaultCost, bcryptCost)
	assert.False(t, bcryptCostChanged)

	err = SetBcryptCost(0) // use default value
	assert.NoError(t, err)
	assert.Equal(t, bcryptDefaultCost, bcryptCost)
	assert.False(t, bcryptCostChanged) // Not explicitly changed

	err = SetBcryptCost(bcryptDefaultCost + 1) // use increased value
	assert.NoError(t, err)
	assert.Equal(t, bcryptDefaultCost+1, bcryptCost)
	assert.True(t, bcryptCostChanged)

	err = SetBcryptCost(bcryptDefaultCost) // back to explicit default value, check changed is still true
	assert.NoError(t, err)
	assert.Equal(t, bcryptDefaultCost, bcryptCost)
	assert.True(t, bcryptCostChanged)
}

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
	cache := NewCache(kMaxCacheSize)
	for i := 0; i < kMaxCacheSize; i++ {
		cache.Put(key())
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cache.Put(key())
	}
}

func BenchmarkParallelCachePutOverflow(b *testing.B) {
	b.ReportAllocs()
	cache := NewCache(kMaxCacheSize)
	for i := 0; i < kMaxCacheSize; i++ {
		cache.Put(key())
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			cache.Put(key())
		}
	})
}

func BenchmarkAuthKeyCacheContains(b *testing.B) {
	cache := NewCache(1)
	cache.Put("foo")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = authKey([]byte("foo"), []byte("bar"))
		_ = cache.Contains("foo")
	}
}

func BenchmarkCompareHashAndPassword(b *testing.B) {
	// Get the bcrypt hash of the given password.
	bcryptHash := func(password []byte) []byte {
		hash, err := bcrypt.GenerateFromPassword(password, bcryptDefaultCost)
		require.NoError(b, err)
		return hash
	}

	// Setup cache of size N
	const numPasswords = 1000

	// hashAndPassword represents the password hash pair.
	type hashAndPassword struct {
		hash     []byte // bcrypt hashed password
		password []byte // plaintext equivalent
	}

	// Generate the test data and warm up the cache if required.
	generateTestData := func(cache *Cache, warmCache bool, key string) []hashAndPassword {
		hashAndPasswords := make([]hashAndPassword, numPasswords)
		for i := 0; i < numPasswords; i++ {
			password := []byte(fmt.Sprintf("%s%d", key, i))
			hashAndPassword := hashAndPassword{
				hash:     bcryptHash(password),
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

	b.Run("100% cache hit", func(b *testing.B) {
		cache := NewCache(numPasswords)
		testData := generateTestData(cache, true, "foo")

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			hashAndPassword := testData[i%numPasswords]
			_ = cache.CompareHashAndPassword(hashAndPassword.hash, hashAndPassword.password)
		}
	})

	b.Run("cache miss and fill", func(b *testing.B) {
		cache := NewCache(numPasswords)
		testData := generateTestData(cache, false, "bar")

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			hashAndPassword := testData[i%numPasswords]
			_ = cache.CompareHashAndPassword(hashAndPassword.hash, hashAndPassword.password)
		}
	})

	b.Run("100% incorrect", func(b *testing.B) {
		cache := NewCache(numPasswords)
		hash := bcryptHash([]byte("foo"))

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = cache.CompareHashAndPassword(hash, []byte("baz"))
		}
	})
}
