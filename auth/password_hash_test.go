package auth

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/bcrypt"
)

// BenchmarkBcryptCostTime will ouput the time it takes to hash a password with each bcrypt cost value
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

// Unique correct hash/passwords (to test filling the cache)
func BenchmarkCacheFilling(b *testing.B) {
	b.ReportAllocs()
	maxCacheSize := 10
	for i := 0; i < b.N; i++ {
		cachedHashes = NewCache(maxCacheSize)
		for i := 0; i < maxCacheSize; i++ {
			password := []byte(fmt.Sprintf("foo%d", i))
			hash, err := bcrypt.GenerateFromPassword(password, bcryptDefaultCost)
			require.NoError(b, err)
			assert.True(b, compareHashAndPassword(hash, password))
		}
	}
}

// Unique correct hash/passwords (to test replacing the cache)
func BenchmarkCacheReplacement(b *testing.B) {
	b.ReportAllocs()
	b.StopTimer()

	maxCacheSize := 10
	cachedHashes = NewCache(maxCacheSize)
	for i := 0; i < maxCacheSize; i++ {
		password := []byte(fmt.Sprintf("foo%d", i))
		hash, err := bcrypt.GenerateFromPassword(password, bcryptDefaultCost)
		require.NoError(b, err)
		require.NotEmpty(b, hash)
		key := authKey(hash, password)
		cachedHashes.Put(key)
	}
	require.Equal(b, maxCacheSize, cachedHashes.Len())

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		password := []byte(fmt.Sprintf("bar%d", rand.Intn(maxCacheSize)))
		hash, err := bcrypt.GenerateFromPassword(password, bcryptDefaultCost)
		require.NoError(b, err)
		assert.True(b, compareHashAndPassword(hash, password))
	}
}

// Same correct hash/password (to test the fast-path of Contains)
func BenchmarkCacheContains(b *testing.B) {
	b.ReportAllocs()
	b.StopTimer()

	maxCacheSize := 10
	cachedHashes = NewCache(maxCacheSize)
	for i := 0; i < maxCacheSize; i++ {
		password := []byte(fmt.Sprintf("foo%d", i))
		hash, err := bcrypt.GenerateFromPassword(password, bcryptDefaultCost)
		require.NoError(b, err)
		require.NotEmpty(b, hash)
		key := authKey(hash, password)
		cachedHashes.Put(key)
	}
	require.Equal(b, maxCacheSize, cachedHashes.Len())

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		password := []byte(fmt.Sprintf("foo%d", rand.Intn(maxCacheSize)))
		hash, err := bcrypt.GenerateFromPassword(password, bcryptDefaultCost)
		require.NoError(b, err)
		assert.True(b, compareHashAndPassword(hash, password))
	}
}

// Same incorrect hash/password (to verify we're still hitting the bcrypt slow-path and failing to insert)
func BenchmarkCacheSlowBcrypt(b *testing.B) {
	b.ReportAllocs()
	b.StopTimer()

	maxCacheSize := 10
	cachedHashes = NewCache(maxCacheSize)
	for i := 0; i < maxCacheSize; i++ {
		password := []byte(fmt.Sprintf("foo%d", i))
		hash, err := bcrypt.GenerateFromPassword(password, bcryptDefaultCost)
		require.NoError(b, err)
		require.NotEmpty(b, hash)
		key := authKey(hash, password)
		cachedHashes.Put(key)
	}
	require.Equal(b, maxCacheSize, cachedHashes.Len())

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		password := []byte(fmt.Sprintf("foo%d", rand.Intn(maxCacheSize)))
		hash, err := bcrypt.GenerateFromPassword(password, bcryptDefaultCost)
		require.NoError(b, err)
		assert.False(b, compareHashAndPassword(hash, []byte("password")))
	}
}
