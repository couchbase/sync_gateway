package auth

import (
	"fmt"
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

func BenchmarkCompareHashAndPassword(b *testing.B) {
	// Get the bcrypt hash of the given password.
	bcryptHash := func(password []byte) []byte {
		hash, err := bcrypt.GenerateFromPassword(password, bcryptDefaultCost)
		require.NoError(b, err)
		return hash
	}
	// hashAndPassword represents the password hash pair.
	type hashAndPassword struct {
		hash     []byte
		password []byte
	}
	// Setup cache of size N
	const numPasswords = 100
	cachedHashes = NewCache(numPasswords)

	// Generate the test data and warm up the cache if required.
	generateTestData := func(warmCache bool, key string) []hashAndPassword {
		hashAndPasswords := make([]hashAndPassword, numPasswords)
		for i := 0; i < numPasswords; i++ {
			password := []byte(fmt.Sprintf("%s%d", key, i))
			hashAndPassword := hashAndPassword{
				hash:     bcryptHash(password),
				password: password,
			}
			hashAndPasswords[i%numPasswords] = hashAndPassword
			if warmCache {
				_ = compareHashAndPassword(hashAndPassword.hash, hashAndPassword.password)
			}
		}
		return hashAndPasswords
	}

	b.Run("100% cache hit", func(b *testing.B) {
		testData := generateTestData(true, "foo")

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = compareHashAndPassword(testData[i%numPasswords].hash, testData[i%numPasswords].password)
		}
	})

	b.Run("cache miss and fill", func(b *testing.B) {
		testData := generateTestData(false, "bar")

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = compareHashAndPassword(testData[i%numPasswords].hash, testData[i%numPasswords].password)
		}
	})

	b.Run("100% incorrect", func(b *testing.B) {
		hash := bcryptHash([]byte("foo"))

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = compareHashAndPassword(hash, []byte("baz"))
		}
	})
}
