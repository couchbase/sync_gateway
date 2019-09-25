package auth

import (
	"fmt"
	"strconv"
	"testing"
	"time"

	goassert "github.com/couchbaselabs/go.assert"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
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
			goassert.Equals(bn, err, nil)
		})
	}
}

func BenchmarkCompareAndHashPassword(b *testing.B) {
	maxLoop := 20
	passwordList := make([]string, 0, maxLoop)
	hashList := [][]byte{}

	for i := 0; i < maxLoop; i++ {
		passwordString := "password"
		if i%2 == 0 {
			passwordString += strconv.Itoa(i)
		}
		hash, _ := bcrypt.GenerateFromPassword([]byte(passwordString), bcryptDefaultCost)
		hashList = append(hashList, hash)
		passwordList = append(passwordList, passwordString)
	}
	compareHashAndPassword(hashList[1], []byte(passwordList[1]))
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for i := 0; i < maxLoop; i++ {
			for pb.Next() {
				compareHashAndPassword(hashList[i], []byte(passwordList[i]))
			}
		}
	})
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
	goassert.True(t, minimumDuration < duration)
}

func TestSetBcryptCost(t *testing.T) {
	err := SetBcryptCost(bcryptDefaultCost - 1) // below minimum allowed value
	goassert.Equals(t, errors.Cause(err), ErrInvalidBcryptCost)
	goassert.Equals(t, bcryptCost, bcryptDefaultCost)
	goassert.False(t, bcryptCostChanged)

	err = SetBcryptCost(0) // use default value
	assert.NoError(t, err)
	goassert.Equals(t, bcryptCost, bcryptDefaultCost)
	goassert.False(t, bcryptCostChanged) // Not explicitly changed

	err = SetBcryptCost(bcryptDefaultCost + 1) // use increased value
	assert.NoError(t, err)
	goassert.Equals(t, bcryptCost, bcryptDefaultCost+1)
	goassert.True(t, bcryptCostChanged)

	err = SetBcryptCost(bcryptDefaultCost) // back to explicit default value, check changed is still true
	assert.NoError(t, err)
	goassert.Equals(t, bcryptCost, bcryptDefaultCost)
	goassert.True(t, bcryptCostChanged)
}
