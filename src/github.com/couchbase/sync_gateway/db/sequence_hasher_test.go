package db

import (
	"sync"
	"testing"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbaselabs/go.assert"
)

func testSequenceHasher(size uint8) (*sequenceHasher, error) {

	hashBucket, err := ConnectToBucket(base.BucketSpec{
		Server:     "walrus:",
		BucketName: "hashBucket"})
	if err != nil {
		return nil, err
	}
	return NewSequenceHasher(size, hashBucket)

}

func TestHashCalculation(t *testing.T) {
	// Create a hasher with a small range (0-256) for testing
	seqHasher, err := testSequenceHasher(8)
	defer seqHasher.bucket.Close()
	assertNoError(t, err, "Error creating new sequence hasher")
	clock := base.NewSequenceClockImpl()
	clock.SetSequence(50, 100)
	clock.SetSequence(80, 20)
	clock.SetSequence(150, 150)
	hashValue := seqHasher.calculateHash(clock)
	assert.Equals(t, hashValue, uint64(14)) // (100 + 20 + 150) mod 256

	clock.SetSequence(55, 300)
	clock.SetSequence(200, 513)
	hashValue = seqHasher.calculateHash(clock)
	assert.Equals(t, hashValue, uint64(59)) // (100 + 20 + 150 + (300 mod 256) + (513 mod 256)) mod 256

}

func TestHashStorage(t *testing.T) {
	// Create a hasher with a small range (0-256) for testing
	seqHasher, err := testSequenceHasher(8)
	defer seqHasher.bucket.Close()
	assertNoError(t, err, "Error creating new sequence hasher")

	// Add first hash entry
	clock := base.NewSequenceClockImpl()
	clock.SetSequence(50, 100)
	clock.SetSequence(80, 20)
	clock.SetSequence(150, 150)
	hashValue, err := seqHasher.GetHash(clock)
	assertNoError(t, err, "Error getting hash")
	assert.Equals(t, hashValue, "14")

	// Retrieve first hash entry
	clockBack := seqHasher.GetClock(hashValue)
	assert.Equals(t, clockBack.GetSequence(50), uint64(100))
	assert.Equals(t, clockBack.GetSequence(80), uint64(20))
	assert.Equals(t, clockBack.GetSequence(150), uint64(150))

	// Create hash for the first clock again - ensure retrieves existing, and doesn't create new
	hashValue, err = seqHasher.GetHash(clock)
	assertNoError(t, err, "Error getting hash")
	assert.Equals(t, hashValue, "14")

	// Add a second clock that hashes to the same value
	secondClock := base.NewSequenceClockImpl()
	secondClock.SetSequence(50, 100)
	secondClock.SetSequence(80, 20)
	secondClock.SetSequence(150, 150)
	secondClock.SetSequence(300, 256)
	hashValue, err = seqHasher.GetHash(secondClock)
	assertNoError(t, err, "Error getting hash")
	assert.Equals(t, hashValue, "14-1")

	// Simulate multiple processes requesting a hash for the same clock concurrently - ensures cas write checks for
	// updates before writing
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			thirdClock := base.NewSequenceClockImpl()
			thirdClock.SetSequence(50, 100)
			thirdClock.SetSequence(80, 20)
			thirdClock.SetSequence(150, 150)
			thirdClock.SetSequence(300, 256)
			thirdClock.SetSequence(500, 256)
			value, err := seqHasher.GetHash(thirdClock)
			assertNoError(t, err, "Error getting hash")
			assert.Equals(t, value, "14-2")
		}()
	}
	wg.Wait()

}
