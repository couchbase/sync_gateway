package db

import (
	"fmt"
	"log"
	"sync"
	"testing"
	"time"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbaselabs/go.assert"
)

func testSequenceHasher(size uint8, expiry uint32) (*sequenceHasher, error) {

	hashBucket, err := ConnectToBucket(base.BucketSpec{
		Server:     "walrus:",
		BucketName: "hash_bucket"}, nil)
	/*hashBucket, err := ConnectToBucket(base.BucketSpec{
	Server:     "http://localhost:8091",
	BucketName: "hash_bucket"})
	*/

	if err != nil {
		return nil, err
	}
	options := &SequenceHashOptions{
		Bucket: hashBucket,
		Size:   size,
		Expiry: &expiry,
	}

	return NewSequenceHasher(options)

}

func testSequenceHasherForBucket(bucket base.Bucket, size uint8, expiry uint32) (*sequenceHasher, error) {

	options := &SequenceHashOptions{
		Bucket: bucket,
		Size:   size,
		Expiry: &expiry,
	}

	return NewSequenceHasher(options)
}

func TestHashCalculation(t *testing.T) {
	// Create a hasher with a small range (0-256) for testing
	seqHasher, err := testSequenceHasher(8, 0)
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
	seqHasher, err := testSequenceHasher(8, 0)
	defer seqHasher.bucket.Close()
	assertNoError(t, err, "Error creating new sequence hasher")

	// Add first hash entry
	clock := base.NewSequenceClockImpl()
	clock.SetSequence(50, 100)
	clock.SetSequence(80, 20)
	clock.SetSequence(150, 150)
	hashValue, err := seqHasher.GetHash(clock)
	assertNoError(t, err, "Error getting hash")
	assert.Equals(t, hashValue, "14-0")

	// Add different hash entry
	clock2 := base.NewSequenceClockImpl()
	clock2.SetSequence(50, 1)
	clock2.SetSequence(80, 2)
	clock2.SetSequence(150, 5)
	hashValue2, err := seqHasher.GetHash(clock2)
	assertNoError(t, err, "Error getting hash")
	assert.Equals(t, hashValue2, "8-0")

	// Retrieve first hash entry
	clockBack, err := seqHasher.GetClock(hashValue)
	assertNoError(t, err, "Error getting clock")
	assert.Equals(t, clockBack.GetSequence(50), uint64(100))
	assert.Equals(t, clockBack.GetSequence(80), uint64(20))
	assert.Equals(t, clockBack.GetSequence(150), uint64(150))

	// Create hash for the first clock again - ensure retrieves existing, and doesn't create new
	hashValue, err = seqHasher.GetHash(clock)
	assertNoError(t, err, "Error getting hash")
	assert.Equals(t, hashValue, "14-0")

	// Add a second clock that hashes to the same value
	secondClock := base.NewSequenceClockImpl()
	secondClock.SetSequence(50, 100)
	secondClock.SetSequence(80, 20)
	secondClock.SetSequence(150, 150)
	secondClock.SetSequence(300, 256)
	hashValue, err = seqHasher.GetHash(secondClock)
	assertNoError(t, err, "Error getting hash")
	assert.Equals(t, hashValue, "14-1")

	// Simulate multiple processes requesting a hash for the same clock concurrently - ensures cas write checks
	// whether clock has already been added before writing
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

	// Retrieve non-existent hash
	missingClock, err := seqHasher.GetClock("1234")
	assertTrue(t, err != nil, "Should return error for non-existent hash")
	assert.Equals(t, missingClock.GetSequence(50), uint64(0))
	assert.Equals(t, missingClock.GetSequence(80), uint64(0))
	assert.Equals(t, missingClock.GetSequence(150), uint64(0))

	// Create a different hasher (simulates another SG node) that creates a collision
	seqHasher2, err := testSequenceHasherForBucket(seqHasher.bucket, 8, 0)
	assertNoError(t, err, "Error creating second sequence hasher")

	// Add a fourth clock that hashes to the same value via the new hasher
	fourthClock := base.NewSequenceClockImpl()
	fourthClock.SetSequence(50, 80)
	fourthClock.SetSequence(80, 40)
	fourthClock.SetSequence(150, 150)
	fourthClock.SetSequence(300, 256)
	hashValue, err = seqHasher2.GetHash(fourthClock)
	assertNoError(t, err, "Error getting hash")
	assert.Equals(t, hashValue, "14-3")

	// Attempt to retrieve the third clock from the first hasher (validate cache reload)
	fourthClockLoad, err := seqHasher.GetClock("14-3")
	assertNoError(t, err, "Error loading hash from other writer.")
	assert.Equals(t, fourthClockLoad.GetSequence(50), uint64(80))
	assert.Equals(t, fourthClockLoad.GetSequence(80), uint64(40))
	assert.Equals(t, fourthClockLoad.GetSequence(150), uint64(150))
	assert.Equals(t, fourthClockLoad.GetSequence(300), uint64(256))

}

func TestConcurrentHashStorage(t *testing.T) {
	// Create a hasher with a small range (0-256) for testing
	seqHasher, err := testSequenceHasher(8, 0)
	defer seqHasher.bucket.Close()
	assertNoError(t, err, "Error creating new sequence hasher")

	// Simulate multiple processes writing hashes for different clocks concurrently - ensure cache is still valid
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			clock := base.NewSequenceClockImpl()
			clock.SetSequence(uint16(i), uint64(i))
			value, err := seqHasher.GetHash(clock)
			assertNoError(t, err, "Error getting hash")
			assert.Equals(t, value, fmt.Sprintf("%d-0", i))
		}(i)
	}
	wg.Wait()

	// Retrieve values
	for i := 0; i < 20; i++ {
		loadedClock, err := seqHasher.GetClock(fmt.Sprintf("%d-0", i))
		assertTrue(t, err == nil, "Shouldn't return error")
		assert.Equals(t, loadedClock.GetSequence(uint16(i)), uint64(i))
	}
}

// Tests hash expiry.  Requires a real couchbase server bucket - walrus doesn't support expiry yet
func CouchbaseOnlyTestHashExpiry(t *testing.T) {
	// Create a hasher with a small range (0-256) and short expiry for testing
	seqHasher, err := testSequenceHasher(8, 5)
	defer seqHasher.bucket.Close()
	assertNoError(t, err, "Error creating new sequence hasher")

	// Add first hash entry
	clock := base.NewSequenceClockImpl()
	clock.SetSequence(50, 100)
	clock.SetSequence(80, 20)
	clock.SetSequence(150, 150)
	hashValue, err := seqHasher.GetHash(clock)
	assertNoError(t, err, "Error creating hash")
	// Validate that expiry is reset every time sequence for hash is requested.
	for i := 0; i < 20; i++ {
		clockBack, err := seqHasher.GetClock(hashValue)
		assertNoError(t, err, "Error getting clock")
		assert.Equals(t, clockBack.GetSequence(50), uint64(100))
		time.Sleep(2 * time.Second)
	}

	// Validate it disappears after expiry time when no active requests
	time.Sleep(10 * time.Second)
	clockBack, err := seqHasher.GetClock(hashValue)
	assertNoError(t, err, "Error getting clock")
	log.Println("Got clockback:", clockBack)
	assert.Equals(t, clockBack.GetSequence(50), uint64(0))

}
