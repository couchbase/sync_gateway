//  Copyright (c) 2012 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package db

import (
	"bytes"
	"container/list"
	"encoding/gob"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/couchbase/sync_gateway/base"
)

const kHashPrefix = "_sequence:"
const kDefaultHashExpiry = uint32(2592000) //30days in seconds
const kDefaultSize = uint8(32)
const kDefaultHasherCacheCapacity = 500
const kDefaultChangesHashFrequency = 100

var sequenceHasherGetHashTime = base.NewIntRollingMeanVar(100)
var sequenceHasherGetClockTime = base.NewIntRollingMeanVar(100)

func init() {
	base.StatsExpvars.Set("indexReader.seqHasher.GetHash", &sequenceHasherGetHashTime)
	base.StatsExpvars.Set("indexReader.seqHasher.GetClockTime", &sequenceHasherGetClockTime)
}

type sequenceHasher struct {
	bucket        base.Bucket              // Bucket to store hashed instances
	exp           uint8                    // Range of hash values is from 0 to 2^exp
	mod           uint64                   // 2^exp.  Stored during init to reduce computation during hash.
	modMinus1     uint64                   // 2^exp - 1.  Stored during init to reduce computation during hash.
	hashExpiry    uint32                   // expiry in bucket
	cache         map[uint64]*list.Element // Fast lookup of clocks by hash
	lruList       *list.List               // List ordered by most recent access (Front is newest)
	cacheCapacity int                      // Max number of hashes to cache
	cacheLock     sync.Mutex               // mutex for cache
	hashFrequency int                      // Changes hash frequency.  See getHashFrequency() for description
}

type sequenceHash struct {
	hashValue      uint64
	collisionIndex uint16
}

type SeqHashCacheLoaderFunc func(hashValue uint64) (clocks *storedClocks, err error)
type hashCacheValue struct {
	key    uint64        // hashValue
	clocks *storedClocks // stored clocks for this hash value (array of clocks, indexed by collision index)
	lock   sync.Mutex    // synchronization for disk load
	err    error         // load error
}

func (s *sequenceHash) String() string {
	return fmt.Sprintf("%d-%d", s.hashValue, s.collisionIndex)
}

// Creates a new sequenceHasher using 2^exp as mod.
func NewSequenceHasher(options *SequenceHashOptions) (*sequenceHasher, error) {

	if options == nil {
		return nil, errors.New("Options must be specified for new sequence hasher")
	}

	if options.Size > 63 {
		return nil, errors.New("Size for sequence hash must be less than 63")
	}

	size := kDefaultSize
	if options.Size > 0 {
		size = options.Size
	}

	expireTime := kDefaultHashExpiry
	if options.Expiry != nil {
		expireTime = *options.Expiry
	}

	changesHashFrequency := kDefaultChangesHashFrequency
	if options.HashFrequency != nil {
		changesHashFrequency = *options.HashFrequency
	}

	return &sequenceHasher{
		bucket:        options.Bucket,
		exp:           size,
		mod:           uint64(1 << size),   // 2^exp
		modMinus1:     uint64(1<<size - 1), // 2^exp - 1
		hashExpiry:    expireTime,
		cache:         map[uint64]*list.Element{},
		lruList:       list.New(),
		cacheCapacity: kDefaultHasherCacheCapacity,
		hashFrequency: changesHashFrequency,
	}, nil

}

// calculateHash does a simple sum of the vector clock values, modulo s.mod.  This intentionally
// doesn't generate a random distribution of clocks to hash values.  Instead, since vbucket values within the
// clock are monotonically increasing, it's going to generate increasing, non-colliding hash values
// until s.mod value is reached.  The goal is to tune s.mod to the update frequency (and so the expiry
// of previously used hash values). so that the previously used hash value (n) is obsolete by the time
// the clock wraps around to (mod + n).
func (s *sequenceHasher) calculateHash(clock base.SequenceClock) uint64 {

	var sum uint64
	sum = uint64(0)
	for _, value := range clock.Value() {
		sum += value & s.modMinus1
	}
	return sum & s.modMinus1
}

func (s *sequenceHasher) GetHash(clock base.SequenceClock) (string, error) {

	defer sequenceHasherGetHashTime.AddSince(time.Now())

	if clock == nil {
		return "", errors.New("Can't calculate hash for nil clock")
	}

	hashValue := s.calculateHash(clock)

	// Load stored clocks for this hash, to see if it's already been defined.
	// Note: getCacheValue and load are handled as separate operations to optimize locking.
	//   1. getCacheValue locks the cache, and retrieves the current cache entry (or creates a new empty entry if not found)
	//   2. cachedValue.load locks the entry, and loads from the DB if no previous entry is found
	cachedValue := s.getCacheValue(hashValue)
	cachedClocks, err := cachedValue.load(s.loadClocks)
	if err != nil {
		return "", err
	}

	// Check whether the cached clocks for the hash value match our clock
	exists, index := cachedClocks.Contains(clock.Value())
	if exists {
		seqHash := sequenceHash{
			hashValue:      hashValue,
			collisionIndex: uint16(index),
		}
		IndexExpvars.Add("seqHash_getHash_hits", 1)
		return seqHash.String(), nil
	}

	// Didn't find a match in cache - update the index and the cache.  Get a write lock on the index value
	// first, to ensure only one goroutine on this SG attempts to write.  writeCas handling below handles
	// the case where other SGs are updating the value concurrently
	IndexExpvars.Add("seqHash_getHash_misses", 1)

	// First copy the clock value, to ensure we store a non-mutable version in the cache
	clockValue := make([]uint64, len(clock.Value()))
	copy(clockValue, clock.Value())

	updateErr := func() error {
		cachedValue.lock.Lock()
		defer cachedValue.lock.Unlock()

		// If the number of cached clocks has changed, check whether someone else has added this clock
		// while we waited for the lock
		if len(cachedValue.clocks.Sequences) > len(cachedClocks.Sequences) {
			exists, index = cachedValue.clocks.Contains(clockValue)
			if exists {
				return nil
			}
		}

		// Add our clock to the cached clocks for this hash
		existingClocks := cachedValue.clocks
		existingClocks.Sequences = append(existingClocks.Sequences, clockValue)

		// Update the hash entry in the bucket
		key := kHashPrefix + strconv.FormatUint(hashValue, 10)
		initialValue, err := existingClocks.Marshal()
		index = len(existingClocks.Sequences) - 1
		if err != nil {
			return err
		}
		_, err = base.WriteCasRaw(s.bucket, key, initialValue, existingClocks.cas, base.SecondsToCbsExpiry(int(s.hashExpiry)), func(value []byte) (updatedValue []byte, err error) {
			// Note: The following is invoked upon cas failure - may be called multiple times
			base.LogTo("DIndex+", "CAS fail - reapplying changes for hash storage for key: %s", key)
			var sClocks storedClocks
			err = sClocks.Unmarshal(value)
			if err != nil {
				base.Warn("Error unmarshalling hash storage during update", err)
				return nil, err
			}
			exists, index = sClocks.Contains(clockValue)
			if exists {
				// return empty byte array to cancel the update
				return []byte{}, nil
			}
			// Not found - add
			sClocks.Sequences = append(sClocks.Sequences, clockValue)
			base.LogTo("DIndex+", "Reattempting stored hash write for key %s:", key)
			index = len(sClocks.Sequences) - 1
			return sClocks.Marshal()
		})
		return nil
	}()

	if updateErr != nil {
		return "", updateErr
	}

	IndexExpvars.Add("writeCasRaw_hash", 1)

	if err != nil && err.Error() != "Already Exists" {
		return "", err
	}

	seqHash := &sequenceHash{
		hashValue:      hashValue,
		collisionIndex: uint16(index),
	}
	return seqHash.String(), nil
}

func (s *sequenceHasher) GetClock(sequence string) (*base.SequenceClockImpl, error) {

	defer sequenceHasherGetClockTime.AddSince(time.Now())
	clock := base.NewSequenceClockImpl()
	var err error
	var seqHash sequenceHash

	components := strings.Split(sequence, "-")
	if len(components) == 1 {
		seqHash.hashValue, err = strconv.ParseUint(sequence, 10, 64)
		if err != nil {
			return clock, errors.New(fmt.Sprintf("Error converting hash sequence %s to string: %v", sequence, err))
		}
	} else if len(components) == 2 {
		seqHash.hashValue, err = strconv.ParseUint(components[0], 10, 64)
		if err != nil {
			return clock, errors.New(fmt.Sprintf("Error converting hash sequence %s to string: %v", sequence, err))
		}
		index, err := strconv.ParseUint(components[1], 10, 16)
		seqHash.collisionIndex = uint16(index)
		if err != nil {
			return clock, errors.New(fmt.Sprintf("Error converting collision index %s to int: %v", components[1], err))
		}
	}

	cachedValue := s.getCacheValue(seqHash.hashValue)
	storedClocks, loadErr := cachedValue.loadForIndex(s.loadClocks, seqHash.collisionIndex)
	if loadErr != nil {
		return clock, loadErr
	}

	if uint16(len(storedClocks.Sequences)) <= seqHash.collisionIndex {
		base.LogTo("Changes+", "Stored hash not found for sequence [%s] collision index [%d], #storedClocks:%d", sequence, seqHash.collisionIndex, len(storedClocks.Sequences))
		return clock, errors.New(fmt.Sprintf("Stored hash not found for sequence [%s], returning zero clock", sequence))
	}
	clock = base.NewSequenceClockImpl()
	clock.Init(storedClocks.Sequences[seqHash.collisionIndex], seqHash.String())
	return clock, nil

}

// s.cache[hashValue] contains the storedClocks for this hash value.  If present, getCacheValue
// returns the storedClocks.  If not present, getValue initializes an empty entry, which will trigger
// index bucket retrieval on subsequent load
func (s *sequenceHasher) getCacheValue(hashValue uint64) (value *hashCacheValue) {

	s.cacheLock.Lock()
	defer s.cacheLock.Unlock()
	if elem := s.cache[hashValue]; elem != nil {
		s.lruList.MoveToFront(elem)
		value = elem.Value.(*hashCacheValue)
	} else {
		value = &hashCacheValue{key: hashValue}
		s.cache[hashValue] = s.lruList.PushFront(value)
		for len(s.cache) > s.cacheCapacity {
			// purge oldest from cache
			value := s.lruList.Remove(s.lruList.Back()).(*hashCacheValue)
			delete(s.cache, value.key)
		}
	}
	return
}

// Adds a revision to the cache.
func (s *sequenceHasher) putCacheValue(hashValue uint64, clocks *storedClocks) {
	value := s.getCacheValue(hashValue)
	value.store(clocks)
}

// getHashFrequency returns how frequently a hash value is calculated for sequences in a changes response.  Defaults to kDefaultChangesHashFrequency.
// Frequent hashing has two main performance impacts.  The first is the time spent storing the hash value in the DB.
// The second is that increased hashing results in increased hash collisions (which will further increase the
// time spent storing the hash value in the DB).
// Changes feeds that are interrupted will resume from the most recent hash - so the number needs to
// be small enough to avoid unreasonably large re-processing/revs_diff of changes entries by clients.
func (s *sequenceHasher) getHashFrequency() int {
	return s.hashFrequency
}

// Gets the stored clocks out of a hashCacheValue.  If nil, will attempt to load from the index bucket
func (value *hashCacheValue) load(loaderFunc SeqHashCacheLoaderFunc) (*storedClocks, error) {
	value.lock.Lock()
	defer value.lock.Unlock()
	if value.clocks == nil {
		IndexExpvars.Add("seqHashCache_misses", 1)
		value.clocks, value.err = loaderFunc(value.key)
	} else {
		IndexExpvars.Add("seqHashCacheCache_hits", 1)
	}

	// return a copy to ensure cache values don't get mutated outside of a hashCacheValue.store
	return value.clocks.Copy(), value.err
}

// Gets the stored clocks out of a hashCacheValue, targeting a specific collision index.
// If the targeted index is not found, will attempt to load from the index bucket
func (value *hashCacheValue) loadForIndex(loaderFunc SeqHashCacheLoaderFunc, collisionIndex uint16) (*storedClocks, error) {
	value.lock.Lock()
	defer value.lock.Unlock()

	// If clocks haven't been cached, or cache doesn't include target index, reload
	if value.clocks == nil || collisionIndex >= uint16(len(value.clocks.Sequences)) {
		IndexExpvars.Add("seqHashCache_misses", 1)
		value.clocks, value.err = loaderFunc(value.key)
	} else {
		IndexExpvars.Add("seqHashCacheCache_hits", 1)
	}

	// return a copy to ensure cache values don't get mutated outside of a hashCacheValue.store
	return value.clocks.Copy(), value.err
}

// Stores a body etc. into a revCacheValue if there isn't one already.
func (value *hashCacheValue) store(clocks *storedClocks) {
	value.lock.Lock()
	defer value.lock.Unlock()
	value.clocks = clocks.Copy()
	IndexExpvars.Add("seqHashCache_adds", 1)
}

func (s *sequenceHasher) loadClocks(hashValue uint64) (*storedClocks, error) {

	stored := storedClocks{}
	key := kHashPrefix + strconv.FormatUint(hashValue, 10)

	bytes, cas, err := s.bucket.GetAndTouchRaw(key, base.SecondsToCbsExpiry(int(s.hashExpiry)))
	IndexExpvars.Add("get_hashLoadClocks", 1)

	if err != nil {
		// Assume no clocks stored for this string
		return &stored, nil
	}
	if err = stored.Unmarshal(bytes); err != nil {
		base.Warn("Error unmarshalling stored clocks for key [%s], returning zero sequence", key)
		return &stored, errors.New("Error unmarshalling stored clocks for key")
	}
	stored.cas = cas
	return &stored, nil
}

/*
func (s *sequenceHasher) logCache(id int) {

	s.cacheLock.Lock()
	defer s.cacheLock.Unlock()
	log.Printf("[%d]---print cache---", id)
	for key, elem := range s.cache {
		cacheValue := elem.Value.(*hashCacheValue)
		cacheValue.lock.Lock()
		log.Printf("key: %d, value: %s", key, cacheValue.clocks.String())
		cacheValue.lock.Unlock()
	}
	log.Printf("[%d]---end print cache---", id)
}
*/

type storedClocks struct {
	cas       uint64
	Sequences [][]uint64
}

// TODO: replace with something more intelligent than gob encode, to take advantage of known
//       clock structure?
func (s *storedClocks) Marshal() ([]byte, error) {
	var output bytes.Buffer
	enc := gob.NewEncoder(&output)
	err := enc.Encode(s.Sequences)
	if err != nil {
		return nil, err
	}
	return output.Bytes(), nil
}

func (s *storedClocks) Unmarshal(value []byte) error {
	input := bytes.NewBuffer(value)
	dec := gob.NewDecoder(input)
	err := dec.Decode(&s.Sequences)
	if err != nil {
		return err
	}
	return nil
}

func (s *storedClocks) Contains(clockValue []uint64) (bool, int) {
	for index, storedClock := range s.Sequences {
		if ClockMatches(storedClock, clockValue) {
			return true, index
		}
	}
	return false, 0
}

func (s *storedClocks) String() string {
	results := ""
	for index, storedClock := range s.Sequences {
		results = fmt.Sprintf("%s[%d--", results, index)
		for vb, sequence := range storedClock {
			if sequence != 0 {
				results = fmt.Sprintf("%s(%d,%d),", results, vb, sequence)
			}
		}
		results = fmt.Sprintf("%s],", results)
	}
	return results
}

func (s *storedClocks) Copy() *storedClocks {
	copyClock := storedClocks{}
	copyClock.cas = s.cas

	for _, storedClock := range s.Sequences {
		copyClock.Sequences = append(copyClock.Sequences, storedClock)
	}
	return &copyClock
}

func ClockMatches(a, b []uint64) bool {
	if len(a) != len(b) {
		return false
	}
	for vb, sequence := range a {
		if sequence != b[vb] {
			return false
		}
	}
	return true
}
