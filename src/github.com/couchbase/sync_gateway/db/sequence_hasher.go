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
	"encoding/gob"
	"errors"
	"fmt"
	"log"
	"strconv"
	"strings"

	"github.com/couchbase/sync_gateway/base"
)

const kHashPrefix = "_sequence:"
const kDefaultHashExpiry = uint32(2592000)
const kDefaultSize = uint8(32)

type sequenceHasher struct {
	bucket     base.Bucket // Bucket to store hashed instances
	exp        uint8       // Range of hash values is from 0 to 2^exp
	mod        uint64      // 2^exp.  Stored during init to reduce computation during hash.
	modMinus1  uint64      // 2^exp - 1.  Stored during init to reduce computation during hash.
	hashExpiry uint32
}

type sequenceHash struct {
	hashValue      uint64
	collisionIndex uint16
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
		log.Println("Creating with expiry:", expireTime)
	}

	return &sequenceHasher{
		bucket:     options.Bucket,
		exp:        size,
		mod:        uint64(1 << size),   // 2^exp
		modMinus1:  uint64(1<<size - 1), // 2^exp - 1
		hashExpiry: expireTime,
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
	base.LogTo("DIndex+", "calculate hash for sum:%v, s:%v", sum, s)
	return sum & s.modMinus1
}

func (s *sequenceHasher) GetHash(clock base.SequenceClock) (string, error) {

	if clock == nil {
		return "", errors.New("Can't calculate hash for nil clock")
	}
	hashValue := s.calculateHash(clock)

	// Load stored clocks for this hash, to see if it's already been defined
	storedClocks, err := s.loadClocks(hashValue)
	if err != nil {
		return "", err
	}
	exists, index := storedClocks.Contains(clock)
	if exists {
		seqHash := sequenceHash{
			hashValue:      hashValue,
			collisionIndex: uint16(index),
		}
		indexExpvars.Add("hash_getHash_hits", 1)
		return seqHash.String(), nil
	}
	indexExpvars.Add("hash_getHash_misses", 1)

	// Didn't find a match
	storedClocks.Sequences = append(storedClocks.Sequences, clock.Value())
	//storedIndex := len(storedClocks.Sequences) - 1
	key := kHashPrefix + strconv.FormatUint(hashValue, 10)
	initialValue, err := storedClocks.Marshal()
	index = len(storedClocks.Sequences) - 1
	if err != nil {
		return "", err
	}
	_, err = writeCasRaw(s.bucket, key, initialValue, storedClocks.cas, int(s.hashExpiry), func(value []byte) (updatedValue []byte, err error) {
		// Note: The following is invoked upon cas failure - may be called multiple times
		base.LogTo("DIndex+", "CAS fail - reapplying changes for hash storage for key: %s", key)
		err = storedClocks.Unmarshal(value)
		if err != nil {
			base.Warn("Error unmarshalling hash storage during update", err)
			return nil, err
		}
		exists, index = storedClocks.Contains(clock)
		if exists {
			// return empty byte array to cancel the update
			return []byte{}, nil
		}
		// Not found - add
		storedClocks.Sequences = append(storedClocks.Sequences, clock.Value())

		base.LogTo("DIndex+", "Reattempting stored hash write for key %s:", key)
		index = len(storedClocks.Sequences) - 1
		return storedClocks.Marshal()
	})

	indexExpvars.Add("writeCasRaw_hash", 1)

	if err != nil && err.Error() != "Already Exists" {
		return "", err
	}

	seqHash := &sequenceHash{
		hashValue:      hashValue,
		collisionIndex: uint16(index),
	}

	return seqHash.String(), nil
}

func (s *sequenceHasher) GetClock(sequence string) (base.SequenceClock, error) {

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

	stored, loadErr := s.loadClocks(seqHash.hashValue)
	if loadErr != nil {
		return clock, loadErr
	}

	if uint16(len(stored.Sequences)) <= seqHash.collisionIndex {
		return clock, errors.New(fmt.Sprintf("Stored hash not found for sequence [%s], returning zero clock", sequence))
	}
	clock = base.NewSequenceClockImpl()
	clock.Init(stored.Sequences[seqHash.collisionIndex], seqHash.String())
	return clock, nil

}

func (s *sequenceHasher) loadClocks(hashValue uint64) (storedClocks, error) {

	stored := storedClocks{}
	key := kHashPrefix + strconv.FormatUint(hashValue, 10)

	bytes, cas, err := s.bucket.GetAndTouchRaw(key, int(s.hashExpiry))
	indexExpvars.Add("get_hashLoadClocks", 1)

	if err != nil {
		// Assume no clocks stored for this string
		return stored, nil
	}
	if err = stored.Unmarshal(bytes); err != nil {
		base.Warn("Error unmarshalling stored clocks for key [%s], returning zero sequence", key)
		return stored, errors.New("Error unmarshalling stored clocks for key")
	}
	stored.cas = cas
	return stored, nil
}

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

func (s *storedClocks) Contains(clock base.SequenceClock) (bool, int) {
	for index, storedClock := range s.Sequences {
		if ClockMatches(storedClock, clock.Value()) {
			return true, index
		}
	}
	return false, 0
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
