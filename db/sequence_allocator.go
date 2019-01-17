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
	"encoding/binary"
	"fmt"
	"sync"
	"time"

	"github.com/couchbase/sync_gateway/base"
)

const (
	kMaxIncrRetries         = 3                  // Max retries for incr operations
	UnusedSequenceKeyPrefix = "_sync:unusedSeq:" // Prefix for unused sequence documents
	UnusedSequenceTTL       = 10 * 60            // 10 minute expiry for unused sequence docs
	SyncSeqKey              = "_sync:seq"        // Key for sequence counter doc
)

type sequenceAllocator struct {
	bucket  base.Bucket    // Bucket whose counter to use
	dbStats *DatabaseStats // For updating per-db stats
	mutex   sync.Mutex     // Makes this object thread-safe
	last    uint64         // Last sequence # assigned
	max     uint64         // Max sequence # reserved
}

func newSequenceAllocator(bucket base.Bucket, dbStats *DatabaseStats) (*sequenceAllocator, error) {

	if dbStats == nil {
		return nil, fmt.Errorf("dbStats parameter must be non-nil")
	}

	s := &sequenceAllocator{
		bucket:  bucket,
		dbStats: dbStats,
	}
	return s, s.reserveSequences(0) // just reads latest sequence from bucket
}

func (s *sequenceAllocator) lastSequence() (uint64, error) {
	s.dbStats.StatsDatabase().Add(base.StatKeySequenceGetCount, 1)
	last, err := s.incrWithRetry(SyncSeqKey, 0)
	if err != nil {
		base.Warnf(base.KeyAll, "Error from Incr in lastSequence(): %v", err)
	}
	return last, err
}

func (s *sequenceAllocator) nextSequence() (uint64, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	if s.last >= s.max {
		if err := s._reserveSequences(1); err != nil {
			return 0, err
		}
	}
	s.last++
	return s.last, nil
}

func (s *sequenceAllocator) _reserveSequences(numToReserve uint64) error {
	if s.last < s.max {
		return nil // Already have some sequences left; don't be greedy and waste them
		//OPT: Could remember multiple discontiguous ranges of free sequences
	}
	s.dbStats.StatsDatabase().Add(base.StatKeySequenceReservedCount, 1)

	max, err := s.incrWithRetry(SyncSeqKey, numToReserve)
	if err != nil {
		base.Warnf(base.KeyAll, "Error from Incr in _reserveSequences(%d): %v", numToReserve, err)
		return err
	}
	s.max = max
	s.last = max - numToReserve
	return nil
}

func (s *sequenceAllocator) reserveSequences(numToReserve uint64) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	return s._reserveSequences(numToReserve)
}

func (s *sequenceAllocator) incrWithRetry(key string, numToReserve uint64) (uint64, error) {

	var err error
	var max uint64
	retries := 0
	maxRetries := kMaxIncrRetries

	// type assertion to hybrid bucket
	_, ok := base.AsGoCBBucket(s.bucket)
	if ok {
		// CouchbaseBucketGoCB already has it's own retry mechanism, so short-circuit
		// retry mechanism in incrWithRetry
		maxRetries = 1
	}

	for retries < maxRetries {
		max, err = s.bucket.Incr(key, numToReserve, numToReserve, 0)
		if err != nil {
			retries++
			base.Warnf(base.KeyAll, "Error from Incr in sequence allocator (%d) - attempt (%d/%d): %v", numToReserve, retries, kMaxIncrRetries, err)
			time.Sleep(10 * time.Millisecond)
		} else {
			return max, err
		}
	}
	base.Warnf(base.KeyAll, "Too many unsuccessful Incr attempts in sequence allocator - giving up (%d): %v", numToReserve, err)
	// Note: 'err' should be non-nil here (from Incr response above) but as described on issue #1810, there are cases where the value
	//       is nil by the time we log the warning above.  This seems most likely to be a race/scope issue with the callback processing
	//       in the go-couchbase Incr/Do, and the sleep after the last attempt above.  Forcing the error to non-nil here to ensure we don't
	//       proceed without an error in this case.
	return 0, err
}

// ReleaseSequence writes an unused sequence document, used to notify sequence buffering that a sequence has been allocated and not used.
func (s *sequenceAllocator) releaseSequence(sequence uint64) error {
	key := fmt.Sprintf("%s%d", UnusedSequenceKeyPrefix, sequence)
	body := make([]byte, 8)
	binary.LittleEndian.PutUint64(body, sequence)
	_, err := s.bucket.AddRaw(key, UnusedSequenceTTL, body)
	s.dbStats.StatsDatabase().Add(base.StatKeySequenceReleasedCount, 1)
	base.Debugf(base.KeyCRUD, "Released unused sequence #%d", sequence)
	return err
}
