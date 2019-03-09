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
	kMaxIncrRetries              = 3                   // Max retries for incr operations
	UnusedSequenceKeyPrefix      = "_sync:unusedSeq:"  // Prefix for single unused sequence marker documents
	UnusedSequenceRangeKeyPrefix = "_sync:unusedSeqs:" // Prefix for range of unused sequence marker documents
	UnusedSequenceTTL            = 10 * 60             // 10 minute expiry for unused sequence docs

	// Sequence batching control
	// Target maximum frequency of incr operations.  Requests at higher than this frequency trigger an increase
	// in batch size
	maxSequenceIncrFrequency = 1000 * time.Millisecond

	// Maximum time to wait after a reserve before releasing sequences
	releaseSequenceWait = 1500 * time.Millisecond

	// Maximum batch size
	maxBatchSize = 100000

	// Factor by which to grow the sequence batch size
	sequenceBatchMultiplier = 2

	// Default batch size
	defaultBatchSize = 1
)

type sequenceAllocator struct {
	bucket                  base.Bucket   // Bucket whose counter to use
	mutex                   sync.Mutex    // Makes this object thread-safe
	last                    uint64        // Last sequence # assigned
	max                     uint64        // Max sequence # reserved
	terminator              chan struct{} // Terminator for releaseUnusedSequences goroutine
	reserveNotify           chan struct{} // Channel for reserve notifications
	sequenceBatchSize       uint64        // Current sequence allocation batch size
	lastSequenceReserveTime time.Time     // Time of most recent sequence reserve
}

func newSequenceAllocator(bucket base.Bucket) (*sequenceAllocator, error) {
	s := &sequenceAllocator{bucket: bucket}
	s.terminator = make(chan struct{})
	s.sequenceBatchSize = defaultBatchSize
	s.reserveNotify = make(chan struct{}, 1)
	go s.releaseSequenceMonitor()
	_, err := s.lastSequence() // just reads latest sequence from bucket
	return s, err
}

func (s *sequenceAllocator) Stop() {
	close(s.terminator)
}

// Release sequence monitor releases allocated sequences that aren't used within 'releaseSequenceTimeout'.
func (s *sequenceAllocator) releaseSequenceMonitor() {
	for {
		select {
		case <-s.reserveNotify:
			select {
			case <-s.reserveNotify:
			case <-time.After(releaseSequenceWait):
				s.releaseUnusedSequences()
			}
		case <-s.terminator:
			return
		}
	}
}

func (s *sequenceAllocator) releaseUnusedSequences() {
	s.mutex.Lock()
	if s.last > s.max {
		s.releaseSequenceRange(s.last, s.max)
	}
	// Reduce batch size for next incr by the unused amount
	unusedAmount := s.max - s.last

	if unusedAmount >= s.sequenceBatchSize {
		s.sequenceBatchSize = defaultBatchSize
	} else {
		s.sequenceBatchSize = s.sequenceBatchSize - unusedAmount
	}

	s.last = s.max
	s.mutex.Unlock()
}

func (s *sequenceAllocator) lastSequence() (uint64, error) {
	if s.last > 0 {
		return s.last, nil
	}
	dbExpvars.Add("sequence_gets", 1)
	last, err := s.incrWithRetry("_sync:seq", 0)
	if err != nil {
		base.Warnf(base.KeyAll, "Error from Incr in lastSequence(): %v", err)
	}
	return last, err
}

func (s *sequenceAllocator) nextSequence() (sequence uint64, err error) {
	s.mutex.Lock()
	if s.last >= s.max {
		if err := s._reserveSequenceRange(); err != nil {
			s.mutex.Unlock()
			return 0, err
		}
	}
	s.last++
	sequence = s.last
	s.mutex.Unlock()
	return sequence, nil
}

func (s *sequenceAllocator) _reserveSequenceRange() error {

	// If time since last reserve is smaller than max frequency, increase batch size
	if time.Since(s.lastSequenceReserveTime) < maxSequenceIncrFrequency {
		s.sequenceBatchSize = uint64(s.sequenceBatchSize * sequenceBatchMultiplier)
		if s.sequenceBatchSize > maxBatchSize {
			s.sequenceBatchSize = maxBatchSize
		}
		base.Debugf(base.KeyChanges, "Increased sequence batch to %d", s.sequenceBatchSize)
	}
	max, err := s.incrWithRetry("_sync:seq", s.sequenceBatchSize)
	if err != nil {
		base.Warnf(base.KeyAll, "Error from Incr in _reserveSequences(%d): %v", s.sequenceBatchSize, err)
		return err
	}
	s.max = max
	s.last = max - s.sequenceBatchSize
	s.lastSequenceReserveTime = time.Now()
	s.reserveNotify <- struct{}{}
	return nil
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
	base.Debugf(base.KeyCRUD, "Released unused sequence #%d", sequence)
	return err
}

func (s *sequenceAllocator) releaseSequenceRange(fromSequence, toSequence uint64) error {
	key := fmt.Sprintf("%s%d:%d", UnusedSequenceRangeKeyPrefix, fromSequence, toSequence)
	body := make([]byte, 16)
	binary.LittleEndian.PutUint64(body[:8], fromSequence)
	binary.LittleEndian.PutUint64(body[8:16], toSequence)
	_, err := s.bucket.AddRaw(key, UnusedSequenceTTL, body)
	base.Debugf(base.KeyCRUD, "Released unused sequences #%d-#%d", fromSequence, toSequence)
	return err
}
