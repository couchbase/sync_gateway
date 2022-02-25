//  Copyright 2012-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package db

import (
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/couchbase/sync_gateway/base"
)

const (
	// 10 minute expiry for unused sequence docs.
	UnusedSequenceTTL = 10 * 60

	// Maximum time to wait after a reserve before releasing sequences
	defaultReleaseSequenceWait = 1500 * time.Millisecond

	// Maximum batch size
	maxBatchSize = 10

	// Factor by which to grow the sequence batch size
	sequenceBatchMultiplier = 2

	// Idle batch size.  Initial batch size if SG is in an idle state (with respect to writes)
	idleBatchSize = 1
)

// MaxSequenceIncrFrequency is the maximum frequency we want to perform incr operations.
// Incr operations occurring more frequently that this value trigger an increase
// in batch size.  Defined as var to simplify test usage
var MaxSequenceIncrFrequency = 1000 * time.Millisecond

type sequenceAllocator struct {
	bucket                  base.Bucket         // Bucket whose counter to use
	dbStats                 *base.DatabaseStats // For updating per-db sequence allocation stats
	mutex                   sync.Mutex          // Makes this object thread-safe
	last                    uint64              // The last sequence allocated by this allocator.
	max                     uint64              // The range from (last+1) to max represents previously reserved sequences available for use.
	terminator              chan struct{}       // Terminator for releaseUnusedSequences goroutine
	reserveNotify           chan struct{}       // Channel for reserve notifications
	sequenceBatchSize       uint64              // Current sequence allocation batch size
	lastSequenceReserveTime time.Time           // Time of most recent sequence reserve
	releaseSequenceWait     time.Duration       // Supports test customization
}

func newSequenceAllocator(bucket base.Bucket, dbStatsMap *base.DatabaseStats) (*sequenceAllocator, error) {
	if dbStatsMap == nil {
		return nil, fmt.Errorf("dbStatsMap parameter must be non-nil")
	}

	s := &sequenceAllocator{
		bucket:  bucket,
		dbStats: dbStatsMap,
	}
	s.terminator = make(chan struct{})
	s.sequenceBatchSize = idleBatchSize
	s.releaseSequenceWait = defaultReleaseSequenceWait

	// The reserveNotify channel manages communication between the releaseSequenceMonitor goroutine and _reserveSequenceRange invocations.
	s.reserveNotify = make(chan struct{}, 1)
	go func() {
		defer base.FatalPanicHandler()
		s.releaseSequenceMonitor()
	}()
	_, err := s.lastSequence() // just reads latest sequence from bucket
	return s, err
}

func (s *sequenceAllocator) Stop() {

	// Trigger stop and release of unused sequences
	close(s.terminator)
	s.releaseUnusedSequences()
}

// Release sequence monitor runs in its own goroutine, and releases allocated sequences
// that aren't used within 'releaseSequenceTimeout'.
func (s *sequenceAllocator) releaseSequenceMonitor() {

	// Terminator is only checked while in idle state - ensures sequence allocation drains and
	// unused sequences are released before exiting.
	timer := time.NewTimer(math.MaxInt64)
	defer timer.Stop()

	for {
		select {
		case <-s.reserveNotify:
			// On reserve, start the timer to release unused sequences. A new reserve resets the timer.
			// On timeout, release sequences and return to idle state
			_ = timer.Reset(s.releaseSequenceWait)
		case <-timer.C:
			s.releaseUnusedSequences()
		case <-s.terminator:
			s.releaseUnusedSequences()
			return
		}
	}
}

// Releases any currently reserved, non-allocated sequences.
func (s *sequenceAllocator) releaseUnusedSequences() {
	s.mutex.Lock()
	if s.last == s.max {
		s.mutex.Unlock()
		return
	}
	if s.last < s.max {
		err := s.releaseSequenceRange(s.last+1, s.max)
		if err != nil {
			base.WarnfCtx(context.TODO(), "Error returned when releasing sequence range [%d-%d]. Falling back to skipped sequence handling.  Error:%v", s.last+1, s.max, err)
		}
	}
	// Reduce batch size for next incr by the unused amount
	unusedAmount := s.max - s.last

	// If no sequences from the last batch were used, assume system is idle
	// and drop back to the idle batch size.
	if unusedAmount >= s.sequenceBatchSize {
		s.sequenceBatchSize = idleBatchSize
	} else {
		// Some sequences were used - reduce batch size by the unused amount.
		s.sequenceBatchSize = s.sequenceBatchSize - unusedAmount
	}

	s.last = s.max
	s.mutex.Unlock()
}

// Retrieves the last allocated sequence.  If there hasn't been an allocation yet by this node,
// retrieves the value of the _sync:seq counter from the bucket by doing an incr(0)
func (s *sequenceAllocator) lastSequence() (uint64, error) {
	s.mutex.Lock()
	lastSeq := s.last
	s.mutex.Unlock()

	if lastSeq > 0 {
		return lastSeq, nil
	}
	s.dbStats.SequenceGetCount.Add(1)
	last, err := s.getSequence()
	if err != nil {
		base.WarnfCtx(context.TODO(), "Error from Get in getSequence(): %v", err)
	}
	return last, err
}

// Returns the next available sequence.
// If previously reserved sequences are available (s.last < s.max), returns one
// and increments s.last.
// If no previously reserved sequences are available, reserves new batch.
func (s *sequenceAllocator) nextSequence() (sequence uint64, err error) {
	s.mutex.Lock()
	sequencesReserved := false
	if s.last >= s.max {
		if err := s._reserveSequenceRange(); err != nil {
			s.mutex.Unlock()
			return 0, err
		}
		sequencesReserved = true
	}
	s.last++
	sequence = s.last
	s.mutex.Unlock()

	// If sequences were reserved, send notification to the release sequence monitor, to start the clock for releasing these sequences.
	// Must be done after mutex is released.
	if sequencesReserved {
		s.reserveNotify <- struct{}{}
	}

	s.dbStats.SequenceAssignedCount.Add(1)
	return sequence, nil
}

// Reserve a new sequence range.  Called by nextSequence when the previously allocated sequences have all been used.
func (s *sequenceAllocator) _reserveSequenceRange() error {

	// If the time elapsed since the last reserveSequenceRange invocation reserve is shorter than our target frequency,
	// this indicates we're making an incr call more frequently than we want to.  Triggers an increase in batch size to
	// reduce incr frequency.
	if time.Since(s.lastSequenceReserveTime) < MaxSequenceIncrFrequency {
		s.sequenceBatchSize = uint64(s.sequenceBatchSize * sequenceBatchMultiplier)
		if s.sequenceBatchSize > maxBatchSize {
			s.sequenceBatchSize = maxBatchSize
		}
		base.DebugfCtx(context.TODO(), base.KeyCRUD, "Increased sequence batch to %d", s.sequenceBatchSize)
	}

	max, err := s.incrementSequence(s.sequenceBatchSize)
	if err != nil {
		base.WarnfCtx(context.TODO(), "Error from incrementSequence in _reserveSequences(%d): %v", s.sequenceBatchSize, err)
		return err
	}

	// Update max and last used sequences.  Last is updated here to account for sequences allocated/used by other
	// Sync Gateway nodes
	s.max = max
	s.last = max - s.sequenceBatchSize
	s.lastSequenceReserveTime = time.Now()

	s.dbStats.SequenceReservedCount.Add(int64(s.sequenceBatchSize))
	return nil
}

// Gets the _sync:seq document value.  Retry handling provided by bucket.Get.
func (s *sequenceAllocator) getSequence() (max uint64, err error) {
	return base.GetCounter(s.bucket, base.SyncSeqKey)
}

// Increments the _sync:seq document.  Retry handling provided by bucket.Incr.
func (s *sequenceAllocator) incrementSequence(numToReserve uint64) (max uint64, err error) {
	value, err := s.bucket.Incr(base.SyncSeqKey, numToReserve, numToReserve, 0)
	if err == nil {
		s.dbStats.SequenceIncrCount.Add(1)
	}
	return value, err
}

// ReleaseSequence writes an unused sequence document, used to notify sequence buffering that a sequence has been allocated and not used.
// Sequence is stored as the document body to avoid null doc issues.
func (s *sequenceAllocator) releaseSequence(sequence uint64) error {
	key := fmt.Sprintf("%s%d", base.UnusedSeqPrefix, sequence)
	body := make([]byte, 8)
	binary.LittleEndian.PutUint64(body, sequence)
	_, err := s.bucket.AddRaw(key, UnusedSequenceTTL, body)
	if err != nil {
		return err
	}
	s.dbStats.SequenceReleasedCount.Add(1)
	base.DebugfCtx(context.TODO(), base.KeyCRUD, "Released unused sequence #%d", sequence)
	return nil
}

// releaseSequenceRange writes a binary document with the key _sync:unusedSeqs:fromSeq:toSeq.
// fromSeq and toSeq are inclusive (i.e. both fromSeq and toSeq are unused).
// From and to seq are stored as the document contents to avoid null doc issues.
func (s *sequenceAllocator) releaseSequenceRange(fromSequence, toSequence uint64) error {
	key := fmt.Sprintf("%s%d:%d", base.UnusedSeqRangePrefix, fromSequence, toSequence)
	body := make([]byte, 16)
	binary.LittleEndian.PutUint64(body[:8], fromSequence)
	binary.LittleEndian.PutUint64(body[8:16], toSequence)
	_, err := s.bucket.AddRaw(key, UnusedSequenceTTL, body)
	if err != nil {
		return err
	}
	s.dbStats.SequenceReleasedCount.Add(int64(toSequence - fromSequence + 1))
	base.DebugfCtx(context.TODO(), base.KeyCRUD, "Released unused sequences #%d-#%d", fromSequence, toSequence)
	return nil
}

// waitForReleasedSequences blocks for 'releaseSequenceWait' past the provided startTime.
// Used to guarantee assignment of allocated sequences on other nodes.
func (s *sequenceAllocator) waitForReleasedSequences(startTime time.Time) (waitedFor time.Duration) {

	requiredWait := s.releaseSequenceWait - time.Since(startTime)
	if requiredWait < 0 {
		return 0
	}
	base.InfofCtx(context.TODO(), base.KeyCache, "Waiting %v for sequence allocation...", requiredWait)
	time.Sleep(requiredWait)
	return requiredWait
}
