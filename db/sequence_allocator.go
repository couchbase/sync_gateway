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

	// minimum number that _sync:seq will be corrected by in event of a rollback
	// Accounts for a maximum SG cluster size of 50 nodes, each allocating a full batch size of 10
	// if this value is too low and this correction has potential to allocate sequences that other nodes have already reserved a batch for
	syncSeqCorrectionValue = 500

	// Maximum number of sequences to release as part of nextSequenceGreaterThan
	MaxSequencesToRelease = 10000000
)

// MaxSequenceIncrFrequency is the maximum frequency we want to perform incr operations.
// Incr operations occurring more frequently that this value trigger an increase
// in batch size.  Defined as var to simplify test usage
var MaxSequenceIncrFrequency = 1000 * time.Millisecond

type sequenceAllocator struct {
	datastore               base.DataStore      // Bucket whose counter to use
	dbStats                 *base.DatabaseStats // For updating per-db sequence allocation stats
	mutex                   sync.Mutex          // Makes this object thread-safe
	last                    uint64              // The last sequence allocated by this allocator.
	max                     uint64              // The range from (last+1) to max represents previously reserved sequences available for use.
	terminator              chan struct{}       // Terminator for releaseUnusedSequences goroutine
	reserveNotify           chan struct{}       // Channel for reserve notifications
	sequenceBatchSize       uint64              // Current sequence allocation batch size
	lastSequenceReserveTime time.Time           // Time of most recent sequence reserve
	releaseSequenceWait     time.Duration       // Supports test customization
	metaKeys                *base.MetadataKeys  // Key generator for sequence and unused sequence documents
}

func newSequenceAllocator(ctx context.Context, datastore base.DataStore, dbStatsMap *base.DatabaseStats, metaKeys *base.MetadataKeys) (*sequenceAllocator, error) {
	if dbStatsMap == nil {
		return nil, fmt.Errorf("dbStatsMap parameter must be non-nil")
	}

	s := &sequenceAllocator{
		datastore: datastore,
		dbStats:   dbStatsMap,
		metaKeys:  metaKeys,
	}
	s.terminator = make(chan struct{})
	s.sequenceBatchSize = idleBatchSize
	s.releaseSequenceWait = defaultReleaseSequenceWait

	// The reserveNotify channel manages communication between the releaseSequenceMonitor goroutine and _reserveSequenceRange invocations.
	s.reserveNotify = make(chan struct{}, 1)
	_, err := s.lastSequence(ctx) // just reads latest sequence from bucket
	if err != nil {
		return nil, err
	}
	go func() {
		defer base.FatalPanicHandler()
		s.releaseSequenceMonitor(ctx)
	}()
	return s, err
}

func (s *sequenceAllocator) Stop(ctx context.Context) {

	// Trigger stop and release of unused sequences
	close(s.terminator)
	s.releaseUnusedSequences(ctx)
}

// Release sequence monitor runs in its own goroutine, and releases allocated sequences
// that aren't used within 'releaseSequenceTimeout'.
func (s *sequenceAllocator) releaseSequenceMonitor(ctx context.Context) {

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
			s.releaseUnusedSequences(ctx)
		case <-s.terminator:
			s.releaseUnusedSequences(ctx)
			return
		}
	}
}

// Releases any currently reserved, non-allocated sequences.
func (s *sequenceAllocator) releaseUnusedSequences(ctx context.Context) {
	s.mutex.Lock()
	if s.last == s.max {
		s.mutex.Unlock()
		return
	}
	if s.last < s.max {
		_, err := s.releaseSequenceRange(ctx, s.last+1, s.max)
		if err != nil {
			base.WarnfCtx(ctx, "Error returned when releasing sequence range [%d-%d]. Falling back to skipped sequence handling.  Error:%v", s.last+1, s.max, err)
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
func (s *sequenceAllocator) lastSequence(ctx context.Context) (uint64, error) {
	s.mutex.Lock()
	lastSeq := s.last
	s.mutex.Unlock()

	if lastSeq > 0 {
		return lastSeq, nil
	}
	s.dbStats.SequenceGetCount.Add(1)
	last, err := s.getSequence()
	if err != nil {
		return 0, fmt.Errorf("Couldn't get sequence from bucket: %w", err)
	}
	return last, err
}

// Returns the next available sequence.
// If previously reserved sequences are available (s.last < s.max), returns one
// and increments s.last.
// If no previously reserved sequences are available, reserves new batch.
func (s *sequenceAllocator) nextSequence(ctx context.Context) (sequence uint64, err error) {
	s.mutex.Lock()
	sequence, sequencesReserved, err := s._nextSequence(ctx)
	s.mutex.Unlock()
	if err != nil {
		return 0, err
	}
	// If sequences were reserved, send notification to the release sequence monitor, to start the clock for releasing these sequences.
	// Must be done after mutex is released.
	if sequencesReserved {
		s.reserveNotify <- struct{}{}
	}
	return sequence, nil
}

// _releaseCurrentBatch releases any unused sequences currently held by the allocator.
// Writes unused sequence document while holding s.lock, shouldn't be used by high-throughput operations.
func (s *sequenceAllocator) _releaseCurrentBatch(ctx context.Context) (numReleased uint64, err error) {
	if s.max > s.last {
		numReleased, err = s.releaseSequenceRange(ctx, s.last+1, s.max)
		if err != nil {
			return 0, err
		}
		s.last = s.max
	}
	return numReleased, nil
}

// nextSequenceGreaterThan increments _sync:seq such that it's greater than existingSequence + s.sequenceBatchSize
// In the case where our local s.max < _sync:seq (another node has incremented _sync:seq), we may be releasing
// sequences greater than existingSequence, but we will only ever release sequences allocated by this node's incr operation
func (s *sequenceAllocator) nextSequenceGreaterThan(ctx context.Context, existingSequence uint64) (sequence uint64, releasedSequenceCount uint64, err error) {

	targetSequence := existingSequence + 1
	s.mutex.Lock()
	// If the target sequence is less than or equal to one we've already allocated, can assign the sequence in the standard way
	if targetSequence <= s.last {
		sequence, sequencesReserved, err := s._nextSequence(ctx)
		s.mutex.Unlock()
		if err != nil {
			return 0, 0, err
		}
		if sequencesReserved {
			s.reserveNotify <- struct{}{}
		}
		return sequence, 0, nil
	}

	// If the target sequence is in our existing batch (between s.last and s.max), we want to release all unused sequences in the batch earlier
	// than targetSequence, and then assign as targetSequence
	if targetSequence <= s.max {
		releaseFrom := s.last + 1
		s.last = targetSequence
		s.dbStats.LastSequenceAssignedValue.Set(int64(targetSequence))
		s.mutex.Unlock()
		if releaseFrom < targetSequence {
			released, err := s.releaseSequenceRange(ctx, releaseFrom, targetSequence-1)
			if err != nil {
				base.WarnfCtx(ctx, "Error returned when releasing sequence range [%d-%d] from existing batch. Will be handled by skipped sequence handling.  Error:%v", releaseFrom, targetSequence-1, err)
			}
			releasedSequenceCount += released
		}
		s.dbStats.SequenceAssignedCount.Add(1)
		return targetSequence, releasedSequenceCount, nil

	}

	// At this point we need to allocate a sequence that's larger than what's in our current batch, so we first need to release the current batch.
	var numReleasedBatch uint64
	numReleasedBatch, err = s._releaseCurrentBatch(ctx)
	if err != nil {
		base.InfofCtx(ctx, base.KeyCache, "Unable to release current batch during nextSequenceGreaterThan for existing sequence %d. Will be handled by skipped sequence handling. %v", existingSequence, err)
	}
	releasedSequenceCount += numReleasedBatch

	syncSeq, err := s.getSequence()
	if err != nil {
		base.WarnfCtx(ctx, "Unable to fetch current sequence during nextSequenceGreaterThan for existing sequence %d. Error:%v", existingSequence, err)
		s.mutex.Unlock()
		return 0, 0, err
	}

	// check for rollback of _sync:seq before continuing
	if syncSeq < s.last {
		// rollback of _sync:seq detected
		syncSeq, err = s._fixSyncSeqRollback(ctx, syncSeq, s.last)
		if err != nil {
			s.mutex.Unlock()
			return 0, 0, err
		}
	}

	// If the target sequence is less than the current syncSeq, allocate as normal using _nextSequence
	if syncSeq >= targetSequence {
		sequence, sequencesReserved, err := s._nextSequence(ctx)
		s.mutex.Unlock()
		if err != nil {
			return 0, 0, err
		}
		if sequencesReserved {
			s.reserveNotify <- struct{}{}
		}
		return sequence, releasedSequenceCount, nil
	}

	// If the target sequence is greater than the current _sync:seq, we want to:
	// (a) Reserve n sequences past _sync:seq, where n = existingSequence - syncSeq.  It's ok if the resulting sequence exceeds targetSequence (if other nodes have allocated sequences and
	//   updated _sync:seq since we last updated s.max.)
	// (b) Allocate a standard batch of sequences, and assign a sequence from that batch in the usual way.
	// (c) Release the reserved sequences from part (a)
	numberToRelease := existingSequence - syncSeq
	numberToAllocate := s.sequenceBatchSize
	incrVal := numberToRelease + numberToAllocate

	// if sequences to release are above the max allowed, return error to cancel update
	if numberToRelease > MaxSequencesToRelease {
		s.mutex.Unlock()
		s.dbStats.CorruptSequenceCount.Add(1) // increment corrupt sequence count
		return 0, 0, base.ErrMaxSequenceReleasedExceeded
	}

	allocatedToSeq, err := s._incrementSequence(incrVal)
	if err != nil {
		base.WarnfCtx(ctx, "Error from _incrementSequence in nextSequenceGreaterThan(%d): %v", existingSequence, err)
		s.mutex.Unlock()
		return 0, 0, err
	}

	s.max = allocatedToSeq
	s.last = allocatedToSeq - numberToAllocate + 1
	sequence = s.last
	s.dbStats.LastSequenceAssignedValue.Set(int64(sequence))
	s.mutex.Unlock()

	// Perform standard batch handling and stats updates
	s.lastSequenceReserveTime = time.Now()
	s.reserveNotify <- struct{}{}
	s.dbStats.SequenceAssignedCount.Add(1)

	// Release the newly allocated sequences that were used to catch up to existingSequence (d)
	if numberToRelease > 0 {
		releaseTo := allocatedToSeq - numberToAllocate
		releaseFrom := releaseTo - numberToRelease + 1 // +1, as releaseSequenceRange is inclusive
		released, err := s.releaseSequenceRange(ctx, releaseFrom, releaseTo)
		if err != nil {
			base.WarnfCtx(ctx, "Error returned when releasing sequence range [%d-%d] to reach target sequence. Will be handled by skipped sequence handling.  Error:%v", releaseFrom, releaseTo, err)
		}
		releasedSequenceCount += released
	}

	return sequence, releasedSequenceCount, err
}

// _nextSequence reserves if needed, and then returns the next sequence
func (s *sequenceAllocator) _nextSequence(ctx context.Context) (sequence uint64, sequencesReserved bool, err error) {
	if s.last >= s.max {
		if err := s._reserveSequenceBatch(ctx); err != nil {
			return 0, false, err
		}
		sequencesReserved = true
	}
	s.last++
	sequence = s.last
	s.dbStats.SequenceAssignedCount.Add(1)
	s.dbStats.LastSequenceAssignedValue.Set(int64(sequence))
	return sequence, sequencesReserved, nil
}

// Reserve a new sequence range, based on batch size.  Called by nextSequence when the previously allocated sequences have all been used.
func (s *sequenceAllocator) _reserveSequenceBatch(ctx context.Context) error {

	// If the time elapsed since the last reserveSequenceRange invocation reserve is shorter than our target frequency,
	// this indicates we're making an incr call more frequently than we want to.  Triggers an increase in batch size to
	// reduce incr frequency.
	if time.Since(s.lastSequenceReserveTime) < MaxSequenceIncrFrequency {
		s.sequenceBatchSize = s.sequenceBatchSize * sequenceBatchMultiplier
		if s.sequenceBatchSize > maxBatchSize {
			s.sequenceBatchSize = maxBatchSize
		}
		base.DebugfCtx(ctx, base.KeyCRUD, "Increased sequence batch to %d", s.sequenceBatchSize)
	}

	max, err := s._incrementSequence(s.sequenceBatchSize)
	if err != nil {
		base.WarnfCtx(ctx, "Error from incrementSequence in _reserveSequences(%d): %v", s.sequenceBatchSize, err)
		return err
	}

	// check for rollback of _sync:seq document
	minimumExpectedValue := s.max + s.sequenceBatchSize
	if max < minimumExpectedValue {
		// rollback of _sync:seq detected
		max, err = s._fixSyncSeqRollback(ctx, max, minimumExpectedValue)
		if err != nil {
			return err
		}
	}

	// Update max and last used sequences.  Last is updated here to account for sequences allocated/used by other
	// Sync Gateway nodes
	s.max = max
	s.last = max - s.sequenceBatchSize
	s.lastSequenceReserveTime = time.Now()

	return nil
}

// Gets the _sync:seq document value.  Retry handling provided by bucket.Get.
func (s *sequenceAllocator) getSequence() (max uint64, err error) {
	return base.GetCounter(s.datastore, s.metaKeys.SyncSeqKey())
}

// Increments the _sync:seq document.  Retry handling provided by bucket.Incr.
// Expects sequenceAllocator.mutex to be held to ensure consistent LastSequenceReservedValue updates.
func (s *sequenceAllocator) _incrementSequence(numToReserve uint64) (uint64, error) {
	value, err := s.datastore.Incr(s.metaKeys.SyncSeqKey(), numToReserve, numToReserve, 0)
	if err != nil {
		return value, err
	}
	s.dbStats.SequenceIncrCount.Add(1)
	s.dbStats.SequenceReservedCount.Add(int64(numToReserve))
	s.dbStats.LastSequenceReservedValue.Set(int64(value))
	return value, nil
}

// ReleaseSequence writes an unused sequence document, used to notify sequence buffering that a sequence has been allocated and not used.
// Sequence is stored as the document body to avoid null doc issues.
func (s *sequenceAllocator) releaseSequence(ctx context.Context, sequence uint64) error {
	key := fmt.Sprintf("%s%d", s.metaKeys.UnusedSeqPrefix(), sequence)
	body := make([]byte, 8)
	binary.LittleEndian.PutUint64(body, sequence)
	_, err := s.datastore.AddRaw(key, UnusedSequenceTTL, body)
	if err != nil {
		return err
	}
	s.dbStats.SequenceReleasedCount.Add(1)
	base.DebugfCtx(ctx, base.KeyCRUD, "Released unused sequence #%d", sequence)
	return nil
}

// releaseSequenceRange writes a binary document with the key _sync:unusedSeqs:fromSeq:toSeq.
// fromSeq and toSeq are inclusive (i.e. both fromSeq and toSeq are unused).
// From and to seq are stored as the document contents to avoid null doc issues.
// Returns the number of sequences released.
func (s *sequenceAllocator) releaseSequenceRange(ctx context.Context, fromSequence, toSequence uint64) (uint64, error) {

	// Exit if there's nothing to release
	if toSequence == 0 || toSequence < fromSequence {
		return 0, nil
	}
	key := s.metaKeys.UnusedSeqRangeKey(fromSequence, toSequence)
	body := make([]byte, 16)
	binary.LittleEndian.PutUint64(body[:8], fromSequence)
	binary.LittleEndian.PutUint64(body[8:16], toSequence)
	_, err := s.datastore.AddRaw(key, UnusedSequenceTTL, body)
	if err != nil {
		return 0, err
	}
	count := toSequence - fromSequence + 1
	s.dbStats.SequenceReleasedCount.Add(int64(count))
	base.DebugfCtx(ctx, base.KeyCRUD, "Released unused sequences #%d-#%d", fromSequence, toSequence)
	return count, nil
}

// waitForReleasedSequences blocks for 'releaseSequenceWait' past the provided startTime.
// Used to guarantee assignment of allocated sequences on other nodes.
func (s *sequenceAllocator) waitForReleasedSequences(ctx context.Context, startTime time.Time) (waitedFor time.Duration) {

	requiredWait := s.releaseSequenceWait - time.Since(startTime)
	if requiredWait < 0 {
		return 0
	}
	base.InfofCtx(ctx, base.KeyCache, "Waiting %v for sequence allocation...", requiredWait)
	time.Sleep(requiredWait)
	return requiredWait
}

// _fixSyncSeqRollback will correct a rolled back _sync:seq document in the bucket
func (s *sequenceAllocator) _fixSyncSeqRollback(ctx context.Context, prevAllocTo, expectedValue uint64) (allocatedToSeq uint64, err error) {
	base.WarnfCtx(ctx, "rollback of _sync:seq document detected. Allocated to %d but expected value of at least %d", prevAllocTo, expectedValue)
	// find diff between current _sync:seq value and what we expected it to be + correction value
	correctionIncrValue := (expectedValue - prevAllocTo) + syncSeqCorrectionValue

	worker := func() (bool, error, interface{}) {
		// grab _sync:seq value + its current cas value
		var result uint64
		cas, err := s.datastore.Get(s.metaKeys.SyncSeqKey(), &result)
		if err != nil {
			return false, err, 0
		}
		// set the value to _sync:seq current value + incr value if result above is below that value
		setVal := result + correctionIncrValue
		if result < setVal {
			_, err = s.datastore.WriteCas(s.metaKeys.SyncSeqKey(), 0, cas, setVal, 0)
			if base.IsCasMismatch(err) {
				// retry on cas error
				return true, err, nil
			}
			if err == nil {
				// successfully corrected _sync:seq value above
				base.DebugfCtx(ctx, base.KeyCRUD, "corrected _sync:seq document from value %d by the value of %d", prevAllocTo, setVal)
				return false, nil, setVal
			}
		}
		// if we get here we either had error above that is not a cas mismatch thus we need to exit with failure or the result
		// from the fetch of _sync:seq was larger than expected (_sync:seq may have been fixed by another node)
		return false, err, 0
	}

	retryErr, _ := base.RetryLoop(ctx, "Fix _sync:seq Value", worker, base.CreateDoublingSleeperFunc(1000, 5))
	if retryErr != nil {
		base.WarnfCtx(ctx, "error: %v in retry loop to correct _sync:seq value", retryErr)
		return 0, retryErr
	}

	base.DebugfCtx(ctx, base.KeyCRUD, "_sync:seq value successfully corrected, entering normal sequence batch processing for this node")
	// if _sync:seq has been fixed successfully just increment by batch size to get new unique batch for this node
	allocatedToSeq, err = s._incrementSequence(s.sequenceBatchSize)
	if err != nil {
		return 0, err
	}

	return allocatedToSeq, err
}
