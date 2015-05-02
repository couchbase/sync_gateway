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
	"sync"
	"time"

	"github.com/couchbase/sync_gateway/base"
)

type sequenceAllocator struct {
	bucket base.Bucket // Bucket whose counter to use
	mutex  sync.Mutex  // Makes this object thread-safe
	last   uint64      // Last sequence # assigned
	max    uint64      // Max sequence # reserved
}

func newSequenceAllocator(bucket base.Bucket) (*sequenceAllocator, error) {
	s := &sequenceAllocator{bucket: bucket}
	return s, s.reserveSequences(0) // just reads latest sequence from bucket
}

func (s *sequenceAllocator) lastSequence() (uint64, error) {
	dbExpvars.Add("sequence_gets", 1)
	last, err := s.bucket.Incr("_sync:seq", 0, 0, 0)
	if err != nil {
		base.Warn("Error from Incr in lastSequence(): %v", err)
	}
	return last, err
}

func (s *sequenceAllocator) nextSequence() (uint64, error) {
	startTime := time.Now()
	startTimeLockAcquire := time.Now()

	s.mutex.Lock()
	delta = time.Since(startTimeLockAcquire)
	if delta.Seconds() > 1 {
		base.Logf("seqAlloc nextSequence() mutex.Lock() took %v seconds", delta.Seconds())
	}
	defer s.mutex.Unlock()

	startTimeReserveSequences := time.Now()
	if s.last >= s.max {
		if err := s._reserveSequences(1); err != nil {
			return 0, err
		}
	}
	s.last++
	delta = time.Since(startTimeReserveSequences)
	if delta.Seconds() > 1 {
		base.Logf("seqAlloc nextSequence() _reserveSequences() took %v seconds", delta.Seconds())
	}

	delta = time.Since(startTime)
	if delta.Seconds() > 1 {
		base.Logf("seqAlloc nextSequence() took %v seconds", delta.Seconds())
	}

	return s.last, nil
}

func (s *sequenceAllocator) _reserveSequences(numToReserve uint64) error {

	startTime := time.Now()
	startTimeAdd := time.Now()

	if s.last < s.max {
		return nil // Already have some sequences left; don't be greedy and waste them
		//OPT: Could remember multiple discontiguous ranges of free sequences
	}
	dbExpvars.Add("sequence_reserves", 1)
	delta := time.Since(startTimeAdd)
	if delta.Seconds() > 1 {
		base.Logf("_reserveSequences expvars.Add() took %v seconds", delta.Seconds())
	}

	startTimeIncr := time.Now()
	max, err := s.bucket.Incr("_sync:seq", numToReserve, numToReserve, 0)
	delta = time.Since(startTimeIncr)
	if delta.Seconds() > 1 {
		base.Logf("_reserveSequences bucket.Incr() took %v seconds", delta.Seconds())
	}

	if err != nil {
		base.Warn("Error from Incr in _reserveSequences(%d): %v", numToReserve, err)
		return err
	}
	s.max = max
	s.last = max - numToReserve

	delta := time.Since(startTime)
	if delta.Seconds() > 1 {
		base.Logf("seqAlloc _reserveSequences took %v seconds", delta.Seconds())
	}

	return nil
}

func (s *sequenceAllocator) reserveSequences(numToReserve uint64) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	return s._reserveSequences(numToReserve)
}
