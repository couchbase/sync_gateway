// Copyright 2025-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package main

import (
	"sync"
	"sync/atomic"
)

type sgwNode struct {
	nodeID   int
	seqAlloc *sequenceAllocator
}

type syncSeqMock struct {
	sequence atomic.Uint64
}

func newSyncSeq() *syncSeqMock {
	return &syncSeqMock{}
}

func (seqAlloc *syncSeqMock) nextBatch(batchSize uint64) uint64 {
	return seqAlloc.sequence.Add(batchSize)
}

type sequenceAllocator struct {
	syncSeqMock   *syncSeqMock
	batchSize     int
	lastSeq       uint64
	maxSeqInBatch uint64
	lock          sync.Mutex
	syncSeqEvent  chan struct{}
}

func newSequenceAllocator(batchSize int, syncSeq *syncSeqMock) *sequenceAllocator {
	return &sequenceAllocator{
		syncSeqMock:   syncSeq,
		batchSize:     batchSize,
		lastSeq:       0,
		maxSeqInBatch: 0,
		syncSeqEvent:  make(chan struct{}, 1), // make chan size 1, this is to aim to simulate sync seq being deduplicated upon rapid update
	}
}

func (s *sequenceAllocator) nextSeq() uint64 {
	var seq [1]uint64
	s.nextSequencesInto(seq[:])
	return seq[0]
}

// nextSequencesInto fills dst with the next len(dst) sequences. It writes into a caller-owned slice
// rather than returning one so the per-write allocation path stays allocation-free: dst does not escape,
// so callers can pass a stack or struct array.
func (s *sequenceAllocator) nextSequencesInto(dst []uint64) {
	s.lock.Lock()
	defer s.lock.Unlock()
	for i := range dst {
		if s.lastSeq >= s.maxSeqInBatch {
			maxSeq := s.syncSeqMock.nextBatch(uint64(s.batchSize))
			s.maxSeqInBatch = maxSeq
			s.lastSeq = maxSeq - uint64(s.batchSize)
			// the motivation to have cap 1 on this buffered channel is to sort of dedupe from KV engine upon
			// rapid updates to _sync:seq doc
			select {
			case s.syncSeqEvent <- struct{}{}:
			default: // default clause to we don't block on this select statement
			}
		}
		s.lastSeq++
		dst[i] = s.lastSeq
	}
}
