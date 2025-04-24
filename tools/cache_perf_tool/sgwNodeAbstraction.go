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
}

func newSequenceAllocator(batchSize int, syncSeq *syncSeqMock) *sequenceAllocator {
	return &sequenceAllocator{
		syncSeqMock:   syncSeq,
		batchSize:     batchSize,
		lastSeq:       0,
		maxSeqInBatch: 0,
	}
}

func (s *sequenceAllocator) nextSeq() uint64 {
	s.lock.Lock()
	defer s.lock.Unlock()
	if s.lastSeq >= s.maxSeqInBatch {
		max := s.syncSeqMock.nextBatch(uint64(s.batchSize))
		s.maxSeqInBatch = max
		s.lastSeq = max - uint64(s.batchSize)
	}
	s.lastSeq++
	return s.lastSeq
}
