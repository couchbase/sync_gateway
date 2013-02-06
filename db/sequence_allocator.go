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
	"github.com/couchbaselabs/go-couchbase"
)

type sequenceAllocator struct {
	bucket	*couchbase.Bucket	// Bucket whose counter to use
	mutex	sync.Mutex			// Makes this object thread-safe
	last 	uint64				// Last sequence # assigned
	max		uint64				// Max sequence # reserved
}

func newSequenceAllocator(bucket *couchbase.Bucket) (*sequenceAllocator, error) {
	s := &sequenceAllocator{bucket: bucket}
	return s, s.reserveSequences(0)		// just reads latest sequence from bucket
}

func (s *sequenceAllocator) lastSequence() (uint64, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	return s.last, nil
}

func (s *sequenceAllocator) nextSequence() (uint64 , error) {
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
	max, err := s.bucket.Incr("__seq", numToReserve, 0, 0)
	if err != nil {
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
