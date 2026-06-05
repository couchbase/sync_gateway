/*
Copyright 2026-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package base

import (
	"iter"
	"sync"
)

// SyncMap is a type-safe wrapper around sync.Map that eliminates runtime type assertions.
type SyncMap[K comparable, V any] struct {
	m sync.Map
}

func (s *SyncMap[K, V]) Load(key K) (V, bool) {
	v, ok := s.m.Load(key)
	if !ok {
		var zero V
		return zero, false
	}
	return v.(V), true
}

func (s *SyncMap[K, V]) Store(key K, value V) {
	s.m.Store(key, value)
}

func (s *SyncMap[K, V]) LoadOrStore(key K, value V) (V, bool) {
	actual, loaded := s.m.LoadOrStore(key, value)
	return actual.(V), loaded
}

func (s *SyncMap[K, V]) Range(yield func(K, V) bool) {
	s.m.Range(func(key, value any) bool {
		return yield(key.(K), value.(V))
	})
}

// All returns an iterator over all key-value pairs.
func (s *SyncMap[K, V]) All() iter.Seq2[K, V] {
	return s.Range
}
