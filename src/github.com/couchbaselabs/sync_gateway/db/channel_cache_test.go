//  Copyright (c) 2015 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package db

import (
	"log"
	"testing"

	"github.com/couchbaselabs/go.assert"
	"github.com/couchbaselabs/sync_gateway/base"
)

func TestDuplicateDocID(t *testing.T) {

	base.LogKeys["Cache"] = true
	context, _ := NewDatabaseContext("db", testBucket(), false, CacheOptions{})
	cache := newChannelCache(context, "Test1", 0)

	// Add some entries to cache
	cache.addToCache(e(1, "doc1", "1-a"), false)
	cache.addToCache(e(2, "doc3", "3-a"), false)
	cache.addToCache(e(3, "doc5", "5-a"), false)

	entries, err := cache.GetChanges(ChangesOptions{Since: SequenceID{Seq: 0}})
	assert.Equals(t, len(entries), 3)
	assert.True(t, verifyChannelSequences(entries, []uint64{1, 2, 3}))
	assert.True(t, verifyChannelDocIDs(entries, []string{"doc1", "doc3", "doc5"}))
	assert.True(t, err == nil)

	// Add a new revision matching mid-list
	cache.addToCache(e(4, "doc3", "3-b"), false)
	entries, err = cache.GetChanges(ChangesOptions{Since: SequenceID{Seq: 0}})
	assert.Equals(t, len(entries), 3)
	assert.True(t, verifyChannelSequences(entries, []uint64{1, 3, 4}))
	assert.True(t, verifyChannelDocIDs(entries, []string{"doc1", "doc5", "doc3"}))
	assert.True(t, err == nil)

	// Add a new revision matching first
	cache.addToCache(e(5, "doc1", "1-b"), false)
	entries, err = cache.GetChanges(ChangesOptions{Since: SequenceID{Seq: 0}})
	assert.Equals(t, len(entries), 3)
	assert.True(t, verifyChannelSequences(entries, []uint64{3, 4, 5}))
	assert.True(t, verifyChannelDocIDs(entries, []string{"doc5", "doc3", "doc1"}))
	assert.True(t, err == nil)

	// Add a new revision matching last
	cache.addToCache(e(6, "doc1", "1-c"), false)
	entries, err = cache.GetChanges(ChangesOptions{Since: SequenceID{Seq: 0}})
	assert.Equals(t, len(entries), 3)
	assert.True(t, verifyChannelSequences(entries, []uint64{3, 4, 6}))
	assert.True(t, verifyChannelDocIDs(entries, []string{"doc5", "doc3", "doc1"}))
	assert.True(t, err == nil)

}

func TestLateArrivingSequence(t *testing.T) {

	base.LogKeys["Cache"] = true
	context, _ := NewDatabaseContext("db", testBucket(), false, CacheOptions{})
	cache := newChannelCache(context, "Test1", 0)

	// Add some entries to cache
	cache.addToCache(e(1, "doc1", "1-a"), false)
	cache.addToCache(e(3, "doc3", "3-a"), false)
	cache.addToCache(e(5, "doc5", "5-a"), false)

	entries, err := cache.GetChanges(ChangesOptions{Since: SequenceID{Seq: 0}})
	assert.Equals(t, len(entries), 3)
	assert.True(t, verifyChannelSequences(entries, []uint64{1, 3, 5}))
	assert.True(t, verifyChannelDocIDs(entries, []string{"doc1", "doc3", "doc5"}))
	assert.True(t, err == nil)

	// Add a late-arriving sequence
	cache.AddLateSequence(e(2, "doc2", "2-a"))
	cache.addToCache(e(2, "doc2", "2-a"), false)
	entries, err = cache.GetChanges(ChangesOptions{Since: SequenceID{Seq: 0}})
	assert.Equals(t, len(entries), 4)
	writeEntries(entries)
	assert.True(t, verifyChannelSequences(entries, []uint64{1, 2, 3, 5}))
	assert.True(t, verifyChannelDocIDs(entries, []string{"doc1", "doc2", "doc3", "doc5"}))
	assert.True(t, err == nil)

}

func TestLateSequenceAsFirst(t *testing.T) {

	base.LogKeys["Cache"] = true
	context, _ := NewDatabaseContext("db", testBucket(), false, CacheOptions{})
	cache := newChannelCache(context, "Test1", 0)

	// Add some entries to cache
	cache.addToCache(e(5, "doc1", "1-a"), false)
	cache.addToCache(e(10, "doc2", "2-a"), false)
	cache.addToCache(e(15, "doc3", "3-a"), false)

	entries, err := cache.GetChanges(ChangesOptions{Since: SequenceID{Seq: 0}})
	assert.Equals(t, len(entries), 3)
	assert.True(t, verifyChannelSequences(entries, []uint64{5, 10, 15}))
	assert.True(t, verifyChannelDocIDs(entries, []string{"doc1", "doc2", "doc3"}))
	assert.True(t, err == nil)

	// Add a late-arriving sequence
	cache.AddLateSequence(e(3, "doc0", "0-a"))
	cache.addToCache(e(3, "doc0", "0-a"), false)
	entries, err = cache.GetChanges(ChangesOptions{Since: SequenceID{Seq: 0}})
	assert.Equals(t, len(entries), 4)
	writeEntries(entries)
	assert.True(t, verifyChannelSequences(entries, []uint64{3, 5, 10, 15}))
	assert.True(t, verifyChannelDocIDs(entries, []string{"doc0", "doc1", "doc2", "doc3"}))
	assert.True(t, err == nil)

}

func TestDuplicateLateArrivingSequence(t *testing.T) {

	base.LogKeys["Cache"] = true
	context, _ := NewDatabaseContext("db", testBucket(), false, CacheOptions{})
	cache := newChannelCache(context, "Test1", 0)

	// Add some entries to cache
	cache.addToCache(e(10, "doc1", "1-a"), false)
	cache.addToCache(e(20, "doc2", "2-a"), false)
	cache.addToCache(e(30, "doc3", "3-a"), false)
	cache.addToCache(e(40, "doc4", "4-a"), false)

	entries, err := cache.GetChanges(ChangesOptions{Since: SequenceID{Seq: 0}})
	assert.Equals(t, len(entries), 4)
	assert.True(t, verifyChannelSequences(entries, []uint64{10, 20, 30, 40}))
	assert.True(t, verifyChannelDocIDs(entries, []string{"doc1", "doc2", "doc3", "doc4"}))
	assert.True(t, err == nil)

	// Add a late-arriving sequence that should replace earlier sequence
	cache.AddLateSequence(e(25, "doc1", "1-c"))
	cache.addToCache(e(25, "doc1", "1-c"), false)
	entries, err = cache.GetChanges(ChangesOptions{Since: SequenceID{Seq: 0}})
	assert.Equals(t, len(entries), 4)
	writeEntries(entries)
	assert.True(t, verifyChannelSequences(entries, []uint64{20, 25, 30, 40}))
	assert.True(t, verifyChannelDocIDs(entries, []string{"doc2", "doc1", "doc3", "doc4"}))
	assert.True(t, err == nil)

	// Add a late-arriving sequence that should be ignored (later sequence exists for that docID)
	cache.AddLateSequence(e(15, "doc1", "1-b"))
	cache.addToCache(e(15, "doc1", "1-b"), false)
	entries, err = cache.GetChanges(ChangesOptions{Since: SequenceID{Seq: 0}})
	assert.Equals(t, len(entries), 4)
	writeEntries(entries)
	assert.True(t, verifyChannelSequences(entries, []uint64{20, 25, 30, 40}))
	assert.True(t, verifyChannelDocIDs(entries, []string{"doc2", "doc1", "doc3", "doc4"}))
	assert.True(t, err == nil)

	// Add a late-arriving sequence adjacent to same ID (cache inserts differently)
	cache.AddLateSequence(e(27, "doc1", "1-d"))
	cache.addToCache(e(27, "doc1", "1-d"), false)
	entries, err = cache.GetChanges(ChangesOptions{Since: SequenceID{Seq: 0}})
	assert.Equals(t, len(entries), 4)
	writeEntries(entries)
	assert.True(t, verifyChannelSequences(entries, []uint64{20, 27, 30, 40}))
	assert.True(t, verifyChannelDocIDs(entries, []string{"doc2", "doc1", "doc3", "doc4"}))
	assert.True(t, err == nil)

	// Add a late-arriving sequence adjacent to same ID (cache inserts differently)
	cache.AddLateSequence(e(41, "doc4", "4-b"))
	cache.addToCache(e(41, "doc4", "4-b"), false)
	entries, err = cache.GetChanges(ChangesOptions{Since: SequenceID{Seq: 0}})
	assert.Equals(t, len(entries), 4)
	writeEntries(entries)
	assert.True(t, verifyChannelSequences(entries, []uint64{20, 27, 30, 41}))
	assert.True(t, verifyChannelDocIDs(entries, []string{"doc2", "doc1", "doc3", "doc4"}))
	assert.True(t, err == nil)

	// Add late arriving that's duplicate of oldest in cache
	cache.AddLateSequence(e(45, "doc2", "2-b"))
	cache.addToCache(e(45, "doc2", "2-b"), false)
	entries, err = cache.GetChanges(ChangesOptions{Since: SequenceID{Seq: 0}})
	assert.Equals(t, len(entries), 4)
	writeEntries(entries)
	assert.True(t, verifyChannelSequences(entries, []uint64{27, 30, 41, 45}))
	assert.True(t, verifyChannelDocIDs(entries, []string{"doc1", "doc3", "doc4", "doc2"}))
	assert.True(t, err == nil)

}

func verifyChannelSequences(entries []*LogEntry, sequences []uint64) bool {
	if len(entries) != len(sequences) {
		log.Printf("verifyChannelSequences: entries size (%v) not equals to sequences size (%v)",
			len(entries), len(sequences))
		return false
	}
	for index, seq := range sequences {
		if entries[index].Sequence != seq {
			log.Printf("verifyChannelSequences: sequence mismatch at index %v, entries=%d, sequences=%d",
				index, entries[index].Sequence, seq)
			return false
		}
	}
	return true
}

func verifyChannelDocIDs(entries []*LogEntry, docIDs []string) bool {
	if len(entries) != len(docIDs) {
		log.Printf("verifyChannelDocIDs: entries size (%v) not equals to DocIDs size (%v)",
			len(entries), len(docIDs))
		return false
	}
	for index, docID := range docIDs {
		if entries[index].DocID != docID {
			log.Printf("verifyChannelDocIDs: DocID mismatch at index %v, entries=%s, DocIDs=%s",
				index, entries[index].DocID, docID)
			return false
		}
	}
	return true
}

func writeEntries(entries []*LogEntry) {
	for index, entry := range entries {
		log.Printf("%d:seq=%d, docID=%s, revID=%s", index, entry.Sequence, entry.DocID, entry.RevID)
	}
}
