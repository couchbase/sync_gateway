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
	"fmt"
	"log"
	"math/rand"
	"testing"
	"time"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbaselabs/go.assert"
	"sort"
	"math"
)

func TestDuplicateDocID(t *testing.T) {

	base.EnableLogKey("Cache")
	context := testBucketContext()
	defer context.Close()
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

	base.EnableLogKey("Cache")
	context := testBucketContext()
	defer context.Close()
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

	base.EnableLogKey("Cache")
	context := testBucketContext()
	defer context.Close()
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

	base.EnableLogKey("Cache")
	context := testBucketContext()
	defer context.Close()
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

// Repro attempt for https://github.com/couchbase/sync_gateway/issues/2662
// I'm not 100% how the channel cache *should* behave, but this test does illustrate
// the problematic behavior when:
//    - The channel cache is at capacity
//    - The channel cache receives changes that are older than it's current oldest entry
func FailingTestExceedChannelCacheSize(t *testing.T) {

	// Make the channel cache smaller
	DefaultChannelCacheMinLength = 2
	DefaultChannelCacheMaxLength = 5

	base.EnableLogKey("Cache")
	context := testBucketContext()
	defer context.Close()
	cache := newChannelCache(context, "Test1", 0)

	// Add some entries to cache
	cache.addToCache(e(1, "doc1", "1-doc1"), false)
	cache.addToCache(e(2, "doc2", "1-doc2"), false)
	cache.addToCache(e(3, "doc3", "1-doc3"), false)
	cache.addToCache(e(4, "doc4", "1-doc4"), false)
	cache.addToCache(e(5, "doc5", "1-doc5"), false)

	// skip sequence 6

	// Increment revs for all docs
	cache.addToCache(e(7, "doc5", "2-doc5"), false)
	cache.addToCache(e(8, "doc1", "2-doc1"), false)
	cache.addToCache(e(9, "doc2", "2-doc2"), false)
	cache.addToCache(e(10, "doc3", "2-doc3"), false)
	cache.addToCache(e(11, "doc4", "2-doc4"), false)

	// Increment revs for all docs one more time
	cache.addToCache(e(12, "doc5", "3-doc5"), false)
	cache.addToCache(e(13, "doc1", "3-doc1"), false)
	cache.addToCache(e(14, "doc2", "3-doc2"), false)
	cache.addToCache(e(15, "doc3", "3-doc3"), false)
	cache.addToCache(e(16, "doc4", "3-doc4"), false)

	sinceZero := SequenceID{Seq: 0}

	expectedSequences := []int{12,13,14,15,16}
	expectedValidFrom := uint64(0)
	if err := verifyExpectedSequences(cache, sinceZero, expectedValidFrom, expectedSequences); err != nil {
		t.Fatalf("verifyExpectedSequences failed: %v", err)
	}

	// now add the previously skipped sequence
	skippedSequence := uint64(6)
	skippedLogEntry := &LogEntry{
		Sequence:     skippedSequence,
		DocID:        "doc6",
		RevID:        "1-doc6",
		Skipped:      true,
		TimeReceived: time.Now(),
	}
	cache.addToCache(skippedLogEntry, false)

	// Verify that the changes contains the skipped sequence
	validFrom, _ := cache.getCachedChanges(ChangesOptions{Since: sinceZero})

	// If the validFrom is greater than the skippedSequence we just tried to add, that means
	// it basically got pruned away instantly and will never have a chance to be returned in
	// the changes feed, which is the bug reported in https://github.com/couchbase/sync_gateway/issues/2662
	// Test currently fails here.
	if validFrom > skippedSequence {
		t.Errorf("validFrom (%d) > skippedSequence (%d)", validFrom, skippedSequence)
	}

	_, changes := cache.getCachedChanges(ChangesOptions{Since: sinceZero})

	foundSeq6 := false
	foundSeq16 := false
	for _, change := range changes {
		if change.Sequence == uint64(6) {
			foundSeq6 = true
		}
		if change.Sequence == uint64(16) {
			foundSeq16 = true
		}
	}
	assert.True(t, foundSeq6)
	assert.True(t, foundSeq16)

	// Add another revision
	cache.addToCache(e(17, "doc7", "1-doc7"), false)

	// Should see sequence 6 in these changes
	validFrom, changes = cache.getCachedChanges(ChangesOptions{Since: sinceZero})

	foundSeq6 = false
	foundSeq17 := false
	for _, change := range changes {
		if change.Sequence == uint64(6) {
			foundSeq6 = true
		}
		if change.Sequence == uint64(17) {
			foundSeq17 = true
		}
	}
	assert.True(t, foundSeq6)
	assert.True(t, foundSeq17)

}


// Repro attempt for https://github.com/couchbase/sync_gateway/issues/2662
// This illustrates the problematic behavior when:
//    - The channel cache receives changes that is older than is allowed by the ChannelCacheAge parameter
//    - The channel cache immediately prunes/discards the change, so that it can never be returned from changes feed
func FailingTestExceedChannelCacheSizeOldEntry(t *testing.T) {

	// Make the channel cache smaller
	DefaultChannelCacheMinLength = 0
	DefaultChannelCacheMaxLength = 5

	base.EnableLogKey("Cache")
	context := testBucketContext()
	defer context.Close()
	cache := newChannelCache(context, "Test1", 0)

	// Add some entries to cache -- skip sequence 1
	cache.addToCache(e(2, "doc1", "1-doc1"), false)
	cache.addToCache(e(3, "doc2", "1-doc2"), false)
	cache.addToCache(e(4, "doc3", "1-doc3"), false)

	changesSince0 := ChangesOptions{Since: SequenceID{Seq: 0}}

	twiceDefaultChannelCacheAge := DefaultChannelCacheAge * -2
	stale := time.Now().Add(twiceDefaultChannelCacheAge)

	// now add the previously skipped sequence with a very old TimeReceived
	skippedSequence := uint64(1)
	skippedLogEntry := &LogEntry{
		Sequence:     skippedSequence,
		DocID:        "doc6",
		RevID:        "1-doc6",
		Skipped:      true,
		TimeReceived: stale,
	}
	cache.addToCache(skippedLogEntry, false)

	validFrom, _ := cache.getCachedChanges(changesSince0)

	// If the validFrom is greater than the skippedSequence we just tried to add, that means
	// it basically got pruned away instantly and will never have a chance to be returned in
	// the changes feed, which is the bug reported in https://github.com/couchbase/sync_gateway/issues/2662
	assert.True(t, validFrom <= skippedSequence)



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

func BenchmarkChannelCacheUniqueDocs_Ordered(b *testing.B) {

	base.SetLogLevel(2) // disables logging
	//base.SetLogLevel(2) // disables logging
	context := testBucketContext()
	defer context.Close()
	cache := newChannelCache(context, "Benchmark", 0)
	// generate doc IDs
	docIDs := make([]string, b.N)
	for i := 0; i < b.N; i++ {
		docIDs[i] = fmt.Sprintf("long_document_id_for_sufficient_equals_complexity_%012d", i)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cache.addToCache(e(uint64(i), docIDs[i], "1-a"), false)
	}
}

func BenchmarkChannelCacheRepeatedDocs5(b *testing.B) {

	base.SetLogLevel(2) // disables logging
	context := testBucketContext()
	defer context.Close()
	cache := newChannelCache(context, "Benchmark", 0)
	// generate doc IDs

	docIDs, revStrings := generateDocs(5.0, b.N)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cache.addToCache(e(uint64(i), docIDs[i], revStrings[i]), false)
	}
}

func BenchmarkChannelCacheRepeatedDocs20(b *testing.B) {

	//base.LogKeys["Cache+"] = true
	base.SetLogLevel(2) // disables logging
	context := testBucketContext()
	defer context.Close()
	cache := newChannelCache(context, "Benchmark", 0)
	// generate doc IDs

	docIDs, revStrings := generateDocs(20.0, b.N)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cache.addToCache(e(uint64(i), docIDs[i], revStrings[i]), false)
	}
}

func BenchmarkChannelCacheRepeatedDocs50(b *testing.B) {

	base.SetLogLevel(2) // disables logging
	context := testBucketContext()
	defer context.Close()
	cache := newChannelCache(context, "Benchmark", 0)
	// generate doc IDs

	docIDs, revStrings := generateDocs(50.0, b.N)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cache.addToCache(e(uint64(i), docIDs[i], revStrings[i]), false)
	}
}

func BenchmarkChannelCacheRepeatedDocs80(b *testing.B) {

	base.SetLogLevel(2) // disables logging
	context := testBucketContext()
	defer context.Close()
	cache := newChannelCache(context, "Benchmark", 0)
	// generate doc IDs

	docIDs, revStrings := generateDocs(80.0, b.N)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cache.addToCache(e(uint64(i), docIDs[i], revStrings[i]), false)
	}
}

func BenchmarkChannelCacheRepeatedDocs95(b *testing.B) {

	base.EnableLogKey("CacheTest")
	//base.SetLogLevel(2) // disables logging
	context := testBucketContext()
	defer context.Close()
	cache := newChannelCache(context, "Benchmark", 0)
	// generate doc IDs

	docIDs, revStrings := generateDocs(95.0, b.N)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cache.addToCache(e(uint64(i), docIDs[i], revStrings[i]), false)
	}
}

func BenchmarkChannelCacheUniqueDocs_Unordered(b *testing.B) {

	base.SetLogLevel(2) // disables logging
	context := testBucketContext()
	defer context.Close()
	cache := newChannelCache(context, "Benchmark", 0)
	// generate docs
	docs := make([]*LogEntry, b.N)
	r := rand.New(rand.NewSource(99))
	for i := 0; i < b.N; i++ {
		docs[i] = e(uint64(i), fmt.Sprintf("long_document_id_for_sufficient_equals_complexity_%012d", i), "1-a")
	}
	// shuffle sequences
	for i := b.N - 1; i >= 0; i-- {
		j := int(r.Float64() * float64(b.N))
		oldSeq := docs[i].Sequence
		docs[i].Sequence = docs[j].Sequence
		docs[j].Sequence = oldSeq
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		cache.addToCache(docs[i], false)
	}
}

func generateDocs(percentInsert float64, N int) ([]string, []string) {

	docIDs := make([]string, N)
	revStrings := make([]string, N)
	revCount := make(map[int]int)
	r := rand.New(rand.NewSource(99))
	uniqueDocs := percentInsert / 100 * float64(N)
	maxRevCount := 0
	for i := 0; i < N; i++ {
		docIndex := int(r.Float64() * uniqueDocs)
		docIDs[i] = fmt.Sprintf("long_document_id_for_sufficient_equals_complexity_%012d", docIndex)
		revCount[docIndex]++
		revStrings[i] = fmt.Sprintf("rev-%d", revCount[docIndex])
		if revCount[docIndex] > maxRevCount {
			maxRevCount = revCount[docIndex]
		}
	}
	return docIDs, revStrings
}


func verifyExpectedSequences(cache *channelCache, since SequenceID, expectedValidFrom uint64, expectedSequences []int) error {

	sort.Ints(expectedSequences)

	changesSince0 := ChangesOptions{Since: since}
	validFrom, changes := cache.getCachedChanges(changesSince0)

	if expectedValidFrom != math.MaxUint64 {  // if caller passed math.MaxUint64 for expectedValidFrom, ignore the verification
		if validFrom != expectedValidFrom {
			return fmt.Errorf("Got validFrom: %v, expected: %v", validFrom, expectedValidFrom)

		}
	}

	changeSequences := []int{}
	for _, change := range changes {
		changeSequences = append(changeSequences, int(change.Sequence))
	}
	sort.Ints(changeSequences)

	for i, changeSequence := range changeSequences {
		if changeSequence != expectedSequences[i] {
			return fmt.Errorf("changeSequence (%d) != expectedSequence (%d) for i=%d", changeSequence, expectedSequences[i], i)
		}
	}

	return nil

}