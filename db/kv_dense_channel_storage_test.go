package db

import (
	"log"
	"testing"

	"github.com/couchbase/sync_gateway/channels"
	"github.com/couchbaselabs/go.assert"
)

const (
	IsAdded      = true
	IsNotAdded   = false
	IsRemoval    = true
	IsNotRemoval = false
)

func makeBlockEntry(docId string, revId string, vbNo int, sequence int, removal bool, added bool) *LogEntry {
	entry := makeEntryForDoc(docId, revId, vbNo, sequence, removal)
	if added {
		entry.Flags |= channels.Added
	}
	return entry
}

func TestDenseBlock(t *testing.T) {

	indexBucket := testIndexBucket()
	defer indexBucket.Close()

	block := NewDenseBlock("block1")

	entries := make([]*LogEntry, 1)
	entries[0] = makeBlockEntry("doc1", "1-abc", 50, 1, IsNotRemoval, IsAdded)

	pendingRemoval, err := block.AddEntrySet(entries, indexBucket)
	assertNoError(t, err, "Error adding entry set")
	assert.Equals(t, len(pendingRemoval), 0)

	foundEntries, _ := block.GetAllEntries()
	assert.Equals(t, len(foundEntries), 0)
	assert.Equals(t, foundEntries[0].DocId, "doc1")
	assert.Equals(t, foundEntries[0].RevId, "1-abc")
	assert.Equals(t, foundEntries[0].VbNo, "50")
	assert.Equals(t, foundEntries[0].Sequence, "1")

}
