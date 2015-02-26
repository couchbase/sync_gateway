package channels

import (
	"fmt"
	"testing"

	"github.com/couchbaselabs/go.assert"
)

func e(seq uint64, docid string, revid string) *LogEntry {
	return &LogEntry{
		Sequence: seq,
		DocID:    docid,
		RevID:    revid,
	}
}

func mklog(since uint64, entries ...*LogEntry) ChangeLog {
	return ChangeLog{Since: since, Entries: entries}
}

func TestEmptyLog(t *testing.T) {
	var cl ChangeLog
	assert.Equals(t, len(cl.EntriesAfter(1234)), 0)

	cl.Add(*e(1, "foo", "1-a"))
	assert.Equals(t, cl.Since, uint64(0))
	assert.DeepEquals(t, cl.EntriesAfter(0), []*LogEntry{e(1, "foo", "1-a")})
	assert.DeepEquals(t, cl.EntriesAfter(1), []*LogEntry{})
}

func TestAddInOrder(t *testing.T) {
	var cl ChangeLog
	cl.Add(*e(1, "foo", "1-a"))
	cl.Add(*e(2, "bar", "1-a"))
	assert.DeepEquals(t, cl.EntriesAfter(0), []*LogEntry{e(1, "foo", "1-a"), e(2, "bar", "1-a")})
	assert.DeepEquals(t, cl.EntriesAfter(1), []*LogEntry{e(2, "bar", "1-a")})
	assert.DeepEquals(t, cl.EntriesAfter(2), []*LogEntry{})
	cl.Add(*e(3, "zog", "1-a"))
	assert.DeepEquals(t, cl.EntriesAfter(2), []*LogEntry{e(3, "zog", "1-a")})
	assert.DeepEquals(t, cl, mklog(0, e(1, "foo", "1-a"), e(2, "bar", "1-a"), e(3, "zog", "1-a")))
}

func TestAddOutOfOrder(t *testing.T) {
	var cl ChangeLog
	cl.Add(*e(20, "bar", "1-a"))
	cl.Add(*e(10, "foo", "1-a"))
	assert.Equals(t, cl.Since, uint64(19))
	assert.DeepEquals(t, cl.EntriesAfter(0), []*LogEntry(nil))
	assert.DeepEquals(t, cl.EntriesAfter(20), []*LogEntry{e(10, "foo", "1-a")})
	assert.DeepEquals(t, cl.EntriesAfter(10), []*LogEntry{})
	cl.Add(*e(30, "zog", "1-a"))
	assert.DeepEquals(t, cl.EntriesAfter(20), []*LogEntry{e(10, "foo", "1-a"), e(30, "zog", "1-a")})
	assert.DeepEquals(t, cl.EntriesAfter(10), []*LogEntry{e(30, "zog", "1-a")})
	assert.DeepEquals(t, cl, mklog(19, e(20, "bar", "1-a"), e(10, "foo", "1-a"), e(30, "zog", "1-a")))
}

func TestTruncate(t *testing.T) {
	const maxLogLength = 50
	var cl ChangeLog
	for i := 1; i <= 2*maxLogLength; i++ {
		cl.Add(*e(uint64(i), "foo", fmt.Sprintf("%d-x", i)))
		cl.TruncateTo(maxLogLength)
	}
	assert.Equals(t, len(cl.Entries), maxLogLength)
	assert.Equals(t, int(cl.Since), maxLogLength)
}

func TestSort(t *testing.T) {
	var cl ChangeLog
	cl.Add(*e(3, "doc3", "1-a"))
	cl.Add(*e(1, "doc1", "1-a"))
	cl.Add(*e(4, "doc4", "1-a"))
	cl.Add(*e(2, "doc2", "1-a"))
	cl.Add(*e(5, "doc5", "1-a"))
	cl.Add(*e(9, "doc9", "1-a"))
	cl.Add(*e(8, "doc8", "1-a"))

	cl.Sort()

	expectedSeqs := []uint64{1, 2, 3, 4, 5, 8, 9}
	for i, entry := range cl.Entries {
		assert.Equals(t, entry.Sequence, expectedSeqs[i])
	}
}
