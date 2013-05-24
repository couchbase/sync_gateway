package channels

import (
	"fmt"
	"testing"

	"github.com/couchbaselabs/go.assert"
)

func e(seq uint64, docid string, revid string) LogEntry {
	return LogEntry{
		Sequence: seq,
		DocID:    docid,
		RevID:    revid,
	}
}

func mklog(since uint64, entries ...LogEntry) ChangeLog {
	return ChangeLog{Since: since, Entries: entries}
}

func TestEmptyLog(t *testing.T) {
	var cl ChangeLog
	assert.Equals(t, len(cl.EntriesAfter(1234)), 0)

	cl.Add(e(1, "foo", "1-a"))
	assert.Equals(t, cl.Since, uint64(0))
	assert.DeepEquals(t, cl.EntriesAfter(0), []LogEntry{e(1, "foo", "1-a")})
	assert.DeepEquals(t, cl.EntriesAfter(1), []LogEntry{})
}

func TestAddInOrder(t *testing.T) {
	var cl ChangeLog
	cl.Add(e(1, "foo", "1-a"))
	cl.Add(e(2, "bar", "1-a"))
	assert.DeepEquals(t, cl.EntriesAfter(0), []LogEntry{e(1, "foo", "1-a"), e(2, "bar", "1-a")})
	assert.DeepEquals(t, cl.EntriesAfter(1), []LogEntry{e(2, "bar", "1-a")})
	assert.DeepEquals(t, cl.EntriesAfter(2), []LogEntry{})
	cl.Add(e(3, "zog", "1-a"))
	assert.DeepEquals(t, cl.EntriesAfter(2), []LogEntry{e(3, "zog", "1-a")})
	assert.DeepEquals(t, cl, mklog(0, e(1, "foo", "1-a"), e(2, "bar", "1-a"), e(3, "zog", "1-a")))
}

func TestAddOutOfOrder(t *testing.T) {
	var cl ChangeLog
	cl.Add(e(20, "bar", "1-a"))
	cl.Add(e(10, "foo", "1-a"))
	assert.Equals(t, cl.Since, uint64(9))
	assert.DeepEquals(t, cl.EntriesAfter(0), []LogEntry{e(20, "bar", "1-a"), e(10, "foo", "1-a")})
	assert.DeepEquals(t, cl.EntriesAfter(20), []LogEntry{e(10, "foo", "1-a")})
	assert.DeepEquals(t, cl.EntriesAfter(10), []LogEntry{})
	cl.Add(e(30, "zog", "1-a"))
	assert.DeepEquals(t, cl.EntriesAfter(20), []LogEntry{e(10, "foo", "1-a"), e(30, "zog", "1-a")})
	assert.DeepEquals(t, cl.EntriesAfter(10), []LogEntry{e(30, "zog", "1-a")})
	assert.DeepEquals(t, cl, mklog(9, e(20, "bar", "1-a"), e(10, "foo", "1-a"), e(30, "zog", "1-a")))
}

func TestReplace(t *testing.T) {
	// Add three sequences in order:
	var cl ChangeLog
	cl.Add(e(1, "foo", "1-a"))
	cl.Add(e(2, "bar", "1-a"))
	cl.Add(e(3, "zog", "1-a"))

	// Replace 'foo'
	cl.Update(e(4, "foo", "2-b"), "1-a")
	assert.DeepEquals(t, cl, mklog(0, e(1, "", ""), e(2, "bar", "1-a"), e(3, "zog", "1-a"), e(4, "foo", "2-b")))

	// Replace 'zog'
	cl.Update(e(5, "zog", "2-b"), "1-a")
	assert.DeepEquals(t, cl, mklog(0, e(1, "", ""), e(2, "bar", "1-a"), e(3, "", ""), e(4, "foo", "2-b"), e(5, "zog", "2-b")))

	// Replace 'zog' again
	cl.Update(e(6, "zog", "3-c"), "2-b")
	assert.DeepEquals(t, cl, mklog(0, e(1, "", ""), e(2, "bar", "1-a"), e(3, "", ""), e(4, "foo", "2-b"), e(5, "", ""), e(6, "zog", "3-c")))
}

func TestTruncate(t *testing.T) {
	const maxLogLength = 50
	var cl ChangeLog
	for i := 1; i <= 2*maxLogLength; i++ {
		cl.Add(e(uint64(i), "foo", fmt.Sprintf("%d-x", i)))
		cl.TruncateTo(maxLogLength)
	}
	assert.Equals(t, len(cl.Entries), maxLogLength)
	assert.Equals(t, int(cl.Since), maxLogLength)
}
