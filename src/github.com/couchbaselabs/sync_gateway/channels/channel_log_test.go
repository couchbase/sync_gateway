package channels

import (
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

func mklog(since uint64, entries ...LogEntry) ChannelLog {
	return ChannelLog{Since: since, Entries: entries}
}

func TestEmptyLog(t *testing.T) {
	var cl ChannelLog
	assert.Equals(t, len(cl.EntriesSince(1234)), 0)

	cl.Add(e(1, "foo", "1-a"))
	assert.Equals(t, cl.Since, uint64(0))
	assert.DeepEquals(t, cl.EntriesSince(0), []LogEntry{e(1, "foo", "1-a")})
	assert.DeepEquals(t, cl.EntriesSince(1), []LogEntry{})
}

func TestAddInOrder(t *testing.T) {
	var cl ChannelLog
	cl.Add(e(1, "foo", "1-a"))
	cl.Add(e(2, "bar", "1-a"))
	assert.DeepEquals(t, cl.EntriesSince(0), []LogEntry{e(1, "foo", "1-a"), e(2, "bar", "1-a")})
	assert.DeepEquals(t, cl.EntriesSince(1), []LogEntry{e(2, "bar", "1-a")})
	assert.DeepEquals(t, cl.EntriesSince(2), []LogEntry{})
	cl.Add(e(3, "zog", "1-a"))
	assert.DeepEquals(t, cl.EntriesSince(2), []LogEntry{e(3, "zog", "1-a")})
	assert.DeepEquals(t, cl, mklog(0, e(1, "foo", "1-a"), e(2, "bar", "1-a"), e(3, "zog", "1-a")))
}

func TestAddOutOfOrder(t *testing.T) {
	var cl ChannelLog
	cl.Add(e(20, "bar", "1-a"))
	cl.Add(e(10, "foo", "1-a"))
	assert.Equals(t, cl.Since, uint64(9))
	assert.DeepEquals(t, cl.EntriesSince(0), []LogEntry{e(10, "foo", "1-a"), e(20, "bar", "1-a")})
	assert.DeepEquals(t, cl.EntriesSince(10), []LogEntry{e(20, "bar", "1-a")})
	assert.DeepEquals(t, cl.EntriesSince(20), []LogEntry{})
	cl.Add(e(30, "zog", "1-a"))
	assert.DeepEquals(t, cl.EntriesSince(20), []LogEntry{e(30, "zog", "1-a")})
	assert.DeepEquals(t, cl, mklog(9, e(10, "foo", "1-a"), e(20, "bar", "1-a"), e(30, "zog", "1-a")))
	cl.Add(e(15, "wow", "1-a"))
	assert.Equals(t, cl.Since, uint64(9))
	assert.DeepEquals(t, cl.EntriesSince(10), []LogEntry{e(15, "wow", "1-a"), e(20, "bar", "1-a"), e(30, "zog", "1-a")})
	assert.DeepEquals(t, cl, mklog(9, e(10, "foo", "1-a"), e(15, "wow", "1-a"), e(20, "bar", "1-a"), e(30, "zog", "1-a")))
}

func TestDuplicate(t *testing.T) {
	var cl ChannelLog
	cl.Add(e(1, "foo", "1-a"))
	cl.Add(e(2, "bar", "1-a"))
	cl.Add(e(3, "zog", "1-a"))
	assert.False(t, cl.Add(e(1, "foo", "1-a")))
	assert.False(t, cl.Add(e(2, "bar", "1-a")))
	assert.False(t, cl.Add(e(3, "zog", "1-a")))
}

func TestObsolete(t *testing.T) {
	var cl ChannelLog
	cl.Add(e(10, "foo", "9-i"))
	cl.Add(e(20, "bar", "9-i"))
	cl.Add(e(30, "zog", "9-i"))
	assert.False(t, cl.Add(e(1, "foo", "1-a")))
	assert.False(t, cl.Add(e(2, "bar", "1-a")))
	assert.False(t, cl.Add(e(3, "zog", "1-a")))
}

func TestReplace(t *testing.T) {
	// Add three sequences in order:
	var cl ChannelLog
	cl.Add(e(1, "foo", "1-a"))
	cl.Add(e(2, "bar", "1-a"))
	cl.Add(e(3, "zog", "1-a"))

	// Replace 'foo'
	cl.Add(e(4, "foo", "2-b"))
	assert.DeepEquals(t, cl, mklog(0, e(2, "bar", "1-a"), e(3, "zog", "1-a"), e(4, "foo", "2-b")))

	// Replace 'zog'
	cl.Add(e(5, "zog", "2-b"))
	assert.DeepEquals(t, cl, mklog(0, e(2, "bar", "1-a"), e(4, "foo", "2-b"), e(5, "zog", "2-b")))

	// Replace 'zog' again
	cl.Add(e(6, "zog", "3-c"))
	assert.DeepEquals(t, cl, mklog(0, e(2, "bar", "1-a"), e(4, "foo", "2-b"), e(6, "zog", "3-c")))

	// Add duplicate 'foo', make sure it's ignored
	assert.False(t, cl.Add(e(1, "foo", "1-a")))
}
