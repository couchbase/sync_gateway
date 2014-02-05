package channels

import (
	"bytes"
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

func TestChangeLogEncoding(t *testing.T) {
	assert.Equals(t, Deleted, 1)
	assert.Equals(t, Removed, 2)
	assert.Equals(t, Hidden, 4)

	var cl ChangeLog
	cl.Add(*e(20, "some document", "1-ajkljkjklj"))
	cl.Add(*e(666, "OtherDocument", "666-fjkldfjdkfjd"))
	cl.Add(*e(123456, "a", "5-cafebabe"))

	var w bytes.Buffer
	cl.Encode(&w)
	data := w.Bytes()
	assert.DeepEquals(t, data, []byte{0x13, 0x0, 0x14, 0xd, 0x73, 0x6f, 0x6d, 0x65, 0x20, 0x64, 0x6f, 0x63, 0x75, 0x6d, 0x65, 0x6e, 0x74, 0xc, 0x31, 0x2d, 0x61, 0x6a, 0x6b, 0x6c, 0x6a, 0x6b, 0x6a, 0x6b, 0x6c, 0x6a, 0x0, 0x0, 0x9a, 0x5, 0xd, 0x4f, 0x74, 0x68, 0x65, 0x72, 0x44, 0x6f, 0x63, 0x75, 0x6d, 0x65, 0x6e, 0x74, 0x10, 0x36, 0x36, 0x36, 0x2d, 0x66, 0x6a, 0x6b, 0x6c, 0x64, 0x66, 0x6a, 0x64, 0x6b, 0x66, 0x6a, 0x64, 0x0, 0x0, 0xc0, 0xc4, 0x7, 0x1, 0x61, 0xa, 0x35, 0x2d, 0x63, 0x61, 0x66, 0x65, 0x62, 0x61, 0x62, 0x65, 0x0})

	cl2 := DecodeChangeLog(bytes.NewReader(data), 0, nil)
	assert.Equals(t, cl2.Since, cl.Since)
	assert.Equals(t, len(cl2.Entries), len(cl.Entries))
	for i, entry := range cl2.Entries {
		assert.DeepEquals(t, entry, cl.Entries[i])
	}

	cl2 = DecodeChangeLog(bytes.NewReader(data), 20, nil)
	assert.Equals(t, cl2.Since, uint64(20))
	assert.Equals(t, len(cl2.Entries), 2)
	for i, entry := range cl2.Entries {
		assert.DeepEquals(t, entry, cl.Entries[i+1])
	}

	// Append a new entry to the encoded bytes:
	newEntry := LogEntry{
		Sequence: 667,
		DocID:    "some document",
		RevID:    "22-x",
		Flags:    Removed,
	}
	var wNew bytes.Buffer
	newEntry.Encode(&wNew, "1-ajkljkjklj") // It will replace the first entry
	moreData := append(data, wNew.Bytes()...)

	cl2 = DecodeChangeLog(bytes.NewReader(moreData), 0, nil)
	assert.Equals(t, cl2.Since, cl.Since)
	assert.Equals(t, len(cl2.Entries), len(cl.Entries))
	assert.DeepEquals(t, cl2.Entries[len(cl2.Entries)-1], &newEntry)

	// Truncate cl2's encoded data:
	var wTrunc bytes.Buffer
	removed, newCount := TruncateEncodedChangeLog(bytes.NewReader(moreData), 2, 1, &wTrunc)
	assert.Equals(t, removed, 2)
	assert.Equals(t, newCount, 2)
	data3 := wTrunc.Bytes()
	assert.DeepEquals(t, data3, []byte{0x9a, 0x5, 0x0, 0xc0, 0xc4, 0x7, 0x1, 0x61, 0xa, 0x35, 0x2d, 0x63, 0x61, 0x66, 0x65, 0x62, 0x61, 0x62, 0x65, 0x0, 0x2, 0x9b, 0x5, 0xd, 0x73, 0x6f, 0x6d, 0x65, 0x20, 0x64, 0x6f, 0x63, 0x75, 0x6d, 0x65, 0x6e, 0x74, 0x4, 0x32, 0x32, 0x2d, 0x78, 0xc, 0x31, 0x2d, 0x61, 0x6a, 0x6b, 0x6c, 0x6a, 0x6b, 0x6a, 0x6b, 0x6c, 0x6a})

	cl3 := DecodeChangeLog(bytes.NewReader(data3), 0, nil)
	assert.Equals(t, cl3.Since, uint64(666))
	assert.Equals(t, len(cl3.Entries), 2)
	assert.DeepEquals(t, cl3.Entries[0], cl2.Entries[1])
	assert.DeepEquals(t, cl3.Entries[1], cl2.Entries[2])
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

func TestFindPivot(t *testing.T) {
	type testCase struct {
		seq      []uint64
		minIndex int
		pivot    int
	}
	testCases := []testCase{
		{[]uint64{1, 2, 3, 4, 5}, 0, 0},
		{[]uint64{1, 2, 3, 4, 5}, 1, 1},
		{[]uint64{1, 2, 3, 4, 5}, 3, 3},
		{[]uint64{1, 2, 3, 4, 5}, 4, 4},
		{[]uint64{1, 3, 4, 2, 5}, 2, 3},
		{[]uint64{1, 3, 4, 2, 5}, 1, 3},
		{[]uint64{1, 3, 4, 2, 5}, 3, 3},
		{[]uint64{1, 3, 4, 2, 5}, 4, 4},
		{[]uint64{3, 4, 2, 5, 1}, 1, 4},
		{[]uint64{3, 2, 4, 1, 5, 6, 8, 7, 9}, 2, 3},
	}
	for _, c := range testCases {
		pivot, _ := findPivot(c.seq, c.minIndex)
		if pivot != c.pivot {
			t.Errorf("findPivot(%v, %d) -> %d; should be %d",
				c.seq, c.minIndex, pivot, c.pivot)
		}
	}
}
