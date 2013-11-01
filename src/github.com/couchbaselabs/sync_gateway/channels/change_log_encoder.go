package channels

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math"

	"github.com/couchbaselabs/sync_gateway/base"
)

// Encodes a ChangeLog into a simple appendable data format.
func (ch *ChangeLog) Encode(w io.Writer) {
	writeSequence(ch.Since, w)
	for _, entry := range ch.Entries {
		entry.Encode(w, "")
	}
}

// Encodes a LogEntry in a format that can be appended to an encoded ChangeLog.
func (entry *LogEntry) Encode(w io.Writer, parent string) {
	binary.Write(w, binary.BigEndian, entry.Flags)
	writeSequence(entry.Sequence, w)
	writeString(entry.DocID, w)
	writeString(entry.RevID, w)
	writeString(parent, w)
}

// Decodes an encoded ChangeLog.
func DecodeChangeLog(r *bytes.Reader, afterSeq uint64) *ChangeLog {
	type docAndRev struct {
		docID, revID string
	}

	ch := ChangeLog{
		Since:   readSequence(r),
		Entries: make([]*LogEntry, 0, 500),
	}
	parents := map[docAndRev]*LogEntry{}
	cleanup := false
	skipping := (afterSeq > 0)
	var buf [1]byte
	for {
		n, _ := r.Read(buf[0:1])
		if n == 0 {
			break // eof
		}
		if buf[0] > 7 {
			panic("bad flags")
		}
		seq := readSequence(r)
		if skipping {
			if seq >= afterSeq {
				skipping = false
			}
			if seq <= afterSeq {
				skipString(r)
				skipString(r)
				skipString(r)
				continue // ignore this sequence
			}
		}

		entry := &LogEntry{
			Flags:    buf[0],
			Sequence: seq,
			DocID:    readString(r),
			RevID:    readString(r),
		}

		if parentID := readString(r); parentID != "" {
			if parent := parents[docAndRev{entry.DocID, parentID}]; parent != nil {
				// Clear out the parent rev that was overwritten by this one
				parent.DocID = ""
				parent.RevID = ""
				cleanup = true
			}
		}
		parents[docAndRev{entry.DocID, entry.RevID}] = entry

		ch.Entries = append(ch.Entries, entry)
	}

	// Now remove any overwritten entries:
	if cleanup {
		iDst := 0
		for iSrc, entry := range ch.Entries {
			if entry.DocID != "" { // only copy non-cleared entries
				if iDst < iSrc {
					ch.Entries[iDst] = entry
				}
				iDst++
			}
		}
		ch.Entries = ch.Entries[0:iDst]
	}

	if afterSeq > ch.Since {
		ch.Since = afterSeq
	}
	return &ch
}

// Removes the oldest entries to limit the log's length to `maxLength`.
// This is the same as ChangeLog.Truncate except it works directly on the encoded form, which is
// much faster than decoding+truncating+encoding.
func TruncateEncodedChangeLog(r *bytes.Reader, maxLength, minLength int, w io.Writer) (removed int, newLength int) {
	since := readSequence(r)
	// Find the starting position and sequence of each entry:
	entryPos := make([]int64, 0, 1000)
	entrySeq := make([]uint64, 0, 1000)
	for {
		pos, _ := r.Seek(0, 1)
		flags, err := r.ReadByte()
		if err != nil {
			break // eof
		}
		seq := readSequence(r)
		skipString(r)
		skipString(r)
		skipString(r)
		if flags > 7 {
			panic(fmt.Sprintf("bad flags %x, entry %d, offset %d", flags, len(entryPos)-1, pos))
		}

		entryPos = append(entryPos, pos)
		entrySeq = append(entrySeq, seq)
	}

	// How many entries to remove?
	// * Leave no more than maxLength entries
	// * Every sequence value removed should be less than every sequence remaining.
	// * The new 'since' value should be the maximum sequence removed.
	oldLength := len(entryPos)
	removed = oldLength - maxLength
	if removed <= 0 {
		removed = 0
	} else {
		pivot, newSince := findPivot(entrySeq, removed-1)
		removed = pivot + 1
		if oldLength-removed >= minLength {
			since = newSince
		} else {
			removed = 0
			base.Warn("TruncateEncodedChangeLog: Couldn't find a safe place to truncate")
			//TODO: Possibly find a pivot earlier than desired?
		}
	}

	// Write the updated Since and the remaining entries:
	writeSequence(since, w)
	r.Seek(entryPos[removed], 0)
	io.Copy(w, r)
	return removed, oldLength - removed
}

//////// UTILITY FUNCTIONS:

func writeSequence(seq uint64, w io.Writer) {
	var buf [16]byte
	len := binary.PutUvarint(buf[0:16], seq)
	w.Write(buf[0:len])
}

func readSequence(r io.ByteReader) uint64 {
	seq, _ := binary.ReadUvarint(r)
	return seq
}

func writeString(s string, w io.Writer) {
	b := []byte(s)
	length := len(b)
	if length > 255 {
		panic("Doc/rev ID too long to encode: " + s)
	}
	if err := binary.Write(w, binary.BigEndian, uint8(length)); err != nil {
		panic("Write failed")
	}
	if _, err := w.Write(b); err != nil {
		panic("writeString failed")
	}
}

func readLength(r io.Reader) uint8 {
	var lengthBuf [1]byte
	if _, err := r.Read(lengthBuf[0:1]); err != nil {
		panic("readString length failed")
	}
	return lengthBuf[0]
}

func readString(r io.Reader) string {
	length := readLength(r)
	data := make([]byte, length)
	if _, err := io.ReadFull(r, data); err != nil {
		panic("readString bytes failed")
	}
	return string(data)
}

func skipString(r io.ReadSeeker) {
	length := readLength(r)
	r.Seek(int64(length), 1)
}

// Finds a 'pivot' index, at or after minIndex, such that all array values before and at the pivot
// are less than all array values after it.
func findPivot(values []uint64, minIndex int) (pivot int, maxBefore uint64) {
	// First construct a table where minRight[i] is the minimum value in [i..n)
	n := len(values)
	minRight := make([]uint64, n)
	var min uint64 = math.MaxUint64
	for i := n - 1; i >= 0; i-- {
		if values[i] < min {
			min = values[i]
		}
		minRight[i] = min
	}
	// Now scan left-to-right tracking the running max and looking for a pivot:
	maxBefore = 0
	for pivot = 0; pivot < n-1; pivot++ {
		if values[pivot] > maxBefore {
			maxBefore = values[pivot]
		}
		if pivot >= minIndex && maxBefore < minRight[pivot+1] {
			break
		}
	}
	//log.Printf("PIVOT: %v @%d -> %d", values, minIndex, pivot)
	return
}
