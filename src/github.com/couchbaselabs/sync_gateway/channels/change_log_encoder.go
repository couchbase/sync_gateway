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
	entry.assertValid()
	writeUInt8(entry.Flags, w)
	writeSequence(entry.Sequence, w)
	writeString(entry.DocID, w)
	writeString(entry.RevID, w)
	writeString(parent, w)
}

// Decodes an encoded ChangeLog.
func DecodeChangeLog(r *bytes.Reader, afterSeq uint64, oldLog *ChangeLog) (log *ChangeLog) {
	defer func() {
		if panicMsg := recover(); panicMsg != nil {
			// decodeChangeLog panicked.
			base.Warn("Panic from DecodeChangeLog: %v", panicMsg)
		}
	}()
	return decodeChangeLog(r, afterSeq, oldLog)
}

func decodeChangeLog(r *bytes.Reader, afterSeq uint64, oldLog *ChangeLog) *ChangeLog {
	type docAndRev struct {
		docID, revID string
	}

	ch := ChangeLog{
		Since:   readSequence(r),
		Entries: make([]*LogEntry, 0, 500),
	}
	parents := map[docAndRev]int{}

	if oldLog != nil {
		// If a pre-existing log is given, copy its entries so we'll append to them:
		ch.Entries = append(ch.Entries, oldLog.Entries...)
		ch.Since = oldLog.Since
		for i, entry := range ch.Entries {
			parents[docAndRev{entry.DocID, entry.RevID}] = i
		}
		afterSeq = oldLog.LastSequence()
	}

	skipping := afterSeq > ch.Since
	var flagBuf [1]byte
	for {
		n, err := r.Read(flagBuf[0:1])
		if n == 0 {
			if err == io.EOF {
				break
			}
			panic("Error reading flags")
		}
		if flagBuf[0] > kMaxFlag {
			pos, _ := r.Seek(0, 1)
			base.Warn("DecodeChangeLog: bad flags 0x%x, entry %d, offset %d",
				flagBuf[0], len(ch.Entries), pos-1)
			return nil
		}
		seq := readSequence(r)
		if skipping {
			if seq == afterSeq {
				skipping = false
			}
			skipString(r)
			skipString(r)
			skipString(r)
			continue // ignore this sequence
		}

		entry := &LogEntry{
			Flags:    flagBuf[0],
			Sequence: seq,
			DocID:    readString(r),
			RevID:    readString(r),
		}
		if !entry.checkValid() {
			return nil
		}

		if parentID := readString(r); parentID != "" {
			if parentIndex, found := parents[docAndRev{entry.DocID, parentID}]; found {
				// Clear out the parent rev that was overwritten by this one
				ch.Entries[parentIndex] = &LogEntry{Sequence: ch.Entries[parentIndex].Sequence}
			}
		}
		parents[docAndRev{entry.DocID, entry.RevID}] = len(ch.Entries)
		ch.Entries = append(ch.Entries, entry)
	}

	if oldLog == nil && afterSeq > ch.Since {
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
		pos, err := r.Seek(0, 1)
		if err != nil {
			panic("Seek??")
		}
		flags, err := r.ReadByte()
		if err != nil {
			if err == io.EOF {
				break // eof
			}
			panic("ReadByte failed")
		}
		seq := readSequence(r)
		skipString(r)
		skipString(r)
		skipString(r)
		if flags > kMaxFlag {
			panic(fmt.Sprintf("TruncateEncodedChangeLog: bad flags 0x%x, entry %d, offset %d",
				flags, len(entryPos), pos))
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
	if removed < oldLength {
		if _, err := r.Seek(entryPos[removed], 0); err != nil {
			panic("Seek back???")
		}
		if _, err := io.Copy(w, r); err != nil {
			panic("Copy???")
		}
	}
	return removed, oldLength - removed
}

//////// UTILITY FUNCTIONS:

func writeUInt8(u uint8, w io.Writer) {
	var buf [1]byte
	buf[0] = u
	if _, err := w.Write(buf[0:1]); err != nil {
		panic("writeUInt8 failed")
	}
}

func readUInt8(r io.Reader) uint8 {
	var buf [1]byte
	if _, err := io.ReadFull(r, buf[0:1]); err != nil {
		panic("readUInt8 failed")
	}
	return buf[0]
}

func writeSequence(seq uint64, w io.Writer) {
	var buf [16]byte
	len := binary.PutUvarint(buf[0:16], seq)
	if _, err := w.Write(buf[0:len]); err != nil {
		panic("writeSequence failed")
	}
}

func readSequence(r io.ByteReader) uint64 {
	seq, err := binary.ReadUvarint(r)
	if err != nil {
		panic("readSequence failed")
	}
	return seq
}

func writeString(s string, w io.Writer) {
	b := []byte(s)
	length := len(b)
	if length > 255 {
		panic("Doc/rev ID too long to encode: " + s)
	}
	writeUInt8(uint8(length), w)
	if _, err := w.Write(b); err != nil {
		panic("writeString failed")
	}
}

func readString(r io.Reader) string {
	length := readUInt8(r)
	data := make([]byte, length)
	if _, err := io.ReadFull(r, data); err != nil {
		panic("readString failed")
	}
	return string(data)
}

func skipString(r io.ReadSeeker) {
	length := readUInt8(r)
	if _, err := r.Seek(int64(length), 1); err != nil {
		panic("skipString failed")
	}
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
