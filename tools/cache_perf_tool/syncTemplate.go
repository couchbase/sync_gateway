// Copyright 2026-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package main

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"strconv"
	"time"

	"github.com/couchbase/sync_gateway/base"
)

// The template generator (-generator=template) builds each document's _sync without marshalling it.
//
// Why: marshalling a fresh _sync for every document was about half of all allocation in a run, and that
// garbage is collected in the same process as the cache under test, so its cost was charged to Sync
// Gateway. In production the bytes arrive from Couchbase Server.
//
// How: only three things in a document's _sync change from write to write: the sequence number(s), the
// CAS and time_saved. Everything else is fixed by which channels the document is in. So, once per
// channel pattern, _sync is marshalled with placeholder values and cut at each placeholder, leaving the
// fixed JSON as segments with a slot between each pair. A write copies the segments and writes its own
// values into the slots.
//
// Worked example, 2 channels per document out of 3 in total, for the document that starts at test-0:
//
//	segment 0  {"rev":"1-abc","sequence":
//	slot       slotSeq0
//	segment 1  ,"recent_sequences":[
//	slot       slotSeq0
//	segment 2  ],"channels":{"test-0":null,"test-1":null},"cas":"
//	slot       slotCas
//	segment 3  ","value_crc32c":"","channel_set":[{"name":"test-0","start":
//	slot       slotSeq0
//	segment 4  },{"name":"test-1","start":
//	slot       slotSeq0
//	segment 5  }],"channel_set_history":null,"time_saved":"
//	slot       slotTimeSaved
//	segment 6  ","history":{"revs":["1-abc"],"parents":[-1],"channelsMap":{"0":["test-0","test-1"]}}}
//
// Filled for sequence 42, CAS 0x1122334455667788:
//
//	{"rev":"1-abc","sequence":42,"recent_sequences":[42],"channels":{"test-0":null,"test-1":null},
//	"cas":"0x8877665544332211","value_crc32c":"","channel_set":[{"name":"test-0","start":42},
//	{"name":"test-1","start":42}],"channel_set_history":null,"time_saved":"2026-11-03T10:00:00.000000005Z",
//	"history":{"revs":["1-abc"],"parents":[-1],"channelsMap":{"0":["test-0","test-1"]}}}
//
// One template is needed per channel cursor a writer can reach. Writers start at channel 0 and advance 2
// per document, wrapping at 3, so the example has three: starting at test-0, then test-2, then test-1,
// then back to test-0. With 10 of 20,000 channels the cycle is 2,000 long.
//
// The placeholders go through the same syncDataFor and base.JSONMarshal as the marshal generator, so the
// fixed segments are exactly what that generator would write. The filled JSON is then wrapped in DCP's
// xattr layout (encodeSyncXattrValue), the only allocation a write makes.

// Placeholder values used when building a template. Each must produce a string that cannot occur
// anywhere else in a marshalled _sync; buildSyncTemplate checks each is found the expected number of
// times, so a placeholder that did collide would fail at startup rather than corrupt documents.
const (
	// seqPlaceholderBase + i stands in for recent sequence i. 19 digits, longer than any other number
	// in _sync, and in a range the run's sequence allocator never reaches.
	seqPlaceholderBase uint64 = 9_000_000_000_000_000_001
	// casPlaceholder's little-endian hex contains letters, so it cannot be confused with a sequence.
	casPlaceholder uint64 = 0xA1B2C3D4E5F60718
)

// timePlaceholder's RFC 3339 form is unlike any time the run produces.
var timePlaceholder = time.Date(1999, 12, 31, 23, 59, 58, 123456789, time.UTC)

// slot says which per-write value fills the gap between two segments of a template.
type slot int

const (
	// slotSeq0 + i is pendingWrite.seqs[i], the document's i'th recent sequence. A normal write uses
	// only slotSeq0; a -rapidUpdateDocs write uses all recentSeqsPerRapidUpdate of them.
	slotSeq0 slot = iota
	slotSeq1
	slotSeq2
	slotSeq3
	slotSeq4
	slotCas       // pendingWrite.cas, as base.CasToString formats it
	slotTimeSaved // the write's time_saved, as time.Time marshals to JSON
)

func (s slot) String() string {
	switch s {
	case slotCas:
		return "slotCas"
	case slotTimeSaved:
		return "slotTimeSaved"
	default:
		return "slotSeq" + strconv.Itoa(int(s-slotSeq0))
	}
}

// placeholder is the text a placeholder value marshals to, and the slot it marks.
type placeholder struct {
	text []byte
	slot slot
}

// syncTemplate is one document shape: a fixed number of recent sequences and a fixed set of channels.
type syncTemplate struct {
	segments [][]byte // literal JSON; len(segments) == len(slots)+1
	slots    []slot   // slots[i] is filled between segments[i] and segments[i+1]
	// nextChanCount is the channel cursor after a write with this template, exactly as syncDataFor
	// returns it, so a writer's progress through the channels is the same with either generator.
	nextChanCount int
}

// syncTemplates holds every template a run can use, indexed by the number of recent sequences and then
// by the channel cursor a document starts at.
type syncTemplates struct {
	numTotalChannels int
	byNumSeqs        [recentSeqsPerRapidUpdate + 1][]*syncTemplate
}

// newSyncTemplates builds every template the run's writers can reach. Each writer starts at channel 0
// and advances numChannelsPerDoc channels per document, wrapping at numTotalChannels, so the reachable
// starts form one cycle of numTotalChannels/gcd(numTotalChannels, numChannelsPerDoc) entries: 2,000 at
// 10 of 20,000 channels. Building them all up front keeps lookups lock-free.
func newSyncTemplates(dcp *dcpDataGen) (*syncTemplates, error) {
	t := &syncTemplates{numTotalChannels: dcp.numTotalChannels}
	numSeqsUsed := []int{1}
	if dcp.simRapidUpdate {
		numSeqsUsed = append(numSeqsUsed, recentSeqsPerRapidUpdate)
	}
	for _, numSeqs := range numSeqsUsed {
		byStart := make([]*syncTemplate, max(dcp.numTotalChannels, 1))
		// Follow the cycle from 0 until it returns to a start already built.
		for start := 0; byStart[start] == nil; {
			tmpl, err := buildSyncTemplate(dcp, numSeqs, start)
			if err != nil {
				return nil, err
			}
			byStart[start] = tmpl
			start = t.normaliseChanCount(tmpl.nextChanCount)
		}
		t.byNumSeqs[numSeqs] = byStart
	}
	return t, nil
}

// normaliseChanCount maps a channel cursor to the channel the next document will actually start at:
// syncDataFor leaves the cursor at numTotalChannels after the last channel and wraps it on the next use.
func (t *syncTemplates) normaliseChanCount(chanCount int) int {
	if chanCount == t.numTotalChannels {
		return 0
	}
	return chanCount
}

// buildSyncTemplate builds the template for a document with numSeqs recent sequences whose channels
// start at chanCount: marshal its _sync with placeholder values, then cut the JSON at each placeholder.
func buildSyncTemplate(dcp *dcpDataGen, numSeqs int, chanCount int) (*syncTemplate, error) {
	placeholderSeqs := make([]uint64, numSeqs)
	for i := range placeholderSeqs {
		placeholderSeqs[i] = seqPlaceholderBase + uint64(i)
	}
	syncData, next := dcp.syncDataFor(placeholderSeqs, chanCount, casPlaceholder, timePlaceholder)
	syncJSON, err := base.JSONMarshal(syncData)
	if err != nil {
		return nil, fmt.Errorf("marshalling template sync data: %w", err)
	}

	placeholders := []placeholder{
		{text: appendCas(nil, casPlaceholder), slot: slotCas},
		{text: timePlaceholder.AppendFormat(nil, time.RFC3339Nano), slot: slotTimeSaved},
	}
	for i, seq := range placeholderSeqs {
		placeholders = append(placeholders, placeholder{text: strconv.AppendUint(nil, seq, 10), slot: slotSeq0 + slot(i)})
	}
	segments, slots := cutAtPlaceholders(syncJSON, placeholders)

	// CAS and time_saved each appear once. Every sequence appears at least once (in recent_sequences),
	// and the current one also as sequence and as each channel's start.
	found := make(map[slot]int, len(placeholders))
	for _, s := range slots {
		found[s]++
	}
	if found[slotCas] != 1 || found[slotTimeSaved] != 1 {
		return nil, fmt.Errorf("template for %d seqs at channel %d: found cas %d times and time_saved %d times, want 1 each: %s",
			numSeqs, chanCount, found[slotCas], found[slotTimeSaved], syncJSON)
	}
	for i := range numSeqs {
		if found[slotSeq0+slot(i)] == 0 {
			return nil, fmt.Errorf("template for %d seqs at channel %d: sequence %d not found: %s", numSeqs, chanCount, i, syncJSON)
		}
	}
	return &syncTemplate{segments: segments, slots: slots, nextChanCount: next}, nil
}

// cutAtPlaceholders splits data at every occurrence of every placeholder, in the order they occur. It
// returns the text around the cuts as segments, and which placeholder was at each cut as slots, so
// segments[i], slots[i], segments[i+1] reads left to right through data and
// len(segments) == len(slots)+1. With no placeholders found, segments is just data.
func cutAtPlaceholders(data []byte, placeholders []placeholder) (segments [][]byte, slots []slot) {
	rest := data
	at, p, found := earliestPlaceholder(rest, placeholders)
	for ; found; at, p, found = earliestPlaceholder(rest, placeholders) {
		segments = append(segments, rest[:at])
		slots = append(slots, p.slot)
		rest = rest[at+len(p.text):]
	}
	return append(segments, rest), slots
}

// earliestPlaceholder returns the position of the first placeholder occurring in data, and which one it is.
func earliestPlaceholder(data []byte, placeholders []placeholder) (at int, first placeholder, found bool) {
	for _, p := range placeholders {
		if i := bytes.Index(data, p.text); i >= 0 && (!found || i < at) {
			at, first, found = i, p, true
		}
	}
	return at, first, found
}

// value returns the DCP value for w, the channel cursor after it, and scratch for reuse by the caller's
// next write. The returned value is freshly allocated, as a real DCP event's is, because the feed may
// still be holding it after the next write.
func (t *syncTemplates) value(scratch []byte, w *pendingWrite, chanCount int, timeSaved time.Time) ([]byte, int, []byte, error) {
	var tmpl *syncTemplate
	if byStart := t.byNumSeqs[w.numSeqs]; byStart != nil {
		tmpl = byStart[t.normaliseChanCount(chanCount)]
	}
	if tmpl == nil {
		// Unreachable unless a writer starts somewhere other than channel 0.
		return nil, chanCount, scratch, fmt.Errorf("no _sync template for %d seqs at channel %d", w.numSeqs, chanCount)
	}
	scratch = tmpl.appendSync(scratch[:0], w, timeSaved)
	return encodeSyncXattrValue(scratch), tmpl.nextChanCount, scratch, nil
}

// appendSync appends the template's _sync JSON for w to dst: each segment in turn, with the write's own
// value written into the slot after it.
func (tmpl *syncTemplate) appendSync(dst []byte, w *pendingWrite, timeSaved time.Time) []byte {
	for i, s := range tmpl.slots {
		dst = append(dst, tmpl.segments[i]...)
		switch s {
		case slotCas:
			dst = appendCas(dst, w.cas)
		case slotTimeSaved:
			dst = timeSaved.AppendFormat(dst, time.RFC3339Nano)
		default: // slotSeq0..slotSeq4
			dst = strconv.AppendUint(dst, w.seqs[s-slotSeq0], 10)
		}
	}
	return append(dst, tmpl.segments[len(tmpl.segments)-1]...)
}

// appendCas appends cas in base.CasToString's format, "0x" and little-endian hex, without allocating.
func appendCas(dst []byte, cas uint64) []byte {
	var littleEndian [8]byte
	binary.LittleEndian.PutUint64(littleEndian[:], cas)
	return hex.AppendEncode(append(dst, '0', 'x'), littleEndian[:])
}

// encodeSyncXattrValue is sgbucket.EncodeValueWithXattrs for a single _sync xattr and no body, as an
// xattr-only DCP feed delivers it, in one allocation of exactly the right size.
func encodeSyncXattrValue(syncJSON []byte) []byte {
	pairLen := len(base.SyncXattrName) + 1 + len(syncJSON) + 1 // name, NUL, value, NUL
	xattrsLen := 4 + pairLen                                   // the pair's own length prefix, then the pair
	out := make([]byte, 0, 4+xattrsLen)
	out = binary.BigEndian.AppendUint32(out, uint32(xattrsLen))
	out = binary.BigEndian.AppendUint32(out, uint32(pairLen))
	out = append(out, base.SyncXattrName...)
	out = append(out, 0)
	out = append(out, syncJSON...)
	return append(out, 0)
}
