/*
Copyright 2026-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package changecachetest

// Byte format of a change cache fuzz script.
//
// A script is a fixed-size config header followed by a sequence of ops. The corpus is checked in, so the
// format is a compatibility contract:
//
//   - Never reorder, renumber or reuse an opcode, and never move or repurpose a config byte. New ops take
//     the next unused opcode number. The header length is fixed forever (growing it would shift every
//     op), so a new config field claims one of the spare bytes at the end of the header.
//   - RESERVED and spare bytes are decoded into the config and ignored by the interpreter, so a later
//     change can start using them without invalidating existing inputs.
//   - Decoding is total: any byte slice decodes to some config and op list. A short header is padded with
//     zero bytes, and an op whose operands are cut short ends the script. Decoding never fails.
//   - An opcode byte is reduced modulo opcodeModulus, a fixed constant. Numbers at or beyond the last
//     defined op decode as opNop with no operands. Raising opcodeModulus would change how existing
//     inputs decode, so it is never changed. Opcode 0 is opNop for the same reason.
//   - Appending an op therefore changes how any input that used its number as a Nop decodes, since the
//     op now consumes operand bytes and shifts the ops after it. When an op is added, re-run every
//     checked-in crasher under testdata/fuzz, and re-minimise any that no longer reproduces. Seeds built
//     with the builder are unaffected.
//
// Config header, one byte per field, in this order:
//
//	0  consumers          1-8                   RESERVED
//	1  run size           1-32                  RESERVED
//	2  tracker capacity   {1,2,4,8,16}          RESERVED
//	3  pending max num    3-50                  CachePendingSeqMaxNum
//	4  channel max length 3-20                  ChannelCacheMaxLength
//	5  flags              bit0: create "*" channel cache at start
//	6-9 feed channel masks, one per simulated feed, bit n = channel "A"+n; a zero mask means {A}
//	10-15 spare           RESERVED for future config fields
//
// Ops are one opcode byte followed by opArity[opcode] operand bytes; see the op constants.

import (
	"fmt"
	"math/rand/v2"
)

const (
	fuzzHeaderLen  = 16
	fuzzMaxOps     = 256
	fuzzNumDocs    = 8
	fuzzNumFeeds   = 4
	fuzzNumChans   = 5
	opcodeModulus  = 32
	fuzzMaxDeliver = 8
)

// fuzzChannelNames are the channels a script can use; a channel mask's bit n selects fuzzChannelNames[n].
var fuzzChannelNames = [fuzzNumChans]string{"A", "B", "C", "D", "E"}

var trackerCapacities = [...]int{1, 2, 4, 8, 16}

// opCode identifies an op. The numeric values are part of the byte format: append only.
type opCode uint8

const (
	opNop            opCode = iota // any opcode number past the last defined op; no operands
	opWrite                        // doc, channel mask, flags
	opDeliver                      // k
	opWithhold                     // -
	opDeliverLate                  // -
	opDuplicate                    // index
	opUnused                       // -
	opUnusedRange                  // n
	opPrincipal                    // user/role
	opTimeoutPending               // -
	opCleanSkipped                 // -
	opCreateCache                  // channel
	opFeedRead                     // feed
	opFlush                        // -
	opStall                        // consumer, RESERVED
	numDefinedOps
)

// opArity is the number of operand bytes following each opcode. It is indexed by opCode and append-only.
var opArity = [numDefinedOps]int{
	opNop:            0,
	opWrite:          3,
	opDeliver:        1,
	opWithhold:       0,
	opDeliverLate:    0,
	opDuplicate:      1,
	opUnused:         0,
	opUnusedRange:    1,
	opPrincipal:      1,
	opTimeoutPending: 0,
	opCleanSkipped:   0,
	opCreateCache:    1,
	opFeedRead:       1,
	opFlush:          0,
	opStall:          1,
}

var opNames = [numDefinedOps]string{
	opNop: "Nop", opWrite: "Write", opDeliver: "Deliver", opWithhold: "Withhold", opDeliverLate: "DeliverLate",
	opDuplicate: "Duplicate", opUnused: "Unused", opUnusedRange: "UnusedRange", opPrincipal: "Principal",
	opTimeoutPending: "TimeoutPending", opCleanSkipped: "CleanSkipped", opCreateCache: "CreateCache",
	opFeedRead: "FeedRead", opFlush: "Flush", opStall: "Stall",
}

func (c opCode) String() string {
	if c < numDefinedOps {
		return opNames[c]
	}
	return fmt.Sprintf("opCode(%d)", uint8(c))
}

// fuzzConfig is the decoded config header. Every field round-trips through the builder when it is within
// the range its byte decodes to.
type fuzzConfig struct {
	Consumers       int // RESERVED
	RunSize         int // RESERVED
	TrackerCapacity int // RESERVED
	PendingMaxNum   int
	ChannelMaxLen   int
	Flags           uint8 // the raw flags byte, so bits not yet used survive a round trip
	FeedMasks       [fuzzNumFeeds]uint8
	Spare           [6]uint8 // RESERVED
}

// defaultFuzzConfig is what an all-zero header decodes to.
func defaultFuzzConfig() fuzzConfig {
	return decodeConfig(make([]byte, fuzzHeaderLen))
}

func decodeConfig(b []byte) fuzzConfig {
	var h [fuzzHeaderLen]byte
	copy(h[:], b)
	cfg := fuzzConfig{
		Consumers:       1 + int(h[0])%8,
		RunSize:         1 + int(h[1])%32,
		TrackerCapacity: trackerCapacities[int(h[2])%len(trackerCapacities)],
		PendingMaxNum:   3 + int(h[3])%48,
		ChannelMaxLen:   3 + int(h[4])%18,
		Flags:           h[5],
	}
	for i := range cfg.FeedMasks {
		cfg.FeedMasks[i] = h[6+i] & (1<<fuzzNumChans - 1)
	}
	copy(cfg.Spare[:], h[10:])
	return cfg
}

// starChannel reports whether the run starts with a "*" channel cache.
func (c fuzzConfig) starChannel() bool {
	return c.Flags&1 != 0
}

func (c fuzzConfig) encode() []byte {
	tracker := 0
	for i, v := range trackerCapacities {
		if v == c.TrackerCapacity {
			tracker = i
		}
	}
	h := make([]byte, fuzzHeaderLen)
	h[0] = byte(c.Consumers - 1)
	h[1] = byte(c.RunSize - 1)
	h[2] = byte(tracker)
	h[3] = byte(c.PendingMaxNum - 3)
	h[4] = byte(c.ChannelMaxLen - 3)
	h[5] = c.Flags
	copy(h[6:], c.FeedMasks[:])
	copy(h[10:], c.Spare[:])
	return h
}

// feedChannels returns the channel names feed reads. A zero mask is treated as {A} so every feed reads
// something.
func (c fuzzConfig) feedChannels(feed int) []string {
	mask := c.FeedMasks[feed%fuzzNumFeeds]
	if mask == 0 {
		mask = 1
	}
	return channelsInMask(mask)
}

func channelsInMask(mask uint8) []string {
	var names []string
	for i, name := range fuzzChannelNames {
		if mask&(1<<i) != 0 {
			names = append(names, name)
		}
	}
	return names
}

// fuzzOp is one decoded op. Operands are kept as the raw bytes so that a script round-trips exactly and
// so a later change can give an operand a wider meaning; the accessors below apply today's reduction.
type fuzzOp struct {
	Code opCode
	Args [3]byte
}

// Write operands.
func (o fuzzOp) writeDoc() int {
	return int(o.Args[0]) % fuzzNumDocs
}

func (o fuzzOp) writeMask() uint8 {
	return o.Args[1] & (1<<fuzzNumChans - 1)
}

func (o fuzzOp) writeExtraSeq() bool {
	return o.Args[2]&1 != 0
}

// deliverWindow is the number of oldest undelivered events Deliver chooses between.
func (o fuzzOp) deliverWindow() int {
	return 1 + int(o.Args[0])%fuzzMaxDeliver
}

// deliverPick is which of the window's events Deliver chooses, taken from the operand's upper bits.
func (o fuzzOp) deliverPick() int {
	return int(o.Args[0]) / fuzzMaxDeliver
}

// unusedRangeLen is the number of sequences an UnusedRange op consumes.
func (o fuzzOp) unusedRangeLen() int {
	return 1 + int(o.Args[0])%8
}

func (o fuzzOp) principalIsUser() bool {
	return o.Args[0]&1 == 0
}

func (o fuzzOp) channelIndex() int {
	return int(o.Args[0]) % fuzzNumChans
}

func (o fuzzOp) feedIndex() int {
	return int(o.Args[0]) % fuzzNumFeeds
}

func (o fuzzOp) String() string {
	n := 0
	if o.Code < numDefinedOps {
		n = opArity[o.Code]
	}
	return fmt.Sprintf("%v%v", o.Code, o.Args[:n])
}

// decodeScript decodes a byte slice into a config and ops. It never fails: see the format notes above.
func decodeScript(data []byte) (fuzzConfig, []fuzzOp) {
	cfg := decodeConfig(data)
	if len(data) <= fuzzHeaderLen {
		return cfg, nil
	}
	rest := data[fuzzHeaderLen:]
	var ops []fuzzOp
	for len(rest) > 0 && len(ops) < fuzzMaxOps {
		code := opCode(rest[0] % opcodeModulus)
		if code >= numDefinedOps {
			code = opNop
		}
		arity := opArity[code]
		if len(rest) < 1+arity {
			break // operands cut short: the script ends here
		}
		op := fuzzOp{Code: code}
		copy(op.Args[:], rest[1:1+arity])
		ops = append(ops, op)
		rest = rest[1+arity:]
	}
	return cfg, ops
}

// scriptBuilder writes seed scripts readably. Its methods take the decoded meaning of each operand.
type scriptBuilder struct {
	cfg fuzzConfig
	ops []fuzzOp
}

func newScriptBuilder() *scriptBuilder {
	return &scriptBuilder{cfg: defaultFuzzConfig()}
}

func (b *scriptBuilder) config(mutate func(*fuzzConfig)) *scriptBuilder {
	mutate(&b.cfg)
	return b
}

func (b *scriptBuilder) add(code opCode, args ...byte) *scriptBuilder {
	op := fuzzOp{Code: code}
	copy(op.Args[:], args)
	b.ops = append(b.ops, op)
	return b
}

// chanMask builds a channel mask from channel names such as "A", "C".
func chanMask(names ...string) byte {
	var mask byte
	for _, name := range names {
		for i, n := range fuzzChannelNames {
			if n == name {
				mask |= 1 << i
			}
		}
	}
	return mask
}

func (b *scriptBuilder) Write(doc int, mask byte, extraSeq bool) *scriptBuilder {
	var flags byte
	if extraSeq {
		flags = 1
	}
	return b.add(opWrite, byte(doc), mask, flags)
}

// Deliver delivers the oldest of the first k undelivered events.
func (b *scriptBuilder) Deliver(k int) *scriptBuilder {
	return b.DeliverAt(k, 0)
}

// DeliverAt delivers the pick'th (modulo the number available) of the first k undelivered events.
func (b *scriptBuilder) DeliverAt(k, pick int) *scriptBuilder {
	return b.add(opDeliver, byte(k-1+pick*fuzzMaxDeliver))
}

func (b *scriptBuilder) Withhold() *scriptBuilder {
	return b.add(opWithhold)
}

func (b *scriptBuilder) DeliverLate() *scriptBuilder {
	return b.add(opDeliverLate)
}

func (b *scriptBuilder) Duplicate(i int) *scriptBuilder {
	return b.add(opDuplicate, byte(i))
}

func (b *scriptBuilder) Unused() *scriptBuilder {
	return b.add(opUnused)
}

func (b *scriptBuilder) UnusedRange(n int) *scriptBuilder {
	return b.add(opUnusedRange, byte(n-1))
}

func (b *scriptBuilder) Principal(user bool) *scriptBuilder {
	if user {
		return b.add(opPrincipal, 0)
	}
	return b.add(opPrincipal, 1)
}

func (b *scriptBuilder) TimeoutPending() *scriptBuilder {
	return b.add(opTimeoutPending)
}

func (b *scriptBuilder) CleanSkipped() *scriptBuilder {
	return b.add(opCleanSkipped)
}

func (b *scriptBuilder) CreateCache(ch int) *scriptBuilder {
	return b.add(opCreateCache, byte(ch))
}

func (b *scriptBuilder) FeedRead(feed int) *scriptBuilder {
	return b.add(opFeedRead, byte(feed))
}

func (b *scriptBuilder) Flush() *scriptBuilder {
	return b.add(opFlush)
}

func (b *scriptBuilder) Stall(consumer int) *scriptBuilder {
	return b.add(opStall, byte(consumer))
}

// Bytes encodes the script.
func (b *scriptBuilder) Bytes() []byte {
	out := b.cfg.encode()
	for _, op := range b.ops {
		out = append(out, byte(op.Code))
		out = append(out, op.Args[:opArity[op.Code]]...)
	}
	return out
}

// randomScriptBytes returns n pseudo-random bytes, for exercising the decoder on arbitrary input.
func randomScriptBytes(r *rand.Rand, n int) []byte {
	out := make([]byte, n)
	for i := range out {
		out[i] = byte(r.UintN(256))
	}
	return out
}
