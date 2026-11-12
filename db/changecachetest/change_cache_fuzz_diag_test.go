/*
Copyright 2026-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package changecachetest

import (
	"fmt"
	"slices"
	"strings"
)

// Failure diagnostics. Everything here records raw values while a script runs and only formats them when a check
// fails, so the cost per op is a struct append.

// fuzzTraceStep is the state after one op, or one step the harness takes after the ops.
type fuzzTraceStep struct {
	index    int
	op       fuzzOp
	phase    string     // set for the harness's own steps, which have no op
	event    *fuzzEvent // the event the step allocated, withheld or delivered
	note     string     // why the step did nothing, if it did
	delivery int        // how many times event had been delivered after this step
	next     uint64
	stable   uint64
	skipped  []uint64
	notified []fuzzNotification // notifications that fired during the step
}

// beginStep marks the start of an op or harness step.
func (h *fuzzHarness) beginStep(index int, op fuzzOp, phase string) {
	h.opIndex, h.curOp, h.phase = index, op, phase
	h.stepEvent, h.stepNote, h.stepDelivery = nil, "", 0
}

// stepName says what is running, for the failure heading.
func (h *fuzzHarness) stepName() string {
	if h.phase != "" {
		return h.phase
	}
	return h.curOp.String()
}

// recordTrace keeps the cache's sequence state after the current step. skipped is a fresh slice from the cache's
// accessor, so it is kept as it is.
func (h *fuzzHarness) recordTrace(next, stable uint64, skipped []uint64) {
	h.trace = append(h.trace, fuzzTraceStep{
		index: h.opIndex, op: h.curOp, phase: h.phase, event: h.stepEvent, note: h.stepNote, delivery: h.stepDelivery,
		next: next, stable: stable, skipped: skipped, notified: h.drainNotifications(),
	})
}

// describeEvent names a feed event by what it carries.
func describeEvent(ev *fuzzEvent) string {
	switch ev.kind {
	case eventDoc:
		if len(ev.rev.unusedSeqs) > 0 {
			return fmt.Sprintf("%s@%d (with unused %v)", ev.docID, ev.fromSeq, ev.rev.unusedSeqs)
		}
		return fmt.Sprintf("%s@%d", ev.docID, ev.fromSeq)
	case eventUnused:
		return fmt.Sprintf("unused %d", ev.fromSeq)
	case eventUnusedRange:
		return fmt.Sprintf("range %d-%d", ev.fromSeq, ev.toSeq)
	default:
		return fmt.Sprintf("%s@%d", ev.docID, ev.fromSeq)
	}
}

func (h *fuzzHarness) formatTraceStep(step fuzzTraceStep) string {
	var what string
	switch {
	case step.phase != "":
		what = step.phase
	default:
		what = step.op.String()
		if step.op.Code == opFeedRead {
			what += ": " + strings.Join(h.cfg.feedChannels(step.op.feedIndex()), ",")
		}
	}
	var did string
	switch {
	case step.note != "":
		did = " -> did nothing: " + step.note
	case step.event != nil && step.op.Code == opWithhold:
		did = " -> withheld " + describeEvent(step.event)
	case step.event != nil && step.delivery > 0:
		did = " -> delivered " + describeEvent(step.event)
		if step.delivery > 1 {
			did += fmt.Sprintf(" (delivery %d)", step.delivery)
		}
	case step.event != nil:
		did = " -> allocated " + describeEvent(step.event)
	}
	line := fmt.Sprintf("  %3d %s%s | next %d stable %d skipped %v", step.index, what, did, step.next, step.stable, step.skipped)
	for _, n := range step.notified {
		line += "\n        notified " + n.String()
	}
	return line
}

// sequenceLegend lists every sequence the simulated server allocated, what it is, and where its event is.
func (h *fuzzHarness) sequenceLegend() string {
	status := map[uint64]string{}
	mark := func(events []*fuzzEvent, what string) {
		for _, ev := range events {
			for seq := ev.fromSeq; seq <= ev.toSeq; seq++ {
				status[seq] = what
			}
			if ev.kind == eventDoc {
				for _, seq := range ev.rev.unusedSeqs {
					status[seq] = what
				}
			}
		}
	}
	mark(h.undelivered, "undelivered")
	mark(h.withheld, "withheld")
	mark(h.delivered, "delivered")

	var lines []string
	for seq := uint64(1); seq < h.nextSeq; seq++ {
		rec := h.model.seqs[seq]
		var what string
		switch rec.kind {
		case eventDoc:
			rev := rec.rev
			var active, left []string
			for _, name := range sortedKeys(rev.channels) {
				if rev.channels[name] == nil {
					active = append(active, name)
				} else if rev.channels[name].Seq == rev.seq {
					left = append(left, name)
				}
			}
			what = fmt.Sprintf("%s r%d {%s}", rev.docID, rev.generation, strings.Join(active, ","))
			if len(left) > 0 {
				what += " left " + strings.Join(left, ",")
			}
		case eventUnused:
			if rec.burnedBy != nil {
				what = fmt.Sprintf("%s unused (burned by r%d)", rec.burnedBy.docID, rec.burnedBy.generation)
			} else {
				what = "unused"
			}
		case eventUnusedRange:
			if seq != rec.from {
				continue
			}
			what = "unused range"
			seq = rec.to // the whole range is one line
			lines = append(lines, fmt.Sprintf("%d-%d=%s [%s]", rec.from, rec.to, what, h.statusOf(status, rec.from)))
			continue
		case eventPrincipal:
			what = rec.name
		}
		lines = append(lines, fmt.Sprintf("%d=%s [%s]", seq, what, h.statusOf(status, seq)))
	}
	return "  " + strings.Join(lines, "\n  ")
}

func (h *fuzzHarness) statusOf(status map[uint64]string, seq uint64) string {
	s := status[seq]
	if s == "" {
		s = "unknown"
	}
	if h.model.abandoned[seq] {
		s += ", abandoned"
	}
	return s
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	return keys
}

// failf fails the run. The message comes first, then everything needed to reproduce and read the run: config, the
// decoded ops, a trace of what each step did to the cache, a legend of every sequence, and the script as builder code
// that can be pasted into a seed.
func (h *fuzzHarness) failf(format string, args ...any) {
	h.t.Helper()
	var b strings.Builder
	fmt.Fprintf(&b, "after step %d (%s): %s\n", h.opIndex, h.stepName(), fmt.Sprintf(format, args...))
	fmt.Fprintf(&b, "\nconfig: %+v\n", h.cfg)
	b.WriteString("\nops:\n")
	for i, op := range h.ops {
		fmt.Fprintf(&b, "  %3d %v\n", i, op)
	}
	b.WriteString("\ntrace (state after each step):\n")
	for _, step := range h.trace {
		b.WriteString(h.formatTraceStep(step))
		b.WriteString("\n")
	}
	b.WriteString("\nsequences:\n")
	b.WriteString(h.sequenceLegend())
	b.WriteString("\n\nbuilder:\n")
	b.WriteString(renderBuilder(h.cfg, h.ops))
	b.WriteString("\n")
	h.t.Fatal(b.String())
}

// builderStep is one call in a rendered script: its Go source, and the call itself so the rendering can be checked
// to rebuild the same bytes.
type builderStep struct {
	text  string
	apply func(*scriptBuilder)
}

// builderSteps renders a decoded script as calls on scriptBuilder. The config call lists only the fields that differ
// from the default. An op is printed with the builder's decoded API when that rebuilds its exact operand bytes, which
// is not so for an operand outside the range the builder takes (a doc number of 8 or more, say), and then it is
// printed as a raw add call so the bytes still round trip.
func builderSteps(cfg fuzzConfig, ops []fuzzOp) []builderStep {
	var steps []builderStep
	if step, ok := configStep(cfg); ok {
		steps = append(steps, step)
	}
	for _, op := range ops {
		steps = append(steps, opStep(op))
	}
	return steps
}

func configStep(cfg fuzzConfig) (builderStep, bool) {
	def := defaultFuzzConfig()
	var texts []string
	var sets []func(*fuzzConfig)
	field := func(differs bool, text string, set func(*fuzzConfig)) {
		if differs {
			texts = append(texts, text)
			sets = append(sets, set)
		}
	}
	field(cfg.Consumers != def.Consumers, fmt.Sprintf("c.Consumers = %d", cfg.Consumers), func(c *fuzzConfig) { c.Consumers = cfg.Consumers })
	field(cfg.RunSize != def.RunSize, fmt.Sprintf("c.RunSize = %d", cfg.RunSize), func(c *fuzzConfig) { c.RunSize = cfg.RunSize })
	field(cfg.TrackerCapacity != def.TrackerCapacity, fmt.Sprintf("c.TrackerCapacity = %d", cfg.TrackerCapacity), func(c *fuzzConfig) { c.TrackerCapacity = cfg.TrackerCapacity })
	field(cfg.PendingMaxNum != def.PendingMaxNum, fmt.Sprintf("c.PendingMaxNum = %d", cfg.PendingMaxNum), func(c *fuzzConfig) { c.PendingMaxNum = cfg.PendingMaxNum })
	field(cfg.ChannelMaxLen != def.ChannelMaxLen, fmt.Sprintf("c.ChannelMaxLen = %d", cfg.ChannelMaxLen), func(c *fuzzConfig) { c.ChannelMaxLen = cfg.ChannelMaxLen })
	field(cfg.Flags != def.Flags, fmt.Sprintf("c.Flags = %d", cfg.Flags), func(c *fuzzConfig) { c.Flags = cfg.Flags })
	field(cfg.FeedMasks != def.FeedMasks, fmt.Sprintf("c.FeedMasks = [fuzzNumFeeds]uint8{%s}", joinInts(cfg.FeedMasks[:])), func(c *fuzzConfig) { c.FeedMasks = cfg.FeedMasks })
	field(cfg.Spare != def.Spare, fmt.Sprintf("c.Spare = [6]uint8{%s}", joinInts(cfg.Spare[:])), func(c *fuzzConfig) { c.Spare = cfg.Spare })
	if len(texts) == 0 {
		return builderStep{}, false
	}
	return builderStep{
		text: "config(func(c *fuzzConfig) { " + strings.Join(texts, "; ") + " })",
		apply: func(b *scriptBuilder) {
			b.config(func(c *fuzzConfig) {
				for _, set := range sets {
					set(c)
				}
			})
		},
	}, true
}

func joinInts[T ~uint8](values []T) string {
	parts := make([]string, len(values))
	for i, v := range values {
		parts[i] = fmt.Sprint(v)
	}
	return strings.Join(parts, ", ")
}

// opStep renders one op.
func opStep(op fuzzOp) builderStep {
	var text string
	var apply func(*scriptBuilder)
	quoted := func(names []string) string {
		for i, n := range names {
			names[i] = fmt.Sprintf("%q", n)
		}
		return strings.Join(names, ", ")
	}
	switch op.Code {
	case opWrite:
		doc, mask, extra := op.writeDoc(), op.writeMask(), op.writeExtraSeq()
		text = fmt.Sprintf("Write(%d, chanMask(%s), %t)", doc, quoted(channelsInMask(mask)), extra)
		apply = func(b *scriptBuilder) { b.Write(doc, mask, extra) }
	case opDeliver:
		k, pick := op.deliverWindow(), op.deliverPick()
		if pick == 0 {
			text = fmt.Sprintf("Deliver(%d)", k)
		} else {
			text = fmt.Sprintf("DeliverAt(%d, %d)", k, pick)
		}
		apply = func(b *scriptBuilder) { b.DeliverAt(k, pick) }
	case opWithhold:
		text, apply = "Withhold()", func(b *scriptBuilder) { b.Withhold() }
	case opDeliverLate:
		text, apply = "DeliverLate()", func(b *scriptBuilder) { b.DeliverLate() }
	case opDuplicate:
		i := int(op.Args[0])
		text, apply = fmt.Sprintf("Duplicate(%d)", i), func(b *scriptBuilder) { b.Duplicate(i) }
	case opUnused:
		text, apply = "Unused()", func(b *scriptBuilder) { b.Unused() }
	case opUnusedRange:
		n := op.unusedRangeLen()
		text, apply = fmt.Sprintf("UnusedRange(%d)", n), func(b *scriptBuilder) { b.UnusedRange(n) }
	case opPrincipal:
		user := op.principalIsUser()
		text, apply = fmt.Sprintf("Principal(%t)", user), func(b *scriptBuilder) { b.Principal(user) }
	case opTimeoutPending:
		text, apply = "TimeoutPending()", func(b *scriptBuilder) { b.TimeoutPending() }
	case opCleanSkipped:
		text, apply = "CleanSkipped()", func(b *scriptBuilder) { b.CleanSkipped() }
	case opCreateCache:
		ch := op.channelIndex()
		text, apply = fmt.Sprintf("CreateCache(%d)", ch), func(b *scriptBuilder) { b.CreateCache(ch) }
	case opFeedRead:
		feed := op.feedIndex()
		text, apply = fmt.Sprintf("FeedRead(%d)", feed), func(b *scriptBuilder) { b.FeedRead(feed) }
	case opFlush:
		text, apply = "Flush()", func(b *scriptBuilder) { b.Flush() }
	case opStall:
		consumer := int(op.Args[0])
		text, apply = fmt.Sprintf("Stall(%d)", consumer), func(b *scriptBuilder) { b.Stall(consumer) }
	}
	if apply != nil {
		probe := &scriptBuilder{}
		apply(probe)
		if len(probe.ops) == 1 && probe.ops[0] == op {
			return builderStep{text: text, apply: apply}
		}
	}
	arity := 0
	if op.Code < numDefinedOps {
		arity = opArity[op.Code]
	}
	args := op.Args[:arity]
	raw := fmt.Sprintf("add(op%v", op.Code)
	for _, a := range args {
		raw += fmt.Sprintf(", %d", a)
	}
	return builderStep{text: raw + ")", apply: func(b *scriptBuilder) { b.add(op.Code, args...) }}
}

// renderBuilder prints a script as Go that rebuilds it with scriptBuilder, one call per line, ready to paste into
// fuzzSeeds.
func renderBuilder(cfg fuzzConfig, ops []fuzzOp) string {
	var b strings.Builder
	b.WriteString("newScriptBuilder().\n")
	for _, step := range builderSteps(cfg, ops) {
		b.WriteString("\t" + step.text + ".\n")
	}
	b.WriteString("\tBytes()")
	return b.String()
}
