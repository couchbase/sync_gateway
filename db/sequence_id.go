/*
Copyright 2016-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package db

import (
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/couchbase/sync_gateway/base"
)

// A change sequence as reported externally in a _changes feed.
// Can support either integer- or vector clock-based sequences

// Most of the time the TriggerSeq is 0, but if a revision is being sent retroactively because
// the user got access to a channel, the TriggerSeq will be equal to the sequence of the change
// that gave the user access.

// SequenceID doesn't do any clock hash management - it's expected that hashing has already been done (if required)
// when the clock is set.
type SequenceID struct {
	TriggeredBy uint64 // Int sequence: The sequence # that triggered this (0 if none)
	LowSeq      uint64 // Int sequence: Lowest contiguous sequence seen on the feed
	Seq         uint64 // Int sequence: The actual internal sequence
}

var MaxSequenceID = SequenceID{
	Seq: math.MaxUint64,
}

// Format sequence ID to send to clients.  Sequence IDs can be in one of the following formats:
//   Seq                    - simple sequence
//   TriggeredBy:Seq        - when TriggeredBy is non-zero, LowSeq is zero
//   LowSeq:TriggeredBy:Seq - when LowSeq is non-zero.
// When LowSeq is non-zero but TriggeredBy is zero, will appear as LowSeq::Seq.
// When LowSeq is non-zero but is greater than s.Seq (occurs when sending previously skipped sequences), ignore LowSeq.
func (s SequenceID) String() string {
	return s.intSeqToString()
}

func (s SequenceID) intSeqToString() string {

	if s.LowSeq > 0 && s.LowSeq < s.Seq {
		if s.TriggeredBy > 0 {
			return fmt.Sprintf("%d:%d:%d", s.LowSeq, s.TriggeredBy, s.Seq)
		} else {
			return fmt.Sprintf("%d::%d", s.LowSeq, s.Seq)

		}
	} else if s.TriggeredBy > 0 {
		return fmt.Sprintf("%d:%d", s.TriggeredBy, s.Seq)
	} else {
		return strconv.FormatUint(s.Seq, 10)
	}
}

// Currently accepts a plain string, but in the future might accept generic JSON objects.
// Calling this with a JSON string will result in an error.
func (dbc *DatabaseContext) ParseSequenceID(str string) (s SequenceID, err error) {
	return parseIntegerSequenceID(str)
}

func parseIntegerSequenceID(str string) (s SequenceID, err error) {
	if str == "" {
		return SequenceID{}, nil
	}
	components := strings.Split(str, ":")
	if len(components) == 1 {
		// Just the internal sequence
		s.Seq, err = ParseIntSequenceComponent(components[0], false)
	} else if len(components) == 2 {
		// TriggeredBy and InternalSequence
		if s.TriggeredBy, err = ParseIntSequenceComponent(components[0], false); err != nil {
			return
		}
		if s.Seq, err = ParseIntSequenceComponent(components[1], false); err != nil {
			return
		}
	} else if len(components) == 3 {
		if s.LowSeq, err = ParseIntSequenceComponent(components[0], false); err != nil {
			return
		}
		if s.TriggeredBy, err = ParseIntSequenceComponent(components[1], true); err != nil {
			return
		}
		if s.Seq, err = ParseIntSequenceComponent(components[2], false); err != nil {
			return
		}
	} else {
		err = base.HTTPErrorf(400, "Invalid sequence")
	}

	if err != nil {
		err = base.HTTPErrorf(400, "Invalid sequence")
	}
	return
}

func ParseIntSequenceComponent(component string, allowEmpty bool) (uint64, error) {
	value := uint64(0)
	if allowEmpty && component == "" {
		return value, nil
	}
	value, err := strconv.ParseUint(component, 10, 64)
	return value, err

}

func (s SequenceID) MarshalJSON() ([]byte, error) {

	if s.TriggeredBy > 0 || s.LowSeq > 0 {
		return []byte(fmt.Sprintf("\"%s\"", s.String())), nil
	} else {
		return []byte(strconv.FormatUint(s.Seq, 10)), nil
	}

}

func (s *SequenceID) UnmarshalJSON(data []byte) error {
	return s.unmarshalIntSequence(data)
}

func (s *SequenceID) unmarshalIntSequence(data []byte) error {
	var raw string
	err := base.JSONUnmarshal(data, &raw)
	if err != nil {
		*s, err = parseIntegerSequenceID(string(data))
	} else {
		*s, err = parseIntegerSequenceID(raw)
	}
	return err

}

func (s SequenceID) SafeSequence() uint64 {
	if s.LowSeq > 0 {
		return s.LowSeq
	} else {
		return s.Seq
	}
}

func (s SequenceID) IsNonZero() bool {
	return s.Seq > 0
}

// Equality of sequences, based on seq, triggered by and low hash
func (s SequenceID) Equals(s2 SequenceID) bool {
	return s.SafeSequence() == s2.SafeSequence() && s.TriggeredBy == s2.TriggeredBy
}

// The most significant value is TriggeredBy, unless it's zero, in which case use Seq.
// The tricky part is that "n" sorts after "n:m" for any nonzero m
func (s SequenceID) Before(s2 SequenceID) bool {
	// using SafeSequence for comparison, which takes the lower of LowSeq and Seq
	if s.TriggeredBy == s2.TriggeredBy {
		return s.SafeSequence() < s2.SafeSequence() // the simple case: untriggered, or triggered by same sequence
	} else if s.TriggeredBy == 0 {
		return s.SafeSequence() < s2.TriggeredBy // s2 triggered but not s
	} else if s2.TriggeredBy == 0 {
		return s.TriggeredBy <= s2.SafeSequence() // s triggered but not s2
	} else {
		return s.TriggeredBy < s2.TriggeredBy // both triggered, but by different sequences
	}
}
