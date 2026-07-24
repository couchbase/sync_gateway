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
	"context"
	"encoding/json"
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
//
//	Seq                    - simple sequence
//	TriggeredBy:Seq        - when TriggeredBy is non-zero, LowSeq is zero
//	LowSeq:TriggeredBy:Seq - when LowSeq is non-zero.
//
// When LowSeq is non-zero but TriggeredBy is zero, will appear as LowSeq::Seq.
// When LowSeq is non-zero but is greater than s.Seq (occurs when sending previously skipped sequences), ignore LowSeq.
func (s SequenceID) String() string {
	return s.intSeqToString()
}

// intSeqToString implements the formatting rules documented on String() above.
func (s SequenceID) intSeqToString() string {
	// LowSeq is omitted from the output here for two independent reasons: it's zero (there's
	// nothing to report), or it's stale - greater than Seq. LowSeq is stale when this entry is a
	// previously-skipped sequence being delivered after the feed's lowest contiguous sequence has
	// already moved past it, so including it here would misrepresent this entry's position.
	// TriggeredBy, if set, reflects an in-progress channel backfill and is unrelated to which of
	// those two reasons applies.
	if s.LowSeq == 0 || s.Seq < s.LowSeq {
		if s.TriggeredBy > 0 {
			return fmt.Sprintf("%d:%d", s.TriggeredBy, s.Seq)
		}
		return strconv.FormatUint(s.Seq, 10)
	}

	// From here, LowSeq is non-zero and still relevant (not stale).
	if s.TriggeredBy > 0 {
		return fmt.Sprintf("%d:%d:%d", s.LowSeq, s.TriggeredBy, s.Seq)
	}

	if s.LowSeq < s.Seq {
		return fmt.Sprintf("%d::%d", s.LowSeq, s.Seq)
	}
	return strconv.FormatUint(s.Seq, 10)
}

// seqStr converts a decoded JSON sequence value - a string or json.Number - to its string form,
// for use with ParseJSONSequenceID/ParsePlainSequenceID. Returns "" for any other type.
func seqStr(ctx context.Context, seq any) string {
	switch seq := seq.(type) {
	case string:
		return seq
	case json.Number:
		return seq.String()
	}
	base.WarnfCtx(ctx, "unknown seq type: %T", seq)
	return ""
}

// ParseJSONSequenceID will parse a JSON string sequence ID. (e.g. accepts: `"1::3"`, `2`, and also a plain sequence like `1::3`)
func ParseJSONSequenceID(str string) (SequenceID, error) {
	plainStr := base.ConvertJSONString(str)
	return ParsePlainSequenceID(plainStr)
}

// ParsePlainSequenceID will parse a plain sequence string - but not a JSON sequence string (e.g. accepts: `1::3` but not `"1::3"`)
// Calling this with a JSON string will result in an error. Use ParseJSONSequenceID instead.
func ParsePlainSequenceID(str string) (s SequenceID, err error) {
	return parseIntegerSequenceID(str)
}

// parseIntegerSequenceID parses a colon-delimited sequence string into a SequenceID. A single
// component is Seq; two components are TriggeredBy:Seq; three are LowSeq:TriggeredBy:Seq. An
// empty string returns a zero-value SequenceID.
func parseIntegerSequenceID(str string) (SequenceID, error) {
	if str == "" {
		return SequenceID{}, nil
	}
	s := SequenceID{}
	components := strings.Split(str, ":")
	var err error
	if len(components) == 1 {
		// Just the internal sequence
		s.Seq, err = ParseIntSequenceComponent(components[0], false)
	} else if len(components) == 2 {
		// TriggeredBy and InternalSequence
		if s.TriggeredBy, err = ParseIntSequenceComponent(components[0], false); err != nil {
			return SequenceID{}, err
		}
		if s.Seq, err = ParseIntSequenceComponent(components[1], false); err != nil {
			return SequenceID{}, err
		}
	} else if len(components) == 3 {
		if s.LowSeq, err = ParseIntSequenceComponent(components[0], false); err != nil {
			return SequenceID{}, err
		}
		if s.TriggeredBy, err = ParseIntSequenceComponent(components[1], true); err != nil {
			return SequenceID{}, err
		}
		if s.Seq, err = ParseIntSequenceComponent(components[2], false); err != nil {
			return SequenceID{}, err
		}
	} else {
		return SequenceID{}, base.HTTPErrorf(400, "Invalid sequence: %q", str)
	}

	if err != nil {
		return SequenceID{}, base.HTTPErrorf(400, "Invalid sequence: %q", str)
	}
	return s, nil
}

// ParseIntSequenceComponent parses a single colon-delimited component of a sequence string. When
// allowEmpty is true, an empty component parses as 0 instead of returning an error - used for the
// optional TriggeredBy component in the LowSeq:TriggeredBy:Seq form (e.g. "5::10").
func ParseIntSequenceComponent(component string, allowEmpty bool) (uint64, error) {
	value := uint64(0)
	if allowEmpty && component == "" {
		return value, nil
	}
	value, err := strconv.ParseUint(component, 10, 64)
	return value, err

}

// MarshalJSON implements json.Marshaler, encoding a SequenceID via String() when TriggeredBy or
// LowSeq is set (any compound form), or as a bare integer for a simple sequence.
func (s SequenceID) MarshalJSON() ([]byte, error) {

	if s.TriggeredBy > 0 || s.LowSeq > 0 {
		return fmt.Appendf(nil, "\"%s\"", s.String()), nil
	} else {
		return []byte(strconv.FormatUint(s.Seq, 10)), nil
	}

}

// UnmarshalJSON implements json.Unmarshaler for SequenceID.
func (s *SequenceID) UnmarshalJSON(data []byte) error {
	return s.unmarshalIntSequence(data)
}

// unmarshalIntSequence parses a SequenceID from JSON data that may be either a quoted string
// (e.g. `"5:10"`) or a bare JSON number (e.g. `10`).
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

// SafeSequence returns the safe sequence after which the changes have to be sent. If LowSeq is
// set, then LowSeq is the SafeSequence, since it's the last contiguous sequence; else it's Seq.
func (s SequenceID) SafeSequence() uint64 {
	if s.LowSeq > 0 {
		//if s.LowSeq < s.Seq {
		return s.LowSeq
		//} else {
		//	return s.Seq
		//}
	} else {
		return s.Seq
	}
}

// IsNonZero reports whether the sequence's Seq value is non-zero. TriggeredBy and LowSeq are not
// considered.
func (s SequenceID) IsNonZero() bool {
	return s.Seq > 0
}

// Equality of sequences, based on seq, triggered by and low hash
func (s SequenceID) Equals(s2 SequenceID) bool {
	return s.SafeSequence() == s2.SafeSequence() && s.TriggeredBy == s2.TriggeredBy
}

// The most significant value is TriggeredBy, unless it's zero, in which case use Seq.
// The tricky part is that "y" sorts after "y:z" for any nonzero z
func (s SequenceID) Before(s2 SequenceID) bool {

	// s in format x:y:z or x::z
	if s.LowSeq != 0 {
		if s.LowSeq == s2.LowSeq {
			return SequenceID{TriggeredBy: s.TriggeredBy, Seq: s.Seq}.Before(SequenceID{TriggeredBy: s2.TriggeredBy, Seq: s2.Seq})
		}
		if s2.LowSeq != 0 {
			return s.LowSeq < s2.LowSeq
		}
		if s2.TriggeredBy != 0 {
			return s.LowSeq < s2.TriggeredBy
		}
		return s.LowSeq < s2.Seq
	}

	// s in format x:y
	if s.TriggeredBy != 0 {
		if s2.LowSeq != 0 {
			return s.TriggeredBy <= s2.LowSeq
		}
		if s2.TriggeredBy != 0 {
			if s.TriggeredBy == s2.TriggeredBy {
				return s.Seq < s2.Seq
			}
			return s.TriggeredBy < s2.TriggeredBy
		}
		return s.TriggeredBy <= s2.Seq // "n" sorts after "n:m" for any nonzero m
	}

	// s in format x (simple sequence)
	if s2.LowSeq != 0 {
		return s.Seq <= s2.LowSeq
	} else if s2.TriggeredBy != 0 {
		return s.Seq < s2.TriggeredBy
	} else {
		return s.Seq < s2.Seq
	}
}

// Create a zero'd out since value (eg, initial since value) based on the sequence type
func CreateZeroSinceValue() SequenceID {
	return SequenceID{}
}
