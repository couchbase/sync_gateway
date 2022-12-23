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
	MetaSeq     uint64 // The metadata sequence (for users, roles, grants)
	BackfillSeq uint64 // When backfilling a channel grant or revocation, the sequence being replicated
	TriggeredBy uint64 // When using the default collection, the sequence # that triggered this (0 if none)
	LowSeq      uint64 // Lowest contiguous sequence seen on the feed
	Seq         uint64 // The actual internal sequence
}

var MaxSequenceID = SequenceID{
	Seq: math.MaxUint64,
}

// Format sequence ID to send to clients.  Sequence IDs can be in one of the following formats:
//  Collection format - used when metadata and data are not stored in the same collection.  TriggeredBy is not used in this case
//   MetaSeq-Seq              				- Simple sequence
//   MetaSeq-LowSeq::Seq      				- When LowSeq is non-zero.  (skipped sequence handling)
//   MetadataSeq:BackfillSeq-Seq  			- When BackfillSeq is non-zero (revocation or backfill)
//   MetadataSeq:BackfillSeq-LowSeq::Seq 	- BackfillSeq and LowSeq both non-zero.  Revocation/backfill with skipped sequences
//
//  Default collection format (used when metadata and data are stored in the _default._default collection)
//	 Seq                    - simple sequence
//	 TriggeredBy:Seq        - when TriggeredBy is non-zero, LowSeq is zero
//	 LowSeq:TriggeredBy:Seq - when LowSeq is non-zero.
//   LowSeq::Seq            - When LowSeq is non-zero, TriggeredBy is zero
// 		When LowSeq is non-zero but is greater than s.Seq (occurs when sending previously skipped sequences), ignore LowSeq.
func (s SequenceID) String() string {
	if s.MetaSeq != 0 {
		return s.MetaFormat()
	} else {
		return s.SingleCollectionFormat()
	}
}

func (s SequenceID) SingleCollectionFormat() string {
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

func (s SequenceID) MetaFormat() string {
	if s.BackfillSeq == 0 {
		return fmt.Sprintf("%d-%s", s.MetaSeq, s.SingleCollectionFormat())
	}
	return fmt.Sprintf("%d:%d-%s", s.MetaSeq, s.BackfillSeq, s.SingleCollectionFormat())
}

func seqStr(seq interface{}) string {
	switch seq := seq.(type) {
	case string:
		return seq
	case json.Number:
		return seq.String()
	}
	base.WarnfCtx(context.Background(), "unknown seq type: %T", seq)
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

func parseIntegerSequenceID(str string) (s SequenceID, err error) {
	if str == "" {
		return SequenceID{}, nil
	}

	// Check for metadata sequence format (MetaSeq, BackfillSeq)
	metaPartStr, nonMetaPartStr, found := strings.Cut(str, "-")
	if found {
		metaSeqStr, backfillSeqStr, found := strings.Cut(metaPartStr, ":")
		if found {
			if s.MetaSeq, err = ParseSequenceComponent(metaSeqStr, false); err != nil {
				return
			}
			if s.BackfillSeq, err = ParseSequenceComponent(backfillSeqStr, false); err != nil {
				return
			}
		} else {
			if s.MetaSeq, err = ParseSequenceComponent(metaPartStr, false); err != nil {
				return
			}
		}
		str = nonMetaPartStr
	}

	components := strings.Split(str, ":")
	if len(components) == 1 {
		// Just the internal sequence
		s.Seq, err = ParseSequenceComponent(components[0], false)
	} else if len(components) == 2 {
		// TriggeredBy and InternalSequence
		if s.TriggeredBy, err = ParseSequenceComponent(components[0], false); err != nil {
			return
		}
		if s.Seq, err = ParseSequenceComponent(components[1], false); err != nil {
			return
		}
	} else if len(components) == 3 {
		if s.LowSeq, err = ParseSequenceComponent(components[0], false); err != nil {
			return
		}
		if s.TriggeredBy, err = ParseSequenceComponent(components[1], true); err != nil {
			return
		}
		if s.Seq, err = ParseSequenceComponent(components[2], false); err != nil {
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

func ParseSequenceComponent(component string, allowEmpty bool) (uint64, error) {
	value := uint64(0)
	if allowEmpty && component == "" {
		return value, nil
	}
	value, err := strconv.ParseUint(component, 10, 64)
	return value, err

}

func (s SequenceID) MarshalJSON() ([]byte, error) {

	if s.TriggeredBy > 0 || s.LowSeq > 0 || s.MetaSeq > 0 {
		return []byte(fmt.Sprintf("\"%s\"", s.String())), nil
	} else {
		return []byte(strconv.FormatUint(s.Seq, 10)), nil
	}

}

func (s *SequenceID) UnmarshalJSON(data []byte) error {
	return s.unmarshalSequence(data)
}

func (s *SequenceID) unmarshalSequence(data []byte) error {
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
	return s.Seq > 0 || s.MetaSeq > 0
}

// Equality of sequences, based on seq, triggered by and low hash
func (s SequenceID) Equals(s2 SequenceID) bool {
	return s.SafeSequence() == s2.SafeSequence() && s.TriggeredBy == s2.TriggeredBy && s.MetaSeq == s2.MetaSeq && s.BackfillSeq == s2.BackfillSeq
}

// The most significant value is TriggeredBy, unless it's zero, in which case use Seq.
// The tricky part is that "n" sorts after "n:m" for any nonzero m, for both triggeredBy:Seq and metaSeq:backfillSeq
func (s SequenceID) Before(s2 SequenceID) bool {

	// Check metaSeq first
	if s.MetaSeq != s2.MetaSeq {
		return s.MetaSeq < s2.MetaSeq
	}
	if s.BackfillSeq != s2.BackfillSeq {
		if s.BackfillSeq == 0 {
			return false
		}
		if s2.BackfillSeq == 0 {
			return true
		}
		return s.BackfillSeq < s2.BackfillSeq
	}

	// using SafeSequence for comparison, which takes the lower of LowSeq and Seq
	if s.TriggeredBy == s2.TriggeredBy {
		if s.SafeSequence() == s2.SafeSequence() {
			// If both are equal, one sequence must be a non-zero LowSeq - sort those after the actual sequence.
			return s.Seq < s2.Seq
		}
		return s.SafeSequence() < s2.SafeSequence() // the simple case: untriggered, or triggered by same sequence
	} else if s.TriggeredBy == 0 {
		return s.SafeSequence() < s2.TriggeredBy // s2 triggered but not s
	} else if s2.TriggeredBy == 0 {
		return s.TriggeredBy <= s2.SafeSequence() // s triggered but not s2
	} else {
		return s.TriggeredBy < s2.TriggeredBy // both triggered, but by different sequences
	}
}

// Create a zero'd out since value (eg, initial since value) based on the sequence type
func CreateZeroSinceValue() SequenceID {
	return SequenceID{}
}
