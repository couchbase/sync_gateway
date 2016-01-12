package db

import (
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
	SeqType          SequenceType       // Sequence Type (Int or Clock)
	TriggeredBy      uint64             // Int sequence: The sequence # that triggered this (0 if none)
	LowSeq           uint64             // Int sequence: Lowest contiguous sequence seen on the feed
	Seq              uint64             // Int sequence: The actual internal sequence
	Clock            base.SequenceClock // Clock sequence: Sequence (distributed index)
	TriggeredByClock base.SequenceClock // Clock sequence: Sequence (distributed index) that triggered this
	ClockHash        string             // String representation of clock hash
	SequenceHasher   *sequenceHasher    // Sequence hasher - used when unmarshalling clock-based sequences
	vbNo             uint16             // Vbucket number for actual sequence
	TriggeredByVbNo  uint16             // Vbucket number for triggered by sequence
	LowHash          string             // Clock hash used for continuous feed where some entries aren't hashed
}

type SequenceType int

const (
	Undefined = SequenceType(iota)
	IntSequenceType
	ClockSequenceType
)

var MaxSequenceID = SequenceID{
	Seq:  math.MaxUint64,
	vbNo: math.MaxUint16,
}

// Format sequence ID to send to clients.  Sequence IDs can be in one of the following formats:
//   Seq                    - simple sequence
//   TriggeredBy:Seq        - when TriggeredBy is non-zero, LowSeq is zero
//   LowSeq:TriggeredBy:Seq - when LowSeq is non-zero.
// When LowSeq is non-zero but TriggeredBy is zero, will appear as LowSeq::Seq.
// When LowSeq is non-zero but is greater than s.Seq (occurs when sending previously skipped sequences), ignore LowSeq.
func (s SequenceID) String() string {
	if s.SeqType == ClockSequenceType {
		return s.clockSeqToString()
	} else {
		return s.intSeqToString()
	}
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

// Clock sequences follow the same LowSeq:TriggeredBy:Seq used for integer sequences, but each
// sequence is represented as either a clock hash (in the form 0-0), or as a vbucket sequence pair
// (in the form vb.seq)
// e.g. sending document with vbucket 10, sequence 5, triggered by the vbucket clock with hash 31-1
// would look like 31-1:10.5
func (s SequenceID) clockSeqToString() string {

	// If TriggeredBy hash has been set, return it and vbucket sequence as triggeredByHash:vb.seq
	if s.TriggeredByClock != nil && s.TriggeredByClock.GetHashedValue() != "" {
		return fmt.Sprintf("%s:%d.%d", s.TriggeredByClock.GetHashedValue(), s.vbNo, s.Seq)
	} else {
		// If lowHash is defined, send that and the vbucket sequence as lowHash::vb.seq
		if s.LowHash != "" {
			return fmt.Sprintf("%s::%d.%d", s.LowHash, s.vbNo, s.Seq)
		}
		// If the clock hash has been set, return it.  Otherwise, return vbucket sequence as vb.seq
		if s.ClockHash != "" {
			return s.ClockHash
		} else if s.Clock != nil && s.Clock.GetHashedValue() != "" {
			return s.Clock.GetHashedValue()
		} else {
			return fmt.Sprintf("%d.%d", s.vbNo, s.Seq)
		}
	}
}

func (dbc *DatabaseContext) ParseSequenceID(str string) (s SequenceID, err error) {
	// If there's a sequence hasher defined, we're expecting clock-based sequences
	if dbc.SequenceHasher != nil {
		return parseClockSequenceID(str, dbc.SequenceHasher)
	} else {
		return parseIntegerSequenceID(str)
	}
}

func parseIntegerSequenceID(str string) (s SequenceID, err error) {
	if str == "" {
		return SequenceID{}, nil
	}
	s.SeqType = IntSequenceType
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

func parseClockSequenceID(str string, sequenceHasher *sequenceHasher) (s SequenceID, err error) {

	if str == "" {
		return SequenceID{
			SeqType: ClockSequenceType,
			Clock:   base.NewSequenceClockImpl(),
		}, nil
	}

	s.SeqType = ClockSequenceType
	components := strings.Split(str, ":")
	if len(components) == 1 {
		// Convert simple zero to empty clock, to handle clients sending zero to mean 'no previous since'
		if components[0] == "0" {
			s.Clock = base.NewSequenceClockImpl()
		} else {
			// Standard clock hash
			if s.Clock, err = sequenceHasher.GetClock(components[0]); err != nil {
				return SequenceID{}, err
			}
		}
	} else if len(components) == 2 {
		// TriggeredBy Clock Hash, and vb.seq sequence
		if s.TriggeredByClock, err = sequenceHasher.GetClock(components[0]); err != nil {
			return SequenceID{}, err
		}
		sequenceComponents := strings.Split(components[1], ".")
		if len(sequenceComponents) != 2 {
			base.Warn("Unexpected sequence format - ignoring and relying on triggered by")
			return
		} else {
			if vb64, err := strconv.ParseUint(sequenceComponents[0], 10, 16); err != nil {
				base.Warn("Unable to convert sequence %v to int.", sequenceComponents[0])
			} else {
				s.vbNo = uint16(vb64)
				s.Seq, err = strconv.ParseUint(sequenceComponents[1], 10, 64)
			}
		}

	} else if len(components) == 3 {
		// Low hash, and vb.seq sequence.  Use low hash as clock, ignore vb.seq
		if s.Clock, err = sequenceHasher.GetClock(components[0]); err != nil {
			return SequenceID{}, err
		}

	} else {
		err = base.HTTPErrorf(400, "Invalid sequence")
	}

	if err != nil {
		err = base.HTTPErrorf(400, "Invalid sequence")
	}
	return s, err
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

	if s.SeqType == ClockSequenceType {
		return []byte(fmt.Sprintf("\"%s\"", s.clockSeqToString())), nil
	} else {
		if s.TriggeredBy > 0 || s.LowSeq > 0 {
			return []byte(fmt.Sprintf("\"%s\"", s.String())), nil
		} else {
			return []byte(strconv.FormatUint(s.Seq, 10)), nil
		}
	}

}

func (s *SequenceID) UnmarshalJSON(data []byte) error {

	if s.SeqType == ClockSequenceType {
		return s.unmarshalClockSequence(data)
	} else if s.SeqType == IntSequenceType {
		return s.unmarshalIntSequence(data)
	} else {
		// Type not explicitly defined.  If sequence is string and either contains "-" or ".", treat as clock (sequence hash format,
		// and vb.seq format).  Otherwise treat as int.
		if len(data) > 0 && data[0] == '"' {
			var raw string
			err := json.Unmarshal(data, &raw)
			if err != nil {
				return err
			}
			if strings.Contains(raw, "-") || strings.Contains(raw, ".") {
				return s.unmarshalClockSequence(data)
			}
		}
		return s.unmarshalIntSequence(data)
	}
}

func (s *SequenceID) unmarshalIntSequence(data []byte) error {
	var raw string
	err := json.Unmarshal(data, &raw)
	if err != nil {
		*s, err = parseIntegerSequenceID(string(data))
	} else {
		*s, err = parseIntegerSequenceID(raw)
	}
	return err

}

// Unmarshals clock sequence.  If s.SequenceHasher is nil, UnmarshalClockSequence only populates the s.ClockHash value.
func (s *SequenceID) unmarshalClockSequence(data []byte) error {

	base.LogTo("Debug", "Unmarshalling clock sequence: %d", s.SeqType)
	var hashValue string
	err := json.Unmarshal(data, &hashValue)
	if err != nil {
		hashValue = string(data)
	}

	if s.SequenceHasher != nil {
		*s, err = parseClockSequenceID(hashValue, s.SequenceHasher)
	} else {
		s.ClockHash = hashValue
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
	if s.SeqType == ClockSequenceType {
		return s.Clock != nil
	} else {
		return s.Seq > 0
	}
}

// The most significant value is TriggeredBy, unless it's zero, in which case use Seq.
// The tricky part is that "n" sorts after "n:m" for any nonzero m
func (s SequenceID) Before(s2 SequenceID) bool {
	if s.SeqType == ClockSequenceType {
		vbefore := s.vectorBefore(s2)
		return vbefore
	} else {
		return s.intBefore(s2)
	}
}

// The most significant value is TriggeredBy, unless it's zero, in which case use Seq.
// The tricky part is that "n" sorts after "n:m" for any nonzero m
func (s SequenceID) intBefore(s2 SequenceID) bool {

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

// For vector sequences, triggered by is always a full clock.
func (s SequenceID) vectorBefore(s2 SequenceID) bool {

	if s.TriggeredByClock == nil && s2.TriggeredByClock == nil { // No triggered by - return based on vb.seq only
		return s.VbucketSequenceBefore(s2.vbNo, s2.Seq)
	} else if s.TriggeredByClock == nil {
		// s2 triggered but not s.  Compare s with s2.triggered by
		return s.VbucketSequenceBefore(s2.TriggeredByVbNo, s2.TriggeredBy)
	} else if s2.TriggeredByClock == nil {
		// s triggered but not s2.  Compare s.Triggered by with s2.
		// Check for equality first, since equality won't return false for !s2.VbucketSequenceBefore
		if s2.Seq == s.TriggeredBy && s2.vbNo == s.TriggeredByVbNo {
			return false
		}
		return !s2.VbucketSequenceBefore(s.TriggeredByVbNo, s.TriggeredBy)
	} else if s.TriggeredByClock.Equals(s2.TriggeredByClock) { // Both triggered by the same clock - return based on vb.seq
		return s.VbucketSequenceBefore(s2.vbNo, s2.Seq)
	} else { // Triggered by different clocks - return based on earlier triggeredBy
		return s.TriggeredByClock.AllBefore(s2.TriggeredByClock)
	}

}

func (s SequenceID) VbucketSequenceBefore(vbNo uint16, seq uint64) bool {
	if s.vbNo == vbNo {
		return s.Seq < seq
	} else {
		return s.vbNo < vbNo
	}
}

func (s SequenceID) VbucketSequenceAfter(vbNo uint16, seq uint64) bool {
	if s.vbNo == vbNo {
		return s.Seq > seq
	} else {
		return s.vbNo > vbNo
	}
}
