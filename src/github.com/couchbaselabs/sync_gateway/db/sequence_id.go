package db

import (
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/couchbaselabs/sync_gateway/base"
)

// A change sequence as reported externally in a _changes feed.
// Most of the time the TriggerSeq is 0, but if a revision is being sent retroactively because
// the user got access to a channel, the TriggerSeq will be equal to the sequence of the change
// that gave the user access.
type SequenceID struct {
	TriggeredBy uint64 // The sequence # that triggered this (0 if none)
	Seq         uint64 // The actual internal sequence
}

var MaxSequenceID = SequenceID{Seq: math.MaxUint64}

func (s SequenceID) String() string {
	if s.TriggeredBy > 0 {
		return fmt.Sprintf("%d:%d", s.TriggeredBy, s.Seq)
	} else {
		return strconv.FormatUint(s.Seq, 10)
	}
}

func ParseSequenceID(str string) (s SequenceID, err error) {
	if str == "" {
		return SequenceID{}, nil
	}
	components := strings.Split(str, ":")
	if len(components) == 1 {
		s.Seq, err = strconv.ParseUint(str, 10, 64)
	} else if len(components) != 2 {
		err = base.HTTPErrorf(400, "Invalid sequence")
	} else {
		s.TriggeredBy, err = strconv.ParseUint(components[0], 10, 64)
		if err == nil {
			s.Seq, err = strconv.ParseUint(components[1], 10, 64)
		}
	}
	if err != nil {
		err = base.HTTPErrorf(400, "Invalid sequence")
	}
	return
}

func (s SequenceID) MarshalJSON() ([]byte, error) {
	if s.TriggeredBy > 0 {
		return []byte(fmt.Sprintf("\"%d:%d\"", s.TriggeredBy, s.Seq)), nil
	} else {
		return []byte(strconv.FormatUint(s.Seq, 10)), nil
	}
}

func (s *SequenceID) UnmarshalJSON(data []byte) error {
	if len(data) > 0 && data[0] == '"' {
		var raw string
		err := json.Unmarshal(data, &raw)
		if err == nil {
			*s, err = ParseSequenceID(string(raw))
		}
		return err
	} else {
		s.TriggeredBy = 0
		return json.Unmarshal(data, &s.Seq)
	}
}

// The most significant value is TriggeredBy, unless it's zero, in which case use Seq.
// The tricky part is that "n" sorts after "n:m" for any nonzero m
func (s SequenceID) Before(s2 SequenceID) bool {
	if s.TriggeredBy == s2.TriggeredBy {
		return s.Seq < s2.Seq // the simple case: untriggered, or triggered by same sequence
	} else if s.TriggeredBy == 0 {
		return s.Seq < s2.TriggeredBy // s2 triggered but not s
	} else if s2.TriggeredBy == 0 {
		return s.TriggeredBy <= s2.Seq // s triggered but not s2
	} else {
		return s.TriggeredBy < s2.TriggeredBy // both triggered, but by different sequences
	}
}
