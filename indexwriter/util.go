package indexwriter

import (
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db"
)

// Returns a clock-base SequenceID with all vb values set to seq
func SimpleClockSequence(seq uint64) db.SequenceID {
	result := db.SequenceID{
		SeqType: db.ClockSequenceType,
		Clock:   base.NewSequenceClockImpl(),
	}
	for i := 0; i < 1024; i++ {
		result.Clock.SetSequence(uint16(i), seq)
	}
	return result
}
