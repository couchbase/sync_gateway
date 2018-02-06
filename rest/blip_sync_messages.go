package rest

import (
	"bytes"
	"fmt"
	"math"
	"strings"

	"github.com/couchbase/go-blip"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"
	"github.com/couchbase/sync_gateway/db"
)

// Function signature for something that parses a sequence id from a string
type SequenceIDParser func(since string) (db.SequenceID, error)

// Helper for handling BLIP subChanges requests.  Supports Stringer() interface to log aspects of the request.
type subChangesParams struct {
	rq               *blip.Message    // The underlying BLIP message
	logger           base.SGLogger    // A logger object which might encompass more state (eg, blipContext id)
	zeroSeq          db.SequenceID    // A zero sequence ID with correct subtype (int sequence / channel clock)
	sequenceIDParser SequenceIDParser // Function which can convert a sequence id string to a db.SequenceID
}

// Create a new subChanges helper
func newSubChangesParams(rq *blip.Message, logger base.SGLogger, zeroSeq db.SequenceID, sequenceIDParser SequenceIDParser) *subChangesParams {
	return &subChangesParams{
		rq:               rq,
		logger:           logger,
		zeroSeq:          zeroSeq,
		sequenceIDParser: sequenceIDParser,
	}
}

func (s *subChangesParams) since() (db.SequenceID, error) {

	// Depending on the db sequence type, use correct zero sequence for since value
	sinceSequenceId := s.zeroSeq

	if sinceStr, found := s.rq.Properties["since"]; found {
		var err error
		if sinceSequenceId, err = s.sequenceIDParser(sinceStr); err != nil {
			s.logger.LogTo("Sync", "%s: Invalid sequence ID in 'since': %s", s.rq, sinceStr)
			return db.SequenceID{}, err
		}
	}

	return sinceSequenceId, nil

}

func (s *subChangesParams) batchSize() int {
	return int(getRestrictedIntFromString(s.rq.Properties["batch"], BlipDefaultBatchSize, 10, math.MaxUint64, true))
}

func (s *subChangesParams) continuous() bool {
	continuous := false
	if val, found := s.rq.Properties["continuous"]; found && val != "false" {
		continuous = true
	}
	return continuous
}

func (s *subChangesParams) activeOnly() bool {
	return (s.rq.Properties["active_only"] == "true")
}

func (s *subChangesParams) filter() string {
	return s.rq.Properties["filter"]
}

func (s *subChangesParams) channels() (channels string, found bool) {
	channels, found = s.rq.Properties["channels"]
	return channels, found
}

func (s *subChangesParams) channelsExpandedSet() (resultChannels base.Set, err error) {
	channelsParam, found := s.rq.Properties["channels"]
	if !found {
		return nil, fmt.Errorf("Missing 'channels' filter parameter")
	}
	channelsArray := strings.Split(channelsParam, ",")
	return channels.SetFromArray(channelsArray, channels.ExpandStar)
}

// Satisfy fmt.Stringer interface for dumping attributes of this subChanges request to logs
func (s *subChangesParams) String() string {

	buffer := bytes.NewBufferString("")

	since, err := s.since()
	if err != nil {
		base.Warn("Error discovering since value from subchanges.  Err: %v", err)
	} else {
		buffer.WriteString(fmt.Sprintf("Since:%v ", since))
	}

	continuous := s.continuous()
	if continuous {
		buffer.WriteString(fmt.Sprintf("Continuous:%v ", continuous))
	}

	activeOnly := s.activeOnly()
	if activeOnly {
		buffer.WriteString(fmt.Sprintf("ActiveOnly:%v ", activeOnly))
	}

	filter := s.filter()
	if len(filter) > 0 {
		buffer.WriteString(fmt.Sprintf("Filter:%v ", filter))
		channels, found := s.channels()
		if found {
			buffer.WriteString(fmt.Sprintf("Channels:%v ", channels))
		}
	}

	batchSize := s.batchSize()
	if batchSize != int(BlipDefaultBatchSize) {
		buffer.WriteString(fmt.Sprintf("BatchSize:%v ", s.batchSize()))
	}

	return buffer.String()

}

type setCheckpointParams struct {
	rq *blip.Message // The underlying BLIP message
}

func newSetCheckpointParams(rq *blip.Message) *setCheckpointParams {
	return &setCheckpointParams{
		rq: rq,
	}
}

func (s *setCheckpointParams) client() string {
	return s.rq.Properties["client"]
}

func (s *setCheckpointParams) rev() string {
	return s.rq.Properties["rev"]
}

func (s *setCheckpointParams) String() string {

	buffer := bytes.NewBufferString("")

	buffer.WriteString(fmt.Sprintf("Client:%v ", s.client()))

	rev := s.rev()
	if len(rev) > 0 {
		buffer.WriteString(fmt.Sprintf("Rev:%v ", rev))
	}

	return buffer.String()

}

type addRevisionParams struct {
	rq *blip.Message // The underlying BLIP message
}

func newAddRevisionParams(rq *blip.Message) *addRevisionParams {
	return &addRevisionParams{
		rq: rq,
	}
}

func (a *addRevisionParams) id() (id string, found bool) {
	id, found = a.rq.Properties["id"]
	return id, found
}

func (a *addRevisionParams) rev() (rev string, found bool) {
	rev, found = a.rq.Properties["rev"]
	return rev, found
}

func (a *addRevisionParams) deleted() bool {
	deleted, found := a.rq.Properties["deleted"]
	if !found {
		return false
	}
	return deleted != "0" && deleted != "false"
}

func (a *addRevisionParams) hasDeletedPropery() bool {
	_, found := a.rq.Properties["deleted"]
	return found
}

func (a *addRevisionParams) sequence() (sequence string, found bool) {
	sequence, found = a.rq.Properties["sequence"]
	return sequence, found
}

func (a *addRevisionParams) String() string {

	buffer := bytes.NewBufferString("")

	if id, foundId := a.id(); foundId {
		buffer.WriteString(fmt.Sprintf("Id:%v ", id))
	}

	if rev, foundRev := a.rev(); foundRev {
		buffer.WriteString(fmt.Sprintf("Rev:%v ", rev))
	}

	if a.hasDeletedPropery() {
		buffer.WriteString(fmt.Sprintf("Deleted:%v ", a.deleted()))
	}

	if sequence, foundSequence := a.sequence(); foundSequence == true {
		buffer.WriteString(fmt.Sprintf("Sequence:%v ", sequence))
	}

	return buffer.String()

}

type getAttachmentParams struct {
	rq *blip.Message // The underlying BLIP message
}

func newGetAttachmentParams(rq *blip.Message) *getAttachmentParams {
	return &getAttachmentParams{
		rq: rq,
	}
}

func (g *getAttachmentParams) digest() string {
	return g.rq.Properties["digest"]
}

func (g *getAttachmentParams) String() string {

	buffer := bytes.NewBufferString("")

	buffer.WriteString(fmt.Sprintf("Digest:%v ", g.digest()))

	return buffer.String()

}
