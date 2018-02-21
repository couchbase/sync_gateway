package rest

import (
	"bytes"
	"encoding/json"
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
	rq      *blip.Message // The underlying BLIP message
	_since  db.SequenceID // Since value on the incoming request
	_docIDs []string      // Document ID filter specified on the incoming request
}

type subChangesBody struct {
	DocIDs []string `json:"docIDs"`
}

// Create a new subChanges helper
func newSubChangesParams(rq *blip.Message, logger base.SGLogger, zeroSeq db.SequenceID, sequenceIDParser SequenceIDParser) (*subChangesParams, error) {

	params := &subChangesParams{
		rq: rq,
	}

	// Determine incoming since and docIDs once, since there is some overhead associated with their calculation
	sinceSequenceId := zeroSeq
	if sinceStr, found := rq.Properties["since"]; found {
		var err error
		if sinceSequenceId, err = sequenceIDParser(base.ConvertJSONString(sinceStr)); err != nil {
			logger.LogTo("Sync", "%s: Invalid sequence ID in 'since': %s", rq, sinceStr)
			return params, err
		}
	}
	params._since = sinceSequenceId

	// rq.BodyReader() returns an EOF for a non-existent body, so using rq.Body() here
	docIDs, err := readDocIDsFromRequest(rq)
	if err != nil {
		logger.LogTo("Sync", "%s: Error reading doc IDs on subChanges request: %s", rq, err)
		return params, err
	}
	params._docIDs = docIDs

	return params, nil
}

func (s *subChangesParams) since() db.SequenceID {
	return s._since
}

func (s *subChangesParams) docIDs() []string {
	return s._docIDs
}

func readDocIDsFromRequest(rq *blip.Message) (docIDs []string, err error) {
	// Get Body from request.  Not using BodyReader(), to avoid EOF on empty body
	rawBody, err := rq.Body()
	if err != nil {
		return nil, err
	}

	// If there's a non-empty body, unmarshal to get the docIDs
	if len(rawBody) > 0 {
		var body subChangesBody
		unmarshalErr := json.Unmarshal(rawBody, &body)
		if unmarshalErr != nil {
			return nil, err
		} else {
			docIDs = body.DocIDs
		}
	}
	return docIDs, err

}

func (s *subChangesParams) batchSize() int {
	return int(getRestrictedIntFromString(s.rq.Properties["batch"], BlipDefaultBatchSize, BlipMinimumBatchSize, math.MaxUint64, true))
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
	buffer.WriteString(fmt.Sprintf("Since:%v ", s.since()))

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

	if len(s.docIDs()) > 0 {
		buffer.WriteString(fmt.Sprintf("DocIDs:%v ", s.docIDs()))
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
