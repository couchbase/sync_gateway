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

// Message types
const (
	messageSetCheckpoint   = "setCheckpoint"
	messageGetCheckpoint   = "getCheckpoint"
	messageSubChanges      = "subChanges"
	messageChanges         = "changes"
	messageRev             = "rev"
	messageNoRev           = "norev"
	messageGetAttachment   = "getAttachment"
	messageProposeChanges  = "proposeChanges"
	messageProveAttachment = "proveAttachment"
)

// Message properties
const (

	// Common message properties
	blipClient   = "client"
	blipCompress = "compress"
	blipProfile  = "Profile"

	// setCheckpoint message properties
	setCheckpointRev = "rev"

	// getCheckpoint message properties
	getCheckpointResponseRev = "rev"

	// subChanges message properties
	subChangesActiveOnly = "active_only"
	subChangesFilter     = "filter"
	subChangesChannels   = "channels"
	subChangesSince      = "since"
	subChangesContinuous = "continuous"

	// rev message properties
	revMessageId          = "id"
	revMessageRev         = "rev"
	revMessageDeleted     = "deleted"
	revMessageSequence    = "sequence"
	revMessageHistory     = "history"
	revMessageNoConflicts = "noconflicts"
	revMessageDeltaSrc    = "deltaSrc"

	// norev message properties
	norevMessageId     = "id"
	norevMessageRev    = "rev"
	norevMessageError  = "error"
	norevMessageReason = "reason"

	// changes message properties
	changesResponseMaxHistory = "maxHistory"
	changesResponseDeltas     = "deltas"

	// proposeChanges message properties
	proposeChangesResponseDeltas = "deltas"

	// getAttachment message properties
	getAttachmentDigest = "digest"

	// proveAttachment
	proveAttachmentDigest = "digest"
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
	if sinceStr, found := rq.Properties[subChangesSince]; found {
		var err error
		if sinceSequenceId, err = sequenceIDParser(base.ConvertJSONString(sinceStr)); err != nil {
			logger.Logf(base.LevelInfo, base.KeySync, "%s: Invalid sequence ID in 'since': %s", rq, sinceStr)
			return params, err
		}
	}
	params._since = sinceSequenceId

	// rq.BodyReader() returns an EOF for a non-existent body, so using rq.Body() here
	docIDs, err := readDocIDsFromRequest(rq)
	if err != nil {
		logger.Logf(base.LevelInfo, base.KeySync, "%s: Error reading doc IDs on subChanges request: %s", rq, err)
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
		unmarshalErr := base.JSONUnmarshal(rawBody, &body)
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
	if val, found := s.rq.Properties[subChangesContinuous]; found && val != "false" {
		continuous = true
	}
	return continuous
}

func (s *subChangesParams) activeOnly() bool {
	return (s.rq.Properties[subChangesActiveOnly] == "true")
}

func (s *subChangesParams) filter() string {
	return s.rq.Properties[subChangesFilter]
}

func (s *subChangesParams) channels() (channels string, found bool) {
	channels, found = s.rq.Properties[subChangesChannels]
	return channels, found
}

func (s *subChangesParams) channelsExpandedSet() (resultChannels base.Set, err error) {
	channelsParam, found := s.rq.Properties[subChangesChannels]
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

// setCheckpoint message
type SetCheckpointMessage struct {
	*blip.Message
}

func NewSetCheckpointMessage() *SetCheckpointMessage {
	scm := &SetCheckpointMessage{blip.NewRequest()}
	scm.SetProfile(messageSetCheckpoint)
	return scm
}

func (scm *SetCheckpointMessage) client() string {
	return scm.Properties[blipClient]
}

func (scm *SetCheckpointMessage) setClient(client string) {
	scm.Properties[blipClient] = client
}

func (scm *SetCheckpointMessage) rev() string {
	return scm.Properties[setCheckpointRev]
}

func (scm *SetCheckpointMessage) setRev(rev string) {
	scm.Properties[setCheckpointRev] = rev
}

func (scm *SetCheckpointMessage) String() string {

	buffer := bytes.NewBufferString("")

	buffer.WriteString(fmt.Sprintf("Client:%v ", scm.client()))

	rev := scm.rev()
	if len(rev) > 0 {
		buffer.WriteString(fmt.Sprintf("Rev:%v ", rev))
	}

	return buffer.String()

}

type SetCheckpointResponse struct {
	*blip.Message
}

func (scr *SetCheckpointResponse) Rev() (rev string) {
	return scr.Properties[setCheckpointRev]
}

func (scr *SetCheckpointResponse) setRev(rev string) {
	scr.Properties[setCheckpointRev] = rev
}

// Rev message
type revMessage struct {
	*blip.Message
}

func NewRevMessage() *revMessage {
	rm := &revMessage{blip.NewRequest()}
	rm.SetProfile(messageRev)
	return rm
}

func (rm *revMessage) id() (id string, found bool) {
	id, found = rm.Properties[revMessageId]
	return id, found
}

func (rm *revMessage) rev() (rev string, found bool) {
	rev, found = rm.Properties[revMessageRev]
	return rev, found
}

func (rm *revMessage) deleted() bool {
	deleted, found := rm.Properties[revMessageDeleted]
	if !found {
		return false
	}
	return deleted != "0" && deleted != "false"
}

func (rm *revMessage) deltaSrc() (deltaSrc string, found bool) {
	deltaSrc, found = rm.Properties[revMessageDeltaSrc]
	return deltaSrc, found
}

func (rm *revMessage) hasDeletedProperty() bool {
	_, found := rm.Properties[revMessageDeleted]
	return found
}

func (rm *revMessage) sequence() (sequence string, found bool) {
	sequence, found = rm.Properties[revMessageSequence]
	return sequence, found
}

func (rm *revMessage) setId(id string) {
	rm.Properties[revMessageId] = id
}

func (rm *revMessage) setRev(rev string) {
	rm.Properties[revMessageRev] = rev
}

// setProperties will add the given properties to the blip message, overwriting any that already exist.
func (rm *revMessage) setProperties(properties blip.Properties) {
	for k, v := range properties {
		rm.Properties[k] = v
	}
}

func (rm *revMessage) String() string {

	buffer := bytes.NewBufferString("")

	if id, foundId := rm.id(); foundId {
		buffer.WriteString(fmt.Sprintf("Id:%v ", base.UD(id).Redact()))
	}

	if rev, foundRev := rm.rev(); foundRev {
		buffer.WriteString(fmt.Sprintf("Rev:%v ", rev))
	}

	if rm.hasDeletedProperty() {
		buffer.WriteString(fmt.Sprintf("Deleted:%v ", rm.deleted()))
	}

	if deltaSrc, isDelta := rm.deltaSrc(); isDelta {
		buffer.WriteString(fmt.Sprintf("DeltaSrc:%v ", deltaSrc))
	}

	if sequence, foundSequence := rm.sequence(); foundSequence == true {
		buffer.WriteString(fmt.Sprintf("Sequence:%v ", sequence))
	}

	return buffer.String()

}

// Rev message
type noRevMessage struct {
	*blip.Message
}

func NewNoRevMessage() *noRevMessage {
	nrm := &noRevMessage{blip.NewRequest()}
	nrm.SetProfile(messageNoRev)
	return nrm
}
func (nrm *noRevMessage) setId(id string) {
	nrm.Properties[norevMessageId] = id
}

func (nrm *noRevMessage) setRev(rev string) {
	nrm.Properties[norevMessageRev] = rev
}

func (nrm *noRevMessage) setReason(reason string) {
	nrm.Properties[norevMessageReason] = reason
}

func (nrm *noRevMessage) setError(message string) {
	nrm.Properties[norevMessageError] = message
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
	return g.rq.Properties[getAttachmentDigest]
}

func (g *getAttachmentParams) String() string {

	buffer := bytes.NewBufferString("")

	buffer.WriteString(fmt.Sprintf("Digest:%v ", g.digest()))

	return buffer.String()

}
