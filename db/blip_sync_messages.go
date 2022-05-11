/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package db

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"strings"

	"github.com/couchbase/go-blip"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"
)

// Message types
const (
	MessageSetCheckpoint   = "setCheckpoint"
	MessageGetCheckpoint   = "getCheckpoint"
	MessageSubChanges      = "subChanges"
	MessageChanges         = "changes"
	MessageRev             = "rev"
	MessageNoRev           = "norev"
	MessageGetAttachment   = "getAttachment"
	MessageProposeChanges  = "proposeChanges"
	MessageProveAttachment = "proveAttachment"
	MessageGetRev          = "getRev"       // Connected Client API
	MessagePutRev          = "putRev"       // Connected Client API
	MessageUnsubChanges    = "unsubChanges" // Connected Client API
)

// Message properties
const (

	// Common message properties
	BlipClient   = "client"
	BlipCompress = "compress"
	BlipProfile  = "Profile"

	// setCheckpoint message properties
	SetCheckpointRev         = "rev"
	SetCheckpointClient      = "client"
	SetCheckpointResponseRev = "rev"

	// getCheckpoint message properties
	GetCheckpointResponseRev = "rev"
	GetCheckpointClient      = "client"

	// subChanges message properties
	SubChangesActiveOnly  = "activeOnly"
	SubChangesFilter      = "filter"
	SubChangesChannels    = "channels"
	SubChangesSince       = "since"
	SubChangesContinuous  = "continuous"
	SubChangesBatch       = "batch"
	SubChangesRevocations = "revocations"

	// rev message properties
	RevMessageId          = "id"
	RevMessageRev         = "rev"
	RevMessageDeleted     = "deleted"
	RevMessageSequence    = "sequence"
	RevMessageHistory     = "history"
	RevMessageNoConflicts = "noconflicts"
	RevMessageDeltaSrc    = "deltaSrc"

	// norev message properties
	NorevMessageId       = "id"
	NorevMessageRev      = "rev"
	NorevMessageSeq      = "seq" // Use when protocol version 2 with client type ISGR
	NorevMessageSequence = "sequence"
	NorevMessageError    = "error"
	NorevMessageReason   = "reason"

	// getRev (Connected Client) message properties
	GetRevMessageId = "id"
	GetRevRevId     = "rev"
	GetRevIfNotRev  = "ifNotRev"

	// changes message properties
	ChangesMessageIgnoreNoConflicts = "ignoreNoConflicts"

	// changes response properties
	ChangesResponseMaxHistory = "maxHistory"
	ChangesResponseDeltas     = "deltas"

	// proposeChanges message properties
	ProposeChangesConflictsIncludeRev = "conflictIncludesRev"

	// proposeChanges response message properties
	ProposeChangesResponseDeltas = "deltas"

	// getAttachment message properties
	GetAttachmentID     = "docID"
	GetAttachmentDigest = "digest"

	// proveAttachment
	ProveAttachmentDigest = "digest"

	// Sync Gateway specific properties (used for testing)
	SGShowHandler = "sgShowHandler" // Used to request a response with sgHandler
	SGHandler     = "sgHandler"     // Used to show which handler processed the message

	// blip error properties
	BlipErrorDomain = "Error-Domain"
	BlipErrorCode   = "Error-Code"
)

// Function signature for something that parses a sequence id from a string
type SequenceIDParser func(since string) (SequenceID, error)

// Function signature that returns the latest sequence
type LatestSequenceFunc func() (SequenceID, error)

// Helper for handling BLIP subChanges requests.  Supports Stringer() interface to log aspects of the request.
type SubChangesParams struct {
	rq      *blip.Message // The underlying BLIP message
	_since  SequenceID    // Since value on the incoming request
	_docIDs []string      // Document ID filter specified on the incoming request
}

type SubChangesBody struct {
	DocIDs []string `json:"docIDs"`
}

// Create a new subChanges helper
func NewSubChangesParams(logCtx context.Context, rq *blip.Message, zeroSeq SequenceID, latestSeq LatestSequenceFunc, sequenceIDParser SequenceIDParser) (*SubChangesParams, error) {

	params := &SubChangesParams{
		rq: rq,
	}

	// Determine incoming since and docIDs once, since there is some overhead associated with their calculation
	sinceSequenceId := zeroSeq
	var err error
	if rq.Properties["future"] == "true" {
		sinceSequenceId, err = latestSeq()
	} else if sinceStr, found := rq.Properties[SubChangesSince]; found {
		if sinceSequenceId, err = sequenceIDParser(base.ConvertJSONString(sinceStr)); err != nil {
			base.InfofCtx(logCtx, base.KeySync, "%s: Invalid sequence ID in 'since': %s", rq, sinceStr)
		}
	}
	if err != nil {
		return params, err
	}
	params._since = sinceSequenceId

	// rq.BodyReader() returns an EOF for a non-existent body, so using rq.Body() here
	docIDs, err := readDocIDsFromRequest(rq)
	if err != nil {
		base.InfofCtx(logCtx, base.KeySync, "%s: Error reading doc IDs on subChanges request: %s", rq, err)
		return params, err
	}
	params._docIDs = docIDs

	return params, nil
}

func (s *SubChangesParams) Since() SequenceID {
	return s._since
}

func (s *SubChangesParams) docIDs() []string {
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
		var body SubChangesBody
		unmarshalErr := base.JSONUnmarshal(rawBody, &body)
		if unmarshalErr != nil {
			return nil, err
		} else {
			docIDs = body.DocIDs
		}
	}
	return docIDs, err

}

func (s *SubChangesParams) batchSize() int {
	return int(base.GetRestrictedIntFromString(s.rq.Properties["batch"], BlipDefaultBatchSize, BlipMinimumBatchSize, math.MaxUint64, true))
}

func (s *SubChangesParams) continuous() bool {
	continuous := false
	if val, found := s.rq.Properties[SubChangesContinuous]; found && val != "false" {
		continuous = true
	}
	return continuous
}

func (s *SubChangesParams) revocations() bool {
	return s.rq.Properties[SubChangesRevocations] == "true"
}

func (s *SubChangesParams) activeOnly() bool {
	return (s.rq.Properties[SubChangesActiveOnly] == "true")
}

func (s *SubChangesParams) filter() string {
	return s.rq.Properties[SubChangesFilter]
}

func (s *SubChangesParams) channels() (channels string, found bool) {
	channels, found = s.rq.Properties[SubChangesChannels]
	return channels, found
}

func (s *SubChangesParams) channelsExpandedSet() (resultChannels base.Set, err error) {
	channelsParam, found := s.rq.Properties[SubChangesChannels]
	if !found {
		return nil, fmt.Errorf("Missing 'channels' filter parameter")
	}
	channelsArray := strings.Split(channelsParam, ",")
	return channels.SetFromArray(channelsArray, channels.ExpandStar)
}

// Satisfy fmt.Stringer interface for dumping attributes of this subChanges request to logs
func (s *SubChangesParams) String() string {

	buffer := bytes.NewBufferString("")
	buffer.WriteString(fmt.Sprintf("Since:%v ", s.Since()))

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
	scm.SetProfile(MessageSetCheckpoint)
	return scm
}

func (scm *SetCheckpointMessage) client() string {
	return scm.Properties[BlipClient]
}

func (scm *SetCheckpointMessage) SetClient(client string) {
	scm.Properties[BlipClient] = client
}

func (scm *SetCheckpointMessage) rev() string {
	return scm.Properties[SetCheckpointRev]
}

func (scm *SetCheckpointMessage) SetRev(rev string) {
	scm.Properties[SetCheckpointRev] = rev
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
	return scr.Properties[SetCheckpointRev]
}

func (scr *SetCheckpointResponse) setRev(rev string) {
	scr.Properties[SetCheckpointRev] = rev
}

// Rev message
type RevMessage struct {
	*blip.Message
}

func NewRevMessage() *RevMessage {
	rm := &RevMessage{blip.NewRequest()}
	rm.SetProfile(MessageRev)
	return rm
}

func (rm *RevMessage) ID() (id string, found bool) {
	id, found = rm.Properties[RevMessageId]
	return id, found
}

func (rm *RevMessage) Rev() (rev string, found bool) {
	rev, found = rm.Properties[RevMessageRev]
	return rev, found
}

func (rm *RevMessage) Deleted() bool {
	deleted, found := rm.Properties[RevMessageDeleted]
	if !found {
		return false
	}
	return deleted != "0" && deleted != "false"
}

func (rm *RevMessage) DeltaSrc() (deltaSrc string, found bool) {
	deltaSrc, found = rm.Properties[RevMessageDeltaSrc]
	return deltaSrc, found
}

func (rm *RevMessage) HasDeletedProperty() bool {
	_, found := rm.Properties[RevMessageDeleted]
	return found
}

func (rm *RevMessage) Sequence() (sequence string, found bool) {
	sequence, found = rm.Properties[RevMessageSequence]
	return sequence, found
}

func (rm *RevMessage) SetID(id string) {
	rm.Properties[RevMessageId] = id
}

func (rm *RevMessage) SetRev(rev string) {
	rm.Properties[RevMessageRev] = rev
}

func (rm *RevMessage) SetNoConflicts(noConflicts bool) {
	if noConflicts {
		rm.Properties[RevMessageNoConflicts] = "true"
	} else {
		rm.Properties[RevMessageNoConflicts] = "false"
	}
}

// setProperties will add the given properties to the blip message, overwriting any that already exist.
func (rm *RevMessage) SetProperties(properties blip.Properties) {
	for k, v := range properties {
		rm.Properties[k] = v
	}
}

func (rm *RevMessage) String() string {

	buffer := bytes.NewBufferString("")

	if id, foundId := rm.ID(); foundId {
		buffer.WriteString(fmt.Sprintf("Id:%v ", base.UD(id).Redact()))
	}

	if rev, foundRev := rm.Rev(); foundRev {
		buffer.WriteString(fmt.Sprintf("Rev:%v ", rev))
	}

	if rm.HasDeletedProperty() {
		buffer.WriteString(fmt.Sprintf("Deleted:%v ", rm.Deleted()))
	}

	if deltaSrc, isDelta := rm.DeltaSrc(); isDelta {
		buffer.WriteString(fmt.Sprintf("DeltaSrc:%v ", deltaSrc))
	}

	if sequence, foundSequence := rm.Sequence(); foundSequence == true {
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
	nrm.SetProfile(MessageNoRev)
	return nrm
}
func (nrm *noRevMessage) SetId(id string) {
	nrm.Properties[NorevMessageId] = id
}

func (nrm *noRevMessage) SetRev(rev string) {
	nrm.Properties[NorevMessageRev] = rev
}

func (nrm *noRevMessage) SetSeq(seq SequenceID) {
	nrm.Properties[NorevMessageSeq] = seq.String()
}

func (nrm *noRevMessage) SetSequence(sequence SequenceID) {
	nrm.Properties[NorevMessageSequence] = sequence.String()
}

func (nrm *noRevMessage) SetReason(reason string) {
	nrm.Properties[NorevMessageReason] = reason
}

func (nrm *noRevMessage) SetError(message string) {
	nrm.Properties[NorevMessageError] = message
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
	return g.rq.Properties[GetAttachmentDigest]
}

func (g *getAttachmentParams) docID() string {
	return g.rq.Properties[GetAttachmentID]
}

func (g *getAttachmentParams) String() string {
	return fmt.Sprintf("Digest:%v, DocID: %v ", g.digest(), base.UD(g.docID()))
}

type IncludeConflictRevEntry struct {
	Status ProposedRevStatus `json:"status"`
	Rev    string            `json:"rev"`
}
