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
	"context"
	"fmt"
	"net/http"
	"strings"

	"github.com/couchbase/go-blip"
	"github.com/couchbase/sync_gateway/base"
)

// blipMessageSender validates specific request types
type blipMessageSender interface {
	Send(s *blip.Sender) (err error)
}

// SubChangesRequest is a strongly typed 'subChanges' request.
type SubChangesRequest struct {
	Continuous     bool       // Continuous can be set to true if the requester wants change notifications to be sent indefinitely (optional)
	Batch          uint16     // Batch controls the maximum number of changes to send in a single change message (optional)
	Since          string     // Since represents the latest sequence ID already known to the requester (optional)
	Filter         string     // Filter is the name of a filter function known to the recipient (optional)
	FilterChannels []string   // FilterChannels are a set of channels used with a 'sync_gateway/bychannel' filter (optional)
	DocIDs         []string   // DocIDs specifies which doc IDs the recipient should send changes for (optional)
	ActiveOnly     bool       // ActiveOnly is set to `true` if the requester doesn't want to be sent tombstones. (optional)
	Revocations    bool       // Revocations is set to `true` if the requester wants to be send revocation messages (optional)
	clientType     clientType // Can be set to SGR2 to apply ISGR-specific behaviour
	CollectionIdx  *int       // If set, specifies the collection index of the replicator for this message
}

var _ blipMessageSender = &SubChangesRequest{}

func (rq *SubChangesRequest) Send(s *blip.Sender) error {
	r, err := rq.marshalBLIPRequest()
	if err != nil {
		return err
	}
	if ok := s.Send(r); !ok {
		return fmt.Errorf("closed blip sender")
	}

	return nil
}

func (rq *SubChangesRequest) marshalBLIPRequest() (*blip.Message, error) {
	msg := blip.NewRequest()
	msg.SetProfile(MessageSubChanges)

	setOptionalProperty(msg.Properties, "client_sgr2", rq.clientType == clientTypeSGR2)
	setOptionalProperty(msg.Properties, SubChangesContinuous, rq.Continuous)
	setOptionalProperty(msg.Properties, SubChangesBatch, rq.Batch)
	setOptionalProperty(msg.Properties, SubChangesSince, rq.Since)
	setOptionalProperty(msg.Properties, SubChangesFilter, rq.Filter)
	setOptionalProperty(msg.Properties, SubChangesChannels, strings.Join(rq.FilterChannels, ","))
	setOptionalProperty(msg.Properties, SubChangesRevocations, rq.Revocations)
	setOptionalProperty(msg.Properties, BlipCollection, rq.CollectionIdx)

	if len(rq.DocIDs) > 0 {
		if err := msg.SetJSONBody(map[string]interface{}{
			"docIDs": rq.DocIDs,
		}); err != nil {
			base.ErrorfCtx(context.Background(), "error marshalling docIDs slice into subChanges request: %v", err)
			return nil, err
		}
	}

	return msg, nil
}

// SetSGR2CheckpointRequest is a strongly typed 'setCheckpoint' request for SG-Replicate 2.
type SetSGR2CheckpointRequest struct {
	Client        string  // Client is the unique ID of client checkpoint to retrieve
	RevID         *string // RevID of the previous checkpoint, if known.
	Checkpoint    Body    // Checkpoint is the actual checkpoint body we're sending.
	CollectionIdx *int

	msg *blip.Message
}

var _ blipMessageSender = &SetSGR2CheckpointRequest{}

func (rq *SetSGR2CheckpointRequest) Send(s *blip.Sender) error {
	msg, err := rq.marshalBLIPRequest()
	if err != nil {
		return err
	}

	if ok := s.Send(msg); !ok {
		return fmt.Errorf("closed blip sender")
	}

	rq.msg = msg

	return nil
}

func (rq *SetSGR2CheckpointRequest) marshalBLIPRequest() (*blip.Message, error) {
	msg := blip.NewRequest()
	msg.SetProfile(MessageSetCheckpoint)

	setProperty(msg.Properties, SetCheckpointClient, rq.Client)
	setOptionalProperty(msg.Properties, SetCheckpointRev, rq.RevID)

	setOptionalProperty(msg.Properties, BlipCollection, rq.CollectionIdx)

	if err := msg.SetJSONBody(rq.Checkpoint); err != nil {
		return nil, err
	}

	return msg, nil
}

type SetSGR2CheckpointResponse struct {
	RevID string // The RevID of the sent checkpoint.
}

func (rq *SetSGR2CheckpointRequest) Response() (*SetSGR2CheckpointResponse, error) {
	if rq.msg == nil {
		return nil, fmt.Errorf("SetCheckpointRequest has not been sent")
	}

	respMsg := rq.msg.Response()

	respBody, err := respMsg.Body()
	if err != nil {
		return nil, err
	}

	if msgType := respMsg.Type(); msgType != blip.ResponseType {
		if msgType == blip.ErrorType && errorDomainIsHTTP(respMsg) &&
			respMsg.Properties["Error-Code"] == "409" {
			// conflict writing checkpoint
			return nil, base.HTTPErrorf(http.StatusConflict, "Document update conflict")
		}
		return nil, fmt.Errorf("unexpected error response: %s", respBody)
	}

	return &SetSGR2CheckpointResponse{
		RevID: respMsg.Properties[SetCheckpointResponseRev],
	}, nil
}

// GetSGR2CheckpointRequest is a strongly typed 'getCheckpoint' request for SG-Replicate 2.
type GetSGR2CheckpointRequest struct {
	Client        string // Client is the unique ID of client checkpoint to retrieve
	CollectionIdx *int   // If set, specifies the collection index of the replicator

	msg *blip.Message
}

var _ blipMessageSender = &GetSGR2CheckpointRequest{}

func (rq *GetSGR2CheckpointRequest) Send(s *blip.Sender) error {
	msg := rq.marshalBLIPRequest()

	if ok := s.Send(msg); !ok {
		return fmt.Errorf("closed blip sender")
	}

	rq.msg = msg

	return nil
}

func (rq *GetSGR2CheckpointRequest) marshalBLIPRequest() *blip.Message {
	msg := blip.NewRequest()
	msg.SetProfile(MessageGetCheckpoint)

	setProperty(msg.Properties, GetCheckpointClient, rq.Client)
	setOptionalProperty(msg.Properties, BlipCollection, rq.CollectionIdx)

	return msg
}

type SGR2Checkpoint struct {
	RevID     string // The RevID of the checkpoint.
	BodyBytes []byte // The checkpoint body
}

func (rq *GetSGR2CheckpointRequest) Response() (*SGR2Checkpoint, error) {
	if rq.msg == nil {
		return nil, fmt.Errorf("GetCheckpointRequest has not been sent")
	}

	respMsg := rq.msg.Response()

	respBody, err := respMsg.Body()
	if err != nil {
		return nil, err
	}

	if msgType := respMsg.Type(); msgType != blip.ResponseType {
		if msgType == blip.ErrorType &&
			errorDomainIsHTTP(respMsg) &&
			respMsg.Properties["Error-Code"] == "404" {
			// no checkpoint found
			return nil, nil
		}
		return nil, fmt.Errorf("unexpected error response: %s", respBody)
	}

	bodyBytes, err := respMsg.Body()
	if err != nil {
		return nil, err
	}

	return &SGR2Checkpoint{
		RevID:     respMsg.Properties[GetCheckpointResponseRev],
		BodyBytes: bodyBytes,
	}, nil
}

func errorDomainIsHTTP(response *blip.Message) bool {
	return response.Properties[BlipErrorDomain] == "HTTP"
}
