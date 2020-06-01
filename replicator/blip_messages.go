package replicator

import (
	"fmt"

	"github.com/couchbase/go-blip"
)

type BLIPMessageSender interface {
	Send(s *blip.Sender) error
}

// SubChangesRequest is a strongly typed 'subChanges' request.
type SubChangesRequest struct {
	Continuous bool   // Continuous can be set to true if the requester wants change notifications to be sent indefinitely (optional)
	Batch      uint16 // Batch controls the maximum number of changes to send in a single change message (optional)
	Since      uint64 // Since represents the latest sequence ID already known to the requester (optional) // TODO: Need a more complex sequence number type
	Filter     string // Filter is the name of a filter function known to the recipient (optional)
}

var _ BLIPMessageSender = &SubChangesRequest{}

func (scr *SubChangesRequest) Send(s *blip.Sender) error {
	if ok := s.Send(scr.marshalBLIPRequest()); !ok {
		return fmt.Errorf("closed blip sender")
	}

	return nil
}

func (scr *SubChangesRequest) marshalBLIPRequest() *blip.Message {
	msg := blip.NewRequest()
	msg.SetProfile("subChanges")

	setOptionalProperty(msg.Properties, "continuous", scr.Continuous)
	setOptionalProperty(msg.Properties, "since", scr.Since)
	setOptionalProperty(msg.Properties, "filter", scr.Filter)
	setOptionalProperty(msg.Properties, "batch", scr.Batch)

	return msg
}
