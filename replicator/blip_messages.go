package replicator

import (
	"fmt"
	"strings"

	"github.com/couchbase/go-blip"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db"
)

type BLIPMessageSender interface {
	Send(s *blip.Sender) error
}

// SubChangesRequest is a strongly typed 'subChanges' request.
type SubChangesRequest struct {
	Continuous     bool          // Continuous can be set to true if the requester wants change notifications to be sent indefinitely (optional)
	Batch          uint16        // Batch controls the maximum number of changes to send in a single change message (optional)
	Since          db.SequenceID // Since represents the latest sequence ID already known to the requester (optional)
	Filter         string        // Filter is the name of a filter function known to the recipient (optional)
	FilterChannels []string      // FilterChannels are a set of channels used with a 'sync_gateway/bychannel' filter (optional)
	DocIDs         []string      // DocIDs specifies which doc IDs the recipient should send changes for (optional)
	ActiveOnly     bool          // ActiveOnly is set to `true` if the requester doesn't want to be sent tombstones. (optional)
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
	setOptionalProperty(msg.Properties, "batch", scr.Batch)
	setOptionalProperty(msg.Properties, "since", scr.Since)
	setOptionalProperty(msg.Properties, "filter", scr.Filter)
	setOptionalProperty(msg.Properties, "channels", strings.Join(scr.FilterChannels, ","))

	if err := msg.SetJSONBody(map[string]interface{}{
		"docIDs": scr.DocIDs,
	}); err != nil {
		base.Errorf("error marshalling docIDs slice into subChanges request: %v", err)
		return nil
	}

	return msg
}
