package db

import (
	"github.com/couchbase/sync_gateway/base"
)

// A goroutine that watches the mutationListener for documents that don't have
// sync metadata, and calls assimilate() on them.
func (dbc *DatabaseContext) watchDocChanges() {
	if dbc.mutationListener.DocChannel == nil {
		return
	}
	base.Infof(base.KeyShadow, "Watching doc changes...")
	for event := range dbc.mutationListener.DocChannel {
		base.Infof(base.KeyShadow, "Got shadow event:%s", base.UD(event.Key))
		doc, err := unmarshalDocument(string(event.Key), event.Value)
		if err == nil {
			if doc.HasValidSyncData(dbc.writeSequences()) {
				if dbc.Shadower != nil {
					dbc.Shadower.PushRevision(doc)
				}
			}
		}
	}
}
