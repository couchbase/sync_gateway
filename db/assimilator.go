package db

import (
	"github.com/couchbase/sync_gateway/base"
)

// A goroutine that watches the mutationListener for documents that don't have
// sync metadata, and calls assimilate() on them.
func (c *DatabaseContext) watchDocChanges() {
	if c.mutationListener.DocChannel == nil {
		return
	}
	c.Infof(base.KeyShadow, "Watching doc changes...")
	for event := range c.mutationListener.DocChannel {
		c.Infof(base.KeyShadow, "Got shadow event:%s", base.UD(event.Key))
		doc, err := unmarshalDocument(string(event.Key), event.Value)
		if err == nil {
			if doc.HasValidSyncData(c.writeSequences()) {
				if c.Shadower != nil {
					c.Shadower.PushRevision(doc)
				}
			}
		}
	}
}
