package db

import (
	"github.com/couchbase/go-couchbase"

	"github.com/couchbase/sync_gateway/base"
)

// A goroutine that watches the tapListener for documents that don't have
// sync metadata, and calls assimilate() on them.
func (c *DatabaseContext) watchDocChanges() {
	if c.tapListener.DocChannel == nil {
		return
	}
	base.LogTo("Shadow", "Watching doc changes...")
	for event := range c.tapListener.DocChannel {
		base.LogTo("Feed", "Got shadow event:%s", event.Key)
		doc, err := unmarshalDocument(string(event.Key), event.Value)
		if err == nil {
			if doc.HasValidSyncData(c.writeSequences()) {
				if c.Shadower != nil {
					c.Shadower.PushRevision(doc)
				}
			} else {
				if c.autoImport {
					go c.assimilate(doc.ID)
				}
			}
		}
	}
}

// Adds sync metadata to a Couchbase document
func (c *DatabaseContext) assimilate(docid string) {
	base.LogTo("CRUD", "Importing new doc %q", docid)
	db := Database{DatabaseContext: c, user: nil}
	_, err := db.updateDoc(docid, true, 0, func(doc *document) (Body, AttachmentData, error) {
		if doc.HasValidSyncData(c.writeSequences()) {
			return nil, nil, couchbase.UpdateCancel // someone beat me to it
		}
		if err := db.initializeSyncData(doc); err != nil {
			return nil, nil, err
		}
		return doc.body, nil, nil
	})
	if err != nil && err != couchbase.UpdateCancel {
		base.Warn("Failed to import new doc %q: %v", docid, err)
	}
}
