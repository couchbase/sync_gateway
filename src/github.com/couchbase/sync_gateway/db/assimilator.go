package db

import (
	"github.com/couchbaselabs/go-couchbase"

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
		doc, err := unmarshalDocument(string(event.Key), event.Value)
		if err == nil {
			if doc.hasValidSyncData() {
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
	_, err := db.updateDoc(docid, true, func(doc *document) (Body, error) {
		if doc.hasValidSyncData() {
			return nil, couchbase.UpdateCancel // someone beat me to it
		}
		if err := db.initializeSyncData(doc); err != nil {
			return nil, err
		}
		return doc.body, nil
	})
	if err != nil && err != couchbase.UpdateCancel {
		base.Warn("Failed to import new doc %q: %v", docid, err)
	}
}
