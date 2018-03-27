package db

import (
	"github.com/couchbase/go-couchbase"

	"github.com/couchbase/sync_gateway/base"
)

// A goroutine that watches the mutationListener for documents that don't have
// sync metadata, and calls assimilate() on them.
func (c *DatabaseContext) watchDocChanges() {
	if c.mutationListener.DocChannel == nil {
		return
	}
	base.LogTo("Shadow", "Watching doc changes...")
	for event := range c.mutationListener.DocChannel {
		base.LogToR("Feed", "Got shadow event:%s", base.UD(event.Key))
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
	base.LogToR("CRUD", "Importing new doc %q", docid)
	db := Database{DatabaseContext: c, user: nil}
	_, err := db.updateDoc(docid, true, 0, func(doc *document) (resultBody Body, resultAttachmentData AttachmentData, updatedExpiry *uint32, resultErr error) {
		if doc.HasValidSyncData(c.writeSequences()) {
			return nil, nil, nil, couchbase.UpdateCancel // someone beat me to it
		}
		if err := db.initializeSyncData(doc); err != nil {
			return nil, nil, nil, err
		}
		return doc.Body(), nil, nil, nil
	})
	if err != nil && err != couchbase.UpdateCancel {
		base.WarnR("Failed to import new doc %q: %v", docid, err)
	}
}
