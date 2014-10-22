package db

import (
	"net/http"
	"strings"

	"github.com/couchbaselabs/sync_gateway/base"
	"github.com/couchbaselabs/walrus"
)

type DesignDoc walrus.DesignDoc

func isInternalDDoc(ddocName string) bool {
	return strings.HasPrefix(ddocName, "sync_")
}

// Enforces access by admins only, and not to the built-in Sync Gateway design docs:
func (db *Database) checkDDocAccess(ddocName string) error {
	if db.user != nil || isInternalDDoc(ddocName) {
		return base.HTTPErrorf(http.StatusForbidden, "forbidden")
	}
	return nil
}

func (db *Database) GetDesignDoc(ddocName string, result interface{}) (err error) {
	if err = db.checkDDocAccess(ddocName); err == nil {
		err = db.Bucket.GetDDoc(ddocName, result)
	}
	return
}

func (db *Database) PutDesignDoc(ddocName string, ddoc DesignDoc) (err error) {
	// Wrap the map functions to ignore special docs and strip _sync metadata:
	for name, view := range ddoc.Views {
		view.Map = `function(doc,meta) {
	                    var sync = doc._sync;
	                    if (sync === undefined || meta.id.substring(0,6) == "_sync:")
	                      return;
	                    if ((sync.flags & 1) || sync.deleted)
	                      return;
	                    delete doc.sync;
	                    meta.rev = sync.rev;
						(` + view.Map + `) (doc, meta); }`
		ddoc.Views[name] = view // view is not a pointer, so have to copy it back
	}

	if err = db.checkDDocAccess(ddocName); err == nil {
		err = db.Bucket.PutDDoc(ddocName, ddoc)
	}
	return
}

func (db *Database) DeleteDesignDoc(ddocName string) (err error) {
	if err = db.checkDDocAccess(ddocName); err == nil {
		err = db.Bucket.DeleteDDoc(ddocName)
	}
	return
}

func (db *Database) QueryDesignDoc(ddocName string, viewName string, options map[string]interface{}) (result walrus.ViewResult, err error) {
	// Query has slightly different access control than checkDDocAccess():
	// * Admins can query any design doc including the internal ones
	// * Regular users can query non-internal design docs
	//NOTE: Restricting query access to admins until we implement proper result-set filtering.
	if db.user != nil /*&& isInternalDDoc(ddocName)*/ {
		err = base.HTTPErrorf(http.StatusForbidden, "forbidden")
		return
	}
	result, err = db.Bucket.View(ddocName, viewName, options)
	if err == nil {
		if includeDocs := options["include_docs"]; includeDocs == true {
			// Stripg "_sync" properties from doc bodies:
			for _, row := range result.Rows {
				if doc := row.Doc; doc != nil {
					delete((*doc).(map[string]interface{}), "_sync")
				}
			}
		}
	}
	return
}
