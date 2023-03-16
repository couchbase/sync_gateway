package rest

import (
	"github.com/couchbase/sync_gateway/db"
	"github.com/couchbase/sync_gateway/document"
)

// Parses a CouchDB _rev or _revisions property into a list of revision IDs
func ParseRevisions(body db.Body) []string {
	// http://wiki.apache.org/couchdb/HTTP_Document_API#GET

	revisionsProperty, ok := body[document.BodyRevisions]
	if !ok {
		revid, ok := body[document.BodyRev].(string)
		if !ok {
			return nil
		}
		if document.GenOfRevID(revid) < 1 {
			return nil
		}
		oneRev := make([]string, 0, 1)
		oneRev = append(oneRev, revid)
		return oneRev
	}

	// Revisions may be stored in a Body as Revisions or map[string]interface{}, depending on the source of the Body
	var revisions document.Revisions
	switch revs := revisionsProperty.(type) {
	case document.Revisions:
		revisions = revs
	case map[string]interface{}:
		revisions = document.Revisions(revs)
	default:
		return nil
	}

	return revisions.ParseRevisions()
}
