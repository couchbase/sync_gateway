//  Copyright 2012-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package rest

import (
	"math"

	"github.com/couchbase/sync_gateway/db"
	"github.com/couchbase/sync_gateway/document"
)

// Retrieves rev with request history specified as collection of revids (historyFrom)
func get1xRevBodyWithHistory(h *handler, docid, revid string, maxHistory int, historyFrom []string, attachmentsSince []string, showExp bool) (map[string]any, error) {
	rev, err := h.collection.GetRev(h.ctx(), docid, revid, maxHistory > 0, attachmentsSince)
	if err != nil {
		return nil, err
	}
	rev.TrimHistory(maxHistory, historyFrom)
	if !showExp {
		rev.Expiry = nil
	}
	bytes, err := rev.BodyBytesWith(document.BodyId, document.BodyRev, document.BodyAttachments, document.BodyDeleted, document.BodyRemoved, document.BodyExpiry, document.BodyRevisions)
	var body db.Body
	if err == nil {
		err = body.Unmarshal(bytes)
	}
	return body, err
}

func get1xRevBody(h *handler, docid, revid string, history bool, attachmentsSince []string) (map[string]any, error) {
	maxHistory := 0
	if history {
		maxHistory = math.MaxInt32
	}
	return get1xRevBodyWithHistory(h, docid, revid, maxHistory, nil, attachmentsSince, false)
}

// Parses a CouchDB _rev or _revisions property into a list of revision IDs
func parseRevisions(body db.Body) []string {
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
