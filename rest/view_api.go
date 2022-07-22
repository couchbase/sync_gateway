/*
Copyright 2016-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package rest

import (
	"crypto/sha1"
	"fmt"
	"io"
	"net/http"

	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db"
)

// HTTP handler for GET _design/$ddoc
func (h *handler) handleGetDesignDoc() error {
	ddocID := h.PathVar("ddoc")
	base.DebugfCtx(h.ctx(), base.KeyAll, "GetDesignDoc %v", base.MD(ddocID))
	var result interface{}
	if ddocID == db.DesignDocSyncGateway() {
		// we serve this content here so that CouchDB 1.2 has something to
		// hash into the replication-id, to correspond to our filter.
		filter := "ok"
		if h.db.DatabaseContext.ChannelMapper != nil {
			hash := sha1.New()
			_, _ = io.WriteString(hash, h.db.DatabaseContext.ChannelMapper.Function())
			filter = fmt.Sprint(hash.Sum(nil))
		}
		result = db.Body{"filters": db.Body{"bychannel": filter}}
	} else {
		var getErr error
		result, getErr = h.db.GetDesignDoc(ddocID)
		if getErr != nil {
			return getErr
		}
	}
	h.writeJSON(result)
	return nil
}

// HTTP handler for PUT _design/$ddoc
func (h *handler) handlePutDesignDoc() error {
	ddocID := h.PathVar("ddoc")
	var ddoc sgbucket.DesignDoc
	err := h.readJSONInto(&ddoc)
	if err != nil {
		return err
	}
	if err = h.db.PutDesignDoc(ddocID, ddoc); err != nil {
		return err
	}
	h.writeStatus(http.StatusCreated, "OK")
	return nil
}

// HTTP handler for DELETE _design/$ddoc
func (h *handler) handleDeleteDesignDoc() error {
	ddocID := h.PathVar("ddoc")
	return h.db.DeleteDesignDoc(ddocID)
}

// HTTP handler for GET _design/$ddoc/_view/$view
func (h *handler) handleView() error {
	// Couchbase Server view API:
	// http://docs.couchbase.com/admin/admin/REST/rest-views-get.html
	ddocName := h.PathVar("ddoc")
	viewName := h.PathVar("view")
	if ddocName == "" {
		ddocName = db.DesignDocSyncGateway()
	}
	opts := db.Body{}

	// Boolean options:
	for _, name := range []string{"inclusive_end", "descending", "include_docs", "reduce", "group"} {
		if val := h.getQuery(name); "" != val {
			opts[name] = (val == "true")
		}
	}

	// Integer options:
	for _, name := range []string{"skip", "limit", "group_level"} {
		if h.getQuery(name) != "" {
			opts[name] = h.getIntQuery(name, 0)
		}
	}

	// String options:
	for _, name := range []string{"startkey_docid", "endkey_docid", "stale"} {
		if val := h.getQuery(name); "" != val {
			opts[name] = val
		}
	}

	// JSON options:
	for _, name := range []string{"startkey", "endkey", "key", "keys"} {
		if rawVal := h.getQuery(name); "" != rawVal {
			var val interface{}
			if err := base.JSONUnmarshal([]byte(rawVal), &val); err != nil {
				return err
			}
			opts[name] = val
		}
	}

	base.InfofCtx(h.ctx(), base.KeyHTTP, "JSON view %q/%q - opts %v", base.MD(ddocName), base.MD(viewName), base.MD(opts))

	result, err := h.db.QueryDesignDoc(ddocName, viewName, opts)
	if err != nil {
		return err
	}
	h.setHeader("Content-Type", `application/json; charset="UTF-8"`)
	h.writeJSON(result)
	return nil
}
