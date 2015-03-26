package rest

import (
	"crypto/sha1"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db"
)

// HTTP handler for GET _design/$ddoc
func (h *handler) handleGetDesignDoc() error {
	ddocID := h.PathVar("ddoc")
	base.TEMP("GetDesignDoc %q", ddocID)
	var result interface{}
	if ddocID == db.DesignDocSyncGateway {
		// we serve this content here so that CouchDB 1.2 has something to
		// hash into the replication-id, to correspond to our filter.
		filter := "ok"
		if h.db.DatabaseContext.ChannelMapper != nil {
			hash := sha1.New()
			io.WriteString(hash, h.db.DatabaseContext.ChannelMapper.Function())
			filter = fmt.Sprint(hash.Sum(nil))
		}
		result = db.Body{"filters": db.Body{"bychannel": filter}}
	} else {
		if err := h.db.GetDesignDoc(ddocID, &result); err != nil {
			return err
		}
	}
	h.writeJSON(result)
	return nil
}

// HTTP handler for PUT _design/$ddoc
func (h *handler) handlePutDesignDoc() error {
	ddocID := h.PathVar("ddoc")
	var ddoc db.DesignDoc
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
	if ddocName == "" {
		ddocName = db.DesignDocSyncGateway
	}
	viewName := h.PathVar("view")
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
			if err := json.Unmarshal([]byte(rawVal), &val); err != nil {
				return err
			}
			opts[name] = val
		}
	}

	base.LogTo("HTTP", "JSON view %q/%q - opts %v", ddocName, viewName, opts)

	result, err := h.db.QueryDesignDoc(ddocName, viewName, opts)
	if err != nil {
		return err
	}
	h.setHeader("Content-Type", `application/json; charset="UTF-8"`)
	h.writeJSON(result)
	return nil
}
