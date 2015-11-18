package rest

import (
	// "crypto/sha1"
	"encoding/json"
	// "fmt"
	// "io"
	// "net/http"

	// "database/sql"
	// _ "github.com/couchbaselabs/go_n1ql"

	"github.com/couchbase/sync_gateway/base"
	// "github.com/couchbase/sync_gateway/db"
)

// HTTP handler for GET _query/$query
func (h *handler) handleN1QLQuery() error {
	queryName := h.PathVar("query")

	var args []interface{}
	if rawVal := h.getQuery("args"); "" != rawVal {
		if err := json.Unmarshal([]byte(rawVal), &args); err != nil {
			return err
		}
	}

	var params map[string]interface{}
	if rawVal := h.getQuery("params"); "" != rawVal {
		if err := json.Unmarshal([]byte(rawVal), &params); err != nil {
			return err
		}
	}

	base.LogTo("HTTP", "N1QL query %q - params %v args %v", queryName, params, args)
	result, err := h.db.N1QLQuery(queryName, params, args)
	if err != nil {
		return err
	}
	h.setHeader("Content-Type", `application/json; charset="UTF-8"`)
	h.writeJSON(result)
	return nil
}
