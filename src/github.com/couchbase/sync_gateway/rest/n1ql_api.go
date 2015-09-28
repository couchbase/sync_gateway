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
	
	var params []interface{}
	
	if rawVal := h.getQuery("args"); "" != rawVal {
		if err := json.Unmarshal([]byte(rawVal), &params); err != nil {
			return err
		}
	}

	base.LogTo("HTTP", "N1QL query %q - params %v", queryName, params)
	result, err := h.db.N1QLQuery(queryName, params)
	if err != nil {
		return err
	}
	h.setHeader("Content-Type", `application/json; charset="UTF-8"`)
	h.writeJSON(result)
	return nil
}
