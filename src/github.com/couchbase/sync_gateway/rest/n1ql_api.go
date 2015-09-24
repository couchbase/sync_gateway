package rest

import (
	// "crypto/sha1"
	// "encoding/json"
	// "fmt"
	// "io"
	// "net/http"

	// "database/sql"
   // _ "github.com/couchbaselabs/go_n1ql"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db"
)


// HTTP handler for GET _n1ql/$query
func (h *handler) handleN1QLQuery() error {
	queryName := h.PathVar("query")
	opts := db.Body{}

	base.LogTo("HTTP", "N1QL query %q - opts %v", queryName, opts)

	result, err := h.db.N1QLQuery(queryName, opts)
	if err != nil {
		return err
	}
	h.setHeader("Content-Type", `application/json; charset="UTF-8"`)
	h.writeJSON(result)
	return nil
}
