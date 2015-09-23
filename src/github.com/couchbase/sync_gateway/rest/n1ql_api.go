package rest

import (
	// "crypto/sha1"
	// "encoding/json"
	// "fmt"
	// "io"
	// "net/http"

	// "github.com/couchbaselabs/go_n1ql"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db"
)


// HTTP handler for GET _design/$ddoc/_n1ql/$query
func (h *handler) handleN1QLQuery() error {
	ddocName := h.PathVar("ddoc")
	if ddocName == "" {
		ddocName = db.DesignDocSyncGateway
	}
	queryName := h.PathVar("query")
	opts := db.Body{}

	// // Boolean options:
	// for _, name := range []string{"inclusive_end", "descending", "include_docs", "reduce", "group"} {
	// 	if val := h.getQuery(name); "" != val {
	// 		opts[name] = (val == "true")
	// 	}
	// }

	// // Integer options:
	// for _, name := range []string{"skip", "limit", "group_level"} {
	// 	if h.getQuery(name) != "" {
	// 		opts[name] = h.getIntQuery(name, 0)
	// 	}
	// }

	// // String options:
	// for _, name := range []string{"startkey_docid", "endkey_docid", "stale"} {
	// 	if val := h.getQuery(name); "" != val {
	// 		opts[name] = val
	// 	}
	// }

	// // JSON options:
	// for _, name := range []string{"startkey", "endkey", "key", "keys"} {
	// 	if rawVal := h.getQuery(name); "" != rawVal {
	// 		var val interface{}
	// 		if err := json.Unmarshal([]byte(rawVal), &val); err != nil {
	// 			return err
	// 		}
	// 		opts[name] = val
	// 	}
	// }

	base.LogTo("HTTP", "N1QL query %q/%q - opts %v", ddocName, queryName, opts)

	result, err := h.db.QueryDesignDoc(ddocName, queryName, opts)
	if err != nil {
		return err
	}
	h.setHeader("Content-Type", `application/json; charset="UTF-8"`)
	h.writeJSON(result)
	return nil
}
