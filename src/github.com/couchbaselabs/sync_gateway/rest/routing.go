//  Copyright (c) 2012 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package rest

import (
	"net/http"
	"fmt"
	"encoding/json"

	"github.com/gorilla/mux"

	"github.com/couchbaselabs/sync_gateway/base"
)

// Creates a GorillaMux router containing the HTTP handlers for a server.
func createHandler(sc *serverContext) http.Handler {
	r := mux.NewRouter()
	r.StrictSlash(true)
	// Global operations:
	r.Handle("/", makeHandler(sc, (*handler).handleRoot)).Methods("GET", "HEAD")
	r.Handle("/_all_dbs", makeHandler(sc, (*handler).handleAllDbs)).Methods("GET", "HEAD")

	if len(sc.databases) == 1 {
		// If there is exactly one database we can handle the standard /_session by just redirecting
		// it to that database's _session handler.
		for _, db := range sc.databases {
			path := "/" + db.dbcontext.Name
			r.Handle("/_session", http.RedirectHandler(path+"/_session", http.StatusTemporaryRedirect))
			r.Handle("/_persona", http.RedirectHandler(path+"/_persona", http.StatusTemporaryRedirect))
		}
	} else {
		r.Handle("/_session", http.NotFoundHandler())
		r.Handle("/_persona", http.NotFoundHandler())
	}

	// Operations on databases:
	r.Handle("/{newdb}/", makeHandler(sc, (*handler).handleCreateDB)).Methods("PUT")
	r.Handle("/{db}/", makeHandler(sc, (*handler).handleGetDB)).Methods("GET", "HEAD")
	r.Handle("/{db}/", makeHandler(sc, (*handler).handleDeleteDB)).Methods("DELETE")
	r.Handle("/{db}/", makeHandler(sc, (*handler).handlePostDoc)).Methods("POST")

	// Special database URLs:
	dbr := r.PathPrefix("/{db}/").Subrouter()
	dbr.Handle("/_all_docs", makeHandler(sc, (*handler).handleAllDocs)).Methods("GET", "HEAD", "POST")
	dbr.Handle("/_bulk_docs", makeHandler(sc, (*handler).handleBulkDocs)).Methods("POST")
	dbr.Handle("/_bulk_get", makeHandler(sc, (*handler).handleBulkGet)).Methods("GET", "HEAD")
	dbr.Handle("/_changes", makeHandler(sc, (*handler).handleChanges)).Methods("GET", "HEAD")
	dbr.Handle("/_design/sync_gateway", makeHandler(sc, (*handler).handleDesign)).Methods("GET", "HEAD")
	dbr.Handle("/_ensure_full_commit", makeHandler(sc, (*handler).handleEFC)).Methods("POST")
	dbr.Handle("/_revs_diff", makeHandler(sc, (*handler).handleRevsDiff)).Methods("POST")

	// Session/login URLs are per-database (unlike in CouchDB)
	dbr.Handle("/_session", makeAdminHandler(sc, (*handler).handleSessionGET)).Methods("GET", "HEAD")
	dbr.Handle("/_session", makeAdminHandler(sc, (*handler).handleSessionPOST)).Methods("POST")
	if sc.config.Persona != nil {
		dbr.Handle("/_persona", makeAdminHandler(sc, (*handler).handlePersonaPOST)).Methods("POST")
	}

	// Document URLs:
	dbr.Handle("/_local/{docid}", makeHandler(sc, (*handler).handleGetLocalDoc)).Methods("GET", "HEAD")
	dbr.Handle("/_local/{docid}", makeHandler(sc, (*handler).handlePutLocalDoc)).Methods("PUT")
	dbr.Handle("/_local/{docid}", makeHandler(sc, (*handler).handleDelLocalDoc)).Methods("DELETE")

	dbr.Handle("/{docid}", makeHandler(sc, (*handler).handleGetDoc)).Methods("GET", "HEAD")
	dbr.Handle("/{docid}", makeHandler(sc, (*handler).handlePutDoc)).Methods("PUT")
	dbr.Handle("/{docid}", makeHandler(sc, (*handler).handleDeleteDoc)).Methods("DELETE")

	dbr.Handle("/{docid}/{attach}", makeHandler(sc, (*handler).handleGetAttachment)).Methods("GET", "HEAD")

	// Fallbacks that have to be added last:
	r.PathPrefix("/").Methods("OPTIONS").Handler(makeHandler(sc, (*handler).handleOptions))
	r.PathPrefix("/").Handler(makeHandler(sc, (*handler).handleBadRoute))

	return r
}

//////// ADMIN API:

// Starts a simple REST listener that will get and set user credentials.
func createAdminHandler(sc *serverContext) http.Handler {
	r := mux.NewRouter()
	r.StrictSlash(true)

	r.HandleFunc("/{db}/_session",
		handleAdminReq(sc, createUserSession)).Methods("POST")

	r.HandleFunc("/{db}/_user",
		handleAdminReq(sc, getUsers)).Methods("GET", "HEAD")
	r.HandleFunc("/{db}/_user/{name}",
		handleAdminReq(sc, getUserInfo)).Methods("GET", "HEAD")
	r.HandleFunc("/{db}/_user/{name}",
		handleAdminReq(sc, putUser)).Methods("PUT")
	r.HandleFunc("/{db}/_user/{name}",
		handleAdminReq(sc, deleteUser)).Methods("DELETE")
	r.HandleFunc("/{db}/_user/",
		handleAdminReq(sc, putUser)).Methods("POST")

	r.HandleFunc("/{db}/_role",
		handleAdminReq(sc, getRoles)).Methods("GET", "HEAD")
	r.HandleFunc("/{db}/_role/{name}",
		handleAdminReq(sc, getRoleInfo)).Methods("GET", "HEAD")
	r.HandleFunc("/{db}/_role/{name}",
		handleAdminReq(sc, putRole)).Methods("PUT")
	r.HandleFunc("/{db}/_role/{name}",
		handleAdminReq(sc, deleteRole)).Methods("DELETE")
	r.HandleFunc("/{db}/_role/",
		handleAdminReq(sc, putRole)).Methods("POST")

	// The routes below are part of the CouchDB REST API but should only be available to admins,
	// so the handlers are moved to the admin port.
	r.Handle("/{db}/", makeAdminHandler(sc, (*handler).handleDeleteDB)).Methods("DELETE")
	dbr := r.PathPrefix("/{db}/").Subrouter()
	dbr.Handle("/_compact",
		makeAdminHandler(sc, (*handler).handleCompact)).Methods("POST")
	dbr.Handle("/_vacuum",
		makeAdminHandler(sc, (*handler).handleVacuum)).Methods("POST")
	dbr.Handle("/_dump/{view}",
		makeAdminHandler(sc, (*handler).handleDump)).Methods("GET")

	// These routes are available on both the regular and admin ports; the admin port is useful
	// because it gives 'superuser' access.
	dbr.Handle("/_all_docs",
		makeAdminHandler(sc, (*handler).handleAllDocs)).Methods("GET", "HEAD", "POST")
	dbr.Handle("/_changes",
		makeAdminHandler(sc, (*handler).handleChanges)).Methods("GET", "HEAD")

	r.HandleFunc("/_profile", handleProfiling).Methods("POST")

	return r
}

func renderError(err error, r http.ResponseWriter) {
	status, message := base.ErrorAsHTTPStatus(err)
	r.Header().Set("Content-Type", "application/json")
	r.WriteHeader(status)
	jsonOut, _ := json.Marshal(map[string]interface{}{"error": status, "reason": message})
	r.Write(jsonOut)
}

type adminHandler func(http.ResponseWriter, *http.Request, *context) error

func handleAdminReq(sc *serverContext, fun adminHandler) func(http.ResponseWriter, *http.Request) {
	return func(r http.ResponseWriter, rq *http.Request) {
		base.LogTo("HTTP", "(admin) %s %s", rq.Method, rq.URL)
		dbContext := sc.databases[mux.Vars(rq)["db"]]
		if dbContext == nil {
			r.WriteHeader(http.StatusNotFound)
			r.Write([]byte(fmt.Sprintf(`{"error":"No such database"}`)))
			return
		}
		err := fun(r, rq, dbContext)
		if err != nil {
			renderError(err, r)
		}
	}
}
