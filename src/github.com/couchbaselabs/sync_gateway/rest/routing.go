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
	"strings"

	"github.com/couchbaselabs/sync_gateway_admin_ui"
	"github.com/gorilla/mux"
)

// Regexes that match database or doc ID component of a path.
// These are needed to avoid conflict with handlers that match special underscore-prefixed paths
// like "/_profile" and "/db/_all_docs".
const dbRegex = "[^_/][^/]*"
const docRegex = "[^_/][^/]*"

// Creates a GorillaMux router containing the basic HTTP handlers for a server.
// This is the common functionality of the public and admin ports.
// The 'privs' parameter specifies the authentication the handler will use.
func createHandler(sc *ServerContext, privs handlerPrivs) (*mux.Router, *mux.Router) {
	r := mux.NewRouter()
	r.StrictSlash(true)
	// Global operations:
	r.Handle("/", makeHandler(sc, privs, (*handler).handleRoot)).Methods("GET", "HEAD")

	// Operations on databases:
	r.Handle("/{db:"+dbRegex+"}/", makeHandler(sc, privs, (*handler).handleGetDB)).Methods("GET", "HEAD")
	r.Handle("/{db:"+dbRegex+"}/", makeHandler(sc, privs, (*handler).handlePostDoc)).Methods("POST")

	// Special database URLs:
	dbr := r.PathPrefix("/{db:" + dbRegex + "}/").Subrouter()
	dbr.StrictSlash(true)
	dbr.Handle("/_all_docs", makeHandler(sc, privs, (*handler).handleAllDocs)).Methods("GET", "HEAD", "POST")
	dbr.Handle("/_bulk_docs", makeHandler(sc, privs, (*handler).handleBulkDocs)).Methods("POST")
	dbr.Handle("/_bulk_get", makeHandler(sc, privs, (*handler).handleBulkGet)).Methods("POST")
	dbr.Handle("/_changes", makeHandler(sc, privs, (*handler).handleChanges)).Methods("GET", "HEAD")
	dbr.Handle("/_design/sync_gateway", makeHandler(sc, privs, (*handler).handleDesign)).Methods("GET", "HEAD")
	dbr.Handle("/_ensure_full_commit", makeHandler(sc, privs, (*handler).handleEFC)).Methods("POST")
	dbr.Handle("/_revs_diff", makeHandler(sc, privs, (*handler).handleRevsDiff)).Methods("POST")

	// Document URLs:
	dbr.Handle("/_local/{docid}", makeHandler(sc, privs, (*handler).handleGetLocalDoc)).Methods("GET", "HEAD")
	dbr.Handle("/_local/{docid}", makeHandler(sc, privs, (*handler).handlePutLocalDoc)).Methods("PUT")
	dbr.Handle("/_local/{docid}", makeHandler(sc, privs, (*handler).handleDelLocalDoc)).Methods("DELETE")

	dbr.Handle("/{docid:"+docRegex+"}", makeHandler(sc, privs, (*handler).handleGetDoc)).Methods("GET", "HEAD")
	dbr.Handle("/{docid:"+docRegex+"}", makeHandler(sc, privs, (*handler).handlePutDoc)).Methods("PUT")
	dbr.Handle("/{docid:"+docRegex+"}", makeHandler(sc, privs, (*handler).handleDeleteDoc)).Methods("DELETE")

	dbr.Handle("/{docid:"+docRegex+"}/{attach}", makeHandler(sc, privs, (*handler).handleGetAttachment)).Methods("GET", "HEAD")
	dbr.Handle("/{docid:"+docRegex+"}/{attach}", makeHandler(sc, privs, (*handler).handlePutAttachment)).Methods("PUT")

	// Session/login URLs are per-database (unlike in CouchDB)
	// These have public privileges so that they can be called without being logged in already
	dbr.Handle("/_session", makeHandler(sc, publicPrivs, (*handler).handleSessionGET)).Methods("GET", "HEAD")
	if sc.config.Persona != nil {
		dbr.Handle("/_persona", makeHandler(sc, publicPrivs,
			(*handler).handlePersonaPOST)).Methods("POST")
	}
	if sc.config.Facebook != nil {
		dbr.Handle("/_facebook", makeHandler(sc, publicPrivs,
			(*handler).handleFacebookPOST)).Methods("POST")
	}

	return r, dbr
}

// Creates the HTTP handler for the public API of a gateway server.
func CreatePublicHandler(sc *ServerContext) http.Handler {
	r, dbr := createHandler(sc, regularPrivs)
	dbr.Handle("/_session", makeHandler(sc, publicPrivs,
		(*handler).handleSessionPOST)).Methods("POST")
	return wrapRouter(sc, regularPrivs, r)
}

//////// ADMIN API:

// Creates the HTTP handler for the PRIVATE admin API of a gateway server.
func CreateAdminHandler(sc *ServerContext) http.Handler {
	r, dbr := createHandler(sc, adminPrivs)

	r.PathPrefix("/_admin/").HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if sc.config.AdminUI != nil {
			http.ServeFile(w, r, *sc.config.AdminUI)
		} else {
			w.Write(sync_gateway_admin_ui.Admin_bundle_html())
		}
	})

	dbr.Handle("/_session",
		makeHandler(sc, adminPrivs, (*handler).createUserSession)).Methods("POST")

	dbr.Handle("/_raw/{docid:"+docRegex+"}",
		makeHandler(sc, adminPrivs, (*handler).handleGetRawDoc)).Methods("GET", "HEAD")

	dbr.Handle("/_user/",
		makeHandler(sc, adminPrivs, (*handler).getUsers)).Methods("GET", "HEAD")
	dbr.Handle("/_user/",
		makeHandler(sc, adminPrivs, (*handler).putUser)).Methods("POST")
	dbr.Handle("/_user/{name}",
		makeHandler(sc, adminPrivs, (*handler).getUserInfo)).Methods("GET", "HEAD")
	dbr.Handle("/_user/{name}",
		makeHandler(sc, adminPrivs, (*handler).putUser)).Methods("PUT")
	dbr.Handle("/_user/{name}",
		makeHandler(sc, adminPrivs, (*handler).deleteUser)).Methods("DELETE")

	dbr.Handle("/_role/",
		makeHandler(sc, adminPrivs, (*handler).getRoles)).Methods("GET", "HEAD")
	dbr.Handle("/_role/",
		makeHandler(sc, adminPrivs, (*handler).putRole)).Methods("POST")
	dbr.Handle("/_role/{name}",
		makeHandler(sc, adminPrivs, (*handler).getRoleInfo)).Methods("GET", "HEAD")
	dbr.Handle("/_role/{name}",
		makeHandler(sc, adminPrivs, (*handler).putRole)).Methods("PUT")
	dbr.Handle("/_role/{name}",
		makeHandler(sc, adminPrivs, (*handler).deleteRole)).Methods("DELETE")

	r.Handle("/_profile/{name}",
		makeHandler(sc, adminPrivs, (*handler).handleProfiling)).Methods("POST")
	r.Handle("/_profile",
		makeHandler(sc, adminPrivs, (*handler).handleProfiling)).Methods("POST")
	r.Handle("/_heap",
		makeHandler(sc, adminPrivs, (*handler).handleHeapProfiling)).Methods("POST")
	r.Handle("/_stats",
		makeHandler(sc, adminPrivs, (*handler).handleStats)).Methods("GET")
	r.Handle(kDebugURLPathPrefix,
		makeHandler(sc, adminPrivs, (*handler).handleExpvar)).Methods("GET")

	dbr.Handle("/_config",
		makeHandler(sc, adminPrivs, (*handler).handleGetDbConfig)).Methods("GET")
	dbr.Handle("/_vacuum",
		makeHandler(sc, adminPrivs, (*handler).handleVacuum)).Methods("POST")
	dbr.Handle("/_dump/{view}",
		makeHandler(sc, adminPrivs, (*handler).handleDump)).Methods("GET")
	dbr.Handle("/_view/{view}",
		makeHandler(sc, adminPrivs, (*handler).handleView)).Methods("GET")
	dbr.Handle("/_dumpchannel/{channel}",
		makeHandler(sc, adminPrivs, (*handler).handleDumpChannel)).Methods("GET")

	// The routes below are part of the CouchDB REST API but should only be available to admins,
	// so the handlers are moved to the admin port.
	r.Handle("/{newdb:"+dbRegex+"}/",
		makeHandler(sc, adminPrivs, (*handler).handleCreateDB)).Methods("PUT")
	r.Handle("/{db:"+dbRegex+"}/",
		makeHandler(sc, adminPrivs, (*handler).handleDeleteDB)).Methods("DELETE")

	r.Handle("/_all_dbs",
		makeHandler(sc, adminPrivs, (*handler).handleAllDbs)).Methods("GET", "HEAD")
	dbr.Handle("/_compact",
		makeHandler(sc, adminPrivs, (*handler).handleCompact)).Methods("POST")

	return wrapRouter(sc, adminPrivs, r)
}

// Returns a top-level HTTP handler for a Router. This adds behavior for URLs that don't
// match anything -- it handles the OPTIONS method as well as returning either a 404 or 405
// for URLs that don't match a route.
func wrapRouter(sc *ServerContext, privs handlerPrivs, router *mux.Router) http.Handler {
	return http.HandlerFunc(func(response http.ResponseWriter, rq *http.Request) {
		fixQuotedSlashes(rq)
		var match mux.RouteMatch
		if router.Match(rq, &match) {
			router.ServeHTTP(response, rq)
		} else {
			// Log the request
			h := newHandler(sc, privs, response, rq)
			h.logRequestLine()

			// What methods would have matched?
			var options []string
			for _, method := range []string{"GET", "HEAD", "POST", "PUT", "DELETE"} {
				if wouldMatch(router, rq, method) {
					options = append(options, method)
				}
			}
			if len(options) == 0 {
				h.writeStatus(http.StatusNotFound, "unknown URL")
			} else {
				response.Header().Add("Allow", strings.Join(options, ", "))
				if rq.Method != "OPTIONS" {
					h.writeStatus(http.StatusMethodNotAllowed, "")
				}
			}
		}
	})
}

func fixQuotedSlashes(rq *http.Request) {
	uri := rq.RequestURI
	if strings.Contains(uri, "%2f") || strings.Contains(uri, "%2F") {
		if stop := strings.IndexAny(uri, "?#"); stop >= 0 {
			uri = uri[0:stop]
		}
		rq.URL.Path = uri
	}
}

func wouldMatch(router *mux.Router, rq *http.Request, method string) bool {
	savedMethod := rq.Method
	rq.Method = method
	defer func() { rq.Method = savedMethod }()
	var matchInfo mux.RouteMatch
	return router.Match(rq, &matchInfo)
}
