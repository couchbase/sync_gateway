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
	"regexp"
	"strconv"
	"strings"

	"github.com/couchbaselabs/sync_gateway_admin_ui"
	"github.com/gorilla/mux"
)

// Regexes that match database or doc ID component of a path.
// These are needed to avoid conflict with handlers that match special underscore-prefixed paths
// like "/_profile" and "/db/_all_docs".
const dbRegex = "[^_/][^/]*"
const docRegex = "[^_/][^/]*"

// Regex that matches a URI containing either:
//  - A regular doc ID with an escaped "/" character
//  - A user name with an escaped "/" character
var docWithSlashPathRegex *regexp.Regexp

func init() {
	docWithSlashPathRegex, _ = regexp.Compile("/" + dbRegex + "/([^_]|_user/).*%2[fF]")
}

// Creates a GorillaMux router containing the basic HTTP handlers for a server.
// This is the common functionality of the public and admin ports.
// The 'privs' parameter specifies the authentication the handler will use.
func createHandler(sc *ServerContext, privs handlerPrivs) (*mux.Router, *mux.Router) {
	r := mux.NewRouter()
	r.StrictSlash(true)
	// Global operations:
	r.Handle("/", makeHandler(sc, privs, (*handler).handleRoot)).Methods("GET", "HEAD")

	// Operations on databases:
	r.Handle("/{db:"+dbRegex+"}/", makeOfflineHandler(sc, privs, (*handler).handleGetDB)).Methods("GET", "HEAD")
	r.Handle("/{db:"+dbRegex+"}/", makeHandler(sc, privs, (*handler).handlePostDoc)).Methods("POST")

	// Special database URLs:
	dbr := r.PathPrefix("/{db:" + dbRegex + "}/").Subrouter()
	dbr.StrictSlash(true)
	dbr.Handle("/_all_docs", makeHandler(sc, privs, (*handler).handleAllDocs)).Methods("GET", "HEAD", "POST")
	dbr.Handle("/_bulk_docs", makeHandler(sc, privs, (*handler).handleBulkDocs)).Methods("POST")
	dbr.Handle("/_bulk_get", makeHandler(sc, privs, (*handler).handleBulkGet)).Methods("POST")
	dbr.Handle("/_changes", makeHandler(sc, privs, (*handler).handleChanges)).Methods("GET", "HEAD", "POST")
	dbr.Handle("/_design/{ddoc}", makeHandler(sc, privs, (*handler).handleGetDesignDoc)).Methods("GET", "HEAD")
	dbr.Handle("/_design/{ddoc}", makeHandler(sc, privs, (*handler).handlePutDesignDoc)).Methods("PUT")
	dbr.Handle("/_design/{ddoc}", makeHandler(sc, privs, (*handler).handleDeleteDesignDoc)).Methods("DELETE")
	dbr.Handle("/_design/{ddoc}/_view/{view}", makeHandler(sc, privs, (*handler).handleView)).Methods("GET")
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
	if sc.config.Facebook != nil {
		dbr.Handle("/_facebook", makeHandler(sc, publicPrivs,
			(*handler).handleFacebookPOST)).Methods("POST")
	}
	if sc.config.Google != nil {
		dbr.Handle("/_google", makeHandler(sc, publicPrivs,
			(*handler).handleGooglePOST)).Methods("POST")
	}

	// OpenID Connect endpoints
	dbr.Handle("/_oidc", makeHandler(sc, publicPrivs, (*handler).handleOIDC)).Methods("GET")
	dbr.Handle("/_oidc_callback", makeHandler(sc, publicPrivs, (*handler).handleOIDCCallback)).Methods("GET")
	dbr.Handle("/_oidc_refresh", makeHandler(sc, publicPrivs, (*handler).handleOIDCRefresh)).Methods("GET")
	dbr.Handle("/_oidc_challenge", makeHandler(sc, publicPrivs, (*handler).handleOIDCChallenge)).Methods("GET")

	oidcr := dbr.PathPrefix("/_oidc_testing").Subrouter()

	//Client discovery endpoint
	oidcr.Handle("/.well-known/openid-configuration", makeHandler(sc, publicPrivs, (*handler).handleOidcProviderConfiguration)).Methods("GET")

	oidcr.Handle("/authorize", makeHandler(sc, publicPrivs,
		(*handler).handleOidcTestProviderAuthorize)).Methods("GET", "POST")

	oidcr.Handle("/token", makeHandler(sc, publicPrivs,
		(*handler).handleOidcTestProviderToken)).Methods("POST")

	oidcr.Handle("/certs", makeHandler(sc, publicPrivs,
		(*handler).handleOidcTestProviderCerts)).Methods("GET")

	oidcr.Handle("/authenticate", makeHandler(sc, publicPrivs,
		(*handler).handleOidcTestProviderAuthenticate)).Methods("GET", "POST")

	return r, dbr
}

// Creates the HTTP handler for the public API of a gateway server.
func CreatePublicHandler(sc *ServerContext) http.Handler {
	r, dbr := createHandler(sc, regularPrivs)

	dbr.Handle("/_session", makeHandler(sc, publicPrivs,
		(*handler).handleSessionPOST)).Methods("POST")
	dbr.Handle("/_session", makeHandler(sc, regularPrivs,
		(*handler).handleSessionDELETE)).Methods("DELETE")
	// The routine below is part of the CouchDB REST API, users can't create DB's via the pblic API
	// but if the client set the 'createTarget' property of the Replicatior SG should return HTTP status 412
	// if the db exists, and 403 if it doesn't.
	r.Handle("/{targetdb:"+dbRegex+"}/",
		makeHandler(sc, publicPrivs, (*handler).handleCreateTarget)).Methods("PUT")
	return wrapRouter(sc, regularPrivs, r)
}

//////// ADMIN API:

// Creates the HTTP handler for the PRIVATE admin API of a gateway server.
func CreateAdminHandler(sc *ServerContext) http.Handler {
	router := CreateAdminRouter(sc)
	return wrapRouter(sc, adminPrivs, router)
}

func CreateAdminHandlerForRouter(sc *ServerContext, r *mux.Router) http.Handler {
	return wrapRouter(sc, adminPrivs, r)
}

// Creates the HTTP handler for the PRIVATE admin API of a gateway server.
func CreateAdminRouter(sc *ServerContext) *mux.Router {
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

	dbr.Handle("/_session/{sessionid}",
		makeHandler(sc, adminPrivs, (*handler).getUserSession)).Methods("GET")

	dbr.Handle("/_session/{sessionid}",
		makeHandler(sc, adminPrivs, (*handler).deleteUserSession)).Methods("DELETE")

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

	dbr.Handle("/_user/{name}/_session",
		makeHandler(sc, adminPrivs, (*handler).deleteUserSessions)).Methods("DELETE")
	dbr.Handle("/_user/{name}/_session/{sessionid}",
		makeHandler(sc, adminPrivs, (*handler).deleteUserSession)).Methods("DELETE")

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

	r.Handle("/_logging",
		makeHandler(sc, adminPrivs, (*handler).handleGetLogging)).Methods("GET")
	r.Handle("/_logging",
		makeHandler(sc, adminPrivs, (*handler).handleSetLogging)).Methods("PUT", "POST")
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
	r.Handle("/_config",
		makeHandler(sc, adminPrivs, (*handler).handleGetConfig)).Methods("GET")
	r.Handle("/_replicate",
		makeOfflineHandler(sc, adminPrivs, (*handler).handleReplicate)).Methods("POST")
	r.Handle("/_active_tasks",
		makeOfflineHandler(sc, adminPrivs, (*handler).handleActiveTasks)).Methods("GET")

	// Debugging handlers
	r.Handle("/_debug/pprof/goroutine",
		makeHandler(sc, adminPrivs, (*handler).handlePprofGoroutine)).Methods("GET", "POST")
	r.Handle("/_debug/pprof/cmdline",
		makeHandler(sc, adminPrivs, (*handler).handlePprofCmdline)).Methods("GET", "POST")
	r.Handle("/_debug/pprof/symbol",
		makeHandler(sc, adminPrivs, (*handler).handlePprofSymbol)).Methods("GET", "POST")
	r.Handle("/_debug/pprof/heap",
		makeHandler(sc, adminPrivs, (*handler).handlePprofHeap)).Methods("GET", "POST")
	r.Handle("/_debug/pprof/profile",
		makeHandler(sc, adminPrivs, (*handler).handlePprofProfile)).Methods("GET", "POST")
	r.Handle("/_debug/pprof/block",
		makeHandler(sc, adminPrivs, (*handler).handlePprofBlock)).Methods("GET", "POST")
	r.Handle("/_debug/pprof/threadcreate",
		makeHandler(sc, adminPrivs, (*handler).handlePprofThreadcreate)).Methods("GET", "POST")
	r.Handle("/_debug/pprof/trace",
		makeHandler(sc, adminPrivs, (*handler).handlePprofTrace)).Methods("GET", "POST")

	// Database-relative handlers:
	dbr.Handle("/_config",
		makeHandler(sc, adminPrivs, (*handler).handleGetDbConfig)).Methods("GET")
	dbr.Handle("/_config",
		makeOfflineHandler(sc, adminPrivs, (*handler).handlePutDbConfig)).Methods("PUT")
	dbr.Handle("/_resync",
		makeOfflineHandler(sc, adminPrivs, (*handler).handleResync)).Methods("POST")
	dbr.Handle("/_vacuum",
		makeHandler(sc, adminPrivs, (*handler).handleVacuum)).Methods("POST")
	dbr.Handle("/_purge",
		makeHandler(sc, adminPrivs, (*handler).handlePurge)).Methods("POST")
	dbr.Handle("/_flush",
		makeHandler(sc, adminPrivs, (*handler).handleFlush)).Methods("POST")
	dbr.Handle("/_online",
		makeOfflineHandler(sc, adminPrivs, (*handler).handleDbOnline)).Methods("POST")
	dbr.Handle("/_offline",
		makeOfflineHandler(sc, adminPrivs, (*handler).handleDbOffline)).Methods("POST")
	dbr.Handle("/_dump/{view}",
		makeHandler(sc, adminPrivs, (*handler).handleDump)).Methods("GET")
	dbr.Handle("/_view/{view}", // redundant; just for backward compatibility with 1.0
		makeHandler(sc, adminPrivs, (*handler).handleView)).Methods("GET")
	dbr.Handle("/_dumpchannel/{channel}",
		makeHandler(sc, adminPrivs, (*handler).handleDumpChannel)).Methods("GET")
	dbr.Handle("/_index",
		makeHandler(sc, adminPrivs, (*handler).handleIndex)).Methods("GET")
	dbr.Handle("/_index/channel/{channel}",
		makeHandler(sc, adminPrivs, (*handler).handleIndexChannel)).Methods("GET")
	dbr.Handle("/_index/channels",
		makeHandler(sc, adminPrivs, (*handler).handleIndexAllChannels)).Methods("GET")

	// The routes below are part of the CouchDB REST API but should only be available to admins,
	// so the handlers are moved to the admin port.
	r.Handle("/{newdb:"+dbRegex+"}/",
		makeHandler(sc, adminPrivs, (*handler).handleCreateDB)).Methods("PUT")
	r.Handle("/{db:"+dbRegex+"}/",
		makeOfflineHandler(sc, adminPrivs, (*handler).handleDeleteDB)).Methods("DELETE")

	r.Handle("/_all_dbs",
		makeHandler(sc, adminPrivs, (*handler).handleAllDbs)).Methods("GET", "HEAD")
	dbr.Handle("/_compact",
		makeHandler(sc, adminPrivs, (*handler).handleCompact)).Methods("POST")

	return r
}

// Returns a top-level HTTP handler for a Router. This adds behavior for URLs that don't
// match anything -- it handles the OPTIONS method as well as returning either a 404 or 405
// for URLs that don't match a route.
func wrapRouter(sc *ServerContext, privs handlerPrivs, router *mux.Router) http.Handler {
	return http.HandlerFunc(func(response http.ResponseWriter, rq *http.Request) {
		FixQuotedSlashes(rq)
		var match mux.RouteMatch

		// Inject CORS if enabled and requested and not admin port
		originHeader := rq.Header["Origin"]
		if privs != adminPrivs && sc.config.CORS != nil && len(originHeader) > 0 {
			origin := matchedOrigin(sc.config.CORS.Origin, originHeader)
			response.Header().Add("Access-Control-Allow-Origin", origin)
			response.Header().Add("Access-Control-Allow-Credentials", "true")
			response.Header().Add("Access-Control-Allow-Headers", strings.Join(sc.config.CORS.Headers, ", "))
		}

		if router.Match(rq, &match) {
			router.ServeHTTP(response, rq)
		} else {
			// Log the request
			h := newHandler(sc, privs, response, rq, false)
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
				if privs != adminPrivs && sc.config.CORS != nil && len(originHeader) > 0 {
					response.Header().Add("Access-Control-Max-Age", strconv.Itoa(sc.config.CORS.MaxAge))
					response.Header().Add("Access-Control-Allow-Methods", strings.Join(options, ", "))
				}
				if rq.Method != "OPTIONS" {
					h.writeStatus(http.StatusMethodNotAllowed, "")
				} else {
					h.writeStatus(http.StatusNoContent, "")
				}
			}
			h.logDuration(true)
		}
	})
}

func matchedOrigin(allowOrigins []string, rqOrigins []string) string {
	for _, rv := range rqOrigins {
		for _, av := range allowOrigins {
			if rv == av {
				return av
			}
		}
	}
	for _, av := range allowOrigins {
		if av == "*" {
			return "*"
		}
	}
	return ""
}

func FixQuotedSlashes(rq *http.Request) {
	uri := rq.RequestURI
	if docWithSlashPathRegex.MatchString(uri) {
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
