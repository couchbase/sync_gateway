//  Copyright 2012-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package rest

import (
	"net/http"
	"regexp"
	"strconv"
	"strings"

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

// CreateCommonRouter
// Creates a GorillaMux router containing the basic HTTP handlers for a server.
// This is the common functionality of the public and admin ports.
// The 'privs' parameter specifies the authentication the handler will use.
func createCommonRouter(sc *ServerContext, privs handlerPrivs) (*mux.Router, *mux.Router) {
	r := mux.NewRouter()
	r.StrictSlash(true)
	// Global operations:
	r.Handle("/", makeHandler(sc, privs, nil, nil, (*handler).handleRoot)).Methods("GET", "HEAD")

	// Operations on databases:
	r.Handle("/{db:"+dbRegex+"}/", makeOfflineHandler(sc, privs, []Permission{PermDevOps}, nil, (*handler).handleGetDB)).Methods("GET", "HEAD")
	r.Handle("/{db:"+dbRegex+"}/", makeHandler(sc, privs, []Permission{PermWriteAppData}, nil, (*handler).handlePostDoc)).Methods("POST")

	// Special database URLs:
	dbr := r.PathPrefix("/{db:" + dbRegex + "}/").Subrouter()
	dbr.StrictSlash(true)
	dbr.Handle("/_all_docs", makeHandler(sc, privs, []Permission{PermReadAppData}, nil, (*handler).handleAllDocs)).Methods("GET", "HEAD", "POST")
	dbr.Handle("/_bulk_docs", makeHandler(sc, privs, []Permission{PermWriteAppData}, nil, (*handler).handleBulkDocs)).Methods("POST")
	dbr.Handle("/_bulk_get", makeHandler(sc, privs, []Permission{PermReadAppData}, nil, (*handler).handleBulkGet)).Methods("POST")
	dbr.Handle("/_changes", makeHandler(sc, privs, []Permission{PermReadAppData}, nil, (*handler).handleChanges)).Methods("GET", "HEAD", "POST")
	dbr.Handle("/_design/{ddoc}", makeHandler(sc, privs, []Permission{PermReadAppData}, nil, (*handler).handleGetDesignDoc)).Methods("GET", "HEAD")
	dbr.Handle("/_design/{ddoc}", makeHandler(sc, privs, []Permission{PermWriteAppData}, nil, (*handler).handlePutDesignDoc)).Methods("PUT")
	dbr.Handle("/_design/{ddoc}", makeHandler(sc, privs, []Permission{PermWriteAppData}, nil, (*handler).handleDeleteDesignDoc)).Methods("DELETE")
	dbr.Handle("/_design/{ddoc}/_view/{view}", makeHandler(sc, privs, []Permission{PermReadAppData}, nil, (*handler).handleView)).Methods("GET")
	dbr.Handle("/_ensure_full_commit", makeHandler(sc, privs, []Permission{PermReadAppData}, nil, (*handler).handleEFC)).Methods("POST")
	dbr.Handle("/_revs_diff", makeHandler(sc, privs, []Permission{PermWriteAppData}, nil, (*handler).handleRevsDiff)).Methods("POST")

	// Document URLs:
	dbr.Handle("/_local/{docid}", makeHandler(sc, privs, []Permission{PermReadAppData}, nil, (*handler).handleGetLocalDoc)).Methods("GET", "HEAD")
	dbr.Handle("/_local/{docid}", makeHandler(sc, privs, []Permission{PermWriteAppData}, nil, (*handler).handlePutLocalDoc)).Methods("PUT")
	dbr.Handle("/_local/{docid}", makeHandler(sc, privs, []Permission{PermWriteAppData}, nil, (*handler).handleDelLocalDoc)).Methods("DELETE")

	dbr.Handle("/{docid:"+docRegex+"}", makeHandler(sc, privs, []Permission{PermReadAppData}, nil, (*handler).handleGetDoc)).Methods("GET", "HEAD")
	dbr.Handle("/{docid:"+docRegex+"}", makeHandler(sc, privs, []Permission{PermWriteAppData}, nil, (*handler).handlePutDoc)).Methods("PUT")
	dbr.Handle("/{docid:"+docRegex+"}", makeHandler(sc, privs, []Permission{PermWriteAppData}, nil, (*handler).handleDeleteDoc)).Methods("DELETE")

	dbr.Handle("/{docid:"+docRegex+"}/{attach}", makeHandler(sc, privs, []Permission{PermReadAppData}, nil, (*handler).handleGetAttachment)).Methods("GET", "HEAD")
	dbr.Handle("/{docid:"+docRegex+"}/{attach}", makeHandler(sc, privs, []Permission{PermWriteAppData}, nil, (*handler).handlePutAttachment)).Methods("PUT")

	// Session/login URLs are per-database (unlike in CouchDB)
	// These have public privileges so that they can be called without being logged in already
	dbr.Handle("/_session", makeHandler(sc, publicPrivs, nil, nil, (*handler).handleSessionGET)).Methods("GET", "HEAD")

	if sc.config.DeprecatedConfig != nil {
		if sc.config.DeprecatedConfig.Facebook != nil {
			dbr.Handle("/_facebook", makeHandler(sc, publicPrivs, nil, nil, (*handler).handleFacebookPOST)).Methods("POST")
		}
		if sc.config.DeprecatedConfig.Google != nil {
			dbr.Handle("/_google", makeHandler(sc, publicPrivs, nil, nil, (*handler).handleGooglePOST)).Methods("POST")
		}
	}

	// OpenID Connect endpoints
	dbr.Handle("/_oidc", makeHandler(sc, publicPrivs, nil, nil, (*handler).handleOIDC)).Methods("GET")
	dbr.Handle("/_oidc_callback", makeHandler(sc, publicPrivs, nil, nil, (*handler).handleOIDCCallback)).Methods("GET")
	dbr.Handle("/_oidc_refresh", makeHandler(sc, publicPrivs, nil, nil, (*handler).handleOIDCRefresh)).Methods("GET")
	dbr.Handle("/_oidc_challenge", makeHandler(sc, publicPrivs, nil, nil, (*handler).handleOIDCChallenge)).Methods("GET")

	oidcr := dbr.PathPrefix("/_oidc_testing").Subrouter()

	// Client discovery endpoint
	oidcr.Handle("/.well-known/openid-configuration", makeHandler(sc, publicPrivs, nil, nil, (*handler).handleOidcProviderConfiguration)).Methods("GET")

	oidcr.Handle("/authorize", makeHandler(sc, publicPrivs, nil, nil, (*handler).handleOidcTestProviderAuthorize)).Methods("GET", "POST")

	oidcr.Handle("/token", makeHandler(sc, publicPrivs, nil, nil, (*handler).handleOidcTestProviderToken)).Methods("POST")

	oidcr.Handle("/certs", makeHandler(sc, publicPrivs, nil, nil, (*handler).handleOidcTestProviderCerts)).Methods("GET")

	oidcr.Handle("/authenticate", makeHandler(sc, publicPrivs, nil, nil, (*handler).handleOidcTestProviderAuthenticate)).Methods("GET", "POST")

	dbr.Handle("/_blipsync", makeHandler(sc, privs, []Permission{PermWriteAppData}, nil, (*handler).handleBLIPSync)).Methods("GET")

	return r, dbr
}

// CreatePublicHandler Creates the HTTP handler for the public API of a gateway server.
func CreatePublicHandler(sc *ServerContext) http.Handler {
	r, dbr := createCommonRouter(sc, regularPrivs)

	dbr.Handle("/_session", makeHandler(sc, publicPrivs, nil, nil, (*handler).handleSessionPOST)).Methods("POST")
	dbr.Handle("/_session", makeHandler(sc, regularPrivs, nil, nil, (*handler).handleSessionDELETE)).Methods("DELETE")
	// The routine below is part of the CouchDB REST API, users can't create DB's via the pblic API
	// but if the client set the 'createTarget' property of the Replicatior SG should return HTTP status 412
	// if the db exists, and 403 if it doesn't.
	r.Handle("/{targetdb:"+dbRegex+"}/",
		makeHandler(sc, publicPrivs, nil, nil, (*handler).handleCreateTarget)).Methods("PUT")
	return wrapRouter(sc, regularPrivs, r)
}

// ////// ADMIN API:

// CreateAdminHandler Creates the HTTP handler for the PRIVATE admin API of a gateway server.
func CreateAdminHandler(sc *ServerContext) http.Handler {
	router := CreateAdminRouter(sc)
	return wrapRouter(sc, adminPrivs, router)
}

// CreateAdminRouter Creates the HTTP handler for the PRIVATE admin API of a gateway server.
func CreateAdminRouter(sc *ServerContext) *mux.Router {
	r, dbr := createCommonRouter(sc, adminPrivs)

	dbr.Handle("/_session",
		makeHandler(sc, adminPrivs, []Permission{PermWritePrincipal}, nil, (*handler).createUserSession)).Methods("POST")

	dbr.Handle("/_session/{sessionid}",
		makeHandler(sc, adminPrivs, []Permission{PermReadPrincipal}, nil, (*handler).getUserSession)).Methods("GET")

	dbr.Handle("/_session/{sessionid}",
		makeHandler(sc, adminPrivs, []Permission{PermWritePrincipal}, nil, (*handler).deleteUserSession)).Methods("DELETE")

	dbr.Handle("/_raw/{docid:"+docRegex+"}",
		makeHandler(sc, adminPrivs, []Permission{PermReadAppData}, nil, (*handler).handleGetRawDoc)).Methods("GET", "HEAD")

	dbr.Handle("/_revtree/{docid:"+docRegex+"}",
		makeHandler(sc, adminPrivs, []Permission{PermReadAppData}, nil, (*handler).handleGetRevTree)).Methods("GET")

	dbr.Handle("/_user/",
		makeHandler(sc, adminPrivs, []Permission{PermReadPrincipal}, nil, (*handler).getUsers)).Methods("GET", "HEAD")
	dbr.Handle("/_user/",
		makeHandler(sc, adminPrivs, []Permission{PermWritePrincipal}, nil, (*handler).putUser)).Methods("POST")
	dbr.Handle("/_user/{name}",
		makeHandler(sc, adminPrivs, []Permission{PermReadPrincipal}, []Permission{PermReadPrincipalAppData}, (*handler).getUserInfo)).Methods("GET", "HEAD")
	dbr.Handle("/_user/{name}",
		makeHandler(sc, adminPrivs, []Permission{PermWritePrincipal}, nil, (*handler).putUser)).Methods("PUT")
	dbr.Handle("/_user/{name}",
		makeHandler(sc, adminPrivs, []Permission{PermWritePrincipal}, nil, (*handler).deleteUser)).Methods("DELETE")

	dbr.Handle("/_user/{name}/_session",
		makeHandler(sc, adminPrivs, []Permission{PermWritePrincipal}, nil, (*handler).deleteUserSessions)).Methods("DELETE")
	dbr.Handle("/_user/{name}/_session/{sessionid}",
		makeHandler(sc, adminPrivs, []Permission{PermWritePrincipal}, nil, (*handler).deleteUserSession)).Methods("DELETE")

	dbr.Handle("/_role/",
		makeHandler(sc, adminPrivs, []Permission{PermReadPrincipal}, nil, (*handler).getRoles)).Methods("GET", "HEAD")
	dbr.Handle("/_role/",
		makeHandler(sc, adminPrivs, []Permission{PermWritePrincipal}, nil, (*handler).putRole)).Methods("POST")
	dbr.Handle("/_role/{name}",
		makeHandler(sc, adminPrivs, []Permission{PermReadPrincipal}, []Permission{PermReadPrincipalAppData}, (*handler).getRoleInfo)).Methods("GET", "HEAD")
	dbr.Handle("/_role/{name}",
		makeHandler(sc, adminPrivs, []Permission{PermWritePrincipal}, nil, (*handler).putRole)).Methods("PUT")
	dbr.Handle("/_role/{name}",
		makeHandler(sc, adminPrivs, []Permission{PermWritePrincipal}, nil, (*handler).deleteRole)).Methods("DELETE")

	dbr.Handle("/_replication/",
		makeHandler(sc, adminPrivs, []Permission{PermReadReplications}, nil, (*handler).getReplications)).Methods("GET", "HEAD")
	dbr.Handle("/_replication/",
		makeHandler(sc, adminPrivs, []Permission{PermWriteReplications}, nil, (*handler).putReplication)).Methods("POST")
	dbr.Handle("/_replication/{replicationID}",
		makeHandler(sc, adminPrivs, []Permission{PermReadReplications}, nil, (*handler).getReplication)).Methods("GET", "HEAD")
	dbr.Handle("/_replication/{replicationID}",
		makeHandler(sc, adminPrivs, []Permission{PermWriteReplications}, nil, (*handler).putReplication)).Methods("PUT")
	dbr.Handle("/_replication/{replicationID}",
		makeHandler(sc, adminPrivs, []Permission{PermWriteReplications}, nil, (*handler).deleteReplication)).Methods("DELETE")

	dbr.Handle("/_replicationStatus/",
		makeHandler(sc, adminPrivs, []Permission{PermReadReplications}, nil, (*handler).getReplicationsStatus)).Methods("GET", "HEAD")
	dbr.Handle("/_replicationStatus/{replicationID}",
		makeHandler(sc, adminPrivs, []Permission{PermReadReplications}, nil, (*handler).getReplicationStatus)).Methods("GET", "HEAD")
	dbr.Handle("/_replicationStatus/{replicationID}",
		makeHandler(sc, adminPrivs, []Permission{PermWriteReplications}, nil, (*handler).putReplicationStatus)).Methods("PUT")

	r.Handle("/_logging",
		makeHandler(sc, adminPrivs, []Permission{PermDevOps}, nil, (*handler).handleGetLogging)).Methods("GET")
	r.Handle("/_logging",
		makeHandler(sc, adminPrivs, []Permission{PermDevOps}, nil, (*handler).handleSetLogging)).Methods("PUT", "POST")
	r.Handle("/_profile/{profilename}",
		makeHandler(sc, adminPrivs, []Permission{PermDevOps}, nil, (*handler).handleProfiling)).Methods("POST")
	r.Handle("/_profile",
		makeHandler(sc, adminPrivs, []Permission{PermDevOps}, nil, (*handler).handleProfiling)).Methods("POST")
	r.Handle("/_heap",
		makeHandler(sc, adminPrivs, []Permission{PermDevOps}, nil, (*handler).handleHeapProfiling)).Methods("POST")
	r.Handle("/_stats",
		makeHandler(sc, adminPrivs, []Permission{PermStatsExport}, nil, (*handler).handleStats)).Methods("GET")
	r.Handle(kDebugURLPathPrefix,
		makeHandler(sc, adminPrivs, []Permission{PermStatsExport}, nil, (*handler).handleExpvar)).Methods("GET")
	// TODO: Apply perms
	r.Handle("/_config",
		makeHandler(sc, adminPrivs, []Permission{PermDevOps}, nil, (*handler).handleGetConfig)).Methods("GET")
	r.Handle("/_config",
		makeHandler(sc, adminPrivs, []Permission{PermDevOps}, nil, (*handler).handlePutConfig)).Methods("PUT")

	r.Handle("/_status",
		makeHandler(sc, adminPrivs, []Permission{PermDevOps}, nil, (*handler).handleGetStatus)).Methods("GET")

	r.Handle("/_sgcollect_info",
		makeHandler(sc, adminPrivs, []Permission{PermDevOps}, nil, (*handler).handleSGCollectStatus)).Methods("GET")
	r.Handle("/_sgcollect_info",
		makeHandler(sc, adminPrivs, []Permission{PermDevOps}, nil, (*handler).handleSGCollectCancel)).Methods("DELETE")
	r.Handle("/_sgcollect_info",
		makeHandler(sc, adminPrivs, []Permission{PermDevOps}, nil, (*handler).handleSGCollect)).Methods("POST")

	// Debugging handlers
	r.Handle("/_debug/pprof/goroutine",
		makeHandler(sc, adminPrivs, []Permission{PermDevOps}, nil, (*handler).handlePprofGoroutine)).Methods("GET", "POST")
	r.Handle("/_debug/pprof/cmdline",
		makeHandler(sc, adminPrivs, []Permission{PermDevOps}, nil, (*handler).handlePprofCmdline)).Methods("GET", "POST")
	r.Handle("/_debug/pprof/symbol",
		makeHandler(sc, adminPrivs, []Permission{PermDevOps}, nil, (*handler).handlePprofSymbol)).Methods("GET", "POST")
	r.Handle("/_debug/pprof/heap",
		makeHandler(sc, adminPrivs, []Permission{PermDevOps}, nil, (*handler).handlePprofHeap)).Methods("GET", "POST")
	r.Handle("/_debug/pprof/profile",
		makeHandler(sc, adminPrivs, []Permission{PermDevOps}, nil, (*handler).handlePprofProfile)).Methods("GET", "POST")
	r.Handle("/_debug/pprof/block",
		makeHandler(sc, adminPrivs, []Permission{PermDevOps}, nil, (*handler).handlePprofBlock)).Methods("GET", "POST")
	r.Handle("/_debug/pprof/threadcreate",
		makeHandler(sc, adminPrivs, []Permission{PermDevOps}, nil, (*handler).handlePprofThreadcreate)).Methods("GET", "POST")
	r.Handle("/_debug/pprof/mutex",
		makeHandler(sc, adminPrivs, []Permission{PermDevOps}, nil, (*handler).handlePprofMutex)).Methods("GET", "POST")
	r.Handle("/_debug/pprof/trace",
		makeHandler(sc, adminPrivs, []Permission{PermDevOps}, nil, (*handler).handlePprofTrace)).Methods("GET", "POST")
	r.Handle("/_debug/fgprof",
		makeHandler(sc, adminPrivs, []Permission{PermDevOps}, nil, (*handler).handleFgprof)).Methods("GET", "POST")

	r.Handle("/_post_upgrade",
		makeHandler(sc, adminPrivs, []Permission{PermDevOps}, nil, (*handler).handlePostUpgrade)).Methods("POST")

	// Database-relative handlers:
	dbr.Handle("/_config",
		makeOfflineHandler(sc, adminPrivs, []Permission{PermUpdateDb}, nil, (*handler).handleGetDbConfig)).Methods("GET")
	dbr.Handle("/_config",
		makeOfflineHandler(sc, adminPrivs, []Permission{PermUpdateDb, PermConfigureSyncFn, PermConfigureAuth}, []Permission{PermUpdateDb, PermConfigureSyncFn, PermConfigureAuth}, (*handler).handlePutDbConfig)).Methods("PUT", "POST")

	dbr.Handle("/_config/sync",
		makeOfflineHandler(sc, adminPrivs, []Permission{PermUpdateDb, PermConfigureSyncFn}, nil, (*handler).handleGetDbConfigSync)).Methods("GET")
	dbr.Handle("/_config/sync",
		makeOfflineHandler(sc, adminPrivs, []Permission{PermUpdateDb, PermConfigureSyncFn}, nil, (*handler).handlePutDbConfigSync)).Methods("PUT")
	dbr.Handle("/_config/sync",
		makeOfflineHandler(sc, adminPrivs, []Permission{PermUpdateDb, PermConfigureSyncFn}, nil, (*handler).handleDeleteDbConfigSync)).Methods("DELETE")
	dbr.Handle("/_config/import_filter",
		makeOfflineHandler(sc, adminPrivs, []Permission{PermUpdateDb}, nil, (*handler).handleGetDbConfigImportFilter)).Methods("GET")
	dbr.Handle("/_config/import_filter",
		makeOfflineHandler(sc, adminPrivs, []Permission{PermUpdateDb}, nil, (*handler).handlePutDbConfigImportFilter)).Methods("PUT")
	dbr.Handle("/_config/import_filter",
		makeOfflineHandler(sc, adminPrivs, []Permission{PermUpdateDb}, nil, (*handler).handleDeleteDbConfigImportFilter)).Methods("DELETE")

	dbr.Handle("/_resync",
		makeOfflineHandler(sc, adminPrivs, []Permission{PermUpdateDb}, nil, (*handler).handleGetResync)).Methods("GET")
	dbr.Handle("/_resync",
		makeOfflineHandler(sc, adminPrivs, []Permission{PermUpdateDb}, nil, (*handler).handlePostResync)).Methods("POST")
	dbr.Handle("/_purge",
		makeHandler(sc, adminPrivs, []Permission{PermWriteAppData}, nil, (*handler).handlePurge)).Methods("POST")
	dbr.Handle("/_flush",
		makeHandler(sc, adminPrivs, []Permission{PermDevOps}, nil, (*handler).handleFlush)).Methods("POST")
	dbr.Handle("/_online",
		makeOfflineHandler(sc, adminPrivs, []Permission{PermUpdateDb}, nil, (*handler).handleDbOnline)).Methods("POST")
	dbr.Handle("/_offline",
		makeOfflineHandler(sc, adminPrivs, []Permission{PermUpdateDb}, nil, (*handler).handleDbOffline)).Methods("POST")
	dbr.Handle("/_dump/{view}",
		makeHandler(sc, adminPrivs, []Permission{PermReadAppData}, nil, (*handler).handleDump)).Methods("GET")
	dbr.Handle("/_view/{view}", // redundant; just for backward compatibility with 1.0
		makeHandler(sc, adminPrivs, []Permission{PermReadAppData}, nil, (*handler).handleView)).Methods("GET")
	dbr.Handle("/_dumpchannel/{channel}",
		makeHandler(sc, adminPrivs, []Permission{PermReadAppData}, nil, (*handler).handleDumpChannel)).Methods("GET")
	dbr.Handle("/_repair",
		makeHandler(sc, adminPrivs, []Permission{PermUpdateDb}, nil, (*handler).handleRepair)).Methods("POST")

	// The routes below are part of the CouchDB REST API but should only be available to admins,
	// so the handlers are moved to the admin port.
	r.Handle("/{newdb:"+dbRegex+"}/",
		makeHandlerSpecificAuthScope(sc, adminPrivs, []Permission{PermCreateDb}, nil, (*handler).handleCreateDB, getAuthScopeHandleCreateDB)).Methods("PUT")
	r.Handle("/{db:"+dbRegex+"}/",
		makeOfflineHandler(sc, adminPrivs, []Permission{PermDeleteDb}, nil, (*handler).handleDeleteDB)).Methods("DELETE")

	r.Handle("/_all_dbs",
		makeHandler(sc, adminPrivs, []Permission{PermDevOps}, nil, (*handler).handleAllDbs)).Methods("GET", "HEAD")
	dbr.Handle("/_compact",
		makeHandler(sc, adminPrivs, []Permission{PermUpdateDb}, nil, (*handler).handleCompact)).Methods("POST")
	dbr.Handle("/_compact",
		makeHandler(sc, adminPrivs, []Permission{PermUpdateDb}, nil, (*handler).handleGetCompact)).Methods("GET")

	return r
}

// Prometheus Metrics API

// CreateMetricHandler Creates the HTTP handler for the prometheus metrics API of a gateway server.
func CreateMetricHandler(sc *ServerContext) http.Handler {
	router := CreateMetricRouter(sc)
	return wrapRouter(sc, metricsPrivs, router)
}

func CreateMetricRouter(sc *ServerContext) *mux.Router {
	r := mux.NewRouter()
	r.StrictSlash(true)

	r.Handle("/_metrics", makeHandler(sc, metricsPrivs, []Permission{PermStatsExport}, nil, (*handler).handleMetrics)).Methods("GET")
	r.Handle(kDebugURLPathPrefix, makeHandler(sc, metricsPrivs, []Permission{PermStatsExport}, nil, (*handler).handleExpvar)).Methods("GET")

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
		if privs != adminPrivs && sc.config.API.CORS != nil && len(originHeader) > 0 {
			origin := matchedOrigin(sc.config.API.CORS.Origin, originHeader)
			response.Header().Add("Access-Control-Allow-Origin", origin)
			response.Header().Add("Access-Control-Allow-Credentials", "true")
			response.Header().Add("Access-Control-Allow-Headers", strings.Join(sc.config.API.CORS.Headers, ", "))
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
				if privs != adminPrivs && sc.config.API.CORS != nil && len(originHeader) > 0 {
					response.Header().Add("Access-Control-Max-Age", strconv.Itoa(sc.config.API.CORS.MaxAge))
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
