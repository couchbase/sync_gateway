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
//   - A regular doc ID with an escaped "/" character
//   - A user name with an escaped "/" character
var docWithSlashPathRegex *regexp.Regexp

func init() {
	docWithSlashPathRegex, _ = regexp.Compile("/" + dbRegex + "/([^_]|_user/).*%2[fF]")
}

// CreateCommonRouter
// Creates a GorillaMux router containing the basic HTTP handlers for a server.
// This is the common functionality of the public and admin ports.
// The 'privs' parameter specifies the authentication the handler will use.
func createCommonRouter(sc *ServerContext, privs handlerPrivs) (root, db, keyspace *mux.Router) {
	root = CreatePingRouter(sc)

	// Global operations:
	root.Handle("/", makeHandler(sc, privs, nil, nil, (*handler).handleRoot)).Methods("GET", "HEAD")

	// Operations on databases:
	root.Handle("/{db:"+dbRegex+"}/", makeOfflineHandler(sc, privs, []Permission{PermDevOps}, nil, (*handler).handleGetDB)).Methods("GET", "HEAD")
	root.Handle("/{keyspace:"+dbRegex+"}/", makeHandler(sc, privs, []Permission{PermWriteAppData}, nil, (*handler).handlePostDoc)).Methods("POST")

	// Keyspace operations (i.e. collection-specific):
	keyspace = root.PathPrefix("/{keyspace:" + dbRegex + "}/").Subrouter()
	keyspace.StrictSlash(true)
	keyspace.Handle("/{docid:"+docRegex+"}", makeHandler(sc, privs, []Permission{PermReadAppData}, nil, (*handler).handleGetDoc)).Methods("GET", "HEAD")
	keyspace.Handle("/{docid:"+docRegex+"}", makeHandler(sc, privs, []Permission{PermWriteAppData}, nil, (*handler).handlePutDoc)).Methods("PUT")
	keyspace.Handle("/{docid:"+docRegex+"}", makeHandler(sc, privs, []Permission{PermWriteAppData}, nil, (*handler).handleDeleteDoc)).Methods("DELETE")

	keyspace.Handle("/{docid:"+docRegex+"}/{attach}", makeHandler(sc, privs, []Permission{PermReadAppData}, nil, (*handler).handleGetAttachment)).Methods("GET", "HEAD")
	keyspace.Handle("/{docid:"+docRegex+"}/{attach}", makeHandler(sc, privs, []Permission{PermWriteAppData}, nil, (*handler).handlePutAttachment)).Methods("PUT")
	keyspace.Handle("/{docid:"+docRegex+"}/{attach}", makeHandler(sc, privs, []Permission{PermWriteAppData}, nil, (*handler).handleDeleteAttachment)).Methods("DELETE")

	keyspace.Handle("/_local/{docid}", makeHandler(sc, privs, []Permission{PermReadAppData}, nil, (*handler).handleGetLocalDoc)).Methods("GET", "HEAD")
	keyspace.Handle("/_local/{docid}", makeHandler(sc, privs, []Permission{PermWriteAppData}, nil, (*handler).handlePutLocalDoc)).Methods("PUT")
	keyspace.Handle("/_local/{docid}", makeHandler(sc, privs, []Permission{PermWriteAppData}, nil, (*handler).handleDelLocalDoc)).Methods("DELETE")

	keyspace.Handle("/_bulk_docs", makeHandler(sc, privs, []Permission{PermWriteAppData}, nil, (*handler).handleBulkDocs)).Methods("POST")
	keyspace.Handle("/_bulk_get", makeHandler(sc, privs, []Permission{PermReadAppData}, nil, (*handler).handleBulkGet)).Methods("POST")
	keyspace.Handle("/_revs_diff", makeHandler(sc, privs, []Permission{PermWriteAppData}, nil, (*handler).handleRevsDiff)).Methods("POST")
	keyspace.Handle("/_changes", makeHandler(sc, privs, []Permission{PermReadAppData}, nil, (*handler).handleChanges)).Methods("GET", "HEAD", "POST")
	keyspace.Handle("/_all_docs", makeHandler(sc, privs, []Permission{PermReadAppData}, nil, (*handler).handleAllDocs)).Methods("GET", "HEAD", "POST")

	// Database operations (i.e. multi-collection):
	dbr := root.PathPrefix("/{db:" + dbRegex + "}/").Subrouter()
	dbr.StrictSlash(true)
	dbr.Handle("/_design/{ddoc}", makeHandler(sc, privs, []Permission{PermReadAppData}, nil, (*handler).handleGetDesignDoc)).Methods("GET", "HEAD")
	dbr.Handle("/_design/{ddoc}", makeHandler(sc, privs, []Permission{PermWriteAppData}, nil, (*handler).handlePutDesignDoc)).Methods("PUT")
	dbr.Handle("/_design/{ddoc}", makeHandler(sc, privs, []Permission{PermWriteAppData}, nil, (*handler).handleDeleteDesignDoc)).Methods("DELETE")
	dbr.Handle("/_design/{ddoc}/_view/{view}", makeHandler(sc, privs, []Permission{PermReadAppData}, nil, (*handler).handleView)).Methods("GET")
	dbr.Handle("/_ensure_full_commit", makeHandler(sc, privs, []Permission{PermReadAppData}, nil, (*handler).handleEFC)).Methods("POST")

	// Session/login URLs are per-database (unlike in CouchDB)
	// These have public privileges so that they can be called without being logged in already
	dbr.Handle("/_session", makeHandler(sc, publicPrivs, nil, nil, (*handler).handleSessionGET)).Methods("GET", "HEAD")

	if sc.Config.DeprecatedConfig != nil {
		if sc.Config.DeprecatedConfig.Facebook != nil {
			dbr.Handle("/_facebook", makeHandler(sc, publicPrivs, nil, nil, (*handler).handleFacebookPOST)).Methods("POST")
		}
		if sc.Config.DeprecatedConfig.Google != nil {
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

	// User queries & functions
	if sc.Config.Unsupported.UserQueries != nil && *sc.Config.Unsupported.UserQueries {
		dbr.Handle("/_function/{name}", makeHandler(sc, privs, []Permission{PermReadAppData}, nil, (*handler).handleFunctionCall)).Methods("GET", "POST")
		dbr.Handle("/_graphql", makeHandler(sc, privs, []Permission{PermReadAppData}, nil, (*handler).handleGraphQL)).Methods("GET")
		dbr.Handle("/_graphql", makeHandler(sc, privs, []Permission{PermWriteAppData}, nil, (*handler).handleGraphQL)).Methods("POST")
	}

	return root, dbr, keyspace
}

// CreatePublicHandler Creates the HTTP handler for the public API of a gateway server.
func CreatePublicHandler(sc *ServerContext) http.Handler {
	r, dbr, _ := createCommonRouter(sc, regularPrivs)

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
	r, dbr, keyspace := createCommonRouter(sc, adminPrivs)

	// Keyspace handlers (single collection):
	keyspace.Handle("/_purge",
		makeHandler(sc, adminPrivs, []Permission{PermWriteAppData}, nil, (*handler).handlePurge)).Methods("POST")
	keyspace.Handle("/_raw/{docid:"+docRegex+"}",
		makeHandler(sc, adminPrivs, []Permission{PermReadAppData}, nil, (*handler).handleGetRawDoc)).Methods("GET", "HEAD")
	keyspace.Handle("/_revtree/{docid:"+docRegex+"}",
		makeHandler(sc, adminPrivs, []Permission{PermReadAppData}, nil, (*handler).handleGetRevTree)).Methods("GET")
	keyspace.Handle("/_dumpchannel/{channel}",
		makeHandler(sc, adminPrivs, []Permission{PermReadAppData}, nil, (*handler).handleDumpChannel)).Methods("GET")

	// Database handlers (multi collection):
	dbr.Handle("/_resync",
		makeOfflineHandler(sc, adminPrivs, []Permission{PermUpdateDb}, nil, (*handler).handleGetResync)).Methods("GET")
	dbr.Handle("/_resync",
		makeOfflineHandler(sc, adminPrivs, []Permission{PermUpdateDb}, nil, (*handler).handlePostResync)).Methods("POST")
	dbr.Handle("/_compact",
		makeHandler(sc, adminPrivs, []Permission{PermUpdateDb}, nil, (*handler).handleCompact)).Methods("POST")
	dbr.Handle("/_compact",
		makeHandler(sc, adminPrivs, []Permission{PermUpdateDb}, nil, (*handler).handleGetCompact)).Methods("GET")
	dbr.Handle("/_session",
		makeHandler(sc, adminPrivs, []Permission{PermWritePrincipal}, nil, (*handler).createUserSession)).Methods("POST")
	dbr.Handle("/_session/{sessionid}",
		makeHandler(sc, adminPrivs, []Permission{PermReadPrincipal}, nil, (*handler).getUserSession)).Methods("GET")
	dbr.Handle("/_session/{sessionid}",
		makeHandler(sc, adminPrivs, []Permission{PermWritePrincipal}, nil, (*handler).deleteUserSession)).Methods("DELETE")

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
	dbr.Handle("/_config",
		makeOfflineHandler(sc, adminPrivs, []Permission{PermUpdateDb}, nil, (*handler).handleGetDbConfig)).Methods("GET")
	dbr.Handle("/_config",
		makeOfflineHandler(sc, adminPrivs, []Permission{PermUpdateDb, PermConfigureSyncFn, PermConfigureAuth}, []Permission{PermUpdateDb, PermConfigureSyncFn, PermConfigureAuth}, (*handler).handlePutDbConfig)).Methods("PUT", "POST")

	keyspace.Handle("/_config/sync",
		makeOfflineHandler(sc, adminPrivs, []Permission{PermUpdateDb, PermConfigureSyncFn}, nil, (*handler).handleGetCollectionConfigSync)).Methods("GET")
	keyspace.Handle("/_config/sync",
		makeOfflineHandler(sc, adminPrivs, []Permission{PermUpdateDb, PermConfigureSyncFn}, nil, (*handler).handlePutCollectionConfigSync)).Methods("PUT")
	keyspace.Handle("/_config/sync",
		makeOfflineHandler(sc, adminPrivs, []Permission{PermUpdateDb, PermConfigureSyncFn}, nil, (*handler).handleDeleteCollectionConfigSync)).Methods("DELETE")
	keyspace.Handle("/_config/import_filter",
		makeOfflineHandler(sc, adminPrivs, []Permission{PermUpdateDb}, nil, (*handler).handleGetCollectionConfigImportFilter)).Methods("GET")
	keyspace.Handle("/_config/import_filter",
		makeOfflineHandler(sc, adminPrivs, []Permission{PermUpdateDb}, nil, (*handler).handlePutCollectionConfigImportFilter)).Methods("PUT")
	keyspace.Handle("/_config/import_filter",
		makeOfflineHandler(sc, adminPrivs, []Permission{PermUpdateDb}, nil, (*handler).handleDeleteCollectionConfigImportFilter)).Methods("DELETE")

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
	dbr.Handle("/_repair",
		makeHandler(sc, adminPrivs, []Permission{PermUpdateDb}, nil, (*handler).handleRepair)).Methods("POST")

	r.Handle("/_logging",
		makeHandler(sc, adminPrivs, []Permission{PermDevOps}, nil, (*handler).handleGetLogging)).Methods("GET")
	r.Handle("/_logging",
		makeHandler(sc, adminPrivs, []Permission{PermDevOps}, nil, (*handler).handleSetLogging)).Methods("PUT", "POST")

	r.Handle("/_config",
		makeHandler(sc, adminPrivs, []Permission{PermDevOps}, nil, (*handler).handleGetConfig)).Methods("GET")
	r.Handle("/_config",
		makeHandler(sc, adminPrivs, []Permission{PermDevOps}, nil, (*handler).handlePutConfig)).Methods("PUT")

	r.Handle("/_cluster_info",
		makeHandler(sc, adminPrivs, []Permission{PermDevOps}, nil, (*handler).handleGetClusterInfo)).Methods("GET")

	r.Handle("/_status",
		makeHandler(sc, adminPrivs, []Permission{PermDevOps}, nil, (*handler).handleGetStatus)).Methods("GET")

	r.Handle("/_sgcollect_info",
		makeHandler(sc, adminPrivs, []Permission{PermDevOps}, nil, (*handler).handleSGCollectStatus)).Methods("GET")
	r.Handle("/_sgcollect_info",
		makeHandler(sc, adminPrivs, []Permission{PermDevOps}, nil, (*handler).handleSGCollectCancel)).Methods("DELETE")
	r.Handle("/_sgcollect_info",
		makeHandler(sc, adminPrivs, []Permission{PermDevOps}, nil, (*handler).handleSGCollect)).Methods("POST")

	// Debugging handlers
	r.Handle("/_stats",
		makeHandler(sc, adminPrivs, []Permission{PermStatsExport}, nil, (*handler).handleStats)).Methods("GET")
	r.Handle(kDebugURLPathPrefix,
		makeSilentHandler(sc, adminPrivs, []Permission{PermStatsExport}, nil, (*handler).handleExpvar)).Methods("GET")
	r.Handle("/_profile/{profilename}",
		makeHandler(sc, adminPrivs, []Permission{PermDevOps}, nil, (*handler).handleProfiling)).Methods("POST")
	r.Handle("/_profile",
		makeHandler(sc, adminPrivs, []Permission{PermDevOps}, nil, (*handler).handleProfiling)).Methods("POST")
	r.Handle("/_heap",
		makeHandler(sc, adminPrivs, []Permission{PermDevOps}, nil, (*handler).handleHeapProfiling)).Methods("POST")
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

	// User query config APIs:
	if sc.Config.Unsupported.UserQueries != nil && *sc.Config.Unsupported.UserQueries {
		dbr.Handle("/_config/functions",
			makeOfflineHandler(sc, adminPrivs, []Permission{PermUpdateDb}, nil, (*handler).handleGetDbConfigFunctions)).Methods("GET")
		dbr.Handle("/_config/functions/{function}",
			makeOfflineHandler(sc, adminPrivs, []Permission{PermUpdateDb}, nil, (*handler).handleGetDbConfigFunction)).Methods("GET")
		dbr.Handle("/_config/functions",
			makeOfflineHandler(sc, adminPrivs, []Permission{PermUpdateDb}, nil, (*handler).handlePutDbConfigFunctions)).Methods("PUT", "DELETE")
		dbr.Handle("/_config/functions/{function}",
			makeOfflineHandler(sc, adminPrivs, []Permission{PermUpdateDb}, nil, (*handler).handlePutDbConfigFunction)).Methods("PUT", "DELETE")

		dbr.Handle("/_config/graphql",
			makeOfflineHandler(sc, adminPrivs, []Permission{PermUpdateDb}, nil, (*handler).handleGetDbConfigGraphQL)).Methods("GET")
		dbr.Handle("/_config/graphql",
			makeOfflineHandler(sc, adminPrivs, []Permission{PermUpdateDb}, nil, (*handler).handlePutDbConfigGraphQL)).Methods("PUT", "DELETE")
	}

	// The routes below are part of the CouchDB REST API but should only be available to admins,
	// so the handlers are moved to the admin port.
	r.Handle("/{newdb:"+dbRegex+"}/",
		makeHandlerSpecificAuthScope(sc, adminPrivs, []Permission{PermCreateDb}, nil, (*handler).handleCreateDB, getAuthScopeHandleCreateDB)).Methods("PUT")
	r.Handle("/{db:"+dbRegex+"}/",
		makeMetadataDBOfflineHandler(sc, adminPrivs, []Permission{PermDeleteDb}, nil, (*handler).handleDeleteDB)).Methods("DELETE")

	r.Handle("/_all_dbs",
		makeHandler(sc, adminPrivs, []Permission{PermDevOps}, nil, (*handler).handleAllDbs)).Methods("GET", "HEAD")

	return r
}

// Prometheus Metrics API

// CreateMetricHandler Creates the HTTP handler for the prometheus metrics API of a gateway server.
func CreateMetricHandler(sc *ServerContext) http.Handler {
	router := CreateMetricRouter(sc)
	return wrapRouter(sc, metricsPrivs, router)
}

// createDiagnosticHandler Creates the HTTP handler for the diagnostic API of a gateway server.
func createDiagnosticHandler(sc *ServerContext) http.Handler {
	router := createDiagnosticRouter(sc)
	return wrapRouter(sc, adminPrivs, router)
}

func CreatePingRouter(sc *ServerContext) *mux.Router {
	r := mux.NewRouter()
	r.StrictSlash(true)
	r.Handle("/_ping", makeSilentHandler(sc, publicPrivs, nil, nil, (*handler).handlePing)).Methods("GET", "HEAD")
	return r
}

func CreateMetricRouter(sc *ServerContext) *mux.Router {
	r := CreatePingRouter(sc)

	r.Handle("/metrics", makeSilentHandler(sc, metricsPrivs, []Permission{PermStatsExport}, nil, (*handler).handleMetrics)).Methods("GET")
	r.Handle("/_metrics", makeSilentHandler(sc, metricsPrivs, []Permission{PermStatsExport}, nil, (*handler).handleMetrics)).Methods("GET")
	r.Handle(kDebugURLPathPrefix, makeSilentHandler(sc, metricsPrivs, []Permission{PermStatsExport}, nil, (*handler).handleExpvar)).Methods("GET")

	return r
}

func createDiagnosticRouter(sc *ServerContext) *mux.Router {
	r := CreatePingRouter(sc)
	dbr := r.PathPrefix("/{db:" + dbRegex + "}/").Subrouter()
	dbr.StrictSlash(true)
	keyspace := r.PathPrefix("/{keyspace:" + dbRegex + "}/").Subrouter()
	keyspace.StrictSlash(true)
	keyspace.Handle("/{docid:"+docRegex+"}/_all_channels", makeHandler(sc, adminPrivs, []Permission{PermReadAppData}, nil, (*handler).handleGetDocChannels)).Methods("GET")
	dbr.Handle("/_user/{name}/_all_channels",
		makeHandler(sc, adminPrivs, []Permission{PermReadPrincipal}, nil, (*handler).handleGetAllChannels)).Methods("GET")
	keyspace.Handle("/_sync", makeHandler(sc, adminPrivs, []Permission{PermReadAppData}, nil, (*handler).handleSyncFnDryRun)).Methods("GET")
	keyspace.Handle("/_import_filter", makeHandler(sc, adminPrivs, []Permission{PermReadAppData}, nil, (*handler).handleImportFilterDryRun)).Methods("GET")

	return r
}

// Returns a top-level HTTP handler for a Router. This adds behavior for URLs that don't
// match anything -- it handles the OPTIONS method as well as returning either a 404 or 405
// for URLs that don't match a route.
func wrapRouter(sc *ServerContext, privs handlerPrivs, router *mux.Router) http.Handler {
	return http.HandlerFunc(func(response http.ResponseWriter, rq *http.Request) {
		FixQuotedSlashes(rq)
		var match mux.RouteMatch
		if router.Match(rq, &match) {
			router.ServeHTTP(response, rq)
		} else {
			// Log the request
			h := newHandler(sc, privs, response, rq, handlerOptions{})
			h.logRequestLine()

			// Inject CORS if enabled and requested and not admin port
			// What methods would have matched?
			var options []string
			var keyspace string
			for _, method := range []string{"GET", "HEAD", "POST", "PUT", "DELETE"} {
				found, matchedKeyspace := wouldMatch(router, rq, method)
				if found {
					options = append(options, method)
					if keyspace == "" && matchedKeyspace != "" {
						keyspace = matchedKeyspace
					}
				}
			}

			cors := sc.Config.API.CORS
			dbName, _, _, _ := ParseKeyspace(keyspace)
			if dbName != "" {
				db, err := h.server.GetActiveDatabase(dbName)
				if err == nil {
					cors = db.CORS
				}
			}
			if cors != nil && privs != adminPrivs && privs != metricsPrivs {
				cors.AddResponseHeaders(rq, response)
			}
			if len(options) == 0 {
				h.writeStatus(http.StatusNotFound, "unknown URL")
			} else {
				// Add CORS headers for OPTIONS request, since these are never registered by muxer.
				response.Header().Add("Allow", strings.Join(options, ", "))
				if privs != adminPrivs && cors != nil && len(rq.Header["Origin"]) > 0 {
					response.Header().Add("Access-Control-Max-Age", strconv.Itoa(cors.MaxAge))
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

func FixQuotedSlashes(rq *http.Request) {
	uri := rq.RequestURI
	if docWithSlashPathRegex.MatchString(uri) {
		if stop := strings.IndexAny(uri, "?#"); stop >= 0 {
			uri = uri[0:stop]
		}
		rq.URL.Path = uri
	}
}

func wouldMatch(router *mux.Router, rq *http.Request, method string) (found bool, keyspace string) {
	savedMethod := rq.Method
	rq.Method = method
	defer func() { rq.Method = savedMethod }()
	var matchInfo mux.RouteMatch
	found = router.Match(rq, &matchInfo)
	// If a match is found, check for any db/keyspace path variable in the resolved match.  Some paths may
	// match routes with different path variables depending on the method.
	if found {
		matchVars := matchInfo.Vars
		if dbName, ok := matchVars["db"]; ok {
			keyspace = dbName
		} else if keyspaceName, ok := matchVars["keyspace"]; ok {
			keyspace = keyspaceName
		} else if targetDbName, ok := matchVars["targetdb"]; ok {
			keyspace = targetDbName
		} else if newDbName, ok := matchVars["newdb"]; ok {
			keyspace = newDbName
		}
	}
	return found, keyspace
}
