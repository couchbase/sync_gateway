///  Copyright 2012-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package rest

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"mime"
	"mime/multipart"
	"net/http"
	"net/url"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/couchbase/sync_gateway/auth"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db"
	"github.com/gorilla/mux"
	"github.com/pkg/errors"
)

const (
	minCompressibleJSONSize      = 1000
	sgcollectTokenInvalidRequest = "sgcollect token auth present in non-sgcollect request"
)

var _ http.Flusher = &CountedResponseWriter{}
var _ http.Flusher = &NonCountedResponseWriter{}
var _ http.Flusher = &EncodedResponseWriter{}

var _ http.Hijacker = &CountedResponseWriter{}
var _ http.Hijacker = &NonCountedResponseWriter{}

var ErrInvalidLogin = base.HTTPErrorf(http.StatusUnauthorized, "Invalid login")
var ErrLoginRequired = base.HTTPErrorf(http.StatusUnauthorized, "Login required")

// If set to true, JSON output will be pretty-printed.
var PrettyPrint bool = false

var lastSerialNum uint64 = 0

var kNotFoundError = base.HTTPErrorf(http.StatusNotFound, "missing")

var wwwAuthenticateHeader = `Basic realm="` + base.ProductNameString + `"`

// Admin API Auth Roles
type RouteRole struct {
	RoleName       string
	DatabaseScoped bool
}

const RoleBucketWildcard = "*"

var (
	MobileSyncGatewayRole = RouteRole{"mobile_sync_gateway", true}
	BucketFullAccessRole  = RouteRole{"bucket_full_access", true}
	BucketAdmin           = RouteRole{"bucket_admin", true}
	FullAdminRole         = RouteRole{"admin", false}
	ClusterAdminRole      = RouteRole{"cluster_admin", false}
	ReadOnlyAdminRole     = RouteRole{"ro_admin", false}
)

var BucketScopedEndpointRoles = []RouteRole{MobileSyncGatewayRole, BucketFullAccessRole, BucketAdmin, FullAdminRole}
var ClusterScopedEndpointRolesRead = []RouteRole{ReadOnlyAdminRole, ClusterAdminRole, FullAdminRole}
var ClusterScopedEndpointRolesWrite = []RouteRole{ClusterAdminRole, FullAdminRole}

// Encapsulates the state of handling an HTTP request.
type handler struct {
	server                *ServerContext
	rq                    *http.Request
	response              CountableResponseWriter
	status                int
	statusMessage         string
	requestBody           *CountedRequestReader
	db                    *db.Database
	collection            *db.DatabaseCollectionWithUser
	user                  auth.User
	authorizedAdminUser   string
	privs                 handlerPrivs
	startTime             time.Time
	serialNumber          uint64
	formattedSerialNumber string
	loggedDuration        bool
	runOffline            bool       // allows running on an offline database
	allowNilDBContext     bool       // allow access to a database based only on name, looking up in metadata registry
	queryValues           url.Values // Copy of results of rq.URL.Query()
	permissionsResults    map[string]bool
	authScopeFunc         authScopeFunc
	httpLogLevel          *base.LogLevel // if set, always log HTTP information at this level, instead of the default
	rqCtx                 context.Context
	serverType            serverType
	sgcollect             bool // is called by sgcollect
}

type authScopeFunc func(context.Context, *handler) (string, error)

type handlerPrivs int

const (
	regularPrivs = iota // Handler requires valid authentication
	publicPrivs         // Handler Handler checks auth and falls back to guest if invalid or missing
	adminPrivs          // Handler ignores auth, always runs with root/admin privs
	metricsPrivs
)

// silentRequestLogLevel is the log level used for handlers that are silent, meaning they only log at trace level
const silentRequestLogLevel = base.LevelTrace

type handlerMethod func(*handler) error

// makeHandlerWithOptions creates an http.Handler that will run a handler with the given method handlerOptions
func makeHandlerWithOptions(server *ServerContext, privs handlerPrivs, accessPermissions []Permission, responsePermissions []Permission, method handlerMethod, options handlerOptions) http.Handler {
	return http.HandlerFunc(func(r http.ResponseWriter, rq *http.Request) {
		serverType := getCtxServerType(rq.Context())
		h := newHandler(server, privs, serverType, r, rq, options)
		err := h.invoke(method, accessPermissions, responsePermissions)
		h.writeError(err)
		if !options.skipLogDuration {
			h.logDuration(true)
		}
		h.reportDbStats()
	})
}

// makeHandler creates an http.Handler that will run a handler with the given method
func makeHandler(server *ServerContext, privs handlerPrivs, accessPermissions []Permission, responsePermissions []Permission, method handlerMethod) http.Handler {
	return makeHandlerWithOptions(server, privs, accessPermissions, responsePermissions, method, handlerOptions{})
}

// makeSilentHandler creates an http.Handler that will run a handler with the given method only logging at debug
func makeSilentHandler(server *ServerContext, privs handlerPrivs, accessPermissions []Permission, responsePermissions []Permission, method handlerMethod) http.Handler {
	return makeHandlerWithOptions(server, privs, accessPermissions, responsePermissions, method, handlerOptions{
		httpLogLevel: base.Ptr(silentRequestLogLevel),
	})
}

// Creates an http.Handler that will run a handler with the given method even if the target DB is offline
func makeOfflineHandler(server *ServerContext, privs handlerPrivs, accessPermissions []Permission, responsePermissions []Permission, method handlerMethod) http.Handler {
	return makeHandlerWithOptions(server, privs, accessPermissions, responsePermissions, method, handlerOptions{
		runOffline: true,
	})
}

// makeMetadataDBOfflineHandler creates an http.Handler that will run a handler with the given method even if the target DB couldn't be instantiated
func makeMetadataDBOfflineHandler(server *ServerContext, privs handlerPrivs, accessPermissions []Permission, responsePermissions []Permission, method handlerMethod) http.Handler {
	return makeHandlerWithOptions(server, privs, accessPermissions, responsePermissions, method, handlerOptions{
		runOffline:        true,
		allowNilDBContext: true,
		skipLogDuration:   true,
	})
}

// makeHandlerSpecificAuthScope creates an http.Handler that will run a handler with the given method. It also takes in a callback function which when
// given the endpoint payload returns an auth scope.
func makeHandlerSpecificAuthScope(server *ServerContext, privs handlerPrivs, accessPermissions []Permission, responsePermissions []Permission, method handlerMethod, authScopeFunc authScopeFunc) http.Handler {
	return makeHandlerWithOptions(server, privs, accessPermissions, responsePermissions, method, handlerOptions{
		authScopeFunc: authScopeFunc,
	})
}

// handlerOptions defines behaviour for a given handler's initialization and invocation
type handlerOptions struct {
	runOffline        bool           // if true, allow handler to run when a database is offline
	allowNilDBContext bool           // if true, allow a db-scoped handler to be invoked with a nil dbContext in cases where the database config exists but has an error preventing dbContext initialization"
	skipLogDuration   bool           // if true, will skip logging HTTP response status/duration
	authScopeFunc     authScopeFunc  // if set, this callback function will be used to set the auth scope for a given request body
	httpLogLevel      *base.LogLevel // if set, log HTTP requests to this handler at this level, instead of the usual info level
	sgcollect         bool           // if true, this handler is being invoked as part of sgcollect
}

func newHandler(server *ServerContext, privs handlerPrivs, serverType serverType, r http.ResponseWriter, rq *http.Request, options handlerOptions) *handler {
	h := &handler{
		server:            server,
		privs:             privs,
		rq:                rq,
		response:          NewNonCountedResponseWriter(r),
		status:            http.StatusOK,
		serialNumber:      atomic.AddUint64(&lastSerialNum, 1),
		startTime:         time.Now(),
		runOffline:        options.runOffline,
		allowNilDBContext: options.allowNilDBContext,
		authScopeFunc:     options.authScopeFunc,
		httpLogLevel:      options.httpLogLevel,
		serverType:        serverType,
		sgcollect:         options.sgcollect,
	}

	// initialize h.rqCtx
	_ = h.ctx()

	return h
}

// ctx returns the request-scoped context for logging/cancellation.
func (h *handler) ctx() context.Context {
	if h.rqCtx == nil {
		serverAddr, err := h.getServerAddr()
		if err != nil {
			base.AssertfCtx(h.rq.Context(), "Error getting server address: %v", err)
		}
		ctx := base.RequestLogCtx(h.rq.Context(), base.RequestData{
			CorrelationID:     h.formatSerialNumber(),
			RequestHost:       serverAddr,
			RequestRemoteAddr: h.rq.RemoteAddr,
		})
		h.rqCtx = ctx
	}
	return h.rqCtx
}

func (h *handler) getServerAddr() (string, error) {
	return h.server.getServerAddr(h.serverType)
}

func (h *handler) addDatabaseLogContext(dbName string, logConfig *base.DbLogConfig) {
	if dbName != "" {
		h.rqCtx = base.DatabaseLogCtx(h.ctx(), dbName, logConfig)
	}
}

func (h *handler) addCollectionLogContext(scopeName, collectionName string) {
	if scopeName != "" && collectionName != "" {
		h.rqCtx = base.CollectionLogCtx(h.ctx(), scopeName, collectionName)
	}
}

// ParseKeyspace will return a db, scope and collection for a given '.' separated keyspace string.
// Returns nil for scope and/or collection if not present in the keyspace string.
func ParseKeyspace(ks string) (db string, scope, collection *string, err error) {
	parts := strings.Split(ks, base.ScopeCollectionSeparator)
	switch len(parts) {
	case 1:
		db = parts[0]
	case 2:
		db = parts[0]
		collection = &parts[1]
	case 3:
		db = parts[0]
		scope = &parts[1]
		collection = &parts[2]
	default:
		return "", nil, nil, fmt.Errorf("unknown keyspace format: %q - expected 1-3 fields", ks)
	}

	// make sure all declared fields have stuff in them
	if db == "" ||
		collection != nil && *collection == "" ||
		scope != nil && *scope == "" {
		return "", nil, nil, fmt.Errorf("keyspace fields cannot be empty: %q", ks)
	}

	return db, scope, collection, nil
}

// Top-level handler call. It's passed a pointer to the specific method to run.
func (h *handler) invoke(method handlerMethod, accessPermissions []Permission, responsePermissions []Permission) error {
	if h.server.Config.API.CompressResponses == nil || *h.server.Config.API.CompressResponses {
		var stat *base.SgwIntStat
		if h.shouldUpdateBytesTransferredStats() {
			stat = h.db.DbStats.Database().PublicRestBytesWritten
		}
		if encoded := NewEncodedResponseWriter(h.response, h.rq, stat, defaultBytesStatsReportingInterval); encoded != nil {
			h.response = encoded
			defer encoded.Close()
		}
	}

	err := h.validateAndWriteHeaders(method, accessPermissions, responsePermissions)
	if err != nil {
		return err
	}

	return method(h) // Call the actual handler code
}

// shouldCheckAdminRBAC returns true if the request needs to check the server for permissions to run
func (h *handler) shouldCheckAdminRBAC() bool {
	sgcollectToken := h.server.SGCollect.getToken(h.rq.Header)
	if sgcollectToken != "" && h.sgcollect && h.server.SGCollect.hasValidToken(h.ctx(), sgcollectToken) {
		return false
	}
	if h.privs == adminPrivs && *h.server.Config.API.AdminInterfaceAuthentication {
		if sgcollectToken != "" && !h.sgcollect {
			base.AssertfCtx(h.ctx(), sgcollectTokenInvalidRequest)
		}
		return true
	} else if h.privs == metricsPrivs && *h.server.Config.API.MetricsInterfaceAuthentication {
		return true
	}
	return false
}

// validateAndWriteHeaders sets up handler.db and validates the permission of the user and returns an error if there is not permission.
func (h *handler) validateAndWriteHeaders(method handlerMethod, accessPermissions []Permission, responsePermissions []Permission) (err error) {
	var isRequestLogged bool
	defer func() {
		if !isRequestLogged {
			h.logRequestLine()
		}
		h.logRESTCount()

		// Perform request logging unless it's a silent endpoint
		if h.httpLogLevel == nil {
			auditFields := base.AuditFields{
				base.AuditFieldHTTPMethod: h.rq.Method,
				base.AuditFieldHTTPPath:   h.rq.URL.Path,
				base.AuditFieldHTTPStatus: h.status,
			}
			switch h.serverType {
			case publicServer:
				base.Audit(h.ctx(), base.AuditIDPublicHTTPAPIRequest, auditFields)
			case adminServer:
				base.Audit(h.ctx(), base.AuditIDAdminHTTPAPIRequest, auditFields)
			case metricsServer:
				base.Audit(h.ctx(), base.AuditIDMetricsHTTPAPIRequest, auditFields)
			}
		}
	}()

	defer func() {
		// Now that we know the DB, add CORS headers to the response:
		if h.privs != adminPrivs && h.privs != metricsPrivs {
			cors := h.getCORSConfig()
			if !cors.IsEmpty() {
				cors.AddResponseHeaders(h.rq, h.response)
			}
		}
	}()

	if h.server.Config.Unsupported.AuditInfoProvider != nil && h.server.Config.Unsupported.AuditInfoProvider.RequestInfoHeaderName != nil {
		auditLoggingFields := h.rq.Header.Get(*h.server.Config.Unsupported.AuditInfoProvider.RequestInfoHeaderName)
		if auditLoggingFields != "" {
			var fields base.AuditFields
			err := base.JSONUnmarshal([]byte(auditLoggingFields), &fields)
			if err != nil {
				base.WarnfCtx(h.ctx(), "Error unmarshalling audit logging fields: %v", err)
			} else {
				h.rqCtx = base.AuditLogCtx(h.ctx(), fields)
			}
		}
	}

	if h.server.Config.Unsupported.EffectiveUserHeaderName != nil {
		effectiveUserFields := h.rq.Header.Get(*h.server.Config.Unsupported.EffectiveUserHeaderName)
		if effectiveUserFields != "" {
			var fields base.EffectiveUserPair
			err := base.JSONUnmarshal([]byte(effectiveUserFields), &fields)
			if err != nil {
				base.WarnfCtx(h.ctx(), "Error unmarshalling effective user header fields: %v", err)
			} else {
				h.rqCtx = base.EffectiveUserIDLogCtx(h.ctx(), fields.Domain, fields.UserID)
			}
		}
	}

	switch h.rq.Header.Get("Content-Encoding") {
	case "":
		h.requestBody = NewReaderCounter(h.rq.Body)
	case "gzip":
		// gzip encoding will get approximate bytes read stat
		var err error
		h.requestBody = NewReaderCounter(nil)
		gzipRequestReader, err := gzip.NewReader(h.rq.Body)
		if err != nil {
			return err
		}
		h.requestBody.reader = gzipRequestReader
		h.rq.Header.Del("Content-Encoding") // to prevent double decoding later on
	default:
		return base.HTTPErrorf(http.StatusUnsupportedMediaType, "Unsupported Content-Encoding; use gzip")
	}

	if h.shouldShowProductVersion() {
		h.setHeader("Server", base.VersionString)
	} else {
		h.setHeader("Server", base.ProductNameString)
	}

	// If an Admin Request and admin auth enabled or a metrics request with metrics auth enabled we need to check the
	// user credentials
	shouldCheckAdminAuth := h.shouldCheckAdminRBAC()

	// If admin/metrics endpoint but auth not enabled, set admin_noauth log ctx
	if !shouldCheckAdminAuth && (h.serverType == adminServer || h.serverType == metricsServer) {
		h.rqCtx = base.UserLogCtx(h.ctx(), base.UserSyncGatewayAdmin, base.UserDomainSyncGatewayAdmin, nil)
	}

	keyspaceDb := h.PathVar("db")
	var keyspaceScope, keyspaceCollection *string

	// If there is a "keyspace" path variable in the route, parse the keyspace:
	ks := h.PathVar("keyspace")
	explicitCollectionLogging := false
	if ks != "" {
		var err error
		keyspaceDb, keyspaceScope, keyspaceCollection, err = ParseKeyspace(ks)
		if err != nil {
			return err
		}
		// If the collection is known, add it to the context before getting the db. If we do not know it, or don't know scope, we'll add this information later.
		if keyspaceCollection != nil {
			explicitCollectionLogging = true
			// /db.collectionName/foo is valid since Sync Gateway only has one scope
			scope := ""
			if keyspaceScope != nil {
				scope = *keyspaceScope
			}
			h.addCollectionLogContext(scope, *keyspaceCollection)
		}
	}

	var dbContext *db.DatabaseContext

	var bucketName string

	// look up the database context:
	if keyspaceDb != "" {
		var err error
		if dbContext, err = h.server.GetActiveDatabase(keyspaceDb); err != nil {
			h.addDatabaseLogContext(keyspaceDb, nil)
			if err == base.ErrNotFound {
				if shouldCheckAdminAuth {
					// Check if authenticated before attempting to get inactive database
					authorized, err := h.checkAdminAuthenticationOnly()
					if err != nil {
						return err
					}
					if !authorized {
						return ErrInvalidLogin
					}
				}
				var dbConfigFound bool
				dbContext, dbConfigFound, err = h.server.GetInactiveDatabase(h.ctx(), keyspaceDb)
				if err != nil {
					if httpError, ok := err.(*base.HTTPError); ok && httpError.Status == http.StatusNotFound {
						if shouldCheckAdminAuth && (!h.allowNilDBContext || !dbConfigFound) {
							return base.HTTPErrorf(http.StatusForbidden, "")
						} else if h.privs == regularPrivs || h.privs == publicPrivs {
							if !h.providedAuthCredentials() {

								return ErrLoginRequired
							}
							return ErrInvalidLogin
						}
					}
					if !h.allowNilDBContext || !dbConfigFound {
						base.InfofCtx(h.ctx(), base.KeyHTTP, "Error trying to get db %s: %v", base.MD(keyspaceDb), err)
						return err
					}
					bucketName, _ = h.server.bucketNameFromDbName(h.ctx(), keyspaceDb)
				}
			} else {
				return err
			}
		}
	}

	// If this call is in the context of a DB make sure the DB is in a valid state
	if dbContext != nil {
		h.addDatabaseLogContext(keyspaceDb, dbContext.Options.LoggingConfig)
		if !h.runOffline {
			// get a read lock on the dbContext
			// When the lock is returned we know that the db state will not be changed by
			// any other call

			// defer releasing the dbContext until after the handler method returns, unless it's a blipsync request
			if !h.isBlipSync() {
				dbContext.AccessLock.RLock()
				defer dbContext.AccessLock.RUnlock()
			}

			dbState := atomic.LoadUint32(&dbContext.State)

			// if dbState == db.DBOnline, continue flow and invoke the handler method
			if dbState == db.DBOffline {
				// DB is offline, only handlers with runOffline true can run in this state
				return base.HTTPErrorf(http.StatusServiceUnavailable, "DB is currently under maintenance")
			} else if dbState != db.DBOnline {
				// DB is in transition state, no calls will be accepted until it is Online or Offline state
				return base.HTTPErrorf(http.StatusServiceUnavailable, "DB is %v - try again later", db.RunStateString[dbState])
			}
		}
	}

	// Authenticate, if not on admin port:
	if h.privs != adminPrivs {
		var err error
		if err = h.checkPublicAuth(dbContext); err != nil {
			// if auth fails we still need to record bytes read over the rest api for the stat, to do this we need to call GetBodyBytesCount to read
			// the body to populate the bytes count on the CountedRequestReader struct
			bytesCount := h.requestBody.GetBodyBytesCount()
			if bytesCount > 0 {
				dbContext.DbStats.DatabaseStats.PublicRestBytesRead.Add(bytesCount)
			}
			return err
		}
		if h.user != nil && h.user.Name() == "" && dbContext != nil && dbContext.IsGuestReadOnly() {
			// Prevent read-only guest access to any endpoint requiring write permissions except
			// blipsync.  Read-only guest handling for websocket replication (blipsync) is evaluated
			// at the blip message level to support read-only pull replications.
			if requiresWritePermission(accessPermissions) && !h.isBlipSync() {
				return base.HTTPErrorf(http.StatusForbidden, auth.GuestUserReadOnly)
			}
		}
	}

	// If the user has OIDC roles/channels configured, we need to check if the OIDC issuer they came from is still valid.
	// Note: checkPublicAuth already does this check if the user authenticates with a bearer token, but we still need to recheck
	// for users using session tokens / basic auth. However, updatePrincipal will be idempotent.
	if h.user != nil && h.user.JWTIssuer() != "" {
		updates := checkJWTIssuerStillValid(h.ctx(), dbContext, h.user)
		if updates != nil {
			_, _, err := dbContext.UpdatePrincipal(h.ctx(), updates, true, true)
			if err != nil {
				return fmt.Errorf("failed to revoke stale OIDC roles/channels: %w", err)
			}
			// TODO: could avoid this extra fetch if UpdatePrincipal returned the new principal
			h.user, err = dbContext.Authenticator(h.ctx()).GetUser(*updates.Name)
			if err != nil {
				return err
			}
		}
	}

	if shouldCheckAdminAuth {
		// If server is walrus but auth is enabled we should just kick the user out as invalid as we have nothing to
		// validate credentials against
		if base.ServerIsWalrus(h.server.Config.Bootstrap.Server) {
			return base.HTTPErrorf(http.StatusUnauthorized, "Authorization not possible with Walrus server. "+
				"Either use Couchbase Server or disable admin auth by setting api.admin_interface_authentication and api.metrics_interface_authentication to false.")
		}

		username, password := h.getBasicAuth()
		if username == "" {
			if dbContext == nil || dbContext.Options.SendWWWAuthenticateHeader == nil || *dbContext.Options.SendWWWAuthenticateHeader {
				h.response.Header().Set("WWW-Authenticate", wwwAuthenticateHeader)
			}
			base.Audit(h.ctx(), base.AuditIDAdminUserAuthenticationFailed, base.AuditFields{base.AuditFieldUserName: username})
			return ErrLoginRequired
		}

		var managementEndpoints []string
		var httpClient *http.Client
		var authScope string

		var err error
		if dbContext != nil {
			managementEndpoints, httpClient, err = dbContext.ObtainManagementEndpointsAndHTTPClient(h.ctx())
			authScope = dbContext.Bucket.GetName()
		} else {
			managementEndpoints, httpClient, err = h.server.ObtainManagementEndpointsAndHTTPClient()
			authScope = bucketName
		}
		if err != nil {
			base.WarnfCtx(h.ctx(), "An error occurred whilst obtaining management endpoints: %v", err)
			return base.HTTPErrorf(http.StatusInternalServerError, "")
		}
		if h.authScopeFunc != nil {
			authScope, err = h.authScopeFunc(h.ctx(), h)
			if err != nil {
				return base.HTTPErrorf(http.StatusInternalServerError, "Unable to read body: %v", err)
			}
			if authScope == "" {
				return base.HTTPErrorf(http.StatusBadRequest, "Unable to determine auth scope for endpoint")
			}
		}

		permissions, statusCode, err := checkAdminAuth(h.ctx(), authScope, username, password, h.rq.Method, httpClient,
			managementEndpoints, *h.server.Config.API.EnableAdminAuthenticationPermissionsCheck, accessPermissions,
			responsePermissions)
		if err != nil {
			base.WarnfCtx(h.ctx(), "An error occurred whilst checking whether a user was authorized: %v", err)
			return base.HTTPErrorf(http.StatusInternalServerError, "")
		}

		if statusCode != http.StatusOK {
			base.InfofCtx(h.ctx(), base.KeyAuth, "%s: User %s failed to auth as an admin statusCode: %d", h.formatSerialNumber(), base.UD(username), statusCode)
			if statusCode == http.StatusUnauthorized {
				base.Audit(h.ctx(), base.AuditIDAdminUserAuthenticationFailed, base.AuditFields{base.AuditFieldUserName: username})
				return ErrInvalidLogin
			}
			base.Audit(h.ctx(), base.AuditIDAdminUserAuthorizationFailed, base.AuditFields{base.AuditFieldUserName: username})
			return base.HTTPErrorf(statusCode, "")
		}
		// even though `checkAdminAuth` _can_ issue whoami to find user's roles, it doesn't always...
		// to reduce code complexity, we'll potentially be making this whoami request twice if we need it for audit filtering
		auditRoleNames := getCBUserRolesForAudit(dbContext, authScope, h.ctx(), httpClient, managementEndpoints, username, password)

		h.authorizedAdminUser = username
		h.permissionsResults = permissions
		h.rqCtx = base.UserLogCtx(h.ctx(), username, base.UserDomainCBServer, auditRoleNames)
		// query auditRoleNames above even if this is a silent request can need "real_userid" on a ctx. Example: /_expvar should not log AuditIDAdminUserAuthenticated but it should log AuditIDSyncGatewayStats
		if !h.isSilentRequest() {
			base.Audit(h.ctx(), base.AuditIDAdminUserAuthenticated, base.AuditFields{})
		}
		base.DebugfCtx(h.ctx(), base.KeyAuth, "%s: User %s was successfully authorized as an admin", h.formatSerialNumber(), base.UD(username))
	} else {
		// If admin auth is not enabled we should set any responsePermissions to true so that any handlers checking for
		// these still pass
		h.permissionsResults = make(map[string]bool)
		for _, responsePermission := range responsePermissions {
			h.permissionsResults[responsePermission.PermissionName] = true
		}
	}

	// Collection keyspace handling
	if ks != "" {
		ksNotFound := base.HTTPErrorf(http.StatusNotFound, "keyspace %s not found", ks)
		if dbContext.Scopes != nil {
			// endpoint like /db/doc where this matches /db._default._default/
			// the check whether the _default._default actually exists on this database is performed below
			if keyspaceScope == nil && keyspaceCollection == nil {
				keyspaceScope = base.Ptr(base.DefaultScope)
				keyspaceCollection = base.Ptr(base.DefaultCollection)
			}
			// endpoint like /db.collectionName/doc where this matches /db.scopeName.collectionName/doc because there's a single scope defined
			if keyspaceScope == nil {
				if len(dbContext.Scopes) == 1 {
					for scopeName, _ := range dbContext.Scopes {
						keyspaceScope = base.Ptr(scopeName)
					}

				} else {
					// There are multiple scopes on a DatabaseContext. This isn't allowed in Sync Gateway but if the feature becomes available, it will be ambiguous which scope to match.
					return ksNotFound
				}
			}
			scope, foundScope := dbContext.Scopes[*keyspaceScope]
			if !foundScope {
				return ksNotFound
			}

			_, foundCollection := scope.Collections[*keyspaceCollection]
			if !foundCollection {
				return ksNotFound
			}
		} else {
			if keyspaceScope != nil && *keyspaceScope != base.DefaultScope || keyspaceCollection != nil && *keyspaceCollection != base.DefaultCollection {
				// request tried specifying a named collection on a non-named collections database
				return ksNotFound
			}
			// Set these for handlers that expect a scope/collection to be set, even if not using named collections.
			keyspaceScope = base.Ptr(base.DefaultScope)
			keyspaceCollection = base.Ptr(base.DefaultCollection)
		}
		// explicitCollectionLogging is true if the collection was explicitly set in the keyspace string. When it is used, the log context will add a col:collection in log lines. If it is implicit, in the case of /db/doc, col: is omitted from the log information, but retained for audit logging purposes.
		if explicitCollectionLogging {
			// matches the following:
			//  - /db.scopeName.collectionName/doc
			//  - /db.collectionName/doc
			//  - /db._default/doc
			//  - /db._default._default/doc
			h.addCollectionLogContext(*keyspaceScope, *keyspaceCollection)
		} else {
			h.rqCtx = base.ImplicitDefaultCollectionLogCtx(h.ctx())
		}
	}

	h.logRequestLine()
	isRequestLogged = true

	// Now set the request's Database (i.e. context + user)
	if dbContext != nil {
		var err error
		h.db, err = db.GetDatabase(dbContext, h.user)

		if err != nil {
			return err
		}
		if ks != "" {
			var err error
			h.collection, err = h.db.GetDatabaseCollectionWithUser(*keyspaceScope, *keyspaceCollection)
			if err != nil {
				return err
			}
		}
	}
	h.updateResponseWriter()
	// ensure wrapped ResponseWriter implements http.Flusher
	_, ok := h.response.(http.Flusher)
	if !ok {
		return fmt.Errorf("http.ResponseWriter %T does not implement Flusher interface", h.response)
	}
	return nil
}

func getCBUserRolesForAudit(db *db.DatabaseContext, authScope string, ctx context.Context, httpClient *http.Client, managementEndpoints []string, username, password string) []string {
	if !needRolesForAudit(db, base.UserDomainCBServer) {
		return nil
	}

	whoAmIResponse, whoAmIStatus, whoAmIErr := cbRBACWhoAmI(ctx, httpClient, managementEndpoints, username, password)
	if whoAmIErr != nil || whoAmIStatus != http.StatusOK {
		base.WarnfCtx(ctx, "An error occurred whilst fetching the user's roles for audit filtering - will not filter: %v", whoAmIErr)
		return nil
	}

	var auditRoleNames []string
	for _, role := range whoAmIResponse.Roles {
		// only filter roles applied to this bucket or cluster-scope
		if role.BucketName == "" || role.BucketName == authScope {
			auditRoleNames = append(auditRoleNames, role.RoleName)
		}
	}
	return auditRoleNames
}

// removeCorruptConfigIfExists will remove the config from the bucket and remove it from the map if it exists on the invalid database config map
func (h *handler) removeCorruptConfigIfExists(ctx context.Context, bucket, configGroupID, dbName string) error {
	_, ok := h.server.invalidDatabaseConfigTracking.exists(dbName)
	if !ok {
		// exit early of it doesn't exist
		return nil
	}
	// remove the bad config from the bucket
	err := h.server.BootstrapContext.DeleteConfig(ctx, bucket, configGroupID, dbName)
	if err != nil && !base.IsDocNotFoundError(err) {
		return err
	}
	// delete the database name form the invalid database map on server context
	h.server.invalidDatabaseConfigTracking.remove(dbName)

	return nil
}

// isSilentRequest returns true if the handler represents a request we should suppress http logging on.
func (h *handler) isSilentRequest() bool {
	return h.httpLogLevel != nil && *h.httpLogLevel == silentRequestLogLevel
}

func (h *handler) logRequestLine() {

	logLevel := base.LevelInfo
	if h.httpLogLevel != nil {
		logLevel = *h.httpLogLevel
	}

	// Check Log Level first, as SanitizeRequestURL is expensive to evaluate.
	if !base.LogLevelEnabled(h.ctx(), logLevel, base.KeyHTTP) {
		return
	}

	proto := ""
	if h.rq.ProtoMajor >= 2 {
		proto = " HTTP/2"
	}

	queryValues := h.getQueryValues()

	base.LogLevelCtx(h.ctx(), logLevel, base.KeyHTTP, "%s %s%s%s", h.rq.Method, base.SanitizeRequestURL(h.rq, &queryValues), proto, h.formattedEffectiveUserName())
}

// shouldUpdateBytesTransferredStats returns true if we want to log the bytes transferred. The criteria is if this is db scoped over the public port. Blip is skipped since those stats are tracked separately.
func (h *handler) shouldUpdateBytesTransferredStats() bool {
	if h.db == nil {
		return false
	}
	if h.serverType != publicServer {
		return false
	}
	if h.isBlipSync() {
		return false
	}
	return true
}

// updateResponseWriter will create an updated Response Writer if we need to log db stats.
func (h *handler) updateResponseWriter() {
	if !h.shouldUpdateBytesTransferredStats() {
		return
	}
	h.response = NewCountedResponseWriter(h.response,
		h.db.DbStats.Database().PublicRestBytesWritten,
		defaultBytesStatsReportingInterval)
}

func (h *handler) logDuration(realTime bool) {
	if h.loggedDuration {
		return
	}
	h.loggedDuration = true

	var duration time.Duration
	if realTime {
		duration = time.Since(h.startTime)
	}

	// Log timings/status codes for errors under the HTTP log key
	// and the HTTPResp log key for everything else.
	logKey := base.KeyHTTPResp
	logLevel := base.LevelInfo

	// if error status, log at HTTP/Info
	if h.status >= 300 {
		logKey = base.KeyHTTP
	} else if h.httpLogLevel != nil {
		// if the handler requested a different log level, and the request was successful, we'll honour that log level.
		logLevel = *h.httpLogLevel
	}

	base.LogLevelCtx(h.ctx(), logLevel, logKey, "%s:     --> %d %s  (%.1f ms)",
		h.formatSerialNumber(), h.status, h.statusMessage,
		float64(duration)/float64(time.Millisecond),
	)
}

// logRESTCount will increment the number of public REST requests stat for public REST requests excluding _blipsync requests if they are attached to a database.
func (h *handler) logRESTCount() {
	if h.db == nil {
		return
	}
	if h.serverType != publicServer {
		return
	}
	if h.isBlipSync() {
		return
	}
	h.db.DbStats.DatabaseStats.NumPublicRestRequests.Add(1)
}

// reportDbStats will report the public rest request specific stats back to the database
func (h *handler) reportDbStats() {
	if !h.shouldUpdateBytesTransferredStats() {
		return
	}
	var bytesReadStat int64
	h.response.reportStats(true)
	// load the number of bytes read on the request
	bytesReadStat = h.requestBody.GetBodyBytesCount()
	// as this is int64 lets protect against a situation where a negative is returned from GetBodyBytesCount() function and thus decrementing the stat
	if bytesReadStat > 0 {
		h.db.DbStats.DatabaseStats.PublicRestBytesRead.Add(bytesReadStat)
	}
}

// logStatusWithDuration will log the request status and the duration of the request.
func (h *handler) logStatusWithDuration(status int, message string) {
	h.setStatus(status, message)
	h.logDuration(true)
}

// logStatus will log the request status, but NOT the duration of the request.
// This is used for indefinitely-long handlers like _changes that we don't want to track duration of
func (h *handler) logStatus(status int, message string) {
	h.setStatus(status, message)
	h.logDuration(false) // don't track actual time
}

func needRolesForAudit(db *db.DatabaseContext, domain base.UserIDDomain) bool {
	// early returns when auditing is disabled
	if db == nil ||
		db.Options.LoggingConfig == nil ||
		db.Options.LoggingConfig.Audit == nil ||
		!db.Options.LoggingConfig.Audit.Enabled {
		return false
	}

	// if we have any matching domains then we need the given roles
	// this loop should be ~free for len(0)
	for principal := range db.Options.LoggingConfig.Audit.DisabledRoles {
		if principal.Domain == string(domain) {
			return true
		}
	}

	return false
}

// getSGUserRolesForAudit returns a list of role names for the given user, if audit role filtering is enabled.
func getSGUserRolesForAudit(db *db.DatabaseContext, user auth.User) []string {
	if user == nil {
		return nil
	}

	if !needRolesForAudit(db, base.UserDomainSyncGateway) {
		return nil
	}

	roles := user.GetRoles()
	if len(roles) == 0 {
		return nil
	}

	roleNames := make([]string, 0, len(roles))
	for _, role := range roles {
		roleNames = append(roleNames, role.Name())
	}

	return roleNames
}

// checkPublicAuth verifies that the current request is authenticated for the given database.
//
// NOTE: checkPublicAuth is not used for the admin interface.
func (h *handler) checkPublicAuth(dbCtx *db.DatabaseContext) (err error) {

	h.user = nil
	if dbCtx == nil {
		return nil
	}

	var auditFields base.AuditFields

	// Record Auth stats
	defer func(t time.Time) {
		delta := time.Since(t).Nanoseconds()
		dbCtx.DbStats.Security().TotalAuthTime.Add(delta)
		if err != nil {
			dbCtx.DbStats.Security().AuthFailedCount.Add(1)
			if errors.Is(err, ErrInvalidLogin) {
				base.Audit(h.ctx(), base.AuditIDPublicUserAuthenticationFailed, auditFields)
			}
		} else {
			dbCtx.DbStats.Security().AuthSuccessCount.Add(1)

			username := ""
			if h.isGuest() {
				username = base.GuestUsername
			} else if h.user != nil {
				username = h.user.Name()
			}
			roleNames := getSGUserRolesForAudit(dbCtx, h.user)
			h.rqCtx = base.UserLogCtx(h.ctx(), username, base.UserDomainSyncGateway, roleNames)
			base.Audit(h.ctx(), base.AuditIDPublicUserAuthenticated, auditFields)
		}
	}(time.Now())

	// If oidc enabled, check for bearer ID token
	if dbCtx.Options.OIDCOptions != nil || len(dbCtx.LocalJWTProviders) > 0 {
		if token := h.getBearerToken(); token != "" {
			auditFields = base.AuditFields{base.AuditFieldAuthMethod: "bearer"}
			var updates auth.PrincipalConfig
			h.user, updates, err = dbCtx.Authenticator(h.ctx()).AuthenticateUntrustedJWT(token, dbCtx.OIDCProviders, dbCtx.LocalJWTProviders, h.getOIDCCallbackURL)
			if h.user == nil || err != nil {
				return ErrInvalidLogin
			}
			if issuer := h.user.JWTIssuer(); issuer != "" {
				auditFields["oidc_issuer"] = issuer
			}
			if changes := checkJWTIssuerStillValid(h.ctx(), dbCtx, h.user); changes != nil {
				updates = updates.Merge(*changes)
			}
			_, _, err := dbCtx.UpdatePrincipal(h.ctx(), &updates, true, true)
			if err != nil {
				return fmt.Errorf("failed to update OIDC user after sign-in: %w", err)
			}
			// TODO: could avoid this extra fetch if UpdatePrincipal returned the newly updated principal
			if updates.Name != nil {
				h.user, err = dbCtx.Authenticator(h.ctx()).GetUser(*updates.Name)
			}
			return err
		}

		/*
		* If unsupported/oidc testing is enabled
		* and this is a call on the token endpoint
		* and the username and password match those in the oidc default provider config
		* then authorize this request
		 */
		if strings.HasSuffix(h.rq.URL.Path, "/_oidc_testing/token") && dbCtx.Options.UnsupportedOptions != nil &&
			dbCtx.Options.UnsupportedOptions.OidcTestProvider != nil && dbCtx.Options.UnsupportedOptions.OidcTestProvider.Enabled {
			if username, password := h.getBasicAuth(); username != "" && password != "" {
				provider := dbCtx.Options.OIDCOptions.Providers.GetProviderForIssuer(h.ctx(), issuerUrlForDB(h, dbCtx.Name), testProviderAudiences)
				if provider != nil && provider.ValidationKey != nil {
					if base.ValDefault(provider.ClientID, "") == username && *provider.ValidationKey == password {
						auditFields = base.AuditFields{base.AuditFieldAuthMethod: "basic"}
						return nil
					}
				}
			}
		}
	}

	// Check basic auth first
	if !dbCtx.Options.DisablePasswordAuthentication {
		if userName, password := h.getBasicAuth(); userName != "" {
			auditFields = base.AuditFields{base.AuditFieldAuthMethod: "basic"}
			h.user, err = dbCtx.Authenticator(h.ctx()).AuthenticateUser(userName, password)
			if err != nil {
				return err
			}
			if h.user == nil {
				auditFields["username"] = userName
				if dbCtx.Options.SendWWWAuthenticateHeader == nil || *dbCtx.Options.SendWWWAuthenticateHeader {
					h.response.Header().Set("WWW-Authenticate", wwwAuthenticateHeader)
				}
				return ErrInvalidLogin
			}
			return nil
		}
	}

	// check smuggled websocket token before cookie, since this is sure to be more explicit than cookie
	if h.isBlipSync() {
		sessionID := h.getWebsocketToken()
		if sessionID != "" {
			var err error
			auditFields = base.AuditFields{base.AuditFieldAuthMethod: "websocket_token"}
			h.user, err = dbCtx.Authenticator(h.ctx()).AuthenticateOneTimeSession(h.ctx(), sessionID)
			return err
		}
	}

	// Check cookie
	auditFields = base.AuditFields{base.AuditFieldAuthMethod: "cookie"}
	h.user, err = dbCtx.Authenticator(h.ctx()).AuthenticateCookie(h.rq, h.response)
	if err != nil && h.privs != publicPrivs {
		return err
	} else if h.user != nil {
		return nil
	}

	// No auth given -- check guest access
	auditFields = base.AuditFields{base.AuditFieldAuthMethod: "guest"}
	if h.user, err = dbCtx.Authenticator(h.ctx()).GetUser(""); err != nil {
		return err
	}
	if h.privs == regularPrivs && h.user.Disabled() {
		if dbCtx.Options.SendWWWAuthenticateHeader == nil || *dbCtx.Options.SendWWWAuthenticateHeader {
			h.response.Header().Set("WWW-Authenticate", wwwAuthenticateHeader)
		}
		if h.providedAuthCredentials() {
			return ErrInvalidLogin
		}
		return ErrLoginRequired
	}

	return nil
}

// verifyWebsocketToken checks for a valid websocket token in the request headers. If present, invalidate the token so it can't be reused.
//
// Returns a nil error if there is no token on the header but no user object. If the header is found,
func (h *handler) getWebsocketToken() string {
	protocolHeaders := h.rq.Header.Values(secWebSocketProtocolHeader)
	var sessionID string
	for _, header := range protocolHeaders {
		if !strings.HasPrefix(header, blipSessionIDPrefix) {
			continue
		}
		sessionID = strings.TrimSpace(strings.TrimPrefix(header, blipSessionIDPrefix))
		break
	}
	if sessionID == "" {
		return ""
	}
	// Remove the websocket protocol so it doesn't get logged in BlipWebsocketServer.handshake
	h.rq.Header.Del(secWebSocketProtocolHeader)
	for _, header := range protocolHeaders {
		if !strings.HasPrefix(header, blipSessionIDPrefix) {
			h.rq.Header.Add(secWebSocketProtocolHeader, header)
		}
	}
	return sessionID
}

func checkJWTIssuerStillValid(ctx context.Context, dbCtx *db.DatabaseContext, user auth.User) *auth.PrincipalConfig {
	issuer := user.JWTIssuer()
	if issuer == "" {
		return nil
	}
	providerStillValid := false
	for _, provider := range dbCtx.OIDCProviders {
		// No need to verify audiences, as that was done when the user was authenticated
		if provider.Issuer == issuer {
			providerStillValid = true
			break
		}
	}
	for _, provider := range dbCtx.LocalJWTProviders {
		if provider.Issuer == issuer {
			providerStillValid = true
			break
		}
	}
	if !providerStillValid {
		base.InfofCtx(ctx, base.KeyAuth, "User %v uses OIDC issuer %v which is no longer configured. Revoking OIDC roles/channels.", base.UD(user.Name()), base.UD(issuer))
		return &auth.PrincipalConfig{
			Name:        base.Ptr(user.Name()),
			JWTIssuer:   base.Ptr(""),
			JWTRoles:    base.Set{},
			JWTChannels: base.Set{},
		}
	}
	return nil
}

// checkAdminAuthenticationOnly simply checks whether a username / password combination is authenticated pulling the
// credentials from the handler
func (h *handler) checkAdminAuthenticationOnly() (bool, error) {
	managementEndpoints, httpClient, err := h.server.ObtainManagementEndpointsAndHTTPClient()
	if err != nil {
		return false, base.HTTPErrorf(http.StatusInternalServerError, "Error getting management endpoints: %v", err)
	}

	username, password := h.getBasicAuth()
	if username == "" {
		h.response.Header().Set("WWW-Authenticate", wwwAuthenticateHeader)
		return false, ErrLoginRequired
	}

	statusCode, _, err := doHTTPAuthRequest(h.ctx(), httpClient, username, password, "POST", "/pools/default/checkPermissions", managementEndpoints, nil)
	if err != nil {
		return false, base.HTTPErrorf(http.StatusInternalServerError, "Error performing HTTP auth request: %v", err)
	}

	if statusCode == http.StatusUnauthorized {
		base.Audit(h.ctx(), base.AuditIDAdminUserAuthenticationFailed, base.AuditFields{base.AuditFieldUserName: username})
		return false, nil
	}

	return true, nil
}

func checkAdminAuth(ctx context.Context, bucketName, basicAuthUsername, basicAuthPassword string, attemptedHTTPOperation string, httpClient *http.Client, managementEndpoints []string, shouldCheckPermissions bool, accessPermissions []Permission, responsePermissions []Permission) (responsePermissionResults map[string]bool, statusCode int, err error) {
	anyResponsePermFailed := false
	permissionStatusCode, permResults, err := CheckPermissions(ctx, httpClient, managementEndpoints, bucketName, basicAuthUsername, basicAuthPassword, accessPermissions, responsePermissions)
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}
	if len(responsePermissions) > 0 {
		responsePermissionResults = permResults
		for _, permResult := range permResults {
			if !permResult {
				anyResponsePermFailed = true
				break
			}
		}
	}

	// If the user has not logged in correctly we shouldn't continue to do any more work and return
	if permissionStatusCode == http.StatusUnauthorized {
		return nil, permissionStatusCode, nil
	}

	if shouldCheckPermissions {
		// If user has required accessPerms and all response perms return with statusOK
		// Otherwise we need to fall through to continue as the user may have access to responsePermissions through roles.
		if permissionStatusCode == http.StatusOK && !anyResponsePermFailed {
			return responsePermissionResults, http.StatusOK, nil
		}

		// If status code was not 'ok' or 'forbidden' return
		// If user has authenticated correctly but is not authorized with all permissions. We'll fall through to try
		// with roles.
		if permissionStatusCode != http.StatusOK && permissionStatusCode != http.StatusForbidden {
			return responsePermissionResults, permissionStatusCode, nil
		}
	}

	var requestRoles []RouteRole
	if bucketName != "" {
		requestRoles = BucketScopedEndpointRoles
	} else {
		if attemptedHTTPOperation == http.MethodGet || attemptedHTTPOperation == http.MethodHead || attemptedHTTPOperation == http.MethodOptions {
			requestRoles = ClusterScopedEndpointRolesRead
		} else {
			requestRoles = ClusterScopedEndpointRolesWrite
		}
	}

	rolesStatusCode, err := CheckRoles(ctx, httpClient, managementEndpoints, basicAuthUsername, basicAuthPassword, requestRoles, bucketName)
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}

	// If a user has access through roles we're going to use this to mean they have access to all of the
	// responsePermissions too so we'll iterate over these and set them to true.
	if rolesStatusCode == http.StatusOK {
		responsePermissionResults = make(map[string]bool)
		for _, responsePerm := range responsePermissions {
			responsePermissionResults[responsePerm.PermissionName] = true
		}
		return responsePermissionResults, rolesStatusCode, nil
	}

	// We want to select the most 'optimistic' status code here.
	// If we got a status code 200 in the permissions check case but decided to check roles too (in the case where we
	// didn't have access to all the responsePermissions) and ended up getting a 403 for the role check we want to
	// return the 200 to allow the user handler access.

	// Start with role status code
	resultStatusCode := rolesStatusCode

	// If resultStatus code is not okay (role check did 401, 403 or 500) and we're supposed to be allowing users in
	// based on permissions we will select the code from the permission result as that may allow more access.
	if resultStatusCode != http.StatusOK && shouldCheckPermissions {
		resultStatusCode = permissionStatusCode
	}

	return permResults, resultStatusCode, nil
}

func (h *handler) assertAdminOnly() {
	if h.privs != adminPrivs {
		// TODO: CBG-1948
		panic("Admin-only handler called without admin privileges, on " + h.rq.RequestURI)
	}
}

func (h *handler) PathVar(name string) string {
	v := mux.Vars(h.rq)[name]

	// Escape special chars i.e. '+' otherwise they are removed by QueryUnescape()
	v = strings.Replace(v, "+", "%2B", -1)

	// Before routing the URL we explicitly disabled expansion of %-escapes in the path
	// (see function FixQuotedSlashes). So we have to unescape them now.
	v, _ = url.QueryUnescape(v)
	return v
}

func (h *handler) SetPathVar(name string, value string) {
	mux.Vars(h.rq)[name] = url.QueryEscape(value)
}

func (h *handler) getQueryValues() url.Values {
	if h.queryValues == nil {
		h.queryValues = h.rq.URL.Query()
	}
	return h.queryValues
}

func (h *handler) getQuery(query string) string {
	return h.getQueryValues().Get(query)
}

func (h *handler) getJSONStringQuery(query string) string {
	return base.ConvertJSONString(h.getQuery(query))
}

func (h *handler) getBoolQuery(query string) bool {
	result, _ := h.getOptBoolQuery(query, false)
	return result
}

func (h *handler) getOptBoolQuery(query string, defaultValue bool) (result, isSet bool) {
	q := h.getQuery(query)
	if q == "" {
		return defaultValue, false
	}
	return q == "true", true
}

// Returns the integer value of a URL query, defaulting to 0 if unparseable
func (h *handler) getIntQuery(query string, defaultValue uint64) (value uint64) {
	return base.GetRestrictedIntQuery(h.getQueryValues(), query, defaultValue, 0, 0, false)
}

func (h *handler) getJSONStringArrayQuery(param string) ([]string, error) {
	var strings []string
	value := h.getQuery(param)
	if value != "" {
		if err := base.JSONUnmarshal([]byte(value), &strings); err != nil {
			return nil, base.HTTPErrorf(http.StatusBadRequest, "%s URL param is not a JSON string array", param)
		}
	}
	return strings, nil
}

func (h *handler) userAgentIs(agent string) bool {
	userAgent := h.rq.Header.Get(base.HTTPHeaderUserAgent)
	return len(userAgent) > len(agent) && userAgent[len(agent)] == '/' && strings.HasPrefix(userAgent, agent)
}

// Returns true if the header exists, and its value does NOT match the given etag.
// (The etag parameter should not be double-quoted; the function will take care of that.)
func (h *handler) headerDoesNotMatchEtag(etag string) bool {
	value := h.rq.Header.Get("If-Match")
	return value != "" && !strings.Contains(value, `"`+etag+`"`)
}

func (h *handler) getEtag(headerName string) (string, error) {
	value := h.rq.Header.Get(headerName)
	if len(value) > 0 && !strings.Contains(value, `"`) {
		return "", fmt.Errorf("ETag value is not quoted when trying to read the value")
	}
	eTag := strings.Trim(value, `"`)
	return eTag, nil
}

// Returns the request body as a raw byte array.
func (h *handler) readBody() ([]byte, error) {
	return io.ReadAll(h.requestBody)
}

// Parses a JSON request body, returning it as a Body map.
func (h *handler) readJSON() (db.Body, error) {
	var body db.Body
	return body, h.readJSONInto(&body)
}

// Parses a JSON request body into a custom structure.
func (h *handler) readJSONInto(into any) error {
	return ReadJSONFromMIME(h.rq.Header, h.requestBody, into)
}

// readSanitizeJSONInto reads and sanitizes a JSON request body and returns DatabaseConfig.
// Expands environment variables (if any) referenced in the config.
func (h *handler) readSanitizeJSON(val any) ([]byte, error) {
	// Performs the Content-Type validation and Content-Encoding check.
	input, err := processContentEncoding(h.rq.Header, h.requestBody, "application/json")
	if err != nil {
		return nil, err
	}

	// Read body bytes to sanitize the content and substitute environment variables.
	defer func() { _ = input.Close() }()
	raw, err := io.ReadAll(input)
	if err != nil {
		return nil, err
	}

	content, err := sanitiseConfig(h.ctx(), raw, h.server.Config.Unsupported.AllowDbConfigEnvVars)
	if err != nil {
		return nil, err
	}

	// Decode the body bytes into target structure.
	decoder := base.JSONDecoder(bytes.NewReader(content))
	decoder.DisallowUnknownFields()
	decoder.UseNumber()
	err = decoder.Decode(&val)

	if err != nil {
		err = base.WrapJSONUnknownFieldErr(err)
		if errors.Cause(err) != base.ErrUnknownField {
			err = base.HTTPErrorf(http.StatusBadRequest, "Bad JSON: %s", err.Error())
		}
	}
	return raw, err
}

// readJavascript reads a javascript function from a request body.
func (h *handler) readJavascript() (string, error) {
	// Performs the Content-Type validation and Content-Encoding check.
	input, err := processContentEncoding(h.rq.Header, h.requestBody, "application/javascript")
	if err != nil {
		return "", err
	}

	defer func() { _ = input.Close() }()
	jsBytes, err := io.ReadAll(input)
	if err != nil {
		return "", err
	}

	return string(jsBytes), nil
}

// readSanitizeJSONInto reads and sanitizes a JSON request body and returns raw bytes and DbConfig.
// Expands environment variables (if any) referenced in the config.
func (h *handler) readSanitizeDbConfigJSON() ([]byte, *DbConfig, error) {
	var config DbConfig
	rawBytes, err := h.readSanitizeJSON(&config)
	if err != nil {
		if errors.Cause(base.WrapJSONUnknownFieldErr(err)) == base.ErrUnknownField {
			err = base.HTTPErrorf(http.StatusBadRequest, "JSON Unknown Field: %s", err.Error())
		}
	}
	return rawBytes, &config, err
}

// Reads & parses the request body, handling either JSON or multipart.
func (h *handler) readDocument() (db.Body, error) {
	contentType, attrs, _ := mime.ParseMediaType(h.rq.Header.Get("Content-Type"))
	switch contentType {
	case "", "application/json":
		return h.readJSON()
	case "multipart/related":
		reader := multipart.NewReader(h.requestBody, attrs["boundary"])
		return ReadMultipartDocument(reader)
	default:
		return nil, base.HTTPErrorf(http.StatusUnsupportedMediaType, "Invalid content type %s", contentType)
	}
}

func (h *handler) requestAccepts(mimetype string) bool {
	accept := h.rq.Header.Get("Accept")
	return accept == "" || strings.Contains(accept, mimetype) || strings.Contains(accept, "*/*")
}

func (h *handler) getBasicAuth() (username string, password string) {
	auth := h.rq.Header.Get("Authorization")
	if strings.HasPrefix(auth, "Basic ") {
		decoded, err := base64.StdEncoding.DecodeString(auth[6:])
		if err == nil {
			components := strings.SplitN(string(decoded), ":", 2)
			if len(components) == 2 {
				return components[0], components[1]
			}
		}
	}
	return
}

func (h *handler) getBearerToken() string {
	auth := h.rq.Header.Get("Authorization")
	if strings.HasPrefix(auth, "Bearer ") {
		token := auth[7:]
		return token
	}
	return ""
}

// providedAuthCredentials returns true if basic auth or session auth is enabled
func (h *handler) providedAuthCredentials() bool {
	username, _ := h.getBasicAuth()
	if username != "" {
		return true
	}
	token := h.getBearerToken()
	if token != "" {
		return true
	}
	cookieName := auth.DefaultCookieName
	if h.db != nil {
		authenticator := h.db.Authenticator(h.ctx())
		cookieName = authenticator.SessionCookieName
	}
	cookie, _ := h.rq.Cookie(cookieName)
	return cookie != nil
}

// taggedEffectiveUserName returns the tagged effective name of the user for the request.
// e.g: '<ud>alice</ud>' or 'GUEST'
func (h *handler) taggedEffectiveUserName() string {
	if h.authorizedAdminUser != "" {
		return base.UD(h.authorizedAdminUser).Redact() + " as ADMIN"
	}

	if h.privs == adminPrivs || h.privs == metricsPrivs {
		return "ADMIN"
	}

	if h.user == nil {
		return ""
	}

	if name := h.user.Name(); name != "" {
		return base.UD(name).Redact()
	}

	return base.GuestUsername
}

// formattedEffectiveUserName formats an effective name for appending to logs.
// e.g: 'Did xyz (as %s)' or 'Did xyz (as <ud>alice</ud>)'
func (h *handler) formattedEffectiveUserName() string {
	if name := h.taggedEffectiveUserName(); name != "" {
		return " (as " + name + ")"
	}

	return ""
}

// ////// RESPONSES:

func (h *handler) setHeader(name string, value string) {
	h.response.Header().Set(name, value)
}

// Adds an "Etag" header to the response, whose value is the parameter wrapped in double-quotes.
func (h *handler) setEtag(etag string) {
	h.setHeader("Etag", `"`+etag+`"`)
	// (Note: etags should not contain double-quotes (per RFC7232 2.3) so no escaping needed)
}

func (h *handler) setStatus(status int, message string) {
	h.status = status
	h.statusMessage = message
}

func (h *handler) disableResponseCompression() {
	switch r := h.response.(type) {
	case *EncodedResponseWriter:
		r.disableCompression()
	}
}

// Do not call from HTTP handlers. Use h.writeRawJSON/h.writeRawJSONStatus instead.
// writeRawJSONWithoutClientVerification takes the given bytes and always writes the response as JSON,
// without checking that the client can accept it.
func (h *handler) writeRawJSONWithoutClientVerification(status int, b []byte) {
	if h.rq.Method != "HEAD" {
		h.setHeader("Content-Type", "application/json")
		if len(b) < minCompressibleJSONSize {
			h.disableResponseCompression()
		}
		h.setHeader("Content-Length", fmt.Sprintf("%d", len(b)))
		if status > 0 {
			h.response.WriteHeader(status)
			h.setStatus(status, "")
		}
		_, _ = h.response.Write(b)
	} else if status > 0 {
		h.response.WriteHeader(status)
		h.setStatus(status, "")
	}
}

// writeJSON writes the given value as a JSON response with a 200 OK status.
func (h *handler) writeJSON(value any) {
	h.writeJSONStatus(http.StatusOK, value)
}

// Writes an object to the response in JSON format.
// If status is nonzero, the header will be written with that status.
func (h *handler) writeJSONStatus(status int, value any) {
	if !h.requestAccepts("application/json") {
		base.WarnfCtx(h.ctx(), "Client won't accept JSON, only %s", h.rq.Header.Get("Accept"))
		h.writeStatus(http.StatusNotAcceptable, "only application/json available")
		return
	}

	jsonOut, err := base.JSONMarshalCanonical(value)
	if err != nil {
		base.WarnfCtx(h.ctx(), "Couldn't serialize JSON for %v : %s", base.UD(value), err)
		h.writeStatus(http.StatusInternalServerError, "JSON serialization failed")
		return
	}
	if base.ValDefault(h.server.Config.API.Pretty, false) {
		var buffer bytes.Buffer
		_ = json.Indent(&buffer, jsonOut, "", "  ")
		jsonOut = append(buffer.Bytes(), '\n')
	}

	h.writeRawJSONWithoutClientVerification(status, jsonOut)
}

// writeRawJSON writes the given bytes as a JSON response with a 200 OK status.
func (h *handler) writeRawJSON(b []byte) {
	h.writeRawJSONStatus(http.StatusOK, b)
}

// writeRawJSONStatus writes the given bytes as a JSON response.
// If status is nonzero, the header will be written with that status.
func (h *handler) writeRawJSONStatus(status int, b []byte) {
	if !h.requestAccepts("application/json") {
		base.WarnfCtx(h.ctx(), "Client won't accept JSON, only %s", h.rq.Header.Get("Accept"))
		h.writeStatus(http.StatusNotAcceptable, "only application/json available")
		return
	}

	h.writeRawJSONWithoutClientVerification(status, b)
}

// writeRawJSON writes the given bytes as a plaintext response with a 200 OK status.
func (h *handler) writeText(value []byte) {
	h.writeTextStatus(http.StatusOK, value)
}

// writeTextStatus writes the given bytes as a plaintext response.
// If status is nonzero, the header will be written with that status.
func (h *handler) writeTextStatus(status int, value []byte) {
	h.writeWithMimetypeStatus(status, value, "text/plain")
}

func (h *handler) writeJavascript(js string) {
	h.writeWithMimetypeStatus(http.StatusOK, []byte(js), "application/javascript")
}

// writeTextStatus writes the given bytes as a plaintext response.
// If status is nonzero, the header will be written with that status.
func (h *handler) writeWithMimetypeStatus(status int, value []byte, mimetype string) {
	if !h.requestAccepts(mimetype) {
		base.WarnfCtx(h.ctx(), "Client won't accept %s, only %s", mimetype, h.rq.Header.Get("Accept"))
		h.writeStatus(http.StatusNotAcceptable, fmt.Sprintf("only %s available", mimetype))
		return
	}

	h.setHeader("Content-Type", mimetype+"; charset=UTF-8")
	h.setHeader("Content-Length", fmt.Sprintf("%d", len(value)))
	if status > 0 {
		h.response.WriteHeader(status)
		h.setStatus(status, http.StatusText(status))
	}
	// RFC-9110: "HEAD method .. the server MUST NOT send content in the response"
	if h.rq.Method != http.MethodHead {
		_, _ = h.response.Write(value)
	}
}

func (h *handler) addJSON(value any) error {
	encoder := base.JSONEncoderCanonical(h.response)
	err := encoder.Encode(value)
	if err != nil {
		brokenPipeError := strings.Contains(err.Error(), "write: broken pipe")
		connectionResetError := strings.Contains(err.Error(), "write: connection reset")
		if brokenPipeError || connectionResetError {
			base.DebugfCtx(h.ctx(), base.KeyCRUD, "Couldn't serialize document body, HTTP client closed connection")
			return err
		} else {
			base.WarnfCtx(h.ctx(), "Couldn't serialize JSON for %s", err)
			h.writeStatus(http.StatusInternalServerError, "Couldn't serialize document body")
		}
	}
	return nil
}

func (h *handler) writeMultipart(subtype string, callback func(*multipart.Writer) error) error {
	if !h.requestAccepts("multipart/") {
		return base.HTTPErrorf(http.StatusNotAcceptable, "Response is multipart")
	}

	// Get the output stream. Due to a CouchDB bug, if we're sending to it we need to buffer the
	// output in memory so we can trim the final bytes.
	var output io.Writer
	var buffer bytes.Buffer
	if h.userAgentIs("CouchDB") {
		output = &buffer
	} else {
		output = h.response
	}

	writer := multipart.NewWriter(output)
	h.setHeader("Content-Type",
		fmt.Sprintf("multipart/%s; boundary=%q", subtype, writer.Boundary()))

	err := callback(writer)
	_ = writer.Close()

	if err == nil && output == &buffer {
		// Trim trailing newline; CouchDB is allergic to it:
		_, err = h.response.Write(bytes.TrimRight(buffer.Bytes(), "\r\n"))
	}
	return err
}

func (h *handler) flush() {
	h.response.(http.Flusher).Flush()
}

// If the error parameter is non-nil, sets the response status code appropriately and
// writes a CouchDB-style JSON description to the body.
func (h *handler) writeError(err error) {
	if err != nil {
		status, message := base.ErrorAsHTTPStatus(err)
		h.writeStatus(status, message)
		if status >= 500 {
			if status == http.StatusServiceUnavailable {
				base.InfofCtx(h.ctx(), base.KeyHTTP, "%s: %v", h.formatSerialNumber(), err)
			} else {
				base.ErrorfCtx(h.ctx(), "%s: %v", h.formatSerialNumber(), err)
			}
		}
	}
}

// Writes the response status code, and if it's an error writes a JSON description to the body.
func (h *handler) writeStatus(status int, message string) {
	if status < 300 {
		h.response.WriteHeader(status)
		h.setStatus(status, message)
		return
	}
	// Got an error:
	var errorStr string
	switch status {
	case http.StatusNotFound:
		errorStr = "not_found"
	case http.StatusConflict:
		errorStr = "conflict"
	default:
		errorStr = http.StatusText(status)
		if errorStr == "" {
			errorStr = fmt.Sprintf("%d", status)
		}
	}

	h.disableResponseCompression()
	h.setHeader("Content-Type", "application/json")
	h.response.WriteHeader(status)
	h.setStatus(status, message)

	_, _ = h.response.Write([]byte(`{"error":"` + errorStr + `","reason":` + base.ConvertToJSONString(message) + `}`))
}

var kRangeRegex = regexp.MustCompile("^bytes=(\\d+)?-(\\d+)?$")

// Detects and partially HTTP content range requests.
// If the request _can_ accept ranges, sets the "Accept-Ranges" response header to "bytes".
//
// If there is no Range: request header, or if its valid is invalid, returns a status of 200,
// meaning that the caller should return the entire response as usual.
//
// If there is a request range but it exceeds the contentLength, returns status 416. The caller
// should treat this as an error and abort, returning that HTTP status code.
//
// If there is a useable range, it returns status 206 and the start and end in Go slice terms, i.e.
// starting at 0 and with the end non-inclusive. It also adds a "Content-Range" response header.
// It is then the _caller's_ responsibility to set it as the response status code (by calling
// h.response.WriteHeader(status)), and then write the indicated subrange of the response data.
func (h *handler) handleRange(contentLength uint64) (status int, start uint64, end uint64) {
	status = http.StatusOK
	if h.rq.Method == "GET" || h.rq.Method == "HEAD" {
		h.setHeader("Accept-Ranges", "bytes")
		status, start, end = parseHTTPRangeHeader(h.rq.Header.Get("Range"), contentLength)
		if status == http.StatusPartialContent {
			h.setHeader("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, contentLength))
			h.setStatus(http.StatusPartialContent, "Partial Content")
			end += 1 // make end non-inclusive, as in Go slices
		}
	}
	return
}

// Given an HTTP "Range:" header value, parses it and returns the approppriate HTTP status code,
// and the numeric byte range if appropriate:
//   - If the Range header is empty or syntactically invalid, it ignores it and returns status=200.
//   - If the header is valid but exceeds the contentLength, it returns status=416 (Not Satisfiable).
//   - Otherwise it returns status=206 and sets the start and end values in HTTP terms, i.e. with
//     the first byte numbered 0, and the end value inclusive (so the first 100 bytes are 0-99.)
func parseHTTPRangeHeader(rangeStr string, contentLength uint64) (status int, start uint64, end uint64) {
	// http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.35
	status = http.StatusOK
	if rangeStr == "" {
		return
	}
	match := kRangeRegex.FindStringSubmatch(rangeStr)
	if match == nil || (match[1] == "" && match[2] == "") {
		return
	}
	startStr, endStr := match[1], match[2]
	var err error

	start = 0
	if startStr != "" {
		// byte-range-spec
		if start, err = strconv.ParseUint(startStr, 10, 64); err != nil {
			start = math.MaxUint64 // string is all digits, so must just be too big for uint64
		}
	} else if endStr == "" {
		return // "-" is an invalid range spec
	}

	end = contentLength - 1
	if endStr != "" {
		if end, err = strconv.ParseUint(endStr, 10, 64); err != nil {
			end = math.MaxUint64 // string is all digits, so must just be too big for uint64
		}
		if startStr == "" {
			// suffix-range-spec ("-nnn" means the last nnn bytes)
			if end == 0 {
				return http.StatusRequestedRangeNotSatisfiable, 0, 0
			} else if contentLength == 0 {
				return
			} else if end > contentLength {
				end = contentLength
			}
			start = contentLength - end
			end = contentLength - 1
		} else {
			if end < start {
				return // invalid range
			}
			if end >= contentLength {
				end = contentLength - 1 // trim to end of content
			}
		}
	}
	if start >= contentLength {
		return http.StatusRequestedRangeNotSatisfiable, 0, 0
	} else if start == 0 && end == contentLength-1 {
		return // no-op
	}

	// OK, it's a subrange:
	status = http.StatusPartialContent
	return
}

// formatSerialNumber returns the formatted serial number
func (h *handler) formatSerialNumber() string {
	if h.formattedSerialNumber == "" {
		h.formattedSerialNumber = fmt.Sprintf("#%03d", h.serialNumber)
	}
	return h.formattedSerialNumber
}

// shouldShowProductVersion returns whether the handler should show detailed product info (version).
// Admin requests can always see this, regardless of the HideProductVersion setting.
func (h *handler) shouldShowProductVersion() bool {
	hideProductVersion := base.ValDefault(h.server.Config.API.HideProductVersion, false)
	return h.serverType == adminServer || !hideProductVersion
}

func requiresWritePermission(accessPermissions []Permission) bool {
	return slices.Contains(accessPermissions, PermWriteAppData)
}

func (h *handler) isBlipSync() bool {
	return h.pathTemplateContains("_blipsync")
}

// Checks whether the mux path template for the current route contains the specified pattern
func (h *handler) pathTemplateContains(pattern string) bool {
	route := mux.CurrentRoute(h.rq)
	pathTemplate, err := route.GetPathTemplate()
	if err != nil {
		return false
	}
	return strings.Contains(pathTemplate, pattern)
}

// getCORSConfig will return the CORS config for the handler's database if set, otherwise it will return the server CORS config
func (h *handler) getCORSConfig() *auth.CORSConfig {
	if h.db != nil {
		return h.db.CORS
	}
	return h.server.Config.API.CORS
}
