//  Copyright 2012-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package db

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/couchbase/cbgt"
	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/auth"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"
	pkgerrors "github.com/pkg/errors"
)

const (
	DBOffline uint32 = iota
	DBStarting
	DBOnline
	DBStopping
	DBResyncing
)

var RunStateString = []string{
	DBOffline:   "Offline",
	DBStarting:  "Starting",
	DBOnline:    "Online",
	DBStopping:  "Stopping",
	DBResyncing: "Resyncing",
}

const (
	DBCompactNotRunning uint32 = iota
	DBCompactRunning
)

const (
	DefaultRevsLimitNoConflicts = 50
	DefaultRevsLimitConflicts   = 100

	// DefaultPurgeInterval represents a time duration of 30 days to be
	// used as default metadata purge interval when the server’s purge
	// interval (either bucket specific or cluster wide) is not available.
	DefaultPurgeInterval                    = 30 * 24 * time.Hour
	DefaultSGReplicateEnabled               = true
	DefaultSGReplicateWebsocketPingInterval = time.Minute * 5
)

// Default values for delta sync
var (
	DefaultDeltaSyncEnabled   = false
	DefaultDeltaSyncRevMaxAge = uint32(60 * 60 * 24) // 24 hours in seconds
)

var DefaultCompactInterval = uint32(60 * 60 * 24) // Default compact interval in seconds = 1 Day
var (
	DefaultQueryPaginationLimit = 5000
)

const (
	CompactIntervalMinDays = float32(0.04) // ~1 Hour in days
	CompactIntervalMaxDays = float32(60)   // 60 Days in days
)

// BGTCompletionMaxWait is the maximum amount of time to wait for
// completion of all background tasks and background managers before the server is stopped.
const BGTCompletionMaxWait = 30 * time.Second

// Basic description of a database. Shared between all Database objects on the same database.
// This object is thread-safe so it can be shared between HTTP handlers.
type DatabaseContext struct {
	Name                        string             // Database name
	UUID                        string             // UUID for this database instance. Used by cbgt and sgr
	MetadataStore               base.DataStore     // Storage for database metadata (anything that isn't an end-user's/customer's documents)
	Bucket                      base.Bucket        // Storage
	BucketSpec                  base.BucketSpec    // The BucketSpec
	BucketLock                  sync.RWMutex       // Control Access to the underlying bucket object
	mutationListener            changeListener     // Caching feed listener
	ImportListener              *importListener    // Import feed listener
	sequences                   *sequenceAllocator // Source of new sequence numbers
	StartTime                   time.Time          // Timestamp when context was instantiated
	RevsLimit                   uint32             // Max depth a document's revision tree can grow to
	autoImport                  bool               // Add sync data to new untracked couchbase server docs?  (Xattr mode specific)
	channelCache                ChannelCache
	changeCache                 changeCache            // Cache of recently-access channels
	EventMgr                    *EventManager          // Manages notification events
	AllowEmptyPassword          bool                   // Allow empty passwords?  Defaults to false
	Options                     DatabaseContextOptions // Database Context Options
	AccessLock                  sync.RWMutex           // Allows DB offline to block until synchronous calls have completed
	State                       uint32                 // The runtime state of the DB from a service perspective
	ResyncManager               *BackgroundManager
	TombstoneCompactionManager  *BackgroundManager
	AttachmentCompactionManager *BackgroundManager
	ExitChanges                 chan struct{}        // Active _changes feeds on the DB will close when this channel is closed
	OIDCProviders               auth.OIDCProviderMap // OIDC clients
	LocalJWTProviders           auth.LocalJWTProviderMap
	PurgeInterval               time.Duration // Metadata purge interval
	ServerUUID                  string        // UUID of the server, if available

	DbStats      *base.DbStats // stats that correspond to this database context
	CompactState uint32        // Status of database compaction
	terminator   chan bool     // Signal termination of background goroutines

	backgroundTasks              []BackgroundTask               // List of background tasks that are initiated.
	activeChannels               *channels.ActiveChannels       // Tracks active replications by channel
	CfgSG                        cbgt.Cfg                       // Sync Gateway cluster shared config
	SGReplicateMgr               *sgReplicateManager            // Manages interactions with sg-replicate replications
	Heartbeater                  base.Heartbeater               // Node heartbeater for SG cluster awareness
	ServeInsecureAttachmentTypes bool                           // Attachment content type will bypass the content-disposition handling, default false
	NoX509HTTPClient             *http.Client                   // A HTTP Client from gocb to use the management endpoints
	ServerContextHasStarted      chan struct{}                  // Closed via PostStartup once the server has fully started
	userFunctions                *UserFunctions                 // client-callable JavaScript functions
	graphQL                      *GraphQL                       // GraphQL query evaluator
	Scopes                       map[string]Scope               // A map keyed by scope name containing a set of scopes/collections. Nil if running with only _default._default
	CollectionByID               map[uint32]*DatabaseCollection // A map keyed by collection ID to Collection
	CollectionNames              map[string]map[string]struct{} // Map of scope, collection names
	MetadataKeys                 *base.MetadataKeys             // Factory to generate metadata document keys
	RequireResync                base.ScopeAndCollectionNames   // Collections requiring resync before database can go online
	CORS                         *auth.CORSConfig               // CORS configuration
}

type Scope struct {
	Collections map[string]*DatabaseCollection
}

type DatabaseContextOptions struct {
	CacheOptions                  *CacheOptions
	RevisionCacheOptions          *RevisionCacheOptions
	OldRevExpirySeconds           uint32
	AdminInterface                *string
	UnsupportedOptions            *UnsupportedOptions
	OIDCOptions                   *auth.OIDCOptions
	LocalJWTConfig                auth.LocalJWTConfig
	DBOnlineCallback              DBOnlineCallback // Callback function to take the DB back online
	ImportOptions                 ImportOptions
	EnableXattr                   bool             // Use xattr for _sync
	LocalDocExpirySecs            uint32           // The _local doc expiry time in seconds
	SecureCookieOverride          bool             // Pass-through DBConfig.SecureCookieOverride
	SessionCookieName             string           // Pass-through DbConfig.SessionCookieName
	SessionCookieHttpOnly         bool             // Pass-through DbConfig.SessionCookieHTTPOnly
	UserFunctions                 *UserFunctions   // JS/N1QL functions clients can call
	GraphQL                       GraphQL          // GraphQL query interface
	AllowConflicts                *bool            // False forbids creating conflicts
	SendWWWAuthenticateHeader     *bool            // False disables setting of 'WWW-Authenticate' header
	DisablePasswordAuthentication bool             // True enforces OIDC/guest only
	UseViews                      bool             // Force use of views
	DeltaSyncOptions              DeltaSyncOptions // Delta Sync Options
	CompactInterval               uint32           // Interval in seconds between compaction is automatically ran - 0 means don't run
	SGReplicateOptions            SGReplicateOptions
	SlowQueryWarningThreshold     time.Duration
	QueryPaginationLimit          int    // Limit used for pagination of queries. If not set defaults to DefaultQueryPaginationLimit
	UserXattrKey                  string // Key of user xattr that will be accessible from the Sync Function. If empty the feature will be disabled.
	ClientPartitionWindow         time.Duration
	BcryptCost                    int
	GroupID                       string
	JavascriptTimeout             time.Duration // Max time the JS functions run for (ie. sync fn, import filter)
	Serverless                    bool          // If running in serverless mode
	Scopes                        ScopesOptions
	skipRegisterImportPIndex      bool           // if set, skips the global gocb PIndex registration
	MetadataStore                 base.DataStore // If set, use this location/connection for SG metadata storage - if not set, metadata is stored using the same location/connection as the bucket used for data storage.
	MetadataID                    string         // MetadataID used for metadata storage
	BlipStatsReportingInterval    int64          // interval to report blip stats in milliseconds
	ChangesRequestPlus            bool           // Sets the default value for request_plus, for non-continuous changes feeds
	ConfigPrincipals              *ConfigPrincipals
}

type ConfigPrincipals struct {
	Users map[string]*auth.PrincipalConfig
	Roles map[string]*auth.PrincipalConfig
	Guest *auth.PrincipalConfig
}

type ScopesOptions map[string]ScopeOptions

type ScopeOptions struct {
	Collections map[string]CollectionOptions
}

type CollectionOptions struct {
	Sync         *string               // Collection sync function
	ImportFilter *ImportFilterFunction // Opt-in filter for document import
}

// Check whether the specified ScopesOptions only defines the default collection
func (so ScopesOptions) onlyDefaultCollection() bool {
	if so == nil || len(so) == 0 {
		return true
	}
	if len(so) > 1 {
		return false
	}
	defaultScope, ok := so[base.DefaultScope]
	if !ok || len(defaultScope.Collections) > 1 {
		return false
	}
	_, ok = defaultScope.Collections[base.DefaultCollection]
	return ok
}

type SGReplicateOptions struct {
	Enabled               bool          // Whether this node can be assigned sg-replicate replications
	WebsocketPingInterval time.Duration // BLIP Websocket Ping interval (for active replicators)
}

type OidcTestProviderOptions struct {
	Enabled bool `json:"enabled,omitempty"` // Whether the oidc_test_provider endpoints should be exposed on the public API
}

type UserViewsOptions struct {
	Enabled *bool `json:"enabled,omitempty"` // Whether pass-through view query is supported through public API
}

type DeltaSyncOptions struct {
	Enabled          bool   // Whether delta sync is enabled (EE only)
	RevMaxAgeSeconds uint32 // The number of seconds deltas for old revs are available for
}

type APIEndpoints struct {

	// This setting is only needed for testing purposes.  In the Couchbase Lite unit tests that run in "integration mode"
	// against a running Sync Gateway, the tests need to be able to flush the data in between tests to start with a clean DB.
	EnableCouchbaseBucketFlush bool `json:"enable_couchbase_bucket_flush,omitempty"` // Whether Couchbase buckets can be flushed via Admin REST API
}

// UnsupportedOptions are not supported for external use
type UnsupportedOptions struct {
	UserViews                  *UserViewsOptions        `json:"user_views,omitempty"`                    // Config settings for user views
	OidcTestProvider           *OidcTestProviderOptions `json:"oidc_test_provider,omitempty"`            // Config settings for OIDC Provider
	APIEndpoints               *APIEndpoints            `json:"api_endpoints,omitempty"`                 // Config settings for API endpoints
	WarningThresholds          *WarningThresholds       `json:"warning_thresholds,omitempty"`            // Warning thresholds related to _sync size
	DisableCleanSkippedQuery   bool                     `json:"disable_clean_skipped_query,omitempty"`   // Clean skipped sequence processing bypasses final check (deprecated: CBG-2672)
	OidcTlsSkipVerify          bool                     `json:"oidc_tls_skip_verify,omitempty"`          // Config option to enable self-signed certs for OIDC testing.
	SgrTlsSkipVerify           bool                     `json:"sgr_tls_skip_verify,omitempty"`           // Config option to enable self-signed certs for SG-Replicate testing.
	RemoteConfigTlsSkipVerify  bool                     `json:"remote_config_tls_skip_verify,omitempty"` // Config option to enable self signed certificates for external JavaScript load.
	GuestReadOnly              bool                     `json:"guest_read_only,omitempty"`               // Config option to restrict GUEST document access to read-only
	ForceAPIForbiddenErrors    bool                     `json:"force_api_forbidden_errors,omitempty"`    // Config option to force the REST API to return forbidden errors
	ConnectedClient            bool                     `json:"connected_client,omitempty"`              // Enables BLIP connected-client APIs
	UseQueryBasedResyncManager bool                     `json:"use_query_resync_manager,omitempty"`      // Config option to use Query based resync manager to perform Resync op
	DCPReadBuffer              int                      `json:"dcp_read_buffer,omitempty"`               // Enables user to set their own DCP read buffer
	KVBufferSize               int                      `json:"kv_buffer,omitempty"`                     // Enables user to set their own KV pool buffer
}

type WarningThresholds struct {
	XattrSize       *uint32 `json:"xattr_size_bytes,omitempty"`               // Number of bytes to be used as a threshold for xattr size limit warnings
	ChannelsPerDoc  *uint32 `json:"channels_per_doc,omitempty"`               // Number of channels per document to be used as a threshold for channel count warnings
	GrantsPerDoc    *uint32 `json:"access_and_role_grants_per_doc,omitempty"` // Number of access and role grants per document to be used as a threshold for grant count warnings
	ChannelsPerUser *uint32 `json:"channels_per_user,omitempty"`              // Number of channels per user to be used as a threshold for channel count warnings
	ChannelNameSize *uint32 `json:"channel_name_size,omitempty"`              // Number of channel name characters to be used as a threshold for channel name warnings
}

// Options associated with the import of documents not written by Sync Gateway
type ImportOptions struct {
	BackupOldRev     bool   // Create temporary backup of old revision body when available
	ImportPartitions uint16 // Number of partitions for import
}

// Represents a simulated CouchDB database. A new instance is created for each HTTP request,
// so this struct does not have to be thread-safe.
type Database struct {
	*DatabaseContext
	user auth.User
}

func ValidateDatabaseName(dbName string) error {
	// http://wiki.apache.org/couchdb/HTTP_database_API#Naming_and_Addressing
	if match, _ := regexp.MatchString(`^[a-z][-a-z0-9_$()+/]*$`, dbName); !match {
		return base.HTTPErrorf(http.StatusBadRequest,
			"Illegal database name: %s", dbName)
	}
	return nil
}

// getNewDatabaseSleeperFunc returns a sleeper function during database connection
func getNewDatabaseSleeperFunc() base.RetrySleeper {
	return base.CreateDoublingSleeperFunc(
		13, // MaxNumRetries approx 40 seconds total retry duration
		5,  // InitialRetrySleepTimeMS
	)
}

// connectToBucketErrorHandling takes the given spec and error and returns a formatted error, along with whether it was a fatal error.
func connectToBucketErrorHandling(ctx context.Context, spec base.BucketSpec, gotErr error) (fatalError bool, err error) {
	if gotErr != nil {
		if errors.Is(gotErr, base.ErrAuthError) {
			username, _, _ := spec.Auth.GetCredentials()
			base.WarnfCtx(ctx, "Unable to authenticate as user %q: %v", base.UD(username), gotErr)
			// auth errors will be wrapped with HTTPError further up the stack where appropriate. Return the raw error that can still be checked.
			return false, gotErr
		}

		// Fatal errors get an additional log message, but are otherwise still transformed below.
		if errors.Is(gotErr, base.ErrFatalBucketConnection) {
			base.WarnfCtx(ctx, "Fatal error connecting to bucket: %v", gotErr)
			fatalError = true
		}

		// Remaining errors are appended to the end of a more helpful error message.
		return fatalError, base.HTTPErrorf(http.StatusBadGateway,
			" Unable to connect to Couchbase Server (connection refused). Please ensure it is running and reachable at the configured host and port.  Detailed error: %s", gotErr)
	}

	return false, nil
}

type OpenBucketFn func(context.Context, base.BucketSpec, bool) (base.Bucket, error)

// ConnectToBucket opens a Couchbase connection and return a specific bucket. If failFast is set, fail immediately if the bucket doesn't exist, otherwise retry waiting for bucket to exist.
func ConnectToBucket(ctx context.Context, spec base.BucketSpec, failFast bool) (base.Bucket, error) {
	if failFast {
		bucket, err := base.GetBucket(spec)
		_, err = connectToBucketErrorHandling(ctx, spec, err)
		return bucket, err
	}

	// start a retry loop to connect to the bucket backing off double the delay each time
	worker := func() (bool, error, interface{}) {
		bucket, err := base.GetBucket(spec)

		// Retry if there was a non-fatal error
		fatalError, newErr := connectToBucketErrorHandling(ctx, spec, err)
		shouldRetry := newErr != nil && !fatalError

		return shouldRetry, newErr, bucket
	}

	description := fmt.Sprintf("Attempt to connect to bucket : %v", spec.BucketName)
	err, ibucket := base.RetryLoop(description, worker, getNewDatabaseSleeperFunc())
	if err != nil {
		return nil, err
	}

	return ibucket.(base.Bucket), nil
}

// Returns Couchbase Server Cluster UUID on a timeout. If running against walrus, do return an empty string.
func getServerUUID(ctx context.Context, bucket base.Bucket) (string, error) {
	gocbV2Bucket, err := base.AsGocbV2Bucket(bucket)
	if err != nil {
		return "", nil
	}
	// start a retry loop to get server ID
	worker := func() (bool, error, interface{}) {
		uuid, err := base.GetServerUUID(gocbV2Bucket)
		return err != nil, err, uuid
	}

	err, uuid := base.RetryLoopCtx("Getting ServerUUID", worker, getNewDatabaseSleeperFunc(), ctx)
	return uuid.(string), err
}

// Function type for something that calls NewDatabaseContext and wants a callback when the DB is detected
// to come back online. A rest.ServerContext package cannot be passed since it would introduce a circular dependency
type DBOnlineCallback func(dbContext *DatabaseContext)

// Creates a new DatabaseContext on a bucket. The bucket will be closed when this context closes.
func NewDatabaseContext(ctx context.Context, dbName string, bucket base.Bucket, autoImport bool, options DatabaseContextOptions) (dbc *DatabaseContext, returnedError error) {
	cleanupFunctions := make([]func(), 0)

	defer func() {
		if returnedError != nil {
			for _, cleanupFunc := range cleanupFunctions {
				cleanupFunc()
			}
		}
	}()

	if err := ValidateDatabaseName(dbName); err != nil {
		return nil, err
	}

	// add db info to ctx before having a DatabaseContext (cannot call AddDatabaseLogContext),
	// in order to pass it to RegisterImportPindexImpl
	ctx = base.DatabaseLogCtx(ctx, dbName)

	if err := base.RequireNoBucketTTL(bucket); err != nil {
		return nil, err
	}

	serverUUID, err := getServerUUID(ctx, bucket)
	if err != nil {
		return nil, err
	}

	dbStats, statsError := initDatabaseStats(dbName, autoImport, options)
	if statsError != nil {
		return nil, statsError
	}

	// options.MetadataStore is always passed via rest._getOrAddDatabase...
	// but in db package tests this is unlikely to be set. In this case we'll use the existing bucket connection to store metadata.
	metadataStore := options.MetadataStore
	if metadataStore == nil {
		base.DebugfCtx(ctx, base.KeyConfig, "MetadataStore was nil - falling back to use existing bucket connection %q for database %q", bucket.GetName(), dbName)
		metadataStore = bucket.DefaultDataStore()
	}

	// Register the cbgt pindex type for the configGroup
	RegisterImportPindexImpl(ctx, options.GroupID)

	dbContext := &DatabaseContext{
		Name:           dbName,
		UUID:           cbgt.NewUUID(),
		MetadataStore:  metadataStore,
		Bucket:         bucket,
		StartTime:      time.Now(),
		autoImport:     autoImport,
		Options:        options,
		DbStats:        dbStats,
		CollectionByID: make(map[uint32]*DatabaseCollection),
		ServerUUID:     serverUUID,
	}

	// Initialize metadata ID and keys
	metaKeys := base.NewMetadataKeys(options.MetadataID)
	dbContext.MetadataKeys = metaKeys

	cleanupFunctions = append(cleanupFunctions, func() {
		base.SyncGatewayStats.ClearDBStats(dbName)
	})

	if dbContext.AllowConflicts() {
		dbContext.RevsLimit = DefaultRevsLimitConflicts
	} else {
		dbContext.RevsLimit = DefaultRevsLimitNoConflicts
	}

	dbContext.terminator = make(chan bool)

	dbContext.EventMgr = NewEventManager(dbContext.terminator)

	dbContext.sequences, err = newSequenceAllocator(metadataStore, dbContext.DbStats.Database(), metaKeys)
	if err != nil {
		return nil, err
	}

	cleanupFunctions = append(cleanupFunctions, func() {
		dbContext.sequences.Stop()
	})

	// Initialize the active channel counter
	dbContext.activeChannels = channels.NewActiveChannels(dbContext.DbStats.Cache().NumActiveChannels)

	cacheOptions := options.CacheOptions
	if cacheOptions == nil {
		defaultOpts := DefaultCacheOptions()
		cacheOptions = &defaultOpts
	}
	channelCache, err := NewChannelCacheForContext(ctx, cacheOptions.ChannelCacheOptions, dbContext)
	if err != nil {
		return nil, err
	}
	dbContext.channelCache = channelCache

	// Initialize sg cluster config.  Required even if import and sgreplicate are disabled
	// on this node, to support replication REST API calls
	if base.IsEnterpriseEdition() {
		sgCfg, err := base.NewCfgSG(metadataStore, metaKeys.SGCfgPrefix(dbContext.Options.GroupID))
		if err != nil {
			return nil, err
		}
		dbContext.CfgSG = sgCfg
	} else {
		sgCfg, err := base.NewCbgtCfgMem()
		if err != nil {
			return nil, err
		}
		dbContext.CfgSG = sgCfg
	}

	// Initialize the tap Listener for notify handling
	dbContext.mutationListener.Init(bucket.GetName(), options.GroupID, dbContext.MetadataKeys)

	if len(options.Scopes) == 0 {
		return nil, fmt.Errorf("Setting scopes to be zero is invalid")
	}
	dbContext.Scopes = make(map[string]Scope, len(options.Scopes))
	dbContext.CollectionNames = make(map[string]map[string]struct{}, len(options.Scopes))
	// if any sync functions for any collection, we recommend running a resync
	syncFunctionsChanged := false
	for scopeName, scope := range options.Scopes {
		dbContext.Scopes[scopeName] = Scope{
			Collections: make(map[string]*DatabaseCollection, len(scope.Collections)),
		}
		collectionNameMap := make(map[string]struct{}, len(scope.Collections))
		for collName, collOpts := range scope.Collections {
			ctx := base.CollectionLogCtx(ctx, collName)
			dataStore, err := bucket.NamedDataStore(base.ScopeAndCollectionName{Scope: scopeName, Collection: collName})
			if err != nil {
				return nil, err
			}
			stats, err := dbContext.DbStats.CollectionStat(scopeName, collName)
			if err != nil {
				return nil, err
			}
			dbCollection, err := newDatabaseCollection(ctx, dbContext, dataStore, stats)
			if err != nil {
				return nil, err
			}
			if collOpts.Sync != nil {
				fnChanged, err := dbCollection.UpdateSyncFun(ctx, *collOpts.Sync)
				if err != nil {
					return nil, err
				}
				if fnChanged {
					syncFunctionsChanged = true
				}

			} else {
				defaultSyncFunction := channels.GetDefaultSyncFunction(scopeName, collName)
				base.InfofCtx(ctx, base.KeyAll, "Using default sync function %q for database %s.%s.%s", defaultSyncFunction, base.MD(dbName), base.MD(scopeName), base.MD(collName))
			}

			if collOpts.ImportFilter != nil {
				dbCollection.importFilterFunction = collOpts.ImportFilter
			}

			dbContext.Scopes[scopeName].Collections[collName] = dbCollection

			collectionID := dbCollection.GetCollectionID()
			dbContext.CollectionByID[collectionID] = dbCollection
			collectionNameMap[collName] = struct{}{}
		}
		dbContext.CollectionNames[scopeName] = collectionNameMap
	}

	if syncFunctionsChanged {
		base.InfofCtx(ctx, base.KeyAll, "**NOTE:** %q's sync function has changed. The new function may assign different channels to documents, or permissions to users. You may want to re-sync the database to update these.", base.MD(dbContext.Name))
	}

	// Initialize sg-replicate manager
	dbContext.SGReplicateMgr, err = NewSGReplicateManager(ctx, dbContext, dbContext.CfgSG)
	if err != nil {
		return nil, err
	}

	if dbContext.UseQueryBasedResyncManager() {
		dbContext.ResyncManager = NewResyncManager(metadataStore, metaKeys)
	} else {
		dbContext.ResyncManager = NewResyncManagerDCP(metadataStore, dbContext.UseXattrs(), metaKeys)
	}

	return dbContext, nil
}

func (context *DatabaseContext) GetOIDCProvider(providerName string) (*auth.OIDCProvider, error) {

	// If providerName isn't specified, check whether there's a default provider
	if providerName == "" {
		provider := context.OIDCProviders.GetDefaultProvider()
		if provider == nil {
			return nil, errors.New("No default provider available.")
		}
		return provider, nil
	}

	if provider, ok := context.OIDCProviders[providerName]; ok {
		return provider, nil
	} else {
		return nil, base.RedactErrorf("No provider found for provider name %q", base.MD(providerName))
	}
}

func (context *DatabaseContext) Close(ctx context.Context) {
	context.BucketLock.Lock()
	defer context.BucketLock.Unlock()

	context.OIDCProviders.Stop()
	close(context.terminator)

	// Stop All background processors
	bgManagers := context.stopBackgroundManagers()

	// Wait for database background tasks to finish.
	waitForBGTCompletion(ctx, BGTCompletionMaxWait, context.backgroundTasks, context.Name)
	context.sequences.Stop()
	context.mutationListener.Stop()
	context.changeCache.Stop()
	// Stop the channel cache and it's background tasks.
	context.channelCache.Stop()
	context.ImportListener.Stop()
	if context.Heartbeater != nil {
		context.Heartbeater.Stop()
	}
	if context.SGReplicateMgr != nil {
		context.SGReplicateMgr.Stop()
	}

	waitForBackgroundManagersToStop(ctx, BGTCompletionMaxWait, bgManagers)

	context.Bucket.Close()
	context.Bucket = nil

	base.RemovePerDbStats(context.Name)

}

// stopBackgroundManagers stops any running BackgroundManager.
// Returns a list of BackgroundManager it signalled to stop
func (context *DatabaseContext) stopBackgroundManagers() []*BackgroundManager {
	bgManagers := make([]*BackgroundManager, 0)

	if context.ResyncManager != nil {
		if !isBackgroundManagerStopped(context.ResyncManager.GetRunState()) {
			if err := context.ResyncManager.Stop(); err == nil {
				bgManagers = append(bgManagers, context.ResyncManager)
			}
		}
	}

	if context.AttachmentCompactionManager != nil {
		if !isBackgroundManagerStopped(context.AttachmentCompactionManager.GetRunState()) {
			if err := context.AttachmentCompactionManager.Stop(); err == nil {
				bgManagers = append(bgManagers, context.AttachmentCompactionManager)
			}
		}
	}

	if context.TombstoneCompactionManager != nil {
		if !isBackgroundManagerStopped(context.TombstoneCompactionManager.GetRunState()) {
			if err := context.TombstoneCompactionManager.Stop(); err == nil {
				bgManagers = append(bgManagers, context.TombstoneCompactionManager)
			}
		}
	}

	return bgManagers
}

// waitForBackgroundManagersToStop wait for given BackgroundManagers to stop within given time
func waitForBackgroundManagersToStop(ctx context.Context, waitTimeMax time.Duration, bgManagers []*BackgroundManager) {
	timeout := time.NewTicker(waitTimeMax)
	defer timeout.Stop()
	retryInterval := 1 * time.Millisecond
	maxRetryInterval := 1 * time.Second
	for {
		select {
		case <-timeout.C:
			runningBackgroundManagerNames := ""
			for _, bgManager := range bgManagers {
				if !isBackgroundManagerStopped(bgManager.GetRunState()) {
					runningBackgroundManagerNames += fmt.Sprintf(" %s", bgManager.GetName())
				}
			}
			base.WarnfCtx(ctx, "Background Managers [%s] failed to stop within deadline of %s.", runningBackgroundManagerNames, waitTimeMax)
			return
		case <-time.After(retryInterval):
			stoppedServices := 0
			for _, bgManager := range bgManagers {
				state := bgManager.GetRunState()
				if isBackgroundManagerStopped(state) {
					stoppedServices += 1
				}
			}
			if stoppedServices == len(bgManagers) {
				return
			}

			// exponential backoff with max wait
			if retryInterval < maxRetryInterval {
				retryInterval = retryInterval * 2
				if retryInterval > maxRetryInterval {
					retryInterval = maxRetryInterval
				}
			}
		}
	}
}

func isBackgroundManagerStopped(state BackgroundProcessState) bool {
	return state == BackgroundProcessStateStopped || state == BackgroundProcessStateCompleted || state == BackgroundProcessStateError || state == ""
}

// waitForBGTCompletion waits for all the background tasks to finish.
func waitForBGTCompletion(ctx context.Context, waitTimeMax time.Duration, tasks []BackgroundTask, dbName string) {
	waitTime := waitTimeMax
	for _, t := range tasks {
		start := time.Now()
		select {
		case <-t.doneChan:
			waitTime -= time.Now().Sub(start)
			continue
		case <-time.After(waitTime):
			// Timeout after waiting for background task to terminate.
		}
		base.InfofCtx(ctx, base.KeyAll, "Timeout after %v of waiting for background task %q to "+
			"terminate, database: %s", waitTimeMax, t.taskName, dbName)
	}
}

func (context *DatabaseContext) IsClosed() bool {
	context.BucketLock.RLock()
	defer context.BucketLock.RUnlock()
	return context.Bucket == nil
}

// For testing only!
func (context *DatabaseContext) RestartListener() error {
	context.mutationListener.Stop()
	// Delay needed to properly stop
	time.Sleep(2 * time.Second)
	context.mutationListener.Init(context.Bucket.GetName(), context.Options.GroupID, context.MetadataKeys)
	cacheFeedStatsMap := context.DbStats.Database().CacheFeedMapStats
	if err := context.mutationListener.Start(context.Bucket, cacheFeedStatsMap.Map, context.Scopes, context.MetadataStore); err != nil {
		return err
	}
	return nil
}

// Removes previous versions of Sync Gateway's design docs found on the server
func (dbCtx *DatabaseContext) RemoveObsoleteDesignDocs(previewOnly bool) (removedDesignDocs []string, err error) {
	ds := dbCtx.Bucket.DefaultDataStore()
	viewStore, ok := ds.(sgbucket.ViewStore)
	if !ok {
		return []string{}, fmt.Errorf("Datastore does not support views")
	}
	return removeObsoleteDesignDocs(context.TODO(), viewStore, previewOnly, dbCtx.UseViews())
}

// getDataStores returns all datastores on the database, including metadatastore
func (dbCtx *DatabaseContext) getDataStores() []sgbucket.DataStore {
	datastores := make([]sgbucket.DataStore, 0, len(dbCtx.CollectionByID))
	for _, collection := range dbCtx.CollectionByID {
		datastores = append(datastores, collection.dataStore)
	}
	_, hasDefaultCollection := dbCtx.CollectionByID[base.DefaultCollectionID]
	if !hasDefaultCollection {
		datastores = append(datastores, dbCtx.MetadataStore)
	}
	return datastores
}

// Removes previous versions of Sync Gateway's indexes found on the server. Returns a map of indexes removed by collection name.
func (dbCtx *DatabaseContext) RemoveObsoleteIndexes(ctx context.Context, previewOnly bool) ([]string, error) {

	if !dbCtx.Bucket.IsSupported(sgbucket.BucketStoreFeatureN1ql) {
		return nil, nil
	}
	var errs *base.MultiError
	var removedIndexes []string
	for _, dataStore := range dbCtx.getDataStores() {
		dsName, ok := base.AsDataStoreName(dataStore)
		if !ok {
			err := fmt.Sprintf("Cannot get datastore name from %s", dataStore)
			base.WarnfCtx(ctx, err)
			errs = errs.Append(errors.New(err))
			continue
		}
		collectionName := fmt.Sprintf("`%s`.`%s`", dsName.ScopeName(), dsName.CollectionName())
		n1qlStore, ok := base.AsN1QLStore(dataStore)
		if !ok {
			err := fmt.Sprintf("Cannot remove obsolete indexes for non-gocb collection %s - skipping.", base.MD(collectionName))
			base.WarnfCtx(ctx, err)
			errs = errs.Append(errors.New(err))
			continue
		}
		collectionRemovedIndexes, err := removeObsoleteIndexes(n1qlStore, previewOnly, dbCtx.UseXattrs(), dbCtx.UseViews(), sgIndexes)
		if err != nil {
			errs = errs.Append(err)
			continue
		}
		onlyDefaultCollection := dbCtx.OnlyDefaultCollection()
		for _, idxName := range collectionRemovedIndexes {
			if onlyDefaultCollection {
				removedIndexes = append(removedIndexes, idxName)
			} else {
				removedIndexes = append(removedIndexes,
					fmt.Sprintf("%s.%s", collectionName, idxName))
			}
		}
	}
	return removedIndexes, errs.ErrorOrNil()
}

// Trigger terminate check handling for connected continuous replications.
// TODO: The underlying code (NotifyCheckForTermination) doesn't actually leverage the specific username - should be refactored
//
//	to remove
func (context *DatabaseContext) NotifyTerminatedChanges(username string) {
	context.mutationListener.NotifyCheckForTermination(base.SetOf(base.UserPrefixRoot + username))
}

func (dc *DatabaseContext) TakeDbOffline(ctx context.Context, reason string) error {

	if atomic.CompareAndSwapUint32(&dc.State, DBOnline, DBStopping) {
		// notify all active _changes feeds to close
		close(dc.ExitChanges)

		// Block until all current calls have returned, including _changes feeds
		dc.AccessLock.Lock()
		defer dc.AccessLock.Unlock()

		dc.changeCache.Stop()

		// set DB state to Offline
		atomic.StoreUint32(&dc.State, DBOffline)

		if err := dc.EventMgr.RaiseDBStateChangeEvent(dc.Name, "offline", reason, dc.Options.AdminInterface); err != nil {
			base.DebugfCtx(ctx, base.KeyCRUD, "Error raising database state change event: %v", err)
		}

		return nil
	} else {
		dbState := atomic.LoadUint32(&dc.State)
		// If the DB is already transitioning to: offline or is offline silently return
		if dbState == DBOffline || dbState == DBResyncing || dbState == DBStopping {
			return nil
		}

		msg := "Unable to take Database offline, database must be in Online state but was " + RunStateString[dbState]
		if dbState == DBOnline {
			msg = "Unable to take Database offline, another operation was already in progress. Please try again."
		}

		base.InfofCtx(ctx, base.KeyCRUD, msg)
		return base.HTTPErrorf(http.StatusServiceUnavailable, msg)
	}
}

func (db *Database) TakeDbOffline(nonContextStruct base.NonCancellableContext, reason string) error {
	return db.DatabaseContext.TakeDbOffline(nonContextStruct.Ctx, reason)
}

func (context *DatabaseContext) Authenticator(ctx context.Context) *auth.Authenticator {
	context.BucketLock.RLock()
	defer context.BucketLock.RUnlock()

	sessionCookieName := auth.DefaultCookieName
	if context.Options.SessionCookieName != "" {
		sessionCookieName = context.Options.SessionCookieName
	}

	var channelsWarningThreshold *uint32
	if context.Options.UnsupportedOptions != nil && context.Options.UnsupportedOptions.WarningThresholds != nil {
		channelsWarningThreshold = context.Options.UnsupportedOptions.WarningThresholds.ChannelsPerUser
	}

	// Authenticators are lightweight & stateless, so it's OK to return a new one every time
	authenticator := auth.NewAuthenticator(context.MetadataStore, context, auth.AuthenticatorOptions{
		ClientPartitionWindow:    context.Options.ClientPartitionWindow,
		ChannelsWarningThreshold: channelsWarningThreshold,
		SessionCookieName:        sessionCookieName,
		BcryptCost:               context.Options.BcryptCost,
		LogCtx:                   ctx,
		Collections:              context.CollectionNames,
		MetaKeys:                 context.MetadataKeys,
	})

	return authenticator
}

func (context *DatabaseContext) IsServerless() bool {
	return context.Options.Serverless
}

// Makes a Database object given its name and bucket.
func GetDatabase(context *DatabaseContext, user auth.User) (*Database, error) {
	return &Database{DatabaseContext: context, user: user}, nil
}

func CreateDatabase(context *DatabaseContext) (*Database, error) {
	return &Database{DatabaseContext: context}, nil
}

func (db *Database) SameAs(otherdb *Database) bool {
	return db != nil && otherdb != nil &&
		db.Bucket == otherdb.Bucket
}

func (db *Database) IsCompactRunning() bool {
	return atomic.LoadUint32(&db.CompactState) == DBCompactRunning
}

func (db *Database) User() auth.User {
	return db.user
}

func (db *Database) SetUser(user auth.User) {
	db.user = user
}

// Reloads the database's User object, in case its persistent properties have been changed.
func (db *Database) ReloadUser(ctx context.Context) error {
	if db.user == nil {
		return nil
	}
	user, err := db.Authenticator(ctx).GetUser(db.user.Name())
	if err != nil {
		return err
	}
	if user == nil {
		return errors.New("User not found during reload")
	} else {
		db.user = user
		return nil
	}
}

// ////// ALL DOCUMENTS:

type IDRevAndSequence struct {
	DocID    string
	RevID    string
	Sequence uint64
}

// The ForEachDocID options for limiting query results
type ForEachDocIDOptions struct {
	Startkey string
	Endkey   string
	Limit    uint64
}

type ForEachDocIDFunc func(id IDRevAndSequence, channels []string) (bool, error)

// Iterates over all documents in the database, calling the callback function on each
func (c *DatabaseCollection) ForEachDocID(ctx context.Context, callback ForEachDocIDFunc, resultsOpts ForEachDocIDOptions) error {

	results, err := c.QueryAllDocs(ctx, resultsOpts.Startkey, resultsOpts.Endkey)
	if err != nil {
		return err
	}

	err = c.processForEachDocIDResults(callback, resultsOpts.Limit, results)
	if err != nil {
		return err
	}
	return results.Close()
}

// Iterate over the results of an AllDocs query, performing ForEachDocID handling for each row
func (c *DatabaseCollection) processForEachDocIDResults(callback ForEachDocIDFunc, limit uint64, results sgbucket.QueryResultIterator) error {

	count := uint64(0)
	for {
		var queryRow AllDocsIndexQueryRow
		var found bool
		var docid, revid string
		var seq uint64
		var channels []string
		if c.useViews() {
			var viewRow AllDocsViewQueryRow
			found = results.Next(&viewRow)
			if found {
				docid = viewRow.Key
				revid = viewRow.Value.RevID
				seq = viewRow.Value.Sequence
				channels = viewRow.Value.Channels
			}
		} else {
			found = results.Next(&queryRow)
			if found {
				docid = queryRow.Id
				revid = queryRow.RevID
				seq = queryRow.Sequence
				channels = make([]string, 0)
				// Query returns all channels, but we only want to return active channels
				for channelName, removal := range queryRow.Channels {
					if removal == nil {
						channels = append(channels, channelName)
					}
				}
			}
		}
		if !found {
			break
		}

		if ok, err := callback(IDRevAndSequence{docid, revid, seq}, channels); ok {
			count++
		} else if err != nil {
			return err
		}
		// We have to apply limit check after callback has been called
		// to account for rows that are not in the current users channels
		if limit > 0 && count == limit {
			break
		}

	}
	return nil
}

// Returns the IDs of all users and roles, including deleted Roles
func (db *DatabaseContext) AllPrincipalIDs(ctx context.Context) (users, roles []string, err error) {

	if !db.IsServerless() || db.Options.UseViews {
		return db.getAllPrincipalIDsSyncDocs(ctx)
	}

	// If running in Serverless mode, we can leverage `users` and `roles` index
	// to fetch users and roles
	usersCh := db.getUserNamesInBackground(ctx)
	rolesCh := db.getRoleIDsInBackground(ctx)

	userData := <-usersCh
	if userData.err != nil {
		return nil, nil, userData.err
	}
	users = userData.value

	rolesData := <-rolesCh
	if rolesData.err != nil {
		return nil, nil, rolesData.err
	}
	roles = rolesData.value

	return users, roles, err
}

// used to send users/roles data from background fetch
type data struct {
	value []string
	err   error
}

func (db *DatabaseContext) getUserNamesInBackground(ctx context.Context) <-chan data {
	ch := make(chan data, 1)
	go func() {
		defer close(ch)
		users, err := db.GetUserNames(ctx)
		ch <- data{
			value: users,
			err:   err,
		}
	}()
	return ch
}

func (db *DatabaseContext) getRoleIDsInBackground(ctx context.Context) <-chan data {
	ch := make(chan data, 1)
	go func() {
		defer close(ch)
		roles, err := db.getRoleIDsUsingIndex(ctx, true)
		ch <- data{
			value: roles,
			err:   err,
		}
	}()
	return ch
}

// Returns the Names of all users
func (db *DatabaseContext) GetUserNames(ctx context.Context) (users []string, err error) {
	dbUserPrefix := db.MetadataKeys.UserKeyPrefix()
	startKey := dbUserPrefix
	limit := db.Options.QueryPaginationLimit

	users = []string{}

outerLoop:
	for {
		results, err := db.QueryUsers(ctx, startKey, limit)
		if err != nil {
			return nil, err
		}

		var principalName string

		resultCount := 0

		for {
			// startKey is inclusive for views, so need to skip first result if using non-empty startKey, as this results in an overlapping result
			var skipAddition bool
			if resultCount == 0 && startKey != dbUserPrefix {
				skipAddition = true
			}

			var queryRow QueryUsersRow
			found := results.Next(&queryRow)
			if !found {
				break
			}

			if !strings.HasPrefix(queryRow.ID, dbUserPrefix) {
				break
			}

			principalName = queryRow.Name
			startKey = queryRow.ID

			resultCount++

			if principalName != "" && !skipAddition {
				users = append(users, principalName)
			}
		}

		closeErr := results.Close()
		if closeErr != nil {
			return nil, closeErr
		}

		if resultCount < limit {
			break outerLoop
		}

	}
	return users, nil
}

// Returns the IDs of all users and roles using syncDocs index
func (db *DatabaseContext) getAllPrincipalIDsSyncDocs(ctx context.Context) (users, roles []string, err error) {

	startKey := ""
	limit := db.Options.QueryPaginationLimit

	dbUserPrefix := db.MetadataKeys.UserKeyPrefix()
	dbRolePrefix := db.MetadataKeys.RoleKeyPrefix()
	lenDbUserPrefix := len(dbUserPrefix)
	lenDbRolePrefix := len(dbRolePrefix)

	users = []string{}
	roles = []string{}

outerLoop:
	for {
		results, err := db.QueryPrincipals(ctx, startKey, limit)
		if err != nil {
			return nil, nil, err
		}

		var isDbUser, isDbRole bool
		var principalName string

		resultCount := 0

		for {
			// startKey is inclusive for views, so need to skip first result if using non-empty startKey, as this results in an overlapping result
			var skipAddition bool
			if resultCount == 0 && startKey != "" {
				skipAddition = true
			}

			var rowID string
			if db.Options.UseViews {
				var viewRow principalsViewRow
				found := results.Next(&viewRow)
				if !found {
					break
				}
				rowID = viewRow.ID
				//isUser = viewRow.Value
				//principalName = viewRow.Key
				startKey = viewRow.Key
			} else {
				var queryRow principalRow
				found := results.Next(&queryRow)
				if !found {
					break
				}
				rowID = queryRow.Id
				startKey = queryRow.Name
			}
			if len(rowID) < lenDbUserPrefix && len(rowID) < lenDbRolePrefix {
				continue
			}

			isDbUser = strings.HasPrefix(rowID, dbUserPrefix)
			isDbRole = strings.HasPrefix(rowID, dbRolePrefix)
			if !isDbUser && !isDbRole {
				continue
			}
			if !db.Options.UseViews {
				principalName = startKey
			} else if isDbUser {
				principalName = rowID[lenDbUserPrefix:]
			} else {
				principalName = rowID[lenDbRolePrefix:]

			}
			resultCount++

			if principalName != "" && !skipAddition {
				if isDbUser {
					users = append(users, principalName)
				} else {
					roles = append(roles, principalName)
				}
			}
		}

		closeErr := results.Close()
		if closeErr != nil {
			return nil, nil, closeErr
		}

		if resultCount < limit {
			break outerLoop
		}

	}

	return users, roles, nil
}

// Returns user information for all users (ID, disabled, email)
func (db *DatabaseContext) GetUsers(ctx context.Context, limit int) (users []auth.PrincipalConfig, err error) {

	if db.Options.UseViews {
		return nil, errors.New("GetUsers not supported when running with useViews=true")
	}

	// While using SyncDocs index, must set startKey to the user prefix to avoid unwanted interaction between
	// limit handling and startKey (non-user _sync: prefixed documents being included in the query limit evaluation).
	// This doesn't happen for AllPrincipalIDs, I believe because the role check forces query to not assume
	// a contiguous set of results
	dbUserKeyPrefix := db.MetadataKeys.UserKeyPrefix()
	startKey := dbUserKeyPrefix
	paginationLimit := db.Options.QueryPaginationLimit
	if paginationLimit == 0 {
		paginationLimit = DefaultQueryPaginationLimit
	}

	// If the requested limit is lower than the pagination limit, use requested limit as pagination limit
	if limit > 0 && limit < paginationLimit {
		paginationLimit = limit
	}

	users = []auth.PrincipalConfig{}

	totalCount := 0

outerLoop:
	for {
		results, err := db.QueryUsers(ctx, startKey, paginationLimit)
		if err != nil {
			return nil, err
		}

		resultCount := 0
		for {
			// startKey is inclusive, so need to skip first result if using non-empty startKey, as this results in an overlapping result
			var skipAddition bool
			if resultCount == 0 && startKey != dbUserKeyPrefix {
				skipAddition = true
			}

			var queryRow QueryUsersRow
			found := results.Next(&queryRow)
			if !found {
				break
			}
			if !strings.HasPrefix(queryRow.ID, dbUserKeyPrefix) {
				break
			}

			startKey = queryRow.ID
			resultCount++
			if queryRow.Name != "" && !skipAddition {
				principal := auth.PrincipalConfig{
					Name:     &queryRow.Name,
					Email:    &queryRow.Email,
					Disabled: &queryRow.Disabled,
				}
				users = append(users, principal)
				totalCount++
				if limit > 0 && totalCount >= limit {
					break
				}
			}
		}

		closeErr := results.Close()
		if closeErr != nil {
			return nil, closeErr
		}

		if resultCount < paginationLimit {
			break outerLoop
		}

		if limit > 0 && totalCount >= limit {
			break outerLoop
		}

	}

	return users, nil
}

func (db *DatabaseContext) getRoleIDsUsingIndex(ctx context.Context, includeDeleted bool) (roles []string, err error) {

	dbRoleIDPrefix := db.MetadataKeys.RoleKeyPrefix()
	startKey := dbRoleIDPrefix
	limit := db.Options.QueryPaginationLimit

	roles = []string{}

outerLoop:
	for {
		var results sgbucket.QueryResultIterator
		var err error
		if includeDeleted {
			results, err = db.QueryAllRoles(ctx, startKey, limit)
		} else {
			results, err = db.QueryRoles(ctx, startKey, limit)
		}
		if err != nil {
			return nil, err
		}

		var roleName string
		lenRoleKeyPrefix := len(dbRoleIDPrefix)

		resultCount := 0

		for {
			// startKey is inclusive for views, so need to skip first result if using non-empty startKey, as this results in an overlapping result
			var skipAddition bool
			if resultCount == 0 && startKey != dbRoleIDPrefix {
				skipAddition = true
			}

			var queryRow principalRow
			found := results.Next(&queryRow)
			if !found {
				break
			}
			if len(queryRow.Id) < lenRoleKeyPrefix {
				continue
			}
			if !strings.HasPrefix(queryRow.Id, dbRoleIDPrefix) {
				break
			}
			if !db.UseViews() {
				roleName = queryRow.Name
			} else {
				roleName = queryRow.Id[lenRoleKeyPrefix:]

			}
			startKey = queryRow.Id

			resultCount++

			if roleName != "" && !skipAddition {
				roles = append(roles, roleName)
			}
		}

		closeErr := results.Close()
		if closeErr != nil {
			return nil, closeErr
		}

		if resultCount < limit {
			break outerLoop
		}

	}

	return roles, nil
}

// GetRoleIDs returns IDs of all roles, Includes deleted roles based on given flag
//
// It choses which View/Index to use based on combination of useViews and includeDeleted
// When Views is enabled and includeDeleted is true, ViewPrincipal is used to fetch roles
// when View is enabled and includeDelete is false, ViewRolesExcludeDelete is used
// Otherwise RoleIndex is used to fetch roles
func (db *DatabaseContext) GetRoleIDs(ctx context.Context, useViews, includeDeleted bool) (roles []string, err error) {
	if useViews && includeDeleted {
		_, roles, err = db.AllPrincipalIDs(ctx)
	} else {
		roles, err = db.getRoleIDsUsingIndex(ctx, includeDeleted)
	}

	if err != nil {
		return nil, err
	}

	return roles, nil
}

// Trigger tombstone compaction from view and/or GSI indexes.  Several Sync Gateway indexes server tombstones (deleted documents with an xattr).
// There currently isn't a mechanism for server to remove these docs from the index when the tombstone is purged by the server during
// metadata purge, because metadata purge doesn't trigger a DCP event.
// When compact is run, Sync Gateway initiates a normal delete operation for the document and xattr (a Sync Gateway purge).  This triggers
// removal of the document from the index.  In the event that the document has already been purged by server, we need to recreate and delete
// the document to accomplish the same result.
type compactCallbackFunc func(purgedDocCount *int)

func (db *Database) Compact(ctx context.Context, skipRunningStateCheck bool, callback compactCallbackFunc, terminator *base.SafeTerminator) (int, error) {
	if !skipRunningStateCheck {
		if !atomic.CompareAndSwapUint32(&db.CompactState, DBCompactNotRunning, DBCompactRunning) {
			return 0, base.HTTPErrorf(http.StatusServiceUnavailable, "Compaction already running")
		}

		defer atomic.CompareAndSwapUint32(&db.CompactState, DBCompactRunning, DBCompactNotRunning)
	}

	// Compact should be a no-op if not running w/ xattrs
	if !db.UseXattrs() {
		return 0, nil
	}

	// Trigger view compaction for all tombstoned documents older than the purge interval
	startTime := time.Now()
	purgeOlderThan := startTime.Add(-db.PurgeInterval)

	purgedDocCount := 0

	defer callback(&purgedDocCount)

	base.InfofCtx(ctx, base.KeyAll, "Starting compaction of purged tombstones for %s ...", base.MD(db.Name))

	// Update metadata purge interval if not explicitly set to 0 (used in testing)
	if db.PurgeInterval > 0 {
		cbStore, ok := base.AsCouchbaseBucketStore(db.Bucket)
		if ok {
			serverPurgeInterval, err := cbStore.MetadataPurgeInterval()
			if err != nil {
				base.WarnfCtx(ctx, "Unable to retrieve server's metadata purge interval - using existing purge interval. %s", err)
			} else if serverPurgeInterval > 0 {
				db.PurgeInterval = serverPurgeInterval
			}
		}
	}
	base.InfofCtx(ctx, base.KeyAll, "Tombstone compaction using the metadata purge interval of %.2f days.", db.PurgeInterval.Hours()/24)

	purgeBody := Body{"_purged": true}
	for _, c := range db.CollectionByID {
		// shadow ctx, sot that we can't misuse the parent's inside the loop
		ctx := base.CollectionLogCtx(ctx, c.Name)

		// create admin collection interface
		collection, err := db.GetDatabaseCollectionWithUser(c.ScopeName, c.Name)
		if err != nil {
			base.WarnfCtx(ctx, "Tombstone compaction could not get collection: %s", err)
			continue
		}

		for {
			purgedDocs := make([]string, 0)
			results, err := collection.QueryTombstones(ctx, purgeOlderThan, QueryTombstoneBatch)
			if err != nil {
				return 0, err
			}
			var tombstonesRow QueryIdRow
			var resultCount int
			for results.Next(&tombstonesRow) {
				select {
				case <-terminator.Done():
					closeErr := results.Close()
					if closeErr != nil {
						return 0, closeErr
					}
					return purgedDocCount, nil
				default:
				}

				resultCount++
				base.DebugfCtx(ctx, base.KeyCRUD, "\tDeleting %q", tombstonesRow.Id)
				// First, attempt to purge.
				purgeErr := collection.Purge(ctx, tombstonesRow.Id)
				if purgeErr == nil {
					purgedDocs = append(purgedDocs, tombstonesRow.Id)
				} else if base.IsDocNotFoundError(purgeErr) {
					// If key no longer exists, need to add and remove to trigger removal from view
					_, addErr := collection.dataStore.Add(tombstonesRow.Id, 0, purgeBody)
					if addErr != nil {
						base.WarnfCtx(ctx, "Error compacting key %s (add) - tombstone will not be compacted.  %v", base.UD(tombstonesRow.Id), addErr)
						continue
					}

					// At this point, the doc is not in a usable state for mobile
					// so mark it to be removed from cache, even if the subsequent delete fails
					purgedDocs = append(purgedDocs, tombstonesRow.Id)

					if delErr := collection.dataStore.Delete(tombstonesRow.Id); delErr != nil {
						base.ErrorfCtx(ctx, "Error compacting key %s (delete) - tombstone will not be compacted.  %v", base.UD(tombstonesRow.Id), delErr)
					}
				} else {
					base.WarnfCtx(ctx, "Error compacting key %s (purge) - tombstone will not be compacted.  %v", base.UD(tombstonesRow.Id), purgeErr)
				}
			}

			err = results.Close()
			if err != nil {
				return 0, err
			}

			// Now purge them from all channel caches
			count := len(purgedDocs)
			purgedDocCount += count
			if count > 0 {
				collection.RemoveFromChangeCache(purgedDocs, startTime)
				collection.dbStats().Database().NumTombstonesCompacted.Add(int64(count))
			}
			base.DebugfCtx(ctx, base.KeyAll, "Compacted %v tombstones", count)

			callback(&purgedDocCount)

			if resultCount < QueryTombstoneBatch {
				break
			}
		}
	}
	base.InfofCtx(ctx, base.KeyAll, "Finished compaction of purged tombstones for %s... Total Tombstones Compacted: %d", base.MD(db.Name), purgedDocCount)

	return purgedDocCount, nil
}

// Re-runs the sync function on every current document in the database (if doCurrentDocs==true)
// and/or imports docs in the bucket not known to the gateway (if doImportDocs==true).
// To be used when the JavaScript sync function changes.
type updateAllDocChannelsCallbackFunc func(docsProcessed, docsChanged *int)

func (db *DatabaseCollectionWithUser) UpdateAllDocChannels(ctx context.Context, regenerateSequences bool, callback updateAllDocChannelsCallbackFunc, terminator *base.SafeTerminator) (int, error) {
	base.InfofCtx(ctx, base.KeyAll, "Recomputing document channels...")
	base.InfofCtx(ctx, base.KeyAll, "Re-running sync function on all documents...")

	queryLimit := db.queryPaginationLimit()
	startSeq := uint64(0)
	endSeq, err := db.sequences().getSequence()
	if err != nil {
		return 0, err
	}

	docsChanged := 0
	docsProcessed := 0

	// In the event of an early exit we would like to ensure these values are up to date which they wouldn't be if they
	// were unable to reach the end of the batch iteration.
	defer callback(&docsProcessed, &docsChanged)

	var unusedSequences []uint64

	for {
		results, err := db.QueryResync(ctx, queryLimit, startSeq, endSeq)
		if err != nil {
			return 0, err
		}

		queryRowCount := 0
		highSeq := uint64(0)

		var importRow QueryIdRow
		for results.Next(&importRow) {
			select {
			case <-terminator.Done():
				base.InfofCtx(ctx, base.KeyAll, "Resync was stopped before the operation could be completed. System "+
					"may be in an inconsistent state. Docs changed: %d Docs Processed: %d", docsChanged, docsProcessed)
				closeErr := results.Close()
				if closeErr != nil {
					return 0, closeErr
				}
				return docsChanged, nil
			default:
			}

			docid := importRow.Id
			key := realDocID(docid)
			queryRowCount++
			docsProcessed++
			highSeq, unusedSequences, err = db.resyncDocument(ctx, docid, key, regenerateSequences, unusedSequences)
			if err == nil {
				docsChanged++
			} else if err != base.ErrUpdateCancel {
				base.WarnfCtx(ctx, "Error updating doc %q: %v", base.UD(docid), err)
			}
		}

		callback(&docsProcessed, &docsChanged)

		// Close query results
		closeErr := results.Close()
		if closeErr != nil {
			return 0, closeErr
		}

		if queryRowCount < queryLimit || highSeq >= endSeq {
			break
		}
		startSeq = highSeq + 1
	}

	db.releaseSequences(ctx, unusedSequences)

	if regenerateSequences {
		if err := db.updateAllPrincipalsSequences(ctx); err != nil {
			return docsChanged, err
		}
	}

	base.InfofCtx(ctx, base.KeyAll, "Finished re-running sync function; %d/%d docs changed", docsChanged, docsProcessed)

	if docsChanged > 0 {
		db.invalidateAllPrincipalsCache(ctx, endSeq)
	}
	return docsChanged, nil
}

// invalidate channel cache of all users/roles:
func (c *DatabaseCollection) invalidateAllPrincipalsCache(ctx context.Context, endSeq uint64) {
	base.InfofCtx(ctx, base.KeyAll, "Invalidating channel caches of users/roles...")
	users, roles, _ := c.allPrincipalIDs(ctx)
	for _, name := range users {
		c.invalUserChannels(ctx, name, endSeq)
	}
	for _, name := range roles {
		c.invalRoleChannels(ctx, name, endSeq)
	}
}

func (c *DatabaseCollection) updateAllPrincipalsSequences(ctx context.Context) error {
	users, roles, err := c.allPrincipalIDs(ctx)
	if err != nil {
		return err
	}

	authr := c.Authenticator(ctx)

	for _, role := range roles {
		role, err := authr.GetRole(role)
		if err != nil {
			return err
		}
		err = c.regeneratePrincipalSequences(authr, role)
		if err != nil {
			return err
		}
	}

	for _, user := range users {
		user, err := authr.GetUser(user)
		if err != nil {
			return err
		}
		err = c.regeneratePrincipalSequences(authr, user)
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *DatabaseCollection) regeneratePrincipalSequences(authr *auth.Authenticator, princ auth.Principal) error {
	nextSeq, err := c.sequences().nextSequence()
	if err != nil {
		return err
	}

	err = authr.UpdateSequenceNumber(princ, nextSeq)
	if err != nil {
		return err
	}

	return nil
}

func (c *DatabaseCollection) releaseSequences(ctx context.Context, sequences []uint64) {
	for _, sequence := range sequences {
		err := c.sequences().releaseSequence(sequence)
		if err != nil {
			base.WarnfCtx(ctx, "Error attempting to release sequence %d. Error %v", sequence, err)
		}
	}
}

func (db *DatabaseCollectionWithUser) getResyncedDocument(ctx context.Context, doc *Document, regenerateSequences bool, unusedSequences []uint64) (updatedDoc *Document, shouldUpdate bool, updatedExpiry *uint32, highSeq uint64, updatedUnusedSequences []uint64, err error) {
	docid := doc.ID
	forceUpdate := false
	if !doc.HasValidSyncData() {
		// This is a document not known to the sync gateway. Ignore it:
		return nil, false, nil, doc.Sequence, unusedSequences, base.ErrUpdateCancel
	}

	base.DebugfCtx(ctx, base.KeyCRUD, "\tRe-syncing document %q", base.UD(docid))

	// Run the sync fn over each current/leaf revision, in case there are conflicts:
	changed := 0
	doc.History.forEachLeaf(func(rev *RevInfo) {
		bodyBytes, _, err := db.get1xRevFromDoc(ctx, doc, rev.ID, false)
		if err != nil {
			base.WarnfCtx(ctx, "Error getting rev from doc %s/%s %s", base.UD(docid), rev.ID, err)
		}
		var body Body
		if err := body.Unmarshal(bodyBytes); err != nil {
			base.WarnfCtx(ctx, "Error unmarshalling body %s/%s for sync function %s", base.UD(docid), rev.ID, err)
			return
		}
		metaMap, err := doc.GetMetaMap(db.userXattrKey())
		if err != nil {
			return
		}
		channels, access, roles, syncExpiry, _, err := db.getChannelsAndAccess(ctx, doc, body, metaMap, rev.ID)
		if err != nil {
			// Probably the validator rejected the doc
			base.WarnfCtx(ctx, "Error calling sync() on doc %q: %v", base.UD(docid), err)
			access = nil
			channels = nil
		}
		rev.Channels = channels

		if rev.ID == doc.CurrentRev {
			if regenerateSequences {
				updatedUnusedSequences, err = db.assignSequence(ctx, 0, doc, unusedSequences)
				if err != nil {
					base.WarnfCtx(ctx, "Unable to assign a sequence number: %v", err)
				}
				forceUpdate = true
			}

			changedChannels, err := doc.updateChannels(ctx, channels)
			changed = len(doc.Access.updateAccess(doc, access)) +
				len(doc.RoleAccess.updateAccess(doc, roles)) +
				len(changedChannels)
			if err != nil {
				return
			}
			// Only update document expiry based on the current (active) rev
			if syncExpiry != nil {
				doc.UpdateExpiry(*syncExpiry)
				updatedExpiry = syncExpiry
			}
		}
	})
	shouldUpdate = changed > 0 || forceUpdate
	return doc, shouldUpdate, updatedExpiry, doc.Sequence, updatedUnusedSequences, nil
}

func (db *DatabaseCollectionWithUser) resyncDocument(ctx context.Context, docid, key string, regenerateSequences bool, unusedSequences []uint64) (updatedHighSeq uint64, updatedUnusedSequences []uint64, err error) {
	var updatedDoc *Document
	var shouldUpdate bool
	var updatedExpiry *uint32
	if db.UseXattrs() {
		writeUpdateFunc := func(currentValue []byte, currentXattr []byte, currentUserXattr []byte, cas uint64) (
			raw []byte, rawXattr []byte, deleteDoc bool, expiry *uint32, err error) {
			// There's no scenario where a doc should from non-deleted to deleted during UpdateAllDocChannels processing,
			// so deleteDoc is always returned as false.
			if currentValue == nil || len(currentValue) == 0 {
				return nil, nil, deleteDoc, nil, base.ErrUpdateCancel
			}
			doc, err := unmarshalDocumentWithXattr(docid, currentValue, currentXattr, currentUserXattr, cas, DocUnmarshalAll)
			if err != nil {
				return nil, nil, deleteDoc, nil, err
			}
			updatedDoc, shouldUpdate, updatedExpiry, updatedHighSeq, unusedSequences, err = db.getResyncedDocument(ctx, doc, regenerateSequences, unusedSequences)
			if err != nil {
				return nil, nil, deleteDoc, nil, err
			}
			if shouldUpdate {
				base.InfofCtx(ctx, base.KeyAccess, "Saving updated channels and access grants of %q", base.UD(docid))
				if updatedExpiry != nil {
					updatedDoc.UpdateExpiry(*updatedExpiry)
				}

				doc.SetCrc32cUserXattrHash()
				raw, rawXattr, err = updatedDoc.MarshalWithXattr()
				return raw, rawXattr, deleteDoc, updatedExpiry, err
			} else {
				return nil, nil, deleteDoc, nil, base.ErrUpdateCancel
			}
		}
		_, err = db.dataStore.WriteUpdateWithXattr(key, base.SyncXattrName, db.userXattrKey(), 0, nil, nil, writeUpdateFunc)
	} else {
		_, err = db.dataStore.Update(key, 0, func(currentValue []byte) ([]byte, *uint32, bool, error) {
			// Be careful: this block can be invoked multiple times if there are races!
			if currentValue == nil {
				return nil, nil, false, base.ErrUpdateCancel // someone deleted it?!
			}
			doc, err := unmarshalDocument(docid, currentValue)
			if err != nil {
				return nil, nil, false, err
			}
			updatedDoc, shouldUpdate, updatedExpiry, updatedHighSeq, unusedSequences, err = db.getResyncedDocument(ctx, doc, regenerateSequences, unusedSequences)
			if err != nil {
				return nil, nil, false, err
			}
			if shouldUpdate {
				base.InfofCtx(ctx, base.KeyAccess, "Saving updated channels and access grants of %q", base.UD(docid))
				if updatedExpiry != nil {
					updatedDoc.UpdateExpiry(*updatedExpiry)
				}

				updatedBytes, marshalErr := base.JSONMarshal(updatedDoc)
				return updatedBytes, updatedExpiry, false, marshalErr
			} else {
				return nil, nil, false, base.ErrUpdateCancel
			}
		})
	}
	return updatedHighSeq, unusedSequences, err
}

func (c *DatabaseCollection) invalUserRoles(ctx context.Context, username string, invalSeq uint64) {
	authr := c.Authenticator(ctx)
	if err := authr.InvalidateRoles(username, invalSeq); err != nil {
		base.WarnfCtx(ctx, "Error invalidating roles for user %s: %v", base.UD(username), err)
	}
}

func (c *DatabaseCollection) invalUserChannels(ctx context.Context, username string, invalSeq uint64) {
	authr := c.Authenticator(ctx)
	if err := authr.InvalidateChannels(username, true, c.ScopeName, c.Name, invalSeq); err != nil {
		base.WarnfCtx(ctx, "Error invalidating channels for user %s: %v", base.UD(username), err)
	}
}

func (c *DatabaseCollection) invalRoleChannels(ctx context.Context, rolename string, invalSeq uint64) {
	authr := c.Authenticator(ctx)
	if err := authr.InvalidateChannels(rolename, false, c.ScopeName, c.Name, invalSeq); err != nil {
		base.WarnfCtx(ctx, "Error invalidating channels for role %s: %v", base.UD(rolename), err)
	}
}

func (c *DatabaseCollection) invalUserOrRoleChannels(ctx context.Context, name string, invalSeq uint64) {

	principalName, isRole := channels.AccessNameToPrincipalName(name)
	if isRole {
		c.invalRoleChannels(ctx, principalName, invalSeq)
	} else {
		c.invalUserChannels(ctx, principalName, invalSeq)
	}
}

func (dbCtx *DatabaseContext) ObtainManagementEndpoints(ctx context.Context) ([]string, error) {
	cbStore, ok := base.AsCouchbaseBucketStore(dbCtx.Bucket)
	if !ok {
		base.WarnfCtx(ctx, "Database %v: Unable to get server management endpoints. Underlying bucket type was not GoCBBucket.", base.MD(dbCtx.Name))
		return nil, nil
	}

	return base.GoCBBucketMgmtEndpoints(cbStore)
}

func (dbCtx *DatabaseContext) ObtainManagementEndpointsAndHTTPClient(ctx context.Context) ([]string, *http.Client, error) {
	cbStore, ok := base.AsCouchbaseBucketStore(dbCtx.Bucket)
	if !ok {
		base.WarnfCtx(ctx, "Database %v: Unable to get server management endpoints. Underlying bucket type was not GoCBBucket.", base.MD(dbCtx.Name))
		return nil, nil, nil
	}

	// This shouldn't happen as the only place we don't initialize this is in the case where we're not using a Couchbase
	// Bucket. This means the above check should catch it but check just to be safe.
	if dbCtx.NoX509HTTPClient == nil {
		return nil, nil, fmt.Errorf("unable to obtain http client")
	}

	endpoints, err := base.GoCBBucketMgmtEndpoints(cbStore)
	if err != nil {
		return nil, nil, err
	}

	return endpoints, dbCtx.NoX509HTTPClient, nil
}

func (context *DatabaseContext) GetUserViewsEnabled() bool {
	if context.Options.UnsupportedOptions != nil && context.Options.UnsupportedOptions.UserViews != nil && context.Options.UnsupportedOptions.UserViews.Enabled != nil {
		return *context.Options.UnsupportedOptions.UserViews.Enabled
	}
	return false
}

func (context *DatabaseContext) UseXattrs() bool {
	return context.Options.EnableXattr
}

func (context *DatabaseContext) UseViews() bool {
	return context.Options.UseViews
}

// UseQueryBasedResyncManager returns if query bases resync manager should be used for Resync operation
func (context *DatabaseContext) UseQueryBasedResyncManager() bool {
	if context.Options.UnsupportedOptions != nil {
		return context.Options.UnsupportedOptions.UseQueryBasedResyncManager
	}
	return false
}

func (context *DatabaseContext) DeltaSyncEnabled() bool {
	return context.Options.DeltaSyncOptions.Enabled
}

func (c *DatabaseCollection) AllowExternalRevBodyStorage() bool {

	// Support unit testing w/out xattrs enabled
	if base.TestExternalRevStorage {
		return false
	}
	return !c.UseXattrs()
}

func (context *DatabaseContext) SetUserViewsEnabled(value bool) {
	if context.Options.UnsupportedOptions == nil {
		context.Options.UnsupportedOptions = &UnsupportedOptions{}
	}
	if context.Options.UnsupportedOptions.UserViews == nil {
		context.Options.UnsupportedOptions.UserViews = &UserViewsOptions{}
	}
	context.Options.UnsupportedOptions.UserViews.Enabled = &value
}

func initDatabaseStats(dbName string, autoImport bool, options DatabaseContextOptions) (*base.DbStats, error) {

	enabledDeltaSync := options.DeltaSyncOptions.Enabled
	enabledImport := autoImport || options.EnableXattr
	enabledViews := options.UseViews

	var queryNames []string
	if enabledViews {
		queryNames = []string{
			fmt.Sprintf(base.StatViewFormat, DesignDocSyncGateway(), ViewAccess),
			fmt.Sprintf(base.StatViewFormat, DesignDocSyncGateway(), ViewAccessVbSeq),
			fmt.Sprintf(base.StatViewFormat, DesignDocSyncGateway(), ViewChannels),
			fmt.Sprintf(base.StatViewFormat, DesignDocSyncGateway(), ViewPrincipals),
			fmt.Sprintf(base.StatViewFormat, DesignDocSyncGateway(), ViewRolesExcludeDeleted),
			fmt.Sprintf(base.StatViewFormat, DesignDocSyncGateway(), ViewRoleAccess),
			fmt.Sprintf(base.StatViewFormat, DesignDocSyncGateway(), ViewRoleAccessVbSeq),
			fmt.Sprintf(base.StatViewFormat, DesignDocSyncHousekeeping(), ViewAllDocs),
			fmt.Sprintf(base.StatViewFormat, DesignDocSyncHousekeeping(), ViewImport),
			fmt.Sprintf(base.StatViewFormat, DesignDocSyncHousekeeping(), ViewSessions),
			fmt.Sprintf(base.StatViewFormat, DesignDocSyncHousekeeping(), ViewTombstones),
		}
	} else {
		queryNames = []string{
			QueryTypeAccess,
			QueryTypeRoleAccess,
			QueryTypeChannels,
			QueryTypeChannelsStar,
			QueryTypeSequences,
			QueryTypePrincipals,
			QueryTypeRolesExcludeDeleted,
			QueryTypeRoles,
			QueryTypeSessions,
			QueryTypeTombstones,
			QueryTypeResync,
			QueryTypeAllDocs,
			QueryTypeUsers,
		}
	}

	if options.UserFunctions != nil {
		for _, fn := range options.UserFunctions.Definitions {
			if queryName, ok := fn.N1QLQueryName(); ok {
				queryNames = append(queryNames, queryName)
			}
		}
	}
	if options.GraphQL != nil {
		queryNames = append(queryNames, options.GraphQL.N1QLQueryNames()...)
	}

	var collections []string
	if len(options.Scopes) == 0 {
		base.DebugfCtx(context.TODO(), base.KeyConfig, "No named collections defined for database %q, using default collection for stats", base.MD(dbName))
		collections = append(collections, base.DefaultScope+"."+base.DefaultCollection)
	} else {
		for scopeName, scope := range options.Scopes {
			for collectionName := range scope.Collections {
				collections = append(collections, scopeName+"."+collectionName)
			}
		}
	}

	return base.SyncGatewayStats.NewDBStats(dbName, enabledDeltaSync, enabledImport, enabledViews, queryNames, collections)
}

func (context *DatabaseContext) AllowConflicts() bool {
	if context.Options.AllowConflicts != nil {
		return *context.Options.AllowConflicts
	}
	return base.DefaultAllowConflicts
}

func (context *DatabaseContext) AllowFlushNonCouchbaseBuckets() bool {
	if context.Options.UnsupportedOptions != nil && context.Options.UnsupportedOptions.APIEndpoints != nil {
		return context.Options.UnsupportedOptions.APIEndpoints.EnableCouchbaseBucketFlush
	}
	return false
}

// ////// SEQUENCE ALLOCATION:

func (context *DatabaseContext) LastSequence() (uint64, error) {
	return context.sequences.lastSequence()
}

// Helpers for unsupported options
func (context *DatabaseContext) IsGuestReadOnly() bool {
	return context.Options.UnsupportedOptions != nil && context.Options.UnsupportedOptions.GuestReadOnly
}

// ////// TIMEOUTS

// Calls a function, synchronously, while imposing a timeout on the Database's Context. Any call to CheckTimeout while the function is running will return an error if the timeout has expired.
// The function will *not* be aborted automatically! Its code must check for timeouts by calling CheckTimeout periodically, returning once that produces an error.
func WithTimeout(ctx context.Context, timeout time.Duration, operation func(tmCtx context.Context) error) error {
	newCtx, cancel := context.WithTimeout(ctx, timeout)
	defer func() {
		cancel()
	}()
	return operation(newCtx)
}

// Returns an HTTP timeout (408) error if the Database's Context has an expired timeout or has been explicitly canceled. (See WithTimeout.)
func CheckTimeout(ctx context.Context) error {
	if ctx == nil {
		return nil
	}
	select {
	case <-ctx.Done():
		return base.HTTPErrorf(http.StatusRequestTimeout, "Request timed out")
	default:
		return nil
	}
}

// / LOGGING

// AddDatabaseLogContext adds database name to the parent context for logging
func (dbCtx *DatabaseContext) AddDatabaseLogContext(ctx context.Context) context.Context {
	if dbCtx != nil && dbCtx.Name != "" {
		return base.DatabaseLogCtx(ctx, dbCtx.Name)
	}
	return ctx
}

// onlyDefaultCollection is true if the database is only configured with default collection.
func (dbCtx *DatabaseContext) OnlyDefaultCollection() bool {
	if len(dbCtx.CollectionByID) > 1 {
		return false
	}
	_, exists := dbCtx.CollectionByID[base.DefaultCollectionID]
	return exists
}

// hasDefaultCollection is true if the database is configured with default collection
func (dbCtx *DatabaseContext) HasDefaultCollection() bool {
	_, exists := dbCtx.CollectionByID[base.DefaultCollectionID]
	return exists
}

// GetDatabaseCollectionWithUser returns a DatabaseCollectionWithUser if the collection exists on the database, otherwise error.
func (dbc *Database) GetDatabaseCollectionWithUser(scopeName, collectionName string) (*DatabaseCollectionWithUser, error) {
	collection, err := dbc.GetDatabaseCollection(scopeName, collectionName)
	if err != nil {
		return nil, err
	}
	return &DatabaseCollectionWithUser{
		DatabaseCollection: collection,
		user:               dbc.user,
	}, nil
}

// GetDatabaseCollection returns a collection if one exists, otherwise error.
func (dbc *DatabaseContext) GetDatabaseCollection(scopeName, collectionName string) (*DatabaseCollection, error) {
	if base.IsDefaultCollection(scopeName, collectionName) {
		return dbc.GetDefaultDatabaseCollection()
	}
	if dbc.Scopes == nil {
		return nil, fmt.Errorf("scope %q does not exist on this database", base.UD(scopeName))
	}
	collections, exists := dbc.Scopes[scopeName]
	if !exists {
		return nil, fmt.Errorf("scope %q does not exist on this database", base.UD(scopeName))
	}
	collection, exists := collections.Collections[collectionName]
	if !exists {
		return nil, fmt.Errorf("collection \"%s.%s\" does not exist on this database", base.UD(scopeName), base.UD(collectionName))
	}
	return collection, nil
}

func (dbc *DatabaseContext) GetDefaultDatabaseCollection() (*DatabaseCollection, error) {
	col, exists := dbc.CollectionByID[base.DefaultCollectionID]
	if !exists {
		return nil, fmt.Errorf("default collection is not configured on this database")
	}
	return col, nil
}

// GetDefaultDatabaseCollectionWithUser will return the default collection if the default collection is supplied in the database config.
func (dbc *Database) GetDefaultDatabaseCollectionWithUser() (*DatabaseCollectionWithUser, error) {
	col, err := dbc.GetDefaultDatabaseCollection()
	if err != nil {
		return nil, err
	}
	return &DatabaseCollectionWithUser{
		DatabaseCollection: col,
		user:               dbc.user,
	}, nil
}

func (dbc *DatabaseContext) AuthenticatorOptions() auth.AuthenticatorOptions {
	defaultOptions := auth.DefaultAuthenticatorOptions()
	defaultOptions.MetaKeys = dbc.MetadataKeys
	return defaultOptions
}

// GetRequestPlusSequence fetches the current value of the sequence counter for the database.
// Uses getSequence (instead of lastSequence) as it's intended to be up to date with allocations
// across all nodes, while lastSequence is just the latest allocation from this node
func (dbc *DatabaseContext) GetRequestPlusSequence() (uint64, error) {
	return dbc.sequences.getSequence()
}

func (db *DatabaseContext) StartOnlineProcesses(ctx context.Context) (returnedError error) {
	cleanupFunctions := make([]func(), 0)

	defer func() {
		if returnedError != nil {
			for _, cleanupFunc := range cleanupFunctions {
				cleanupFunc()
			}
		}
	}()

	// Create config-based principals
	// Create default users & roles:
	if db.Options.ConfigPrincipals != nil {
		if err := db.InstallPrincipals(ctx, db.Options.ConfigPrincipals.Roles, "role"); err != nil {
			return err
		}
		if err := db.InstallPrincipals(ctx, db.Options.ConfigPrincipals.Users, "user"); err != nil {
			return err
		}

		if db.Options.ConfigPrincipals.Guest != nil {
			guest := map[string]*auth.PrincipalConfig{base.GuestUsername: db.Options.ConfigPrincipals.Guest}
			if err := db.InstallPrincipals(ctx, guest, "user"); err != nil {
				return err
			}
		}
	}

	// Callback that is invoked whenever a set of channels is changed in the ChangeCache
	notifyChange := func(changedChannels channels.Set) {
		db.mutationListener.Notify(changedChannels)
	}

	// Initialize the ChangeCache.  Will be locked and unusable until .Start() is called (SG #3558)
	if err := db.changeCache.Init(
		ctx,
		db,
		db.channelCache,
		notifyChange,
		db.Options.CacheOptions,
		db.MetadataKeys,
	); err != nil {
		base.DebugfCtx(ctx, base.KeyDCP, "Error initializing the change cache", err)
		return err
	}

	db.mutationListener.OnChangeCallback = db.changeCache.DocChanged

	if base.IsEnterpriseEdition() {
		cfgSG, ok := db.CfgSG.(*base.CfgSG)
		if !ok {
			return fmt.Errorf("Could not cast %V to CfgSG", db.CfgSG)
		}
		db.changeCache.cfgEventCallback = cfgSG.FireEvent
	}

	importEnabled := db.UseXattrs() && db.autoImport
	sgReplicateEnabled := db.Options.SGReplicateOptions.Enabled

	// Initialize node heartbeater in EE mode if sg-replicate or import enabled on the node.  This node must start
	// sending heartbeats before registering itself to the cfg, to avoid triggering immediate removal by other active nodes.
	if base.IsEnterpriseEdition() && (importEnabled || sgReplicateEnabled) {
		// Create heartbeater
		heartbeaterPrefix := db.MetadataKeys.HeartbeaterPrefix(db.Options.GroupID)
		heartbeater, err := base.NewCouchbaseHeartbeater(db.MetadataStore, heartbeaterPrefix, db.UUID)
		if err != nil {
			return pkgerrors.Wrapf(err, "Error starting heartbeater for bucket %s", base.MD(db.Bucket.GetName()).Redact())
		}
		err = heartbeater.StartSendingHeartbeats()
		if err != nil {
			return err
		}
		db.Heartbeater = heartbeater

		cleanupFunctions = append(cleanupFunctions, func() {
			db.Heartbeater.Stop()
		})
	}

	// If sgreplicate is enabled on this node, register this node to accept notifications
	if sgReplicateEnabled {
		registerNodeErr := db.SGReplicateMgr.StartLocalNode(db.UUID, db.Heartbeater)
		if registerNodeErr != nil {
			return registerNodeErr
		}

		cleanupFunctions = append(cleanupFunctions, func() {
			db.SGReplicateMgr.Stop()
		})
	}

	// Start DCP feed
	base.InfofCtx(ctx, base.KeyDCP, "Starting mutation feed on bucket %v", base.MD(db.Bucket.GetName()))
	cacheFeedStatsMap := db.DbStats.Database().CacheFeedMapStats
	if err := db.mutationListener.Start(db.Bucket, cacheFeedStatsMap.Map, db.Scopes, db.MetadataStore); err != nil {
		db.channelCache = nil
		return err
	}

	cleanupFunctions = append(cleanupFunctions, func() {
		db.mutationListener.Stop()
	})

	// Get current value of _sync:seq
	initialSequence, seqErr := db.sequences.lastSequence()
	if seqErr != nil {
		return seqErr
	}
	initialSequenceTime := time.Now()

	// Unlock change cache.  Validate that any allocated sequences on other nodes have either been assigned or released
	// before starting
	if initialSequence > 0 {
		_ = db.sequences.waitForReleasedSequences(initialSequenceTime)
	}

	if err := db.changeCache.Start(initialSequence); err != nil {
		return err
	}
	cleanupFunctions = append(cleanupFunctions, func() {
		db.changeCache.Stop()
	})

	// If this is an xattr import node, start import feed.  Must be started after the caching DCP feed, as import cfg
	// subscription relies on the caching feed.
	if importEnabled {
		db.ImportListener = NewImportListener(ctx, db.MetadataKeys.DCPCheckpointPrefix(db.Options.GroupID), db)
		if importFeedErr := db.ImportListener.StartImportFeed(db); importFeedErr != nil {
			return importFeedErr
		}

		cleanupFunctions = append(cleanupFunctions, func() {
			db.ImportListener.Stop()
		})
	}

	// Load providers into provider map.  Does basic validation on the provider definition, and identifies the default provider.
	if db.Options.OIDCOptions != nil {
		db.OIDCProviders = make(auth.OIDCProviderMap)

		for name, provider := range db.Options.OIDCOptions.Providers {
			if provider.Issuer == "" || base.StringDefault(provider.ClientID, "") == "" {
				// TODO: this duplicates a check in DbConfig.validate to avoid a backwards compatibility issue
				base.WarnfCtx(ctx, "Issuer and Client ID not defined for provider %q - skipping", base.UD(name))
				continue
			}
			provider.Name = name

			// If this is the default provider, or there's only one provider defined, set IsDefault
			if (db.Options.OIDCOptions.DefaultProvider != nil && name == *db.Options.OIDCOptions.DefaultProvider) || len(db.Options.OIDCOptions.Providers) == 1 {
				provider.IsDefault = true
			}

			insecureSkipVerify := false
			if db.Options.UnsupportedOptions != nil {
				insecureSkipVerify = db.Options.UnsupportedOptions.OidcTlsSkipVerify
			}
			provider.InsecureSkipVerify = insecureSkipVerify

			// If this isn't the default provider, add the provider to the callback URL (needed to identify provider to _oidc_callback)
			if !provider.IsDefault && provider.CallbackURL != nil {
				updatedCallback, err := auth.SetURLQueryParam(*provider.CallbackURL, auth.OIDCAuthProvider, name)
				if err != nil {
					return base.RedactErrorf("Failed to add provider %q to OIDC callback URL: %v", base.UD(name), err)
				}
				provider.CallbackURL = &updatedCallback
			}

			db.OIDCProviders[name] = provider
		}
	}

	db.LocalJWTProviders = make(auth.LocalJWTProviderMap, len(db.Options.LocalJWTConfig))
	for name, cfg := range db.Options.LocalJWTConfig {
		db.LocalJWTProviders[name] = cfg.BuildProvider(name)
	}

	if db.UseXattrs() {
		// Set the purge interval for tombstone compaction
		db.PurgeInterval = DefaultPurgeInterval
		cbStore, ok := base.AsCouchbaseBucketStore(db.Bucket)
		if ok {
			serverPurgeInterval, err := cbStore.MetadataPurgeInterval()
			if err != nil {
				base.WarnfCtx(ctx, "Unable to retrieve server's metadata purge interval - will use default value. %s", err)
			} else if serverPurgeInterval > 0 {
				db.PurgeInterval = serverPurgeInterval
			}
		}
		base.InfofCtx(ctx, base.KeyAll, "Using metadata purge interval of %.2f days for tombstone compaction.", db.PurgeInterval.Hours()/24)

		if db.Options.CompactInterval != 0 {
			if db.autoImport {
				db := Database{DatabaseContext: db}
				// Wrap the dbContext's terminator in a SafeTerminator for the compaction task
				bgtTerminator := base.NewSafeTerminator()
				go func() {
					<-db.terminator
					bgtTerminator.Close()
				}()
				bgt, err := NewBackgroundTask(ctx, "Compact", func(ctx context.Context) error {
					_, err := db.Compact(ctx, false, func(purgedDocCount *int) {}, bgtTerminator)
					if err != nil {
						base.WarnfCtx(ctx, "Error trying to compact tombstoned documents for %q with error: %v", db.Name, err)
					}
					return nil
				}, time.Duration(db.Options.CompactInterval)*time.Second, db.terminator)
				if err != nil {
					return err
				}
				db.backgroundTasks = append(db.backgroundTasks, bgt)
			} else {
				base.WarnfCtx(ctx, "Automatic compaction can only be enabled on nodes running an Import process")
			}
		}

	}

	if err := base.RequireNoBucketTTL(db.Bucket); err != nil {
		return err
	}

	db.ExitChanges = make(chan struct{})

	// Start checking heartbeats for other nodes.  Must be done after caching feed starts, to ensure any removals
	// are detected and processed by this node.
	if db.Heartbeater != nil {
		if err := db.Heartbeater.StartCheckingHeartbeats(); err != nil {
			return err
		}
		// No cleanup necessary, stop heartbeater above will take care of it
	}

	db.TombstoneCompactionManager = NewTombstoneCompactionManager()
	db.AttachmentCompactionManager = NewAttachmentCompactionManager(db.MetadataStore, db.MetadataKeys)

	db.startReplications(ctx)

	return nil
}

func (dbc *DatabaseContext) InstallPrincipals(ctx context.Context, spec map[string]*auth.PrincipalConfig, what string) error {
	for name, princ := range spec {
		isGuest := name == base.GuestUsername
		if isGuest {
			internalName := ""
			princ.Name = &internalName
		} else {
			n := name
			princ.Name = &n
		}

		createdPrincipal := true
		worker := func() (shouldRetry bool, err error, value interface{}) {
			_, err = dbc.UpdatePrincipal(ctx, princ, (what == "user"), isGuest)
			if err != nil {
				if status, _ := base.ErrorAsHTTPStatus(err); status == http.StatusConflict {
					// Ignore and absorb this error if it's a conflict error, which just means that updatePrincipal didn't overwrite an existing user.
					// Since if there's an existing user it's "mission accomplished", this can be treated as a success case.
					createdPrincipal = false
					return false, nil, nil
				}

				if err == base.ErrViewTimeoutError {
					// Timeout error, possibly due to view re-indexing, so retry
					base.InfofCtx(ctx, base.KeyAuth, "Error calling UpdatePrincipal(): %v.  Will retry in case this is a temporary error", err)
					return true, err, nil
				}

				// Unexpected error, return error don't retry
				return false, err, nil
			}

			// No errors, assume it worked
			return false, nil, nil

		}

		err, _ := base.RetryLoop("installPrincipals", worker, base.CreateDoublingSleeperFunc(16, 10))
		if err != nil {
			return err
		}

		if isGuest {
			base.InfofCtx(ctx, base.KeyAll, "Reset guest user to config")
		} else if createdPrincipal {
			base.InfofCtx(ctx, base.KeyAll, "Created %s %q", what, base.UD(name))
		}

	}
	return nil
}
