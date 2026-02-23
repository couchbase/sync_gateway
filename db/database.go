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
	"testing"
	"time"

	"github.com/couchbase/cbgt"
	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/auth"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"
	pkgerrors "github.com/pkg/errors"
	"golang.org/x/exp/maps"
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
	Import DocUpdateType = iota
	NewVersion
	ExistingVersion
	ExistingVersionLegacyRev
	ExistingVersionWithUpdateToHLV
	NoHLVUpdateForTest
)

type DocUpdateType uint32

const (
	DefaultRevsLimitNoConflicts = 50
	DefaultRevsLimitConflicts   = 100

	// DefaultPurgeInterval represents a time duration of 30 days to be
	// used as default metadata purge interval when the server’s purge
	// interval (either bucket specific or cluster wide) is not available.
	DefaultPurgeInterval                    = 30 * 24 * time.Hour
	DefaultVersionPruningWindow             = DefaultPurgeInterval
	DefaultSGReplicateEnabled               = true
	DefaultSGReplicateWebsocketPingInterval = time.Minute * 5
	DefaultCompactInterval                  = 24 * time.Hour
	DefaultStoreLegacyRevTreeData           = true // for 4.0, this is opt-out - we can switch to opt-in at a later date
)

// Default values for delta sync
var (
	DefaultDeltaSyncEnabled   = false
	DefaultDeltaSyncRevMaxAge = uint32(60 * 60 * 24) // 24 hours in seconds
)

var (
	DefaultQueryPaginationLimit = 5000
)

var (
	BypassReleasedSequenceWait atomic.Bool // Used to optimize single-node testing, see DisableSequenceWaitOnDbRestart
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
	bucketUsername              string             // name of the connecting user for audit logging
	BucketUUID                  string             // The bucket UUID for the bucket the database is created against
	EncodedSourceID             string             // The md5 hash of bucket UUID + cluster UUID for the bucket/cluster the database is created against but encoded in base64
	BucketLock                  sync.RWMutex       // Control Access to the underlying bucket object
	mutationListener            changeListener     // Caching feed listener
	ImportListener              *importListener    // Import feed listener
	sequences                   *sequenceAllocator // Source of new sequence numbers
	StartTime                   time.Time          // Timestamp when context was instantiated
	RevsLimit                   uint32             // Max depth a document's revision tree can grow to
	autoImport                  bool               // Add sync data to new untracked couchbase server docs?  (Xattr mode specific)
	revisionCache               RevisionCache      // Cache of recently-accessed doc revisions
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
	AttachmentMigrationManager  *BackgroundManager
	AsyncIndexInitManager       *BackgroundManager
	ExitChanges                 chan struct{}        // Active _changes feeds on the DB will close when this channel is closed
	OIDCProviders               auth.OIDCProviderMap // OIDC clients
	LocalJWTProviders           auth.LocalJWTProviderMap
	ServerUUID                  string // UUID of the server, if available

	DbStats                      *base.DbStats                  // stats that correspond to this database context
	CompactState                 uint32                         // Status of database compaction
	terminator                   chan bool                      // Signal termination of background goroutines
	CancelContext                context.Context                // Cancelled when the database is closed - used to notify associated processes (e.g. blipContext)
	cancelContextFunc            context.CancelFunc             // Cancel function for cancelContext
	backgroundTasks              []BackgroundTask               // List of background tasks that are initiated.
	activeChannels               *channels.ActiveChannels       // Tracks active replications by channel
	CfgSG                        cbgt.Cfg                       // Sync Gateway cluster shared config
	SGReplicateMgr               *sgReplicateManager            // Manages interactions with sg-replicate replications
	Heartbeater                  base.Heartbeater               // Node heartbeater for SG cluster awareness
	ServeInsecureAttachmentTypes bool                           // Attachment content type will bypass the content-disposition handling, default false
	NoX509HTTPClient             *http.Client                   // A HTTP Client from gocb to use the management endpoints
	ServerContextHasStarted      chan struct{}                  // Closed via PostStartup once the server has fully started
	UserFunctionTimeout          time.Duration                  // Default timeout for N1QL & JavaScript queries. (Applies to REST and BLIP requests.)
	Scopes                       map[string]Scope               // A map keyed by scope name containing a set of scopes/collections. Nil if running with only _default._default
	CollectionByID               map[uint32]*DatabaseCollection // A map keyed by collection ID to Collection
	CollectionNames              map[string]map[string]struct{} // Map of scope, collection names
	MetadataKeys                 *base.MetadataKeys             // Factory to generate metadata document keys
	RequireResync                base.ScopeAndCollectionNames   // Collections requiring resync before database can go online
	RequireAttachmentMigration   base.ScopeAndCollectionNames   // Collections that require the attachment migration background task to run against
	CORS                         *auth.CORSConfig               // CORS configuration
	EnableMou                    bool                           // Write _mou xattr when performing metadata-only update.  Set based on bucket capability on connect
	WasInitializedSynchronously  bool                           // true if the database was initialized synchronously
	BroadcastSlowMode            atomic.Bool                    // bool to indicate if a slower ticker value should be used to notify changes feeds of changes
	DatabaseStartupError         *DatabaseError                 // Error that occurred during database online processes startup
	CachedPurgeInterval          atomic.Pointer[time.Duration]  // If set, the cached value of the purge interval to avoid repeated lookups
	CachedVersionPruningWindow   atomic.Pointer[time.Duration]  // If set, the cached value of the version pruning window to avoid repeated lookups
	CachedCCVStartingCas         *base.VBucketCAS               // If set, the cached value of the CCV starting CAS value to avoid repeated lookups
	CachedCCVEnabled             atomic.Bool                    // If set, the cached value of the CCV Enabled flag (this is not expected to transition from true->false, but could go false->true)
	numVBuckets                  uint16                         // Number of vbuckets in the bucket
	SameSiteCookieMode           http.SameSite

	scopeName string // name of the single scope for the database
}

type Scope struct {
	Collections map[string]*DatabaseCollection
}

type DatabaseContextOptions struct {
	CacheOptions                     *CacheOptions
	RevisionCacheOptions             *RevisionCacheOptions
	OldRevExpirySeconds              uint32
	AdminInterface                   *string
	UnsupportedOptions               *UnsupportedOptions
	OIDCOptions                      *auth.OIDCOptions
	LocalJWTConfig                   auth.LocalJWTConfig
	ImportOptions                    ImportOptions
	EnableXattr                      bool             // Use xattr for _sync
	LocalDocExpirySecs               uint32           // The _local doc expiry time in seconds
	SecureCookieOverride             bool             // Pass-through DBConfig.SecureCookieOverride
	SessionCookieName                string           // Pass-through DbConfig.SessionCookieName
	SessionCookieHttpOnly            bool             // Pass-through DbConfig.SessionCookieHTTPOnly
	UserFunctions                    *UserFunctions   // JS/N1QL functions clients can call
	AllowConflicts                   *bool            // False forbids creating conflicts
	SendWWWAuthenticateHeader        *bool            // False disables setting of 'WWW-Authenticate' header
	DisablePasswordAuthentication    bool             // True enforces OIDC/guest only
	UseViews                         bool             // Force use of views
	DeltaSyncOptions                 DeltaSyncOptions // Delta Sync Options
	CompactInterval                  uint32           // Interval in seconds between compaction is automatically ran - 0 means don't run
	SGReplicateOptions               SGReplicateOptions
	SlowQueryWarningThreshold        time.Duration
	QueryPaginationLimit             int    // Limit used for pagination of queries. If not set defaults to DefaultQueryPaginationLimit
	UserXattrKey                     string // Key of user xattr that will be accessible from the Sync Function. If empty the feature will be disabled.
	ClientPartitionWindow            time.Duration
	BcryptCost                       int
	GroupID                          string
	JavascriptTimeout                time.Duration // Max time the JS functions run for (ie. sync fn, import filter)
	UseLegacySyncDocsIndex           bool
	Scopes                           ScopesOptions
	MetadataStore                    base.DataStore // If set, use this location/connection for SG metadata storage - if not set, metadata is stored using the same location/connection as the bucket used for data storage.
	MetadataID                       string         // MetadataID used for metadata storage
	BlipStatsReportingInterval       int64          // interval to report blip stats in milliseconds
	ChangesRequestPlus               bool           // Sets the default value for request_plus, for non-continuous changes feeds
	ConfigPrincipals                 *ConfigPrincipals
	TestPurgeIntervalOverride        *time.Duration    // If set, use this value for db.GetMetadataPurgeInterval - test seam to force specific purge interval for tests
	TestVersionPruningWindowOverride *time.Duration    // If set, use this value for db.GetVersionPruningWindow - test seam to force specific pruning window for tests
	LoggingConfig                    *base.DbLogConfig // Per-database log configuration
	MaxConcurrentChangesBatches      *int              // Maximum number of changes batches to process concurrently per replication
	MaxConcurrentRevs                *int              // Maximum number of revs to process concurrently per replication
	NumIndexReplicas                 uint              // Number of replicas for GSI indexes
	NumIndexPartitions               *uint32           // Number of partitions for GSI indexes, if not set will default to 1
	ImportVersion                    uint64            // Version included in import DCP checkpoints, incremented when collections added to db
	DisablePublicAllDocs             bool              // Disable public access to the _all_docs endpoint for this database
	StoreLegacyRevTreeData           *bool             // Whether to store additional data for legacy rev tree support in delta sync and replication backup revs
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
	UserViews                        *UserViewsOptions        `json:"user_views,omitempty"`                           // Config settings for user views
	OidcTestProvider                 *OidcTestProviderOptions `json:"oidc_test_provider,omitempty"`                   // Config settings for OIDC Provider
	APIEndpoints                     *APIEndpoints            `json:"api_endpoints,omitempty"`                        // Config settings for API endpoints
	WarningThresholds                *WarningThresholds       `json:"warning_thresholds,omitempty"`                   // Warning thresholds related to _sync size
	DisableCleanSkippedQuery         bool                     `json:"disable_clean_skipped_query,omitempty"`          // Clean skipped sequence processing bypasses final check (deprecated: CBG-2672)
	OidcTlsSkipVerify                bool                     `json:"oidc_tls_skip_verify,omitempty"`                 // Config option to enable self-signed certs for OIDC testing.
	SgrTlsSkipVerify                 bool                     `json:"sgr_tls_skip_verify,omitempty"`                  // Config option to enable self-signed certs for SG-Replicate testing.
	RemoteConfigTlsSkipVerify        bool                     `json:"remote_config_tls_skip_verify,omitempty"`        // Config option to enable self signed certificates for external JavaScript load.
	GuestReadOnly                    bool                     `json:"guest_read_only,omitempty"`                      // Config option to restrict GUEST document access to read-only
	ForceAPIForbiddenErrors          bool                     `json:"force_api_forbidden_errors,omitempty"`           // Config option to force the REST API to return forbidden errors
	ConnectedClient                  bool                     `json:"connected_client,omitempty"`                     // Enables BLIP connected-client APIs
	UseQueryBasedResyncManager       bool                     `json:"use_query_resync_manager,omitempty"`             // Config option to use Query based resync manager to perform Resync op
	DCPReadBuffer                    int                      `json:"dcp_read_buffer,omitempty"`                      // Enables user to set their own DCP read buffer
	KVBufferSize                     int                      `json:"kv_buffer,omitempty"`                            // Enables user to set their own KV pool buffer
	BlipSendDocsWithChannelRemoval   bool                     `json:"blip_send_docs_with_channel_removal,omitempty"`  // Enables sending docs with channel removals using channel filters
	RejectWritesWithSkippedSequences bool                     `json:"reject_writes_with_skipped_sequences,omitempty"` // Reject writes if there are skipped sequences in the database
	SameSiteCookie                   *string                  `json:"same_site_cookie,omitempty"`                     // Sets the SameSite attribute on session cookies.
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
		bucket, err := base.GetBucket(ctx, spec)
		_, err = connectToBucketErrorHandling(ctx, spec, err)
		return bucket, err
	}

	// start a retry loop to connect to the bucket backing off double the delay each time
	worker := func() (bool, error, any) {
		bucket, err := base.GetBucket(ctx, spec)

		// Retry if there was a non-fatal error
		fatalError, newErr := connectToBucketErrorHandling(ctx, spec, err)
		shouldRetry := newErr != nil && !fatalError

		return shouldRetry, newErr, bucket
	}

	description := fmt.Sprintf("Attempt to connect to bucket : %v", spec.BucketName)
	err, ibucket := base.RetryLoop(ctx, description, worker, base.GetNewDatabaseSleeperFunc())
	if err != nil {
		return nil, err
	}

	return ibucket.(base.Bucket), nil
}

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
	ctx = base.DatabaseLogCtx(ctx, dbName, options.LoggingConfig)

	if err := base.RequireNoBucketTTL(ctx, bucket); err != nil {
		return nil, err
	}

	serverUUID, err := base.GetServerUUID(ctx, bucket)
	if err != nil {
		return nil, err
	}

	dbStats, statsError := initDatabaseStats(ctx, dbName, autoImport, options)
	if statsError != nil {
		return nil, statsError
	}

	// options.MetadataStore is always passed via rest._getOrAddDatabase...
	// but in db package tests this is unlikely to be set. In this case we'll use the existing bucket connection to store metadata.
	metadataStore := options.MetadataStore
	if metadataStore == nil {
		base.DebugfCtx(ctx, base.KeyAll, "MetadataStore was nil - falling back to use existing bucket connection %q for database %q", bucket.GetName(), dbName)
		metadataStore = bucket.DefaultDataStore()
	}

	bucketUUID, err := bucket.UUID()
	if err != nil {
		return nil, err
	}
	sourceID, err := base.GetSourceID(ctx, bucket)
	if err != nil {
		return nil, err
	}

	bucketUsername := "rosmar_noauth"
	b, err := base.AsGocbV2Bucket(bucket)
	if err == nil {
		bucketUsername, _, _ = b.GetSpec().Auth.GetCredentials()
	}

	// Register the cbgt pindex type for the configGroup
	RegisterImportPindexImpl(ctx, options.GroupID)

	dbContext := &DatabaseContext{
		Name:                 dbName,
		UUID:                 cbgt.NewUUID(),
		MetadataStore:        metadataStore,
		Bucket:               bucket,
		bucketUsername:       bucketUsername,
		BucketUUID:           bucketUUID,
		EncodedSourceID:      sourceID,
		StartTime:            time.Now(),
		autoImport:           autoImport,
		Options:              options,
		DbStats:              dbStats,
		CollectionByID:       make(map[uint32]*DatabaseCollection),
		ServerUUID:           serverUUID,
		UserFunctionTimeout:  defaultUserFunctionTimeout,
		CachedCCVStartingCas: &base.VBucketCAS{},
		SameSiteCookieMode:   http.SameSiteDefaultMode,
	}
	dbContext.numVBuckets, err = bucket.GetMaxVbno()
	if err != nil {
		return nil, err
	}
	err = dbContext.updateCCVSettings(ctx)
	if err != nil {
		return nil, err
	}

	// set up cancellable context based on the background context (context lifecycle for the database
	// must be distinct from the request context associated with the db create/update).  Used to trigger
	// teardown of connected replications on database close.
	dbContext.CancelContext, dbContext.cancelContextFunc = context.WithCancel(context.Background())
	cleanupFunctions = append(cleanupFunctions, func() {
		dbContext.cancelContextFunc()
	})

	// Check if server version supports multi-xattr operations, required for mou handling
	dbContext.EnableMou = bucket.IsSupported(sgbucket.BucketStoreFeatureMultiXattrSubdocOperations)

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

	dbContext.sequences, err = newSequenceAllocator(ctx, metadataStore, dbContext.DbStats.Database(), metaKeys)
	if err != nil {
		return nil, err
	}

	cleanupFunctions = append(cleanupFunctions, func() {
		dbContext.sequences.Stop(ctx)
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
		sgCfg, err := base.NewCfgSG(ctx, metadataStore, metaKeys.SGCfgPrefix(dbContext.Options.GroupID))
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
	dbContext.mutationListener.Init(bucket.GetName(), options.GroupID, dbContext)

	if len(options.Scopes) == 0 {
		return nil, fmt.Errorf("Setting scopes to be zero is invalid")
	}
	dbContext.Scopes = make(map[string]Scope, len(options.Scopes))
	dbContext.CollectionNames = make(map[string]map[string]struct{}, len(options.Scopes))

	// if any sync functions for any collection, we recommend running a resync
	syncFunctionsChanged := false
	// Create new backing store map to map from collection ID's to their associated rev cache backing stores for rev cache document loads
	collectionIDToRevCacheBackingStore := make(map[uint32]RevisionCacheBackingStore)
	if len(options.Scopes) > 1 {
		return nil, fmt.Errorf("Multiple scopes %v are not supported on a single database", maps.Keys(options.Scopes))
	}
	for scopeName, scope := range options.Scopes {
		dbContext.scopeName = scopeName
		dbContext.Scopes[scopeName] = Scope{
			Collections: make(map[string]*DatabaseCollection, len(scope.Collections)),
		}
		collectionNameMap := make(map[string]struct{}, len(scope.Collections))
		for collName, collOpts := range scope.Collections {
			// intentional shadow - we want each collection to have its own context inside this loop body
			ctx := ctx
			if !base.IsDefaultCollection(scopeName, collName) {
				ctx = base.CollectionLogCtx(ctx, scopeName, collName)
			}
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
			collectionIDToRevCacheBackingStore[collectionID] = dbCollection
		}
		dbContext.CollectionNames[scopeName] = collectionNameMap
	}

	// Init the rev cache
	dbContext.revisionCache = NewRevisionCache(
		dbContext.Options.RevisionCacheOptions,
		collectionIDToRevCacheBackingStore,
		dbContext.DbStats.Cache(),
	)

	if syncFunctionsChanged {
		base.InfofCtx(ctx, base.KeyAll, "**NOTE:** %q's sync function has changed. The new function may assign different channels to documents, or permissions to users. You may want to re-sync the database to update these.", base.MD(dbContext.Name))
	}

	// Initialize sg-replicate manager
	dbContext.SGReplicateMgr, err = NewSGReplicateManager(ctx, dbContext, dbContext.CfgSG)
	if err != nil {
		return nil, err
	}

	dbContext.ResyncManager = NewResyncManagerDCP(metadataStore, dbContext.UseXattrs(), metaKeys)
	dbContext.AsyncIndexInitManager = NewAsyncIndexInitManager(dbContext.MetadataStore, dbContext.MetadataKeys)

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

// _stopOnlineProcesses is called to represent an error condition from startOnlineProcesses, or from DatabaseContext.Close. Most of the objects are not safe to close twice, since they have internal terminator objects and goroutines that wait on closed channels. Acquire the bucket lock, to avoid calling this function multiple times.
func (db *DatabaseContext) _stopOnlineProcesses(ctx context.Context) {
	db.mutationListener.Stop(ctx)
	db.changeCache.Stop(ctx)
	if db.ImportListener != nil {
		db.ImportListener.Stop()
		db.ImportListener = nil
	}
	if db.Heartbeater != nil {
		db.Heartbeater.Stop(ctx)
		db.Heartbeater = nil
	}
	if db.SGReplicateMgr != nil {
		db.SGReplicateMgr.Stop()
		db.SGReplicateMgr = nil
	}
}

func (context *DatabaseContext) Close(ctx context.Context) {
	context.BucketLock.Lock()
	defer context.BucketLock.Unlock()

	context.OIDCProviders.Stop()
	close(context.terminator)
	if context.cancelContextFunc != nil {
		context.cancelContextFunc()
	}

	// Stop All background processors
	bgManagers := context.stopBackgroundManagers()

	// Wait for database background tasks to finish.
	waitForBGTCompletion(ctx, BGTCompletionMaxWait, context.backgroundTasks, context.Name)
	context.sequences.Stop(ctx)
	context._stopOnlineProcesses(ctx)
	// Stop the channel cache and its background tasks.
	context.channelCache.Stop(ctx)

	waitForBackgroundManagersToStop(ctx, BGTCompletionMaxWait, bgManagers)

	context.Bucket.Close(ctx)
	context.Bucket = nil

	base.RemovePerDbStats(context.Name)

}

// stopBackgroundManagers stops any running BackgroundManager.
// Returns a list of BackgroundManager it signalled to stop
func (dbCtx *DatabaseContext) stopBackgroundManagers() (stopped []*BackgroundManager) {
	for _, manager := range []*BackgroundManager{
		dbCtx.ResyncManager,
		dbCtx.AttachmentCompactionManager,
		dbCtx.TombstoneCompactionManager,
		dbCtx.AsyncIndexInitManager,
		dbCtx.AttachmentMigrationManager,
	} {
		if manager != nil && !isBackgroundManagerStopped(manager.GetRunState()) && manager.Stop() == nil {
			stopped = append(stopped, manager)
		}
	}
	return stopped
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
				retryInterval = min(retryInterval*2, maxRetryInterval)
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
func (context *DatabaseContext) RestartListener(ctx context.Context) error {
	context.mutationListener.Stop(ctx)
	// Delay needed to properly stop
	time.Sleep(2 * time.Second)
	context.mutationListener.Init(context.Bucket.GetName(), context.Options.GroupID, context)
	cacheFeedStatsMap := context.DbStats.Database().CacheFeedMapStats
	if err := context.mutationListener.Start(ctx, context.Bucket, cacheFeedStatsMap.Map, context.Scopes, context.MetadataStore); err != nil {
		return err
	}
	return nil
}

// Removes previous versions of Sync Gateway's design docs found on the server
func (dbCtx *DatabaseContext) RemoveObsoleteDesignDocs(ctx context.Context, previewOnly bool) (removedDesignDocs []string, err error) {
	ds := dbCtx.Bucket.DefaultDataStore()
	viewStore, ok := ds.(sgbucket.ViewStore)
	if !ok {
		return []string{}, fmt.Errorf("Datastore does not support views")
	}
	return removeObsoleteDesignDocs(ctx, viewStore, previewOnly, dbCtx.UseViews())
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

// GetInUseIndexes returns the names of the indexes on this database.
func (dbCtx *DatabaseContext) GetInUseIndexes() CollectionIndexes {
	return dbCtx.GetInUseIndexesFromDefs(sgIndexes)
}

// GetInUseIndexesFromDefs returns the name of the indexes on this database. indexDefs refers to the global indexDefs as sgIndexes, or a testing seam passed in.
func (dbCtx *DatabaseContext) GetInUseIndexesFromDefs(indexDefs map[SGIndexType]SGIndex) CollectionIndexes {
	dataStores := dbCtx.getDataStores()
	inUseIndexes := make(CollectionIndexes)
	for _, ds := range dataStores {
		options := InitializeIndexOptions{
			NumPartitions:       dbCtx.numIndexPartitions(),
			NumReplicas:         dbCtx.Options.NumIndexReplicas,
			UseXattrs:           dbCtx.UseXattrs(),
			LegacySyncDocsIndex: dbCtx.UseLegacySyncDocsIndex(),
		}
		if base.IsDefaultCollection(ds.ScopeName(), ds.CollectionName()) {
			if dbCtx.HasDefaultCollection() {
				options.MetadataIndexes = IndexesAll
			} else {
				options.MetadataIndexes = IndexesMetadataOnly
			}
		} else {
			options.MetadataIndexes = IndexesWithoutMetadata
		}
		dsName := base.ScopeAndCollectionName{
			Scope:      ds.ScopeName(),
			Collection: ds.CollectionName(),
		}
		// datastore can be listed twice for explicit default and for metadata store
		if _, ok := inUseIndexes[dsName]; !ok {
			inUseIndexes[dsName] = make(map[string]struct{})
		}
		if dbCtx.UseViews() {
			continue
		}
		for _, index := range GetIndexNames(options, indexDefs) {
			inUseIndexes[dsName][index] = struct{}{}
		}
	}
	return inUseIndexes
}

// Trigger terminate check handling for connected continuous replications.
// TODO: The underlying code (NotifyCheckForTermination) doesn't actually leverage the specific username - should be refactored
//
//	to remove
func (context *DatabaseContext) NotifyTerminatedChanges(ctx context.Context, username string) {
	context.mutationListener.NotifyCheckForTermination(ctx, base.SetOf(base.UserPrefixRoot+username))
}

func (dc *DatabaseContext) TakeDbOffline(ctx context.Context, reason string) error {

	if atomic.CompareAndSwapUint32(&dc.State, DBOnline, DBStopping) {
		// notify all active _changes feeds to close
		close(dc.ExitChanges)

		// Block until all current calls have returned, including _changes feeds
		dc.AccessLock.Lock()
		defer dc.AccessLock.Unlock()

		dc.changeCache.Stop(ctx)

		// set DB state to Offline
		atomic.StoreUint32(&dc.State, DBOffline)

		if err := dc.EventMgr.RaiseDBStateChangeEvent(ctx, dc.Name, "offline", reason, dc.Options.AdminInterface); err != nil {
			base.InfofCtx(ctx, base.KeyCRUD, "Error raising database state change event: %v", err)
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

		base.InfofCtx(ctx, base.KeyCRUD, "%s", msg)
		return base.NewHTTPError(http.StatusServiceUnavailable, msg)
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

// UseLegacySyncDocsIndex returns true there is an index over all docs starting with _sync, or if there are individual indexes for users and roles.
func (context *DatabaseContext) UseLegacySyncDocsIndex() bool {
	return context.Options.UseLegacySyncDocsIndex
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

// NextSequence returns the new sequence number.
func (db *DatabaseContext) NextSequence(ctx context.Context) (uint64, error) {
	return db.sequences.nextSequence(ctx)
}

// ////// ALL DOCUMENTS:

type IDRevAndSequence struct {
	DocID    string
	RevID    string
	Sequence uint64
	CV       string
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

	err = c.processForEachDocIDResults(ctx, callback, resultsOpts.Limit, results)
	if err != nil {
		return err
	}
	return results.Close()
}

// Iterate over the results of an AllDocs query, performing ForEachDocID handling for each row
func (c *DatabaseCollection) processForEachDocIDResults(ctx context.Context, callback ForEachDocIDFunc, limit uint64, results sgbucket.QueryResultIterator) error {

	count := uint64(0)
	for {
		var queryRow AllDocsIndexQueryRow
		var found bool
		var docid, revid string
		var seq uint64
		var cv string
		var channels []string
		if c.useViews() {
			var viewRow AllDocsViewQueryRow
			found = results.Next(ctx, &viewRow)
			if found {
				docid = viewRow.Key
				revid = viewRow.Value.Rev.RevTreeID
				cv = viewRow.Value.Rev.CV()
				seq = viewRow.Value.Sequence
				channels = viewRow.Value.Channels
			}
		} else {
			found = results.Next(ctx, &queryRow)
			if found {
				docid = queryRow.Id
				revid = queryRow.Rev.RevTreeID
				cv = queryRow.Rev.CV()
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

		if ok, err := callback(IDRevAndSequence{DocID: docid, RevID: revid, Sequence: seq, CV: cv}, channels); ok {
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

	if db.UseLegacySyncDocsIndex() || db.Options.UseViews {
		return db.getAllPrincipalIDsSyncDocs(ctx)
	}

	wg := sync.WaitGroup{}
	wg.Add(2)
	var getUsersErr error
	go func() {
		defer wg.Done()
		users, getUsersErr = db.GetUserNames(ctx)
	}()
	var getRolesErr error
	go func() {
		defer wg.Done()
		includeDeleted := true
		roles, getRolesErr = db.getRoleIDsUsingIndex(ctx, includeDeleted)
	}()
	wg.Wait()
	if getUsersErr != nil {
		return nil, nil, getUsersErr
	}
	if getRolesErr != nil {
		return nil, nil, getRolesErr
	}
	return users, roles, err
}

// Returns the Names of all users
func (db *DatabaseContext) GetUserNames(ctx context.Context) (users []string, err error) {
	dbUserPrefix := db.MetadataKeys.UserKeyPrefix()
	startKey := dbUserPrefix
	limit := db.Options.QueryPaginationLimit
	if limit == 0 {
		limit = DefaultQueryPaginationLimit
	}

	userRe, err := regexp.Compile(`^` + dbUserPrefix + `[^:]*$`)
	if err != nil {
		return nil, err
	}
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
			found := results.Next(ctx, &queryRow)
			if !found {
				break
			}

			if !strings.HasPrefix(queryRow.ID, dbUserPrefix) {
				break
			}

			principalName = queryRow.Name
			startKey = queryRow.ID
			resultCount++

			if !userRe.MatchString(queryRow.ID) {
				continue
			}

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
	if limit == 0 {
		limit = DefaultQueryPaginationLimit
	}

	dbUserPrefix := db.MetadataKeys.UserKeyPrefix()
	dbRolePrefix := db.MetadataKeys.RoleKeyPrefix()
	lenDbUserPrefix := len(dbUserPrefix)
	lenDbRolePrefix := len(dbRolePrefix)

	userRe, err := regexp.Compile(`^` + dbUserPrefix + `[^:]*$`)
	if err != nil {
		return nil, nil, err
	}
	roleRe, err := regexp.Compile(`^` + dbRolePrefix + `[^:]*$`)
	if err != nil {
		return nil, nil, err
	}
	anyPrincipalRe, err := regexp.Compile(fmt.Sprintf(`^(%s|%s).*$`, base.DefaultMetadataKeys.UserKeyPrefix(), base.DefaultMetadataKeys.RoleKeyPrefix()))
	if err != nil {
		return nil, nil, err
	}
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
				found := results.Next(ctx, &viewRow)
				if !found {
					break
				}
				rowID = viewRow.ID
				//isUser = viewRow.Value
				//principalName = viewRow.Key
				startKey = viewRow.Key
			} else {
				var queryRow principalRow
				found := results.Next(ctx, &queryRow)
				if !found {
					break
				}
				rowID = queryRow.Id
				startKey = queryRow.Name
			}
			if len(rowID) < lenDbUserPrefix && len(rowID) < lenDbRolePrefix {
				// make sure to increment the resultCount to not skip the next row if the results are
				// _sync:user:alice and we are looking for _sync:user:verylongdbname:alice
				if resultCount == 0 && anyPrincipalRe.MatchString(rowID) {
					resultCount++
				}
				continue
			}
			isDbUser = userRe.MatchString(rowID)
			isDbRole = roleRe.MatchString(rowID)
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
	userRe, err := regexp.Compile(`^` + dbUserKeyPrefix + `[^:]*$`)
	if err != nil {
		return nil, err
	}
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
			found := results.Next(ctx, &queryRow)
			if !found {
				break
			}
			if !strings.HasPrefix(queryRow.ID, dbUserKeyPrefix) {
				break
			}

			startKey = queryRow.ID
			resultCount++
			if !userRe.MatchString(queryRow.ID) {
				continue
			}
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

// getRoleIDsUsingIndex returns names of all roles. includDeleted toggles whether or not to include deleted roles.
func (db *DatabaseContext) getRoleIDsUsingIndex(ctx context.Context, includeDeleted bool) (roles []string, err error) {

	dbRoleIDPrefix := db.MetadataKeys.RoleKeyPrefix()

	roleRe, err := regexp.Compile(`^` + dbRoleIDPrefix + `[^:]*$`)
	if err != nil {
		return nil, err
	}
	startKey := dbRoleIDPrefix
	limit := db.Options.QueryPaginationLimit
	if limit == 0 {
		limit = DefaultQueryPaginationLimit
	}

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
			found := results.Next(ctx, &queryRow)
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

			if !roleRe.MatchString(queryRow.Id) {
				continue
			}
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

type compactProgressCallbackFunc func(purgedDocCount *int)

// Compact runs tombstone compaction from view and/or GSI indexes - ensuring there's nothing left in the indexes for tombstoned documents that have been purged by the server.
//
// Several Sync Gateway indexes server tombstones (deleted documents with an xattr).
// There currently isn't a mechanism for server to remove these docs from the index when the tombstone is purged by the server during
// metadata purge, because metadata purge doesn't trigger a DCP event.
// When compact is run, Sync Gateway initiates a normal delete operation for the document and xattr (a Sync Gateway purge).  This triggers
// removal of the document from the index.  In the event that the document has already been purged by server, we need to recreate and delete
// the document to accomplish the same result.
//
// The `isScheduledBackgroundTask` parameter is used to indicate if the compaction is being run as part of a scheduled background task, or an ad-hoc user-initiated `/{db}/_compact` request.
func (db *Database) Compact(ctx context.Context, skipRunningStateCheck bool, optionalProgressCallback compactProgressCallbackFunc, terminator *base.SafeTerminator, isScheduledBackgroundTask bool) (purgedDocCount int, err error) {
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

	purgeInterval := db.GetMetadataPurgeInterval(ctx, true)
	base.InfofCtx(ctx, base.KeyAll, "Tombstone compaction using the metadata purge interval of %.2f days.", purgeInterval.Hours()/24)

	// Trigger view compaction for all tombstoned documents older than the purge interval
	startTime := time.Now()
	purgeOlderThan := startTime.Add(-purgeInterval)

	purgeErrorCount := 0
	addErrorCount := 0
	deleteErrorCount := 0

	if optionalProgressCallback != nil {
		defer optionalProgressCallback(&purgedDocCount)
	}

	base.InfofCtx(ctx, base.KeyAll, "Starting compaction of purged tombstones for %s ...", base.MD(db.Name))

	purgeBody := Body{"_purged": true}
	for _, c := range db.CollectionByID {
		// shadow ctx, so that we can't misuse the parent's inside the loop
		ctx := c.AddCollectionContext(ctx)

		// create admin collection interface
		collection, err := db.GetDatabaseCollectionWithUser(c.ScopeName, c.Name)
		if err != nil {
			base.WarnfCtx(ctx, "Tombstone compaction could not get collection: %s", err)
			continue
		}

		for {
			purgedDocs := make([]string, 0)
			results, err := collection.QueryTombstones(ctx, purgeOlderThan, QueryTombstoneBatch)
			if isScheduledBackgroundTask {
				base.SyncGatewayStats.GlobalStats.ResourceUtilizationStats().NumIdleQueryOps.Add(1)
			}
			if err != nil {
				return 0, err
			}
			var tombstonesRow QueryIdRow
			var resultCount int
			for results.Next(ctx, &tombstonesRow) {
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
				purgeErr := collection.Purge(ctx, tombstonesRow.Id, false)
				if isScheduledBackgroundTask {
					base.SyncGatewayStats.GlobalStats.ResourceUtilizationStats().NumIdleKvOps.Add(1)
				}
				if purgeErr == nil {
					purgedDocs = append(purgedDocs, tombstonesRow.Id)
				} else if base.IsDocNotFoundError(purgeErr) {
					// If key no longer exists, need to add and remove to trigger removal from view
					_, addErr := collection.dataStore.Add(tombstonesRow.Id, 0, purgeBody)
					if isScheduledBackgroundTask {
						base.SyncGatewayStats.GlobalStats.ResourceUtilizationStats().NumIdleKvOps.Add(1)
					}
					if addErr != nil {
						addErrorCount++
						base.InfofCtx(ctx, base.KeyAll, "Couldn't compact key %s (add): %v", base.UD(tombstonesRow.Id), addErr)
						continue
					}

					// At this point, the doc is not in a usable state for mobile
					// so mark it to be removed from cache, even if the subsequent delete fails
					purgedDocs = append(purgedDocs, tombstonesRow.Id)

					delErr := collection.dataStore.Delete(tombstonesRow.Id)
					if isScheduledBackgroundTask {
						base.SyncGatewayStats.GlobalStats.ResourceUtilizationStats().NumIdleKvOps.Add(1)
					}
					if delErr != nil {
						deleteErrorCount++
						base.InfofCtx(ctx, base.KeyAll, "Couldn't compact key %s (delete): %v", base.UD(tombstonesRow.Id), delErr)
					}
				} else {
					purgeErrorCount++
					base.InfofCtx(ctx, base.KeyAll, "Couldn't compact key %s (purge): %v", base.UD(tombstonesRow.Id), purgeErr)
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
				collection.RemoveFromChangeCache(ctx, purgedDocs, startTime)
				collection.dbStats().Database().NumTombstonesCompacted.Add(int64(count))
			}
			base.InfofCtx(ctx, base.KeyAll, "Compacted %v tombstones", count)

			if optionalProgressCallback != nil {
				optionalProgressCallback(&purgedDocCount)
			}

			if resultCount < QueryTombstoneBatch {
				break
			}
		}
	}
	base.InfofCtx(ctx, base.KeyAll, "Finished compaction of purged tombstones for %s... Total Tombstones Compacted: %d", base.MD(db.Name), purgedDocCount)

	if purgeErrorCount > 0 || deleteErrorCount > 0 || addErrorCount > 0 {
		base.WarnfCtx(ctx, "Compaction finished with %d errors (add: %d, delete: %d, purge: %d)", addErrorCount+deleteErrorCount+purgeErrorCount, addErrorCount, deleteErrorCount, purgeErrorCount)
	}

	return purgedDocCount, nil
}

// GetMetadataPurgeInterval returns the current value for the metadata purge interval for the backing bucket.
// if forceRefresh is set, we'll always fetch a new Metadata Purge Interval from the bucket, even if we had one cached.
func (db *DatabaseContext) GetMetadataPurgeInterval(ctx context.Context, forceRefresh bool) time.Duration {
	// look for metadata purge interval preferentially:
	// 1. test override value specified in DatabaseContextOptions
	// 2. cached metadata purge interval (if forceRefresh is false)
	// 3. bucket level
	// 4. cluster level
	// 5. default fallback value
	if db.Options.TestPurgeIntervalOverride != nil {
		return *db.Options.TestPurgeIntervalOverride
	}

	// fetch cached value if available
	if !forceRefresh {
		mpi := db.CachedPurgeInterval.Load()
		if mpi != nil {
			return *mpi
		}
	}

	// fetch from server
	cbStore, ok := base.AsCouchbaseBucketStore(db.Bucket)
	if !ok {
		return DefaultPurgeInterval
	}
	serverPurgeInterval, err := cbStore.MetadataPurgeInterval(ctx)
	if err != nil {
		base.WarnfCtx(ctx, "Unable to retrieve server's metadata purge interval - using default purge interval %.2f days. %s", DefaultPurgeInterval.Hours()/24, err)
	}

	mpi := DefaultPurgeInterval
	if serverPurgeInterval > 0 {
		mpi = serverPurgeInterval
	}

	db.CachedPurgeInterval.Store(&mpi)
	return mpi
}

// GetVersionPruningWindow returns the current value for the XDCR Version Pruning Window for the backing bucket.
// if forceRefresh is set, we'll always fetch a new value from the bucket, even if we had one cached.
func (db *DatabaseContext) GetVersionPruningWindow(ctx context.Context, forceRefresh bool) time.Duration {
	// test override
	if db.Options.TestVersionPruningWindowOverride != nil {
		return *db.Options.TestVersionPruningWindowOverride
	}

	// fetch cached value if available
	if !forceRefresh {
		vpw := db.CachedVersionPruningWindow.Load()
		if vpw != nil {
			return *vpw
		}
	}

	// fetch from server
	cbStore, ok := base.AsCouchbaseBucketStore(db.Bucket)
	if !ok {
		return DefaultVersionPruningWindow
	}

	serverVersionPruningWindow, err := cbStore.VersionPruningWindow(ctx)
	if err != nil {
		base.WarnfCtx(ctx, "Unable to retrieve server's version pruning window - using default %.2f days. %s", DefaultVersionPruningWindow.Hours()/24, err)
	}

	vpw := DefaultVersionPruningWindow
	if serverVersionPruningWindow > 0 {
		vpw = serverVersionPruningWindow
	}

	db.CachedVersionPruningWindow.Store(&vpw)
	return vpw
}

// updateCCVSettings performs a management query to determine the latest crossClusterVersioning and max cas settings.
func (db *DatabaseContext) updateCCVSettings(ctx context.Context) error {
	cbStore, ok := base.AsCouchbaseBucketStore(db.Bucket)
	if !ok {
		// for rosmar, ECCV is always enabled, mark starting cas as 0
		db.CachedCCVEnabled.Store(true)
		for vbNo := range db.numVBuckets {
			db.CachedCCVStartingCas.Store(base.VBNo(vbNo), 0)
		}
		return nil
	}

	// Fetch from Couchbase Server
	enabled, maxCAS, err := cbStore.GetCCVSettings(ctx)
	if err != nil {
		return fmt.Errorf("Unable to retrieve server's CCV Starting CAS: %w", err)
	}

	db.CachedCCVEnabled.Store(enabled)
	if !enabled {
		db.CachedCCVStartingCas.Clear()
	}
	for vbNo, cas := range maxCAS {
		db.CachedCCVStartingCas.Store(vbNo, cas)
	}
	return nil
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
		err = c.regeneratePrincipalSequences(ctx, authr, role)
		if err != nil {
			return err
		}
	}

	for _, user := range users {
		user, err := authr.GetUser(user)
		if err != nil {
			return err
		}
		err = c.regeneratePrincipalSequences(ctx, authr, user)
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *DatabaseCollection) regeneratePrincipalSequences(ctx context.Context, authr *auth.Authenticator, princ auth.Principal) error {
	nextSeq, err := c.sequences().nextSequence(ctx)
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
		err := c.sequences().releaseSequence(ctx, sequence)
		if err != nil {
			base.WarnfCtx(ctx, "Error attempting to release sequence %d. Error %v", sequence, err)
		}
	}
}

// getResyncedDocument runs the sync function on the document and determines if any channels or access grants have changed. Returns ErrUpdateCancel if the document does not need to be rewritten. Any unused sequences should be released by the caller.
func (db *DatabaseCollectionWithUser) getResyncedDocument(ctx context.Context, doc *Document, regenerateSequences bool) (updatedDoc *Document, updatedUnusedSequences []uint64, err error) {
	docid := doc.ID
	forceUpdate := false
	if !doc.HasValidSyncData() {
		// This is a document not known to the sync gateway. Ignore it:
		return nil, nil, base.ErrUpdateCancel
	}

	base.TracefCtx(ctx, base.KeyCRUD, "\tRe-syncing document %q", base.UD(docid))

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
		metaMap, err := doc.GetMetaMap(db.UserXattrKey())
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

		if rev.ID == doc.GetRevTreeID() {
			if regenerateSequences {
				var unusedSequences []uint64
				updatedUnusedSequences, err = db.assignSequence(ctx, 0, doc, unusedSequences)
				if err != nil {
					base.WarnfCtx(ctx, "Unable to assign a sequence number: %v", err)
				}
				forceUpdate = true
			}

			changedChannels, _, err := doc.updateChannels(ctx, channels)
			changed = len(doc.Access.updateAccess(ctx, doc, access)) +
				len(doc.RoleAccess.updateAccess(ctx, doc, roles)) +
				len(changedChannels)
			if err != nil {
				return
			}
			// Only update document expiry based on the current (active) rev
			if syncExpiry != nil {
				doc.UpdateExpiry(*syncExpiry)
			}
		}
	})
	if changed == 0 && !forceUpdate {
		return nil, nil, base.ErrUpdateCancel
	}
	doc.SetCrc32cUserXattrHash()
	return doc, updatedUnusedSequences, nil
}

// ResyncDocument will re-run the sync function on the document and write an updated version to the bucket. If
// the sync function doesn't change any channels or access grants, no write will be performed.
func (db *DatabaseCollectionWithUser) ResyncDocument(ctx context.Context, docid string, previousDoc *sgbucket.BucketDocument, regenerateSequences bool) error {
	var updatedDoc *Document
	var updatedExpiry *uint32
	var unusedSequences []uint64
	writeUpdateFunc := func(currentValue []byte, currentXattrs map[string][]byte, cas uint64) (sgbucket.UpdatedDoc, error) {
		// resyncDocument is not called on tombstoned documents, so this value will only be empty if the document was
		// deleted between DCP event and calling this function. In any case, we do not need to update it.
		if len(currentValue) == 0 {
			return sgbucket.UpdatedDoc{}, base.ErrUpdateCancel
		}
		doc, err := db.unmarshalDocumentWithXattrs(ctx, docid, currentValue, currentXattrs, cas, DocUnmarshalAll)
		if err != nil {
			return sgbucket.UpdatedDoc{}, err
		}
		updatedDoc, unusedSequences, err = db.getResyncedDocument(ctx, doc, regenerateSequences)
		if err != nil {
			return sgbucket.UpdatedDoc{}, err
		}
		base.TracefCtx(ctx, base.KeyAccess, "Saving updated channels and access grants of %q on resync", base.UD(docid))

		// Update MetadataOnlyUpdate based on previous Cas, MetadataOnlyUpdate
		doc.MetadataOnlyUpdate = computeMetadataOnlyUpdate(doc.Cas, doc.RevSeqNo, doc.MetadataOnlyUpdate)
		_, rawSyncXattr, _, rawMouXattr, _, err := updatedDoc.MarshalWithXattrs()
		updatedDoc := sgbucket.UpdatedDoc{
			Doc: nil, // Resync does not require document body update
			Xattrs: map[string][]byte{
				base.SyncXattrName: rawSyncXattr,
				base.MouXattrName:  rawMouXattr,
			},
			Expiry: updatedExpiry,
			Spec: []sgbucket.MacroExpansionSpec{
				sgbucket.NewMacroExpansionSpec(XattrMouCasPath(), sgbucket.MacroCas),
			},
		}
		return updatedDoc, err
	}
	db.releaseSequences(ctx, unusedSequences)

	// these values are updated by the callback function
	mutateInOpts := sgbucket.MutateInOptions{}
	var expiry uint32
	_, err := db.dataStore.WriteUpdateWithXattrs(ctx, docid, db.syncGlobalSyncMouRevSeqNoAndUserXattrKeys(), expiry, previousDoc, &mutateInOpts, writeUpdateFunc)
	if err == nil {
		base.Audit(ctx, base.AuditIDDocumentResync, base.AuditFields{
			base.AuditFieldDocID:      docid,
			base.AuditFieldDocVersion: updatedDoc.CVOrRevTreeID(),
		})
	}
	return err
}

// invalidateAllPrincipals invalidates computed channels and roles for all users/roles, for the specified collections:
func (dbCtx *DatabaseContext) invalidateAllPrincipals(ctx context.Context, collectionNames base.ScopeAndCollectionNames, endSeq uint64) error {
	base.InfofCtx(ctx, base.KeyAll, "Invalidating channel caches of users/roles...")
	users, roles, err := dbCtx.AllPrincipalIDs(ctx)
	if err != nil {
		return err
	}
	for _, name := range users {
		dbCtx.invalUserRolesAndChannels(ctx, name, collectionNames, endSeq)
	}
	for _, name := range roles {
		dbCtx.invalRoleChannels(ctx, name, collectionNames, endSeq)
	}
	return nil
}

// invalUserChannels invalidates a user's computed channels for the specified collections
func (dbCtx *DatabaseContext) invalUserChannels(ctx context.Context, username string, collections base.ScopeAndCollectionNames, invalSeq uint64) {
	authr := dbCtx.Authenticator(ctx)
	if err := authr.InvalidateChannels(username, true, collections, invalSeq); err != nil {
		base.WarnfCtx(ctx, "Error invalidating channels for user %s: %v", base.UD(username), err)
	}
}

// invalRoleChannels invalidates a role's computed channels for the specified collections
func (dbCtx *DatabaseContext) invalRoleChannels(ctx context.Context, rolename string, collections base.ScopeAndCollectionNames, invalSeq uint64) {
	authr := dbCtx.Authenticator(ctx)
	if err := authr.InvalidateChannels(rolename, false, collections, invalSeq); err != nil {
		base.WarnfCtx(ctx, "Error invalidating channels for role %s: %v", base.UD(rolename), err)
	}
}

// invalUserRoles invalidates a user's computed roles
func (dbCtx *DatabaseContext) invalUserRoles(ctx context.Context, username string, invalSeq uint64) {

	authr := dbCtx.Authenticator(ctx)
	if err := authr.InvalidateRoles(username, invalSeq); err != nil {
		base.WarnfCtx(ctx, "Error invalidating roles for user %s: %v", base.UD(username), err)
	}
}

// invalUserRolesAndChannels invalidates the user's computed roles, and invalidates the computed channels for all specified collections
func (dbCtx *DatabaseContext) invalUserRolesAndChannels(ctx context.Context, username string, collections base.ScopeAndCollectionNames, invalSeq uint64) {
	authr := dbCtx.Authenticator(ctx)
	if err := authr.InvalidateRolesAndChannels(username, collections, invalSeq); err != nil {
		base.WarnfCtx(ctx, "Error invalidating roles for user %s: %v", base.UD(username), err)
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

// numIndexPartitions returns the number of index partitions to use for the database's indexes.
func (context *DatabaseContext) numIndexPartitions() uint32 {
	if context.Options.NumIndexPartitions != nil {
		return *context.Options.NumIndexPartitions
	}
	return DefaultNumIndexPartitions
}

func (context *DatabaseContext) UseViews() bool {
	return context.Options.UseViews
}

func (context *DatabaseContext) UseMou() bool {
	return context.EnableMou
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

func initDatabaseStats(ctx context.Context, dbName string, autoImport bool, options DatabaseContextOptions) (*base.DbStats, error) {

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

	var collections []string
	if len(options.Scopes) == 0 {
		base.DebugfCtx(ctx, base.KeyAll, "No named collections defined for database %q, using default collection for stats", base.MD(dbName))
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

func (context *DatabaseContext) LastSequence(ctx context.Context) (uint64, error) {
	return context.sequences.lastSequence(ctx)
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
		dbLogCtx := base.DatabaseLogCtx(ctx, dbCtx.Name, dbCtx.Options.LoggingConfig)
		return dbLogCtx
	}
	return ctx
}

// AddBucketUserLogContext adds bucket user to the parent context for logging. This is used to mark actions not caused by a user.
func (dbCtx *DatabaseContext) AddBucketUserLogContext(ctx context.Context) context.Context {
	return base.UserLogCtx(ctx, dbCtx.bucketUsername, base.UserDomainBuiltin, nil)
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

func (dbc *DatabaseContext) AuthenticatorOptions(ctx context.Context) auth.AuthenticatorOptions {
	defaultOptions := auth.DefaultAuthenticatorOptions(ctx)
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

	defer func() {
		if returnedError != nil {
			// indicate something has gone wrong in the online processes
			db.DatabaseStartupError = NewDatabaseError(DatabaseOnlineProcessError)
			// grab bucket lock so stopOnlineProcesses is not called at the same time as db.Close()
			db.BucketLock.RLock()
			defer db.BucketLock.RUnlock()
			db._stopOnlineProcesses(ctx)
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
	notifyChange := func(ctx context.Context, changedChannels channels.Set) {
		db.mutationListener.Notify(ctx, changedChannels)
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
		base.InfofCtx(ctx, base.KeyCache, "Error initializing the change cache: %s", err)
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

	sgReplicateEnabled := db.Options.SGReplicateOptions.Enabled

	// Initialize node heartbeater in EE mode if sg-replicate or import enabled on the node.  This node must start
	// sending heartbeats before registering itself to the cfg, to avoid triggering immediate removal by other active nodes.
	if base.IsEnterpriseEdition() && (db.autoImport || sgReplicateEnabled) {
		// Create heartbeater
		heartbeaterPrefix := db.MetadataKeys.HeartbeaterPrefix(db.Options.GroupID)
		heartbeater, err := base.NewCouchbaseHeartbeater(db.MetadataStore, heartbeaterPrefix, db.UUID)
		if err != nil {
			return pkgerrors.Wrapf(err, "Error starting heartbeater for bucket %s", base.MD(db.Bucket.GetName()).Redact())
		}
		err = heartbeater.StartSendingHeartbeats(ctx)
		if err != nil {
			return err
		}
		db.Heartbeater = heartbeater
	}

	// If sgreplicate is enabled on this node, register this node to accept notifications
	if sgReplicateEnabled {
		registerNodeErr := db.SGReplicateMgr.StartLocalNode(db.UUID, db.Heartbeater)
		if registerNodeErr != nil {
			return registerNodeErr
		}
	}

	// Start DCP feed
	base.InfofCtx(ctx, base.KeyChanges, "Starting mutation feed on bucket %v", base.MD(db.Bucket.GetName()))
	cacheFeedStatsMap := db.DbStats.Database().CacheFeedMapStats
	if err := db.mutationListener.Start(ctx, db.Bucket, cacheFeedStatsMap.Map, db.Scopes, db.MetadataStore); err != nil {
		return err
	}

	// Get current value of _sync:seq
	initialSequence, seqErr := db.sequences.lastSequence(ctx)
	if seqErr != nil {
		return seqErr
	}
	initialSequenceTime := time.Now()

	base.InfofCtx(ctx, base.KeyCRUD, "Database has _sync:seq value on startup of %d", initialSequence)

	// Unlock change cache.  Validate that any allocated sequences on other nodes have either been assigned or released
	// before starting
	if initialSequence > 0 && !BypassReleasedSequenceWait.Load() {
		_ = db.sequences.waitForReleasedSequences(ctx, initialSequenceTime)
	}

	if err := db.changeCache.Start(initialSequence); err != nil {
		return err
	}

	// If this is an xattr import node, start import feed.  Must be started after the caching DCP feed, as import cfg
	// subscription relies on the caching feed.
	if db.autoImport {
		ctx := db.AddBucketUserLogContext(ctx)
		db.ImportListener = NewImportListener(ctx, db.MetadataKeys.DCPVersionedCheckpointPrefix(db.Options.GroupID, db.Options.ImportVersion), db)
		if importFeedErr := db.ImportListener.StartImportFeed(db); importFeedErr != nil {
			db.ImportListener = nil
			return importFeedErr
		}
	}

	// Load providers into provider map.  Does basic validation on the provider definition, and identifies the default provider.
	if db.Options.OIDCOptions != nil {
		db.OIDCProviders = make(auth.OIDCProviderMap)

		for name, provider := range db.Options.OIDCOptions.Providers {
			if provider.Issuer == "" || base.ValDefault(provider.ClientID, "") == "" {
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
		db.LocalJWTProviders[name] = cfg.BuildProvider(ctx, name)
	}

	if db.UseXattrs() {
		// Log the purge interval for tombstone compaction
		mpi := db.GetMetadataPurgeInterval(ctx, true)
		base.InfofCtx(ctx, base.KeyAll, "Using metadata purge interval of %.2f days for tombstone compaction.", mpi.Hours()/24)

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
					_, err := db.Compact(ctx, false, nil, bgtTerminator, true)
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

	// create a background task to keep track of the number of active replication connections the database has each second
	bgtSyncTime, err := NewBackgroundTask(ctx, "TotalSyncTimeStat", func(ctx context.Context) error {
		db.UpdateTotalSyncTimeStat()
		return nil
	}, 1*time.Second, db.terminator)
	if err != nil {
		return err
	}
	db.backgroundTasks = append(db.backgroundTasks, bgtSyncTime)

	db.AttachmentMigrationManager = NewAttachmentMigrationManager(db)
	// if we have collections requiring migration, run the job
	if len(db.RequireAttachmentMigration) > 0 && !db.usingRosmar() {
		err := db.AttachmentMigrationManager.Start(ctx, nil)
		if err != nil {
			base.WarnfCtx(ctx, "Error trying to migrate attachments for %s with error: %v", db.Name, err)
		}
		base.DebugfCtx(ctx, base.KeyAll, "Migrating attachment metadata automatically to Sync Gateway 4.0+ for collections %v", db.RequireAttachmentMigration)
	}

	if err := base.RequireNoBucketTTL(ctx, db.Bucket); err != nil {
		return err
	}

	db.ExitChanges = make(chan struct{})

	// Start checking heartbeats for other nodes.  Must be done after caching feed starts, to ensure any removals
	// are detected and processed by this node.
	if db.Heartbeater != nil {
		if err := db.Heartbeater.StartCheckingHeartbeats(ctx); err != nil {
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
		worker := func() (shouldRetry bool, err error, value any) {
			_, _, err = dbc.UpdatePrincipal(ctx, princ, (what == "user"), isGuest)
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

		err, _ := base.RetryLoop(ctx, "installPrincipals", worker, base.CreateDoublingSleeperFunc(16, 10))
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

// DataStoreNames returns the names of all datastores connected to this database
func (db *Database) DataStoreNames() base.ScopeAndCollectionNames {
	if db.Scopes == nil {
		return base.ScopeAndCollectionNames{
			base.DefaultScopeAndCollectionName(),
		}
	}
	var names base.ScopeAndCollectionNames
	for scopeName, scope := range db.Scopes {
		for collectionName := range scope.Collections {
			names = append(names, base.ScopeAndCollectionName{Scope: scopeName, Collection: collectionName})
		}
	}
	return names
}

// GetCollectionIDs will return all collection IDs for all collections configured on the database
func (db *DatabaseContext) GetCollectionIDs() []uint32 {
	return maps.Keys(db.CollectionByID)
}

// PurgeDCPCheckpoints will purge all DCP metadata from previous run in the bucket, used to reset dcp client to 0
func PurgeDCPCheckpoints(ctx context.Context, database *DatabaseContext, checkpointPrefix string, taskID string) error {

	bucket, err := base.AsGocbV2Bucket(database.Bucket)
	if err != nil {
		return err
	}
	numVbuckets, err := bucket.GetMaxVbno()
	if err != nil {
		return err
	}

	datastore := database.MetadataStore
	metadata := base.NewDCPMetadataCS(ctx, datastore, numVbuckets, base.DefaultNumWorkers, checkpointPrefix)
	metadata.Purge(ctx, base.DefaultNumWorkers)
	return nil
}

func (db *DatabaseContext) EnableAllowConflicts(tb testing.TB) {
	db.Options.AllowConflicts = base.Ptr(true)
}

// useShardedDCP returns true if the database supports sharded DCP feeds.
func (db *DatabaseContext) useShardedDCP() bool {
	return base.IsEnterpriseEdition() && !db.usingRosmar()
}

// collectionNames returns the names of the collections on this database.
func (db *DatabaseContext) collectionNames() base.CollectionNames {
	names := base.NewCollectionNames()
	for _, col := range db.CollectionByID {
		names.Add(col.dataStore)
	}
	return names
}

// GetSameSiteCookieMode returns the http.SameSite mode based on the unsupported database options. Returns an error if
// an invalid string is set.
func (o *UnsupportedOptions) GetSameSiteCookieMode() (http.SameSite, error) {
	if o == nil || o.SameSiteCookie == nil {
		return http.SameSiteDefaultMode, nil
	}
	switch *o.SameSiteCookie {
	case "Lax":
		return http.SameSiteLaxMode, nil
	case "Strict":
		return http.SameSiteStrictMode, nil
	case "None":
		return http.SameSiteNoneMode, nil
	case "Default":
		return http.SameSiteDefaultMode, nil
	default:
		return http.SameSiteDefaultMode, fmt.Errorf("unsupported_options.same_site_cookie option %q is not valid, choices are \"Lax\", \"Strict\", and \"None", *o.SameSiteCookie)
	}
}

// usingRosmar returns true if the database is configured to use Rosmar.
func (db *DatabaseContext) usingRosmar() bool {
	_, err := base.AsRosmarBucket(db.Bucket)
	return err == nil
}
