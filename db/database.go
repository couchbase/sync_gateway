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
// completion of all background tasks before the server is stopped.
const BGTCompletionMaxWait = 30 * time.Second

// Basic description of a database. Shared between all Database objects on the same database.
// This object is thread-safe so it can be shared between HTTP handlers.
type DatabaseContext struct {
	Name                         string                  // Database name
	UUID                         string                  // UUID for this database instance. Used by cbgt and sgr
	Bucket                       base.Bucket             // Storage
	BucketSpec                   base.BucketSpec         // The BucketSpec
	BucketLock                   sync.RWMutex            // Control Access to the underlying bucket object
	mutationListener             changeListener          // Caching feed listener
	ImportListener               *importListener         // Import feed listener
	sequences                    *sequenceAllocator      // Source of new sequence numbers
	ChannelMapper                *channels.ChannelMapper // Runs JS 'sync' function
	StartTime                    time.Time               // Timestamp when context was instantiated
	RevsLimit                    uint32                  // Max depth a document's revision tree can grow to
	autoImport                   bool                    // Add sync data to new untracked couchbase server docs?  (Xattr mode specific)
	revisionCache                RevisionCache           // Cache of recently-accessed doc revisions
	changeCache                  *changeCache            // Cache of recently-access channels
	EventMgr                     *EventManager           // Manages notification events
	AllowEmptyPassword           bool                    // Allow empty passwords?  Defaults to false
	Options                      DatabaseContextOptions  // Database Context Options
	AccessLock                   sync.RWMutex            // Allows DB offline to block until synchronous calls have completed
	State                        uint32                  // The runtime state of the DB from a service perspective
	ResyncManager                *BackgroundManager
	TombstoneCompactionManager   *BackgroundManager
	AttachmentCompactionManager  *BackgroundManager
	ExitChanges                  chan struct{}            // Active _changes feeds on the DB will close when this channel is closed
	OIDCProviders                auth.OIDCProviderMap     // OIDC clients
	PurgeInterval                time.Duration            // Metadata purge interval
	serverUUID                   string                   // UUID of the server, if available
	DbStats                      *base.DbStats            // stats that correspond to this database context
	CompactState                 uint32                   // Status of database compaction
	terminator                   chan bool                // Signal termination of background goroutines
	backgroundTasks              []BackgroundTask         // List of background tasks that are initiated.
	activeChannels               *channels.ActiveChannels // Tracks active replications by channel
	CfgSG                        cbgt.Cfg                 // Sync Gateway cluster shared config
	SGReplicateMgr               *sgReplicateManager      // Manages interactions with sg-replicate replications
	Heartbeater                  base.Heartbeater         // Node heartbeater for SG cluster awareness
	ServeInsecureAttachmentTypes bool                     // Attachment content type will bypass the content-disposition handling, default false
	NoX509HTTPClient             *http.Client             // A HTTP Client from gocb to use the management endpoints
	ServerContextHasStarted      chan struct{}            // Closed via PostStartup once the server has fully started
}

type DatabaseContextOptions struct {
	CacheOptions                  *CacheOptions
	RevisionCacheOptions          *RevisionCacheOptions
	OldRevExpirySeconds           uint32
	AdminInterface                *string
	UnsupportedOptions            *UnsupportedOptions
	OIDCOptions                   *auth.OIDCOptions
	DBOnlineCallback              DBOnlineCallback // Callback function to take the DB back online
	ImportOptions                 ImportOptions
	EnableXattr                   bool             // Use xattr for _sync
	LocalDocExpirySecs            uint32           // The _local doc expiry time in seconds
	SecureCookieOverride          bool             // Pass-through DBConfig.SecureCookieOverride
	SessionCookieName             string           // Pass-through DbConfig.SessionCookieName
	SessionCookieHttpOnly         bool             // Pass-through DbConfig.SessionCookieHTTPOnly
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
	UserViews                 *UserViewsOptions        `json:"user_views,omitempty"`                    // Config settings for user views
	OidcTestProvider          *OidcTestProviderOptions `json:"oidc_test_provider,omitempty"`            // Config settings for OIDC Provider
	APIEndpoints              *APIEndpoints            `json:"api_endpoints,omitempty"`                 // Config settings for API endpoints
	WarningThresholds         *WarningThresholds       `json:"warning_thresholds,omitempty"`            // Warning thresholds related to _sync size
	DisableCleanSkippedQuery  bool                     `json:"disable_clean_skipped_query,omitempty"`   // Clean skipped sequence processing bypasses final check
	OidcTlsSkipVerify         bool                     `json:"oidc_tls_skip_verify,omitempty"`          // Config option to enable self-signed certs for OIDC testing.
	SgrTlsSkipVerify          bool                     `json:"sgr_tls_skip_verify,omitempty"`           // Config option to enable self-signed certs for SG-Replicate testing.
	RemoteConfigTlsSkipVerify bool                     `json:"remote_config_tls_skip_verify,omitempty"` // Config option to enable self signed certificates for external JavaScript load.
	GuestReadOnly             bool                     `json:"guest_read_only,omitempty"`               // Config option to restrict GUEST document access to read-only
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
	ImportFilter     *ImportFilterFunction // Opt-in filter for document import
	BackupOldRev     bool                  // Create temporary backup of old revision body when available
	ImportPartitions uint16                // Number of partitions for import
}

// Represents a simulated CouchDB database. A new instance is created for each HTTP request,
// so this struct does not have to be thread-safe.
type Database struct {
	*DatabaseContext
	user auth.User
	Ctx  context.Context
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
func connectToBucketErrorHandling(spec base.BucketSpec, gotErr error) (fatalError bool, err error) {
	if gotErr != nil {
		if errors.Is(gotErr, base.ErrAuthError) {
			username, _, _ := spec.Auth.GetCredentials()
			base.WarnfCtx(context.TODO(), "Unable to authenticate as user %q: %v", base.UD(username), gotErr)
			// auth errors will be wrapped with HTTPError further up the stack where appropriate. Return the raw error that can still be checked.
			return false, gotErr
		}

		// Fatal errors get an additional log message, but are otherwise still transformed below.
		if errors.Is(gotErr, base.ErrFatalBucketConnection) {
			base.WarnfCtx(context.TODO(), "Fatal error connecting to bucket: %v", gotErr)
			fatalError = true
		}

		// Remaining errors are appended to the end of a more helpful error message.
		return fatalError, base.HTTPErrorf(http.StatusBadGateway,
			" Unable to connect to Couchbase Server (connection refused). Please ensure it is running and reachable at the configured host and port.  Detailed error: %s", gotErr)
	}

	return false, nil
}

// ConnectToBucketFailFast opens a Couchbase connect and return a specific bucket without retrying on failure.
func ConnectToBucketFailFast(spec base.BucketSpec) (bucket base.Bucket, err error) {
	bucket, err = base.GetBucket(spec)
	_, err = connectToBucketErrorHandling(spec, err)
	return bucket, err
}

// ConnectToBucket opens a Couchbase connection and return a specific bucket.
func ConnectToBucket(spec base.BucketSpec) (base.Bucket, error) {

	// start a retry loop to connect to the bucket backing off double the delay each time
	worker := func() (bool, error, interface{}) {
		bucket, err := base.GetBucket(spec)

		// Retry if there was a non-fatal error
		fatalError, newErr := connectToBucketErrorHandling(spec, err)
		shouldRetry := newErr != nil && !fatalError

		return shouldRetry, newErr, bucket
	}

	sleeper := base.CreateDoublingSleeperFunc(
		13, // MaxNumRetries approx 40 seconds total retry duration
		5,  // InitialRetrySleepTimeMS
	)

	description := fmt.Sprintf("Attempt to connect to bucket : %v", spec.BucketName)
	err, ibucket := base.RetryLoop(description, worker, sleeper)
	if err != nil {
		return nil, err
	}

	return ibucket.(base.Bucket), nil
}

// Function type for something that calls NewDatabaseContext and wants a callback when the DB is detected
// to come back online. A rest.ServerContext package cannot be passed since it would introduce a circular dependency
type DBOnlineCallback func(dbContext *DatabaseContext)

// Creates a new DatabaseContext on a bucket. The bucket will be closed when this context closes.
func NewDatabaseContext(dbName string, bucket base.Bucket, autoImport bool, options DatabaseContextOptions) (dbc *DatabaseContext, returnedError error) {
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

	// Register the cbgt pindex type for the configGroup
	RegisterImportPindexImpl(options.GroupID)

	dbContext := &DatabaseContext{
		Name:       dbName,
		UUID:       cbgt.NewUUID(),
		Bucket:     bucket,
		StartTime:  time.Now(),
		autoImport: autoImport,
		Options:    options,
		DbStats:    initDatabaseStats(dbName, autoImport, options),
	}

	cleanupFunctions = append(cleanupFunctions, func() {
		base.SyncGatewayStats.ClearDBStats(dbName)
	})

	if dbContext.AllowConflicts() {
		dbContext.RevsLimit = DefaultRevsLimitConflicts
	} else {
		dbContext.RevsLimit = DefaultRevsLimitNoConflicts
	}

	dbContext.terminator = make(chan bool)

	dbContext.revisionCache = NewRevisionCache(
		dbContext.Options.RevisionCacheOptions,
		dbContext,
		dbContext.DbStats.Cache(),
	)

	dbContext.EventMgr = NewEventManager()
	logCtx := context.TODO()

	var err error
	dbContext.sequences, err = newSequenceAllocator(bucket, dbContext.DbStats.Database())
	if err != nil {
		return nil, err
	}

	cleanupFunctions = append(cleanupFunctions, func() {
		dbContext.sequences.Stop()
	})

	// Get current value of _sync:seq
	initialSequence, seqErr := dbContext.sequences.lastSequence()
	if seqErr != nil {
		return nil, seqErr
	}
	initialSequenceTime := time.Now()

	// In-memory channel cache
	dbContext.changeCache = &changeCache{}

	// Callback that is invoked whenever a set of channels is changed in the ChangeCache
	notifyChange := func(changedChannels base.Set) {
		dbContext.mutationListener.Notify(changedChannels)
	}

	// Initialize the active channel counter
	dbContext.activeChannels = channels.NewActiveChannels(dbContext.DbStats.Cache().NumActiveChannels)

	// Initialize the ChangeCache.  Will be locked and unusable until .Start() is called (SG #3558)
	err = dbContext.changeCache.Init(
		dbContext,
		notifyChange,
		options.CacheOptions,
	)
	if err != nil {
		base.DebugfCtx(logCtx, base.KeyDCP, "Error initializing the change cache", err)
	}

	// Set the DB Context notifyChange callback to call back the changecache DocChanged callback
	dbContext.SetOnChangeCallback(dbContext.changeCache.DocChanged)

	// Initialize the tap Listener for notify handling
	dbContext.mutationListener.Init(bucket.GetName(), options.GroupID)

	// Initialize sg cluster config.  Required even if import and sgreplicate are disabled
	// on this node, to support replication REST API calls
	if base.IsEnterpriseEdition() {
		sgCfg, err := base.NewCfgSG(dbContext.Bucket, dbContext.Options.GroupID)
		if err != nil {
			return nil, err
		}
		dbContext.changeCache.cfgEventCallback = sgCfg.FireEvent
		dbContext.CfgSG = sgCfg
	} else {
		dbContext.CfgSG = cbgt.NewCfgMem()
	}

	// Initialize sg-replicate manager
	dbContext.SGReplicateMgr, err = NewSGReplicateManager(dbContext, dbContext.CfgSG)
	if err != nil {
		return nil, err
	}

	importEnabled := dbContext.UseXattrs() && dbContext.autoImport
	sgReplicateEnabled := dbContext.Options.SGReplicateOptions.Enabled

	// Initialize node heartbeater in EE mode if sg-replicate or import enabled on the node.  This node must start
	// sending heartbeats before registering itself to the cfg, to avoid triggering immediate removal by other active nodes.
	if base.IsEnterpriseEdition() && (importEnabled || sgReplicateEnabled) {
		// Create heartbeater
		heartbeaterPrefix := base.SyncPrefix
		if dbContext.Options.GroupID != "" {
			heartbeaterPrefix = heartbeaterPrefix + dbContext.Options.GroupID + ":"
		}
		heartbeater, err := base.NewCouchbaseHeartbeater(bucket, heartbeaterPrefix, dbContext.UUID)
		if err != nil {
			return nil, pkgerrors.Wrapf(err, "Error starting heartbeater for bucket %s", base.MD(bucket.GetName()).Redact())
		}
		err = heartbeater.StartSendingHeartbeats()
		if err != nil {
			return nil, err
		}
		dbContext.Heartbeater = heartbeater

		cleanupFunctions = append(cleanupFunctions, func() {
			dbContext.Heartbeater.Stop()
		})
	}

	// If sgreplicate is enabled on this node, register this node to accept notifications
	if sgReplicateEnabled {
		registerNodeErr := dbContext.SGReplicateMgr.StartLocalNode(dbContext.UUID, dbContext.Heartbeater)
		if registerNodeErr != nil {
			return nil, registerNodeErr
		}

		cleanupFunctions = append(cleanupFunctions, func() {
			dbContext.SGReplicateMgr.Stop()
		})
	}

	// Start DCP feed
	base.InfofCtx(logCtx, base.KeyDCP, "Starting mutation feed on bucket %v due to either channel cache mode or doc tracking (auto-import)", base.MD(bucket.GetName()))
	cacheFeedStatsMap := dbContext.DbStats.Database().CacheFeedMapStats
	err = dbContext.mutationListener.Start(bucket, cacheFeedStatsMap.Map)

	// Check if there is an error starting the DCP feed
	if err != nil {
		dbContext.changeCache = nil
		return nil, err
	}

	cleanupFunctions = append(cleanupFunctions, func() {
		dbContext.mutationListener.Stop()
	})

	// Unlock change cache.  Validate that any allocated sequences on other nodes have either been assigned or released
	// before starting
	if initialSequence > 0 {
		_ = dbContext.sequences.waitForReleasedSequences(initialSequenceTime)
	}

	err = dbContext.changeCache.Start(initialSequence)
	if err != nil {
		return nil, err
	}

	cleanupFunctions = append(cleanupFunctions, func() {
		dbContext.changeCache.Stop()
	})

	// If this is an xattr import node, start import feed.  Must be started after the caching DCP feed, as import cfg
	// subscription relies on the caching feed.
	if importEnabled {
		dbContext.ImportListener = NewImportListener(dbContext.Options.GroupID)
		if importFeedErr := dbContext.ImportListener.StartImportFeed(bucket, dbContext.DbStats, dbContext); importFeedErr != nil {
			return nil, importFeedErr
		}

		cleanupFunctions = append(cleanupFunctions, func() {
			dbContext.ImportListener.Stop()
		})
	}

	// Load providers into provider map.  Does basic validation on the provider definition, and identifies the default provider.
	if options.OIDCOptions != nil {
		dbContext.OIDCProviders = make(auth.OIDCProviderMap)

		for name, provider := range options.OIDCOptions.Providers {
			if provider.Issuer == "" || provider.ClientID == "" {
				base.WarnfCtx(logCtx, "Issuer and ClientID required for OIDC Provider - skipping provider %q", base.UD(name))
				continue
			}

			if provider.ValidationKey == nil {
				base.WarnfCtx(logCtx, "Validation Key not defined in config for provider %q - auth code flow will not be supported for this provider", base.UD(name))
			}

			if strings.Contains(name, "_") {
				return nil, base.RedactErrorf("OpenID Connect provider names cannot contain underscore:%s", base.UD(name))
			}
			provider.Name = name
			if _, ok := dbContext.OIDCProviders[provider.Issuer]; ok {
				return nil, base.RedactErrorf("Multiple OIDC providers defined for issuer %v", base.UD(provider.Issuer))
			}

			// If this is the default provider, or there's only one provider defined, set IsDefault
			if (options.OIDCOptions.DefaultProvider != nil && name == *options.OIDCOptions.DefaultProvider) || len(options.OIDCOptions.Providers) == 1 {
				provider.IsDefault = true
			}

			insecureSkipVerify := false
			if options.UnsupportedOptions != nil {
				insecureSkipVerify = options.UnsupportedOptions.OidcTlsSkipVerify
			}
			provider.InsecureSkipVerify = insecureSkipVerify

			// If this isn't the default provider, add the provider to the callback URL (needed to identify provider to _oidc_callback)
			if !provider.IsDefault && provider.CallbackURL != nil {
				updatedCallback, err := auth.SetURLQueryParam(*provider.CallbackURL, auth.OIDCAuthProvider, name)
				if err != nil {
					return nil, base.RedactErrorf("Failed to add provider %q to OIDC callback URL: %v", base.UD(name), err)
				}
				provider.CallbackURL = &updatedCallback
			}

			dbContext.OIDCProviders[name] = provider
		}
		if len(dbContext.OIDCProviders) == 0 {
			return nil, errors.New("OpenID Connect defined in config, but no valid OpenID Connect providers specified")

		}

	}

	if dbContext.UseXattrs() {
		// Set the purge interval for tombstone compaction
		dbContext.PurgeInterval = DefaultPurgeInterval
		cbStore, ok := base.AsCouchbaseStore(bucket)
		if ok {
			serverPurgeInterval, err := cbStore.MetadataPurgeInterval()
			if err != nil {
				base.WarnfCtx(logCtx, "Unable to retrieve server's metadata purge interval - will use default value. %s", err)
			} else if serverPurgeInterval > 0 {
				dbContext.PurgeInterval = serverPurgeInterval
			}
		}
		base.InfofCtx(logCtx, base.KeyAll, "Using metadata purge interval of %.2f days for tombstone compaction.", dbContext.PurgeInterval.Hours()/24)

		if dbContext.Options.CompactInterval != 0 {
			if autoImport {
				db := Database{DatabaseContext: dbContext}
				// Wrap the dbContext's terminator in a SafeTerminator for the compaction task
				bgtTerminator := base.NewSafeTerminator()
				go func() {
					<-dbContext.terminator
					bgtTerminator.Close()
				}()
				bgt, err := NewBackgroundTask("Compact", dbContext.Name, func(ctx context.Context) error {
					_, err := db.Compact(false, func(purgedDocCount *int) {}, bgtTerminator)
					if err != nil {
						base.WarnfCtx(ctx, "Error trying to compact tombstoned documents for %q with error: %v", dbContext.Name, err)
					}
					return nil
				}, time.Duration(dbContext.Options.CompactInterval)*time.Second, dbContext.terminator)
				if err != nil {
					return nil, err
				}
				db.backgroundTasks = append(db.backgroundTasks, bgt)
			} else {
				base.WarnfCtx(logCtx, "Automatic compaction can only be enabled on nodes running an Import process")
			}
		}

	}

	// Make sure there is no MaxTTL set on the bucket (SG #3314)
	cbs, ok := base.AsCouchbaseStore(bucket)
	if ok {
		maxTTL, err := cbs.MaxTTL()
		if err != nil {
			return nil, err
		}
		if maxTTL != 0 {
			return nil, fmt.Errorf("Backing Couchbase Server bucket has a non-zero MaxTTL value: %d.  Please set MaxTTL to 0 in Couchbase Server Admin UI and try again.", maxTTL)
		}
	}

	dbContext.ExitChanges = make(chan struct{})

	// Start checking heartbeats for other nodes.  Must be done after caching feed starts, to ensure any removals
	// are detected and processed by this node.
	if dbContext.Heartbeater != nil {
		err = dbContext.Heartbeater.StartCheckingHeartbeats()
		if err != nil {
			return nil, err
		}
		// No cleanup necessary, stop heartbeater above will take care of it
	}

	dbContext.ResyncManager = NewResyncManager()
	dbContext.TombstoneCompactionManager = NewTombstoneCompactionManager()
	dbContext.AttachmentCompactionManager = NewAttachmentCompactionManager(bucket)

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

// Create a zero'd out since value (eg, initial since value) based on the sequence type
// of the database (int or vector clock)
func (context *DatabaseContext) CreateZeroSinceValue() SequenceID {
	return SequenceID{}
}

func (context *DatabaseContext) SetOnChangeCallback(callback DocChangedFunc) {
	context.mutationListener.OnDocChanged = callback
}

func (dbCtx *DatabaseContext) GetServerUUID() string {

	dbCtx.BucketLock.RLock()
	defer dbCtx.BucketLock.RUnlock()

	// Lazy load the server UUID, if we can get it.
	if dbCtx.serverUUID == "" {
		cbs, ok := base.AsCouchbaseStore(dbCtx.Bucket)
		if !ok {
			base.WarnfCtx(context.TODO(), "Database %v: Unable to get server UUID. Underlying bucket type was not GoCBBucket.", base.MD(dbCtx.Name))
			return ""
		}

		uuid, err := cbs.ServerUUID()
		if err != nil {
			base.WarnfCtx(context.TODO(), "Database %v: Unable to get server UUID: %v", base.MD(dbCtx.Name), err)
			return ""
		}

		base.DebugfCtx(context.TODO(), base.KeyAll, "Database %v: Got server UUID %v", base.MD(dbCtx.Name), base.MD(uuid))
		dbCtx.serverUUID = uuid
	}

	return dbCtx.serverUUID
}

// Utility function to support cache testing from outside db package
func (context *DatabaseContext) GetChangeCache() *changeCache {
	return context.changeCache
}

func (context *DatabaseContext) Close() {
	context.BucketLock.Lock()
	defer context.BucketLock.Unlock()

	context.OIDCProviders.Stop()
	close(context.terminator)
	// Wait for database background tasks to finish.
	waitForBGTCompletion(BGTCompletionMaxWait, context.backgroundTasks, context.Name)
	context.sequences.Stop()
	context.mutationListener.Stop()
	context.changeCache.Stop()
	context.ImportListener.Stop()
	if context.Heartbeater != nil {
		context.Heartbeater.Stop()
	}
	if context.SGReplicateMgr != nil {
		context.SGReplicateMgr.Stop()
	}
	context.Bucket.Close()
	context.Bucket = nil

	base.RemovePerDbStats(context.Name)

}

// waitForBGTCompletion waits for all the background tasks to finish.
func waitForBGTCompletion(waitTimeMax time.Duration, tasks []BackgroundTask, dbName string) {
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
		base.InfofCtx(context.TODO(), base.KeyAll, "Timeout after %v of waiting for background task %q to "+
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
	context.mutationListener.Init(context.Bucket.GetName(), context.Options.GroupID)
	cacheFeedStatsMap := context.DbStats.Database().CacheFeedMapStats
	if err := context.mutationListener.Start(context.Bucket, cacheFeedStatsMap.Map); err != nil {
		return err
	}
	return nil
}

// Cache flush support.  Currently test-only - added for unit test access from rest package
func (dbCtx *DatabaseContext) FlushChannelCache() error {
	base.InfofCtx(context.TODO(), base.KeyCache, "Flushing channel cache")
	return dbCtx.changeCache.Clear()
}

// Removes previous versions of Sync Gateway's design docs found on the server
func (context *DatabaseContext) RemoveObsoleteDesignDocs(previewOnly bool) (removedDesignDocs []string, err error) {
	return removeObsoleteDesignDocs(context.Bucket, previewOnly, context.UseViews())
}

// Removes previous versions of Sync Gateway's indexes found on the server
func (dbCtx *DatabaseContext) RemoveObsoleteIndexes(previewOnly bool) (removedIndexes []string, err error) {

	if !dbCtx.Bucket.IsSupported(sgbucket.DataStoreFeatureN1ql) {
		return removedIndexes, nil
	}

	n1qlStore, ok := base.AsN1QLStore(dbCtx.Bucket)
	if !ok {
		base.WarnfCtx(context.TODO(), "Cannot remove obsolete indexes for non-gocb bucket - skipping.")
		return make([]string, 0), nil
	}

	return removeObsoleteIndexes(n1qlStore, previewOnly, dbCtx.UseXattrs(), dbCtx.UseViews(), sgIndexes)
}

// Trigger terminate check handling for connected continuous replications.
// TODO: The underlying code (NotifyCheckForTermination) doesn't actually leverage the specific username - should be refactored
//    to remove
func (context *DatabaseContext) NotifyTerminatedChanges(username string) {
	context.mutationListener.NotifyCheckForTermination(base.SetOf(base.UserPrefix + username))
}

func (dc *DatabaseContext) TakeDbOffline(reason string) error {

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
			base.DebugfCtx(context.TODO(), base.KeyCRUD, "Error raising database state change event: %v", err)
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

		base.InfofCtx(context.TODO(), base.KeyCRUD, msg)
		return base.HTTPErrorf(http.StatusServiceUnavailable, msg)
	}
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
	authenticator := auth.NewAuthenticator(context.Bucket, context, auth.AuthenticatorOptions{
		ClientPartitionWindow:    context.Options.ClientPartitionWindow,
		ChannelsWarningThreshold: channelsWarningThreshold,
		SessionCookieName:        sessionCookieName,
		BcryptCost:               context.Options.BcryptCost,
		LogCtx:                   ctx,
	})

	return authenticator
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
func (db *Database) ReloadUser() error {
	if db.user == nil {
		return nil
	}
	user, err := db.Authenticator(db.Ctx).GetUser(db.user.Name())
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
func (db *Database) ForEachDocID(callback ForEachDocIDFunc, resultsOpts ForEachDocIDOptions) error {

	results, err := db.QueryAllDocs(db.Ctx, resultsOpts.Startkey, resultsOpts.Endkey)
	if err != nil {
		return err
	}

	err = db.processForEachDocIDResults(callback, resultsOpts.Limit, results)
	if err != nil {
		return err
	}
	return results.Close()
}

// Iterate over the results of an AllDocs query, performing ForEachDocID handling for each row
func (db *Database) processForEachDocIDResults(callback ForEachDocIDFunc, limit uint64, results sgbucket.QueryResultIterator) error {

	count := uint64(0)
	for {
		var queryRow AllDocsIndexQueryRow
		var found bool
		var docid, revid string
		var seq uint64
		var channels []string
		if db.Options.UseViews {
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

// Returns the IDs of all users and roles
func (db *DatabaseContext) AllPrincipalIDs(ctx context.Context) (users, roles []string, err error) {

	startKey := ""
	limit := db.Options.QueryPaginationLimit

	users = []string{}
	roles = []string{}

outerLoop:
	for {
		results, err := db.QueryPrincipals(ctx, startKey, limit)
		if err != nil {
			return nil, nil, err
		}

		var isUser bool
		var principalName string
		lenUserKeyPrefix := len(base.UserPrefix)

		resultCount := 0

		for {
			// startKey is inclusive for views, so need to skip first result if using non-empty startKey, as this results in an overlapping result
			var skipAddition bool
			if resultCount == 0 && startKey != "" {
				skipAddition = true
			}

			if db.Options.UseViews {
				var viewRow principalsViewRow
				found := results.Next(&viewRow)
				if !found {
					break
				}
				isUser = viewRow.Value
				principalName = viewRow.Key
				startKey = principalName
			} else {
				var queryRow QueryIdRow
				found := results.Next(&queryRow)
				if !found {
					break
				}
				if len(queryRow.Id) < lenUserKeyPrefix {
					continue
				}
				isUser = queryRow.Id[0:lenUserKeyPrefix] == base.UserPrefix
				principalName = queryRow.Id[lenUserKeyPrefix:]
				startKey = queryRow.Id
			}
			resultCount++

			if principalName != "" && !skipAddition {
				if isUser {
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
func (db *DatabaseContext) GetUsers(ctx context.Context, limit int) (users []PrincipalConfig, err error) {

	if db.Options.UseViews {
		return nil, errors.New("GetUsers not supported when running with useViews=true")
	}

	// While using SyncDocs index, must set startKey to the user prefix to avoid unwanted interaction between
	// limit handling and startKey (non-user _sync: prefixed documents being included in the query limit evaluation).
	// This doesn't happen for AllPrincipalIDs, I believe because the role check forces query to not assume
	// a contiguous set of results
	startKey := base.UserPrefix
	paginationLimit := db.Options.QueryPaginationLimit
	if paginationLimit == 0 {
		paginationLimit = DefaultQueryPaginationLimit
	}

	// If the requested limit is lower than the pagination limit, use requested limit as pagination limit
	if limit > 0 && limit < paginationLimit {
		paginationLimit = limit
	}

	users = []PrincipalConfig{}

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
			if resultCount == 0 && startKey != base.UserPrefix {
				skipAddition = true
			}

			var queryRow QueryUsersRow
			found := results.Next(&queryRow)
			if !found {
				break
			}
			startKey = base.UserPrefix + queryRow.Name
			resultCount++
			if queryRow.Name != "" && !skipAddition {
				principal := PrincipalConfig{
					Name:     &queryRow.Name,
					Email:    queryRow.Email,
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

// ////// HOUSEKEEPING:

// Deletes all session documents for a user
func (db *DatabaseContext) DeleteUserSessions(ctx context.Context, userName string) error {

	results, err := db.QuerySessions(ctx, userName)
	if err != nil {
		return err
	}

	var sessionsRow QueryIdRow
	for results.Next(&sessionsRow) {
		base.InfofCtx(ctx, base.KeyCRUD, "\tDeleting %q", sessionsRow.Id)
		if err := db.Bucket.Delete(sessionsRow.Id); err != nil {
			base.WarnfCtx(ctx, "Error deleting %q: %v", sessionsRow.Id, err)
		}
	}
	return results.Close()
}

// Trigger tombstone compaction from view and/or GSI indexes.  Several Sync Gateway indexes server tombstones (deleted documents with an xattr).
// There currently isn't a mechanism for server to remove these docs from the index when the tombstone is purged by the server during
// metadata purge, because metadata purge doesn't trigger a DCP event.
// When compact is run, Sync Gateway initiates a normal delete operation for the document and xattr (a Sync Gateway purge).  This triggers
// removal of the document from the index.  In the event that the document has already been purged by server, we need to recreate and delete
// the document to accomplish the same result.
type compactCallbackFunc func(purgedDocCount *int)

func (db *Database) Compact(skipRunningStateCheck bool, callback compactCallbackFunc, terminator *base.SafeTerminator) (int, error) {
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

	ctx := db.Ctx

	base.InfofCtx(ctx, base.KeyAll, "Starting compaction of purged tombstones for %s ...", base.MD(db.Name))
	purgeBody := Body{"_purged": true}
	for {
		purgedDocs := make([]string, 0)
		results, err := db.QueryTombstones(ctx, purgeOlderThan, QueryTombstoneBatch)
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
			purgeErr := db.Purge(tombstonesRow.Id)
			if purgeErr == nil {
				purgedDocs = append(purgedDocs, tombstonesRow.Id)
			} else if base.IsKeyNotFoundError(db.Bucket, purgeErr) {
				// If key no longer exists, need to add and remove to trigger removal from view
				_, addErr := db.Bucket.Add(tombstonesRow.Id, 0, purgeBody)
				if addErr != nil {
					base.WarnfCtx(ctx, "Error compacting key %s (add) - tombstone will not be compacted.  %v", base.UD(tombstonesRow.Id), addErr)
					continue
				}

				// At this point, the doc is not in a usable state for mobile
				// so mark it to be removed from cache, even if the subsequent delete fails
				purgedDocs = append(purgedDocs, tombstonesRow.Id)

				if delErr := db.Bucket.Delete(tombstonesRow.Id); delErr != nil {
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
			db.changeCache.Remove(purgedDocs, startTime)
			db.DbStats.Database().NumTombstonesCompacted.Add(int64(count))
		}
		base.DebugfCtx(ctx, base.KeyAll, "Compacted %v tombstones", count)

		callback(&purgedDocCount)

		if resultCount < QueryTombstoneBatch {
			break
		}
	}

	base.InfofCtx(ctx, base.KeyAll, "Finished compaction of purged tombstones for %s... Total Tombstones Compacted: %d", base.MD(db.Name), purgedDocCount)

	return purgedDocCount, nil
}

// ////// SYNC FUNCTION:

// Sets the database context's sync function based on the JS code from config.
// Returns a boolean indicating whether the function is different from the saved one.
// If multiple gateway instances try to update the function at the same time (to the same new
// value) only one of them will get a changed=true result.
func (dbCtx *DatabaseContext) UpdateSyncFun(syncFun string) (changed bool, err error) {
	if syncFun == "" {
		dbCtx.ChannelMapper = nil
	} else if dbCtx.ChannelMapper != nil {
		_, err = dbCtx.ChannelMapper.SetFunction(syncFun)
	} else {
		dbCtx.ChannelMapper = channels.NewChannelMapper(syncFun)
	}
	if err != nil {
		base.WarnfCtx(context.TODO(), "Error setting sync function: %s", err)
		return
	}

	var syncData struct { // format of the sync-fn document
		Sync string
	}

	_, err = dbCtx.Bucket.Update(base.SyncDataKeyWithGroupID(dbCtx.Options.GroupID), 0, func(currentValue []byte) ([]byte, *uint32, bool, error) {
		// The first time opening a new db, currentValue will be nil. Don't treat this as a change.
		if currentValue != nil {
			parseErr := base.JSONUnmarshal(currentValue, &syncData)
			if parseErr != nil || syncData.Sync != syncFun {
				changed = true
			}
		}
		if changed || currentValue == nil {
			syncData.Sync = syncFun
			bytes, err := base.JSONMarshal(syncData)
			return bytes, nil, false, err
		} else {
			return nil, nil, false, base.ErrUpdateCancel // value unchanged, no need to save
		}
	})

	if err == base.ErrUpdateCancel {
		err = nil
	}
	return
}

// Re-runs the sync function on every current document in the database (if doCurrentDocs==true)
// and/or imports docs in the bucket not known to the gateway (if doImportDocs==true).
// To be used when the JavaScript sync function changes.
type updateAllDocChannelsCallbackFunc func(docsProcessed, docsChanged *int)

func (db *Database) UpdateAllDocChannels(regenerateSequences bool, callback updateAllDocChannelsCallbackFunc, terminator *base.SafeTerminator) (int, error) {
	base.InfofCtx(db.Ctx, base.KeyAll, "Recomputing document channels...")
	base.InfofCtx(db.Ctx, base.KeyAll, "Re-running sync function on all documents...")

	queryLimit := db.Options.QueryPaginationLimit
	startSeq := uint64(0)
	endSeq, err := db.sequences.getSequence()
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
		results, err := db.QueryResync(db.Ctx, queryLimit, startSeq, endSeq)
		if err != nil {
			return 0, err
		}

		queryRowCount := 0
		highSeq := uint64(0)

		var importRow QueryIdRow
		for results.Next(&importRow) {
			select {
			case <-terminator.Done():
				base.InfofCtx(db.Ctx, base.KeyAll, "Resync was stopped before the operation could be completed. System "+
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
			documentUpdateFunc := func(doc *Document) (updatedDoc *Document, shouldUpdate bool, updatedExpiry *uint32, err error) {
				highSeq = doc.Sequence
				forceUpdate := false
				if !doc.HasValidSyncData() {
					// This is a document not known to the sync gateway. Ignore it:
					return nil, false, nil, base.ErrUpdateCancel
				} else {
					base.DebugfCtx(db.Ctx, base.KeyCRUD, "\tRe-syncing document %q", base.UD(docid))
				}

				// Run the sync fn over each current/leaf revision, in case there are conflicts:
				changed := 0
				doc.History.forEachLeaf(func(rev *RevInfo) {
					bodyBytes, _, err := db.get1xRevFromDoc(doc, rev.ID, false)
					if err != nil {
						base.WarnfCtx(db.Ctx, "Error getting rev from doc %s/%s %s", base.UD(docid), rev.ID, err)
					}
					var body Body
					if err := body.Unmarshal(bodyBytes); err != nil {
						base.WarnfCtx(db.Ctx, "Error unmarshalling body %s/%s for sync function %s", base.UD(docid), rev.ID, err)
						return
					}
					metaMap, err := doc.GetMetaMap(db.Options.UserXattrKey)
					if err != nil {
						return
					}
					channels, access, roles, syncExpiry, _, err := db.getChannelsAndAccess(doc, body, metaMap, rev.ID)
					if err != nil {
						// Probably the validator rejected the doc
						base.WarnfCtx(db.Ctx, "Error calling sync() on doc %q: %v", base.UD(docid), err)
						access = nil
						channels = nil
					}
					rev.Channels = channels

					if rev.ID == doc.CurrentRev {

						if regenerateSequences {
							unusedSequences, err = db.assignSequence(0, doc, unusedSequences)
							if err != nil {
								base.WarnfCtx(db.Ctx, "Unable to assign a sequence number: %v", err)
							}
							forceUpdate = true
						}

						changedChannels, err := doc.updateChannels(db.Ctx, channels)
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
				return doc, shouldUpdate, updatedExpiry, nil
			}
			var err error
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
					updatedDoc, shouldUpdate, updatedExpiry, err := documentUpdateFunc(doc)
					if err != nil {
						return nil, nil, deleteDoc, nil, err
					}
					if shouldUpdate {
						base.InfofCtx(db.Ctx, base.KeyAccess, "Saving updated channels and access grants of %q", base.UD(docid))
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
				_, err = db.Bucket.WriteUpdateWithXattr(key, base.SyncXattrName, db.Options.UserXattrKey, 0, nil, nil, writeUpdateFunc)
			} else {
				_, err = db.Bucket.Update(key, 0, func(currentValue []byte) ([]byte, *uint32, bool, error) {
					// Be careful: this block can be invoked multiple times if there are races!
					if currentValue == nil {
						return nil, nil, false, base.ErrUpdateCancel // someone deleted it?!
					}
					doc, err := unmarshalDocument(docid, currentValue)
					if err != nil {
						return nil, nil, false, err
					}
					updatedDoc, shouldUpdate, updatedExpiry, err := documentUpdateFunc(doc)
					if err != nil {
						return nil, nil, false, err
					}
					if shouldUpdate {
						base.InfofCtx(db.Ctx, base.KeyAccess, "Saving updated channels and access grants of %q", base.UD(docid))
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
			if err == nil {
				docsChanged++
			} else if err != base.ErrUpdateCancel {
				base.WarnfCtx(db.Ctx, "Error updating doc %q: %v", base.UD(docid), err)
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

	for _, sequence := range unusedSequences {
		err := db.sequences.releaseSequence(sequence)
		if err != nil {
			base.WarnfCtx(db.Ctx, "Error attempting to release sequence %d. Error %v", sequence, err)
		}
	}

	if regenerateSequences {
		users, roles, err := db.AllPrincipalIDs(db.Ctx)
		if err != nil {
			return docsChanged, err
		}

		authr := db.Authenticator(db.Ctx)
		regeneratePrincipalSequences := func(princ auth.Principal) error {
			nextSeq, err := db.DatabaseContext.sequences.nextSequence()
			if err != nil {
				return err
			}

			err = authr.UpdateSequenceNumber(princ, nextSeq)
			if err != nil {
				return err
			}

			return nil
		}

		for _, role := range roles {
			role, err := authr.GetRole(role)
			if err != nil {
				return docsChanged, err
			}
			err = regeneratePrincipalSequences(role)
			if err != nil {
				return docsChanged, err
			}
		}

		for _, user := range users {
			user, err := authr.GetUser(user)
			if err != nil {
				return docsChanged, err
			}
			err = regeneratePrincipalSequences(user)
			if err != nil {
				return docsChanged, err
			}
		}

	}

	base.InfofCtx(db.Ctx, base.KeyAll, "Finished re-running sync function; %d/%d docs changed", docsChanged, docsProcessed)

	if docsChanged > 0 {
		// Now invalidate channel cache of all users/roles:
		base.InfofCtx(db.Ctx, base.KeyAll, "Invalidating channel caches of users/roles...")
		users, roles, _ := db.AllPrincipalIDs(db.Ctx)
		for _, name := range users {
			db.invalUserChannels(name, endSeq)
		}
		for _, name := range roles {
			db.invalRoleChannels(name, endSeq)
		}
	}
	return docsChanged, nil
}

func (db *Database) invalUserRoles(username string, invalSeq uint64) {
	authr := db.Authenticator(db.Ctx)
	if err := authr.InvalidateRoles(username, invalSeq); err != nil {
		base.WarnfCtx(db.Ctx, "Error invalidating roles for user %s: %v", base.UD(username), err)
	}
}

func (db *Database) invalUserChannels(username string, invalSeq uint64) {
	authr := db.Authenticator(db.Ctx)
	if err := authr.InvalidateChannels(username, true, invalSeq); err != nil {
		base.WarnfCtx(db.Ctx, "Error invalidating channels for user %s: %v", base.UD(username), err)
	}
}

func (db *Database) invalRoleChannels(rolename string, invalSeq uint64) {
	authr := db.Authenticator(db.Ctx)
	if err := authr.InvalidateChannels(rolename, false, invalSeq); err != nil {
		base.WarnfCtx(db.Ctx, "Error invalidating channels for role %s: %v", base.UD(rolename), err)
	}
}

func (db *Database) invalUserOrRoleChannels(name string, invalSeq uint64) {

	principalName, isRole := channels.AccessNameToPrincipalName(name)
	if isRole {
		db.invalRoleChannels(principalName, invalSeq)
	} else {
		db.invalUserChannels(principalName, invalSeq)
	}
}

func (dbCtx *DatabaseContext) ObtainManagementEndpoints() ([]string, error) {
	cbStore, ok := base.AsCouchbaseStore(dbCtx.Bucket)
	if !ok {
		base.WarnfCtx(context.TODO(), "Database %v: Unable to get server management endpoints. Underlying bucket type was not GoCBBucket.", base.MD(dbCtx.Name))
		return nil, nil
	}

	return base.GoCBBucketMgmtEndpoints(cbStore)
}

func (dbCtx *DatabaseContext) ObtainManagementEndpointsAndHTTPClient() ([]string, *http.Client, error) {
	cbStore, ok := base.AsCouchbaseStore(dbCtx.Bucket)
	if !ok {
		base.WarnfCtx(context.TODO(), "Database %v: Unable to get server management endpoints. Underlying bucket type was not GoCBBucket.", base.MD(dbCtx.Name))
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

func (context *DatabaseContext) DeltaSyncEnabled() bool {
	return context.Options.DeltaSyncOptions.Enabled
}

func (context *DatabaseContext) AllowExternalRevBodyStorage() bool {

	// Support unit testing w/out xattrs enabled
	if base.TestExternalRevStorage {
		return false
	}
	return !context.UseXattrs()
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

// For test usage
func (context *DatabaseContext) FlushRevisionCacheForTest() {

	context.revisionCache = NewRevisionCache(
		context.Options.RevisionCacheOptions,
		context,
		context.DbStats.Cache(),
	)

}

func initDatabaseStats(dbName string, autoImport bool, options DatabaseContextOptions) *base.DbStats {

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
			QueryTypeSessions,
			QueryTypeTombstones,
			QueryTypeResync,
			QueryTypeAllDocs,
			QueryTypeUsers,
		}
	}

	return base.SyncGatewayStats.NewDBStats(dbName, enabledDeltaSync, enabledImport, enabledViews, queryNames...)
}

// For test usage
func (context *DatabaseContext) GetRevisionCacheForTest() RevisionCache {
	return context.revisionCache
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
