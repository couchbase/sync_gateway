//  Copyright (c) 2012 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package db

import (
	"context"
	"errors"
	"expvar"
	"fmt"
	"net/http"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/couchbase/gocb"
	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/auth"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"
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
	DefaultPurgeInterval        = 30 // Default metadata purge interval, in days.  Used if server's purge interval is unavailable

)

// Default values for delta sync
var (
	DefaultDeltaSyncEnabled   = false
	DefaultDeltaSyncRevMaxAge = uint32(60 * 60 * 24) // 24 hours in seconds
)

var DefaultCompactInterval = uint32(60 * 60 * 24) // Default compact interval in seconds = 1 Day

const (
	CompactIntervalMinDays = float32(0.04) // ~1 Hour in days
	CompactIntervalMaxDays = float32(60)   // 60 Days in days
)

// Basic description of a database. Shared between all Database objects on the same database.
// This object is thread-safe so it can be shared between HTTP handlers.
type DatabaseContext struct {
	Name               string                   // Database name
	Bucket             base.Bucket              // Storage
	BucketSpec         base.BucketSpec          // The BucketSpec
	BucketLock         sync.RWMutex             // Control Access to the underlying bucket object
	mutationListener   changeListener           // Caching feed listener
	importListener     *importListener          // Import feed listener
	sequences          *sequenceAllocator       // Source of new sequence numbers
	ChannelMapper      *channels.ChannelMapper  // Runs JS 'sync' function
	StartTime          time.Time                // Timestamp when context was instantiated
	RevsLimit          uint32                   // Max depth a document's revision tree can grow to
	autoImport         bool                     // Add sync data to new untracked couchbase server docs?  (Xattr mode specific)
	revisionCache      RevisionCache            // Cache of recently-accessed doc revisions
	changeCache        *changeCache             // Cache of recently-access channels
	EventMgr           *EventManager            // Manages notification events
	AllowEmptyPassword bool                     // Allow empty passwords?  Defaults to false
	Options            DatabaseContextOptions   // Database Context Options
	AccessLock         sync.RWMutex             // Allows DB offline to block until synchronous calls have completed
	State              uint32                   // The runtime state of the DB from a service perspective
	ExitChanges        chan struct{}            // Active _changes feeds on the DB will close when this channel is closed
	OIDCProviders      auth.OIDCProviderMap     // OIDC clients
	PurgeInterval      int                      // Metadata purge interval, in hours
	serverUUID         string                   // UUID of the server, if available
	DbStats            *DatabaseStats           // stats that correspond to this database context
	CompactState       uint32                   // Status of database compaction
	terminator         chan bool                // Signal termination of background goroutines
	activeChannels     *channels.ActiveChannels // Tracks active replications by channel
}

type DatabaseContextOptions struct {
	CacheOptions              *CacheOptions
	RevisionCacheOptions      *RevisionCacheOptions
	OldRevExpirySeconds       uint32
	AdminInterface            *string
	UnsupportedOptions        UnsupportedOptions
	OIDCOptions               *auth.OIDCOptions
	DBOnlineCallback          DBOnlineCallback // Callback function to take the DB back online
	ImportOptions             ImportOptions
	EnableXattr               bool             // Use xattr for _sync
	LocalDocExpirySecs        uint32           // The _local doc expiry time in seconds
	SessionCookieName         string           // Pass-through DbConfig.SessionCookieName
	AllowConflicts            *bool            // False forbids creating conflicts
	SendWWWAuthenticateHeader *bool            // False disables setting of 'WWW-Authenticate' header
	UseViews                  bool             // Force use of views
	DeltaSyncOptions          DeltaSyncOptions // Delta Sync Options
	CompactInterval           uint32           // Interval in seconds between compaction is automatically ran - 0 means don't run
}

type OidcTestProviderOptions struct {
	Enabled         bool `json:"enabled,omitempty"`           // Whether the oidc_test_provider endpoints should be exposed on the public API
	UnsignedIDToken bool `json:"unsigned_id_token,omitempty"` // Whether the internal test provider returns a signed ID token on a refresh request.  Used to simulate Azure behaviour
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

type UnsupportedOptions struct {
	UserViews                UserViewsOptions        `json:"user_views,omitempty"`                  // Config settings for user views
	OidcTestProvider         OidcTestProviderOptions `json:"oidc_test_provider,omitempty"`          // Config settings for OIDC Provider
	APIEndpoints             APIEndpoints            `json:"api_endpoints,omitempty"`               // Config settings for API endpoints
	WarningThresholds        WarningThresholds       `json:"warning_thresholds,omitempty"`          // Warning thresholds related to _sync size
	DisableCleanSkippedQuery bool                    `json:"disable_clean_skipped_query,omitempty"` // Clean skipped sequence processing bypasses final check
}

type WarningThresholds struct {
	XattrSize      *uint32 `json:"xattr_size_bytes,omitempty"`               // Number of bytes to be used as a threshold for xattr size limit warnings
	ChannelsPerDoc *uint32 `json:"channels_per_doc,omitempty"`               // Number of channels per document to be used as a threshold for channel count warnings
	GrantsPerDoc   *uint32 `json:"access_and_role_grants_per_doc,omitempty"` // Number of access and role grants per document to be used as a threshold for grant count warnings
}

// Options associated with the import of documents not written by Sync Gateway
type ImportOptions struct {
	ImportFilter *ImportFilterFunction // Opt-in filter for document import
	BackupOldRev bool                  // Create temporary backup of old revision body when available
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

// Helper function to open a Couchbase connection and return a specific bucket.
func ConnectToBucket(spec base.BucketSpec) (bucket base.Bucket, err error) {

	//start a retry loop to connect to the bucket backing off double the delay each time
	worker := func() (shouldRetry bool, err error, value interface{}) {
		bucket, err = base.GetBucket(spec)

		// By default, if there was an error, retry
		shouldRetry = err != nil

		if err == base.ErrFatalBucketConnection {
			base.Warnf(base.KeyAll, "Fatal error connecting to bucket: %v.  Not retrying", err)
			shouldRetry = false
		}

		return shouldRetry, err, bucket
	}

	sleeper := base.CreateDoublingSleeperFunc(
		13, //MaxNumRetries approx 40 seconds total retry duration
		5,  //InitialRetrySleepTimeMS
	)

	description := fmt.Sprintf("Attempt to connect to bucket : %v", spec.BucketName)
	err, ibucket := base.RetryLoop(description, worker, sleeper)
	if err != nil {
		return nil, base.HTTPErrorf(http.StatusBadGateway,
			" Unable to connect to Couchbase Server (connection refused). Please ensure it is running and reachable at the configured host and port.  Detailed error: %s", err)
	}

	return ibucket.(base.Bucket), nil
}

// Function type for something that calls NewDatabaseContext and wants a callback when the DB is detected
// to come back online. A rest.ServerContext package cannot be passed since it would introduce a circular dependency
type DBOnlineCallback func(dbContext *DatabaseContext)

// Creates a new DatabaseContext on a bucket. The bucket will be closed when this context closes.
func NewDatabaseContext(dbName string, bucket base.Bucket, autoImport bool, options DatabaseContextOptions) (*DatabaseContext, error) {

	if err := ValidateDatabaseName(dbName); err != nil {
		return nil, err
	}

	dbStats := NewDatabaseStats()

	base.PerDbStats.Set(dbName, dbStats.ExpvarMap())

	dbContext := &DatabaseContext{
		Name:       dbName,
		Bucket:     bucket,
		StartTime:  time.Now(),
		autoImport: autoImport,
		Options:    options,
		DbStats:    dbStats,
	}

	if dbContext.AllowConflicts() {
		dbContext.RevsLimit = DefaultRevsLimitConflicts
	} else {
		dbContext.RevsLimit = DefaultRevsLimitNoConflicts
	}

	dbContext.terminator = make(chan bool)

	dbContext.revisionCache = NewRevisionCache(
		dbContext.Options.RevisionCacheOptions,
		dbContext,
		dbContext.DbStats.StatsCache(),
	)

	dbContext.EventMgr = NewEventManager()

	var err error
	dbContext.sequences, err = newSequenceAllocator(bucket, dbStats.StatsDatabase())
	if err != nil {
		return nil, err
	}

	// In-memory channel cache
	dbContext.changeCache = &changeCache{}

	// Callback that is invoked whenever a set of channels is changed in the ChangeCache
	notifyChange := func(changedChannels base.Set) {
		dbContext.mutationListener.Notify(changedChannels)
	}

	// Initialize the active channel counter
	dbContext.activeChannels = channels.NewActiveChannels(dbStats.StatsCache().Get(base.StatKeyActiveChannels).(*expvar.Int))

	// Initialize the ChangeCache.  Will be locked and unusable until .Start() is called (SG #3558)
	dbContext.changeCache.Init(
		dbContext,
		notifyChange,
		options.CacheOptions,
	)

	// Set the DB Context notifyChange callback to call back the changecache DocChanged callback
	dbContext.SetOnChangeCallback(dbContext.changeCache.DocChanged)

	// Initialize the tap Listener for notify handling
	dbContext.mutationListener.Init(bucket.GetName())

	// If this is an xattr import node, start import feed
	if dbContext.UseXattrs() && dbContext.autoImport {
		dbContext.importListener = NewImportListener()
		if importFeedErr := dbContext.importListener.StartImportFeed(bucket, dbContext.DbStats, dbContext); importFeedErr != nil {
			return nil, importFeedErr
		}
	}

	// Start DCP feed
	base.Infof(base.KeyDCP, "Starting mutation feed on bucket %v due to either channel cache mode or doc tracking (auto-import)", base.MD(bucket.GetName()))
	cacheFeedStatsMap, ok := dbContext.DbStats.statsDatabaseMap.Get(base.StatKeyCachingDcpStats).(*expvar.Map)
	if !ok {
		return nil, errors.New("Cache feed stats map not initialized")
	}
	err = dbContext.mutationListener.Start(bucket, cacheFeedStatsMap)

	// Check if there is an error starting the DCP feed
	if err != nil {
		dbContext.changeCache = nil
		return nil, err
	}

	// Unlock change cache
	err = dbContext.changeCache.Start()
	if err != nil {
		return nil, err
	}

	// Load providers into provider map.  Does basic validation on the provider definition, and identifies the default provider.
	if options.OIDCOptions != nil {
		dbContext.OIDCProviders = make(auth.OIDCProviderMap)

		for name, provider := range options.OIDCOptions.Providers {
			if provider.Issuer == "" || provider.ClientID == nil {
				base.Warnf(base.KeyAll, "Issuer and ClientID required for OIDC Provider - skipping provider %q", base.UD(name))
				continue
			}

			if provider.ValidationKey == nil {
				base.Warnf(base.KeyAll, "Validation Key not defined in config for provider %q - auth code flow will not be supported for this provider", base.UD(name))
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

			// If this isn't the default provider, add the provider to the callback URL (needed to identify provider to _oidc_callback)
			if !provider.IsDefault && provider.CallbackURL != nil {
				var updatedCallback string
				if strings.Contains(*provider.CallbackURL, "?") {
					updatedCallback = fmt.Sprintf("%s&provider=%s", *provider.CallbackURL, name)
				} else {
					updatedCallback = fmt.Sprintf("%s?provider=%s", *provider.CallbackURL, name)
				}
				provider.CallbackURL = &updatedCallback
			}

			dbContext.OIDCProviders[name] = provider
		}
		if len(dbContext.OIDCProviders) == 0 {
			return nil, errors.New("OpenID Connect defined in config, but no valid OpenID Connect providers specified.")
		}

	}

	if dbContext.UseXattrs() {
		// Set the purge interval for tombstone compaction
		dbContext.PurgeInterval = DefaultPurgeInterval
		gocbBucket, ok := base.AsGoCBBucket(bucket)
		if ok {
			serverPurgeInterval, err := gocbBucket.GetMetadataPurgeInterval()
			if err != nil {
				base.Warnf(base.KeyAll, "Unable to retrieve server's metadata purge interval - will use default value. %s", err)
			} else if serverPurgeInterval > 0 {
				dbContext.PurgeInterval = serverPurgeInterval
			}
		}
		base.Infof(base.KeyAll, "Using metadata purge interval of %.2f days for tombstone compaction.", float64(dbContext.PurgeInterval)/24)

		if dbContext.Options.CompactInterval != 0 {
			if autoImport {
				db := Database{DatabaseContext: dbContext}
				NewBackgroundTask("Compact", dbContext.Name, func(ctx context.Context) error {
					_, err := db.Compact()
					if err != nil {
						base.WarnfCtx(ctx, base.KeyAll, "Error trying to compact tombstoned documents for %q with error: %v", dbContext.Name, err)
					}
					return nil
				}, time.Duration(dbContext.Options.CompactInterval)*time.Second, dbContext.terminator)
			} else {
				base.Warnf(base.KeyAll, "Automatic compaction can only be enabled on nodes running an Import process")
			}
		}

	}

	// Make sure there is no MaxTTL set on the bucket (SG #3314)
	gocbBucket, ok := base.AsGoCBBucket(bucket)
	if ok {
		maxTTL, err := gocbBucket.GetMaxTTL()

		if err != nil {
			return nil, err
		}
		if maxTTL != 0 {
			return nil, fmt.Errorf("Backing Couchbase Server bucket has a non-zero MaxTTL value: %d.  Please set MaxTTL to 0 in Couchbase Server Admin UI and try again.", maxTTL)
		}
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
		return nil, base.RedactErrorf("No provider found for provider name %q", base.UD(providerName))
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

func (context *DatabaseContext) GetServerUUID() string {

	context.BucketLock.RLock()
	defer context.BucketLock.RUnlock()

	// Lazy load the server UUID, if we can get it.
	if context.serverUUID == "" {
		b, ok := base.AsGoCBBucket(context.Bucket)
		if !ok {
			base.Warnf(base.KeyAll, "Database %v: Unable to get server UUID. Bucket was type: %T, not GoCBBucket.", base.MD(context.Name), context.Bucket)
			return ""
		}

		uuid, err := b.GetServerUUID()
		if err != nil {
			base.Warnf(base.KeyAll, "Database %v: Unable to get server UUID: %v", base.MD(context.Name), err)
			return ""
		}

		base.Debugf(base.KeyAll, "Database %v: Got server UUID %v", base.MD(context.Name), base.MD(uuid))
		context.serverUUID = uuid
	}

	return context.serverUUID
}

// Utility function to support cache testing from outside db package
func (context *DatabaseContext) GetChangeCache() *changeCache {
	return context.changeCache
}

func (context *DatabaseContext) Close() {
	context.BucketLock.Lock()
	defer context.BucketLock.Unlock()

	close(context.terminator)
	context.sequences.Stop()
	context.mutationListener.Stop()
	context.changeCache.Stop()
	context.importListener.Stop()
	context.Bucket.Close()
	context.Bucket = nil

	base.RemovePerDbStats(context.Name)

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
	context.mutationListener.Init(context.Bucket.GetName())
	if err := context.mutationListener.Start(context.Bucket, context.DbStats.statsDatabaseMap); err != nil {
		return err
	}
	return nil
}

// Cache flush support.  Currently test-only - added for unit test access from rest package
func (context *DatabaseContext) FlushChannelCache() error {
	base.Infof(base.KeyCache, "Flushing channel cache")
	return context.changeCache.Clear()
}

// Removes previous versions of Sync Gateway's design docs found on the server
func (context *DatabaseContext) RemoveObsoleteDesignDocs(previewOnly bool) (removedDesignDocs []string, err error) {
	return removeObsoleteDesignDocs(context.Bucket, previewOnly)
}

// Removes previous versions of Sync Gateway's design docs found on the server
func (context *DatabaseContext) RemoveObsoleteIndexes(previewOnly bool) (removedIndexes []string, err error) {

	gocbBucket, ok := base.AsGoCBBucket(context.Bucket)
	if !ok {
		base.Warnf(base.KeyAll, "Cannot remove obsolete indexes for non-gocb bucket - skipping.")
		return make([]string, 0), nil
	}

	return removeObsoleteIndexes(gocbBucket, previewOnly, context.UseXattrs())
}

// Trigger terminate check handling for connected continuous replications.
// TODO: The underlying code (NotifyCheckForTermination) doesn't actually leverage the specific username - should be refactored
//    to remove
func (context *DatabaseContext) NotifyTerminatedChanges(username string) {
	context.mutationListener.NotifyCheckForTermination(base.SetOf(base.UserPrefix + username))
}

func (dc *DatabaseContext) TakeDbOffline(reason string) error {

	dbState := atomic.LoadUint32(&dc.State)

	//If the DB is already trasitioning to: offline or is offline silently return
	if dbState == DBOffline || dbState == DBResyncing || dbState == DBStopping {
		return nil
	}

	if atomic.CompareAndSwapUint32(&dc.State, DBOnline, DBStopping) {

		//notify all active _changes feeds to close
		close(dc.ExitChanges)

		//Block until all current calls have returned, including _changes feeds
		dc.AccessLock.Lock()
		defer dc.AccessLock.Unlock()

		//set DB state to Offline
		atomic.StoreUint32(&dc.State, DBOffline)

		if dc.EventMgr.HasHandlerForEvent(DBStateChange) {
			dc.EventMgr.RaiseDBStateChangeEvent(dc.Name, "offline", reason, *dc.Options.AdminInterface)
		}

		return nil
	} else {
		msg := "Unable to take Database offline, database must be in Online state"
		base.Infof(base.KeyCRUD, msg)
		return base.HTTPErrorf(http.StatusServiceUnavailable, msg)
	}
}

func (context *DatabaseContext) Authenticator() *auth.Authenticator {
	context.BucketLock.RLock()
	defer context.BucketLock.RUnlock()

	// Authenticators are lightweight & stateless, so it's OK to return a new one every time
	authenticator := auth.NewAuthenticator(context.Bucket, context)
	if context.Options.SessionCookieName != "" {
		authenticator.SetSessionCookieName(context.Options.SessionCookieName)
	}
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

// Reloads the database's User object, in case its persistent properties have been changed.
func (db *Database) ReloadUser() error {
	if db.user == nil {
		return nil
	}
	user, err := db.Authenticator().GetUser(db.user.Name())
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

//////// ALL DOCUMENTS:

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

	results, err := db.QueryAllDocs(resultsOpts.Startkey, resultsOpts.Endkey)
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
		//We have to apply limit check after callback has been called
		//to account for rows that are not in the current users channels
		if limit > 0 && count == limit {
			break
		}

	}
	return nil
}

type principalsViewRow struct {
	Key   string // principal name
	Value bool   // 'isUser' flag
}

// Returns the IDs of all users and roles
func (db *DatabaseContext) AllPrincipalIDs() (users, roles []string, err error) {

	results, err := db.QueryPrincipals()
	if err != nil {
		return nil, nil, err
	}

	var isUser bool
	var principalName string
	users = []string{}
	roles = []string{}
	lenUserKeyPrefix := len(base.UserPrefix)
	for {
		if db.Options.UseViews {
			var viewRow principalsViewRow
			found := results.Next(&viewRow)
			if !found {
				break
			}
			isUser = viewRow.Value
			principalName = viewRow.Key
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
		}

		if principalName != "" {
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

	return users, roles, nil
}

//////// HOUSEKEEPING:

// Deletes all session documents for a user
func (db *DatabaseContext) DeleteUserSessions(userName string) error {

	results, err := db.QuerySessions(userName)
	if err != nil {
		return err
	}

	var sessionsRow QueryIdRow
	for results.Next(&sessionsRow) {
		base.Infof(base.KeyCRUD, "\tDeleting %q", sessionsRow.Id)
		if err := db.Bucket.Delete(sessionsRow.Id); err != nil {
			base.Warnf(base.KeyAll, "Error deleting %q: %v", sessionsRow.Id, err)
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
func (db *Database) Compact() (int, error) {
	if !atomic.CompareAndSwapUint32(&db.CompactState, DBCompactNotRunning, DBCompactRunning) {
		return 0, base.HTTPErrorf(http.StatusServiceUnavailable, "Compaction already running")
	}

	defer atomic.CompareAndSwapUint32(&db.CompactState, DBCompactRunning, DBCompactNotRunning)

	// Compact should be a no-op if not running w/ xattrs
	if !db.UseXattrs() {
		return 0, nil
	}

	// Trigger view compaction for all tombstoned documents older than the purge interval
	purgeIntervalDuration := time.Duration(-db.PurgeInterval) * time.Hour
	startTime := time.Now()
	purgeOlderThan := startTime.Add(purgeIntervalDuration)

	purgedDocCount := 0

	ctx := db.Ctx

	base.InfofCtx(ctx, base.KeyAll, "Starting compaction of purged tombstones for %s ...", base.MD(db.Name))
	purgeBody := Body{"_purged": true}
	for {
		purgedDocs := make([]string, 0)
		results, err := db.QueryTombstones(purgeOlderThan, QueryTombstoneBatch, gocb.RequestPlus)
		if err != nil {
			return 0, err
		}
		var tombstonesRow QueryIdRow
		var resultCount int
		for results.Next(&tombstonesRow) {
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
					base.WarnfCtx(ctx, base.KeyAll, "Error compacting key %s (add) - tombstone will not be compacted.  %v", base.UD(tombstonesRow.Id), addErr)
					continue
				}

				// At this point, the doc is not in a usable state for mobile
				// so mark it to be removed from cache, even if the subsequent delete fails
				purgedDocs = append(purgedDocs, tombstonesRow.Id)

				if delErr := db.Bucket.Delete(tombstonesRow.Id); delErr != nil {
					base.ErrorfCtx(ctx, base.KeyAll, "Error compacting key %s (delete) - tombstone will not be compacted.  %v", base.UD(tombstonesRow.Id), delErr)
				}
			} else {
				base.WarnfCtx(ctx, base.KeyAll, "Error compacting key %s (purge) - tombstone will not be compacted.  %v", base.UD(tombstonesRow.Id), purgeErr)
			}
		}

		// Now purge them from all channel caches
		count := len(purgedDocs)
		purgedDocCount += count
		if count > 0 {
			db.changeCache.Remove(purgedDocs, startTime)
			db.DbStats.StatsDatabase().Add(base.StatKeyNumTombstonesCompacted, int64(count))
		}
		base.DebugfCtx(ctx, base.KeyAll, "Compacted %v tombstones", count)

		if resultCount < QueryTombstoneBatch {
			break
		}
	}

	base.InfofCtx(ctx, base.KeyAll, "Finished compaction of purged tombstones for %s... Total Tombstones Compacted: %d", base.MD(db.Name), purgedDocCount)

	return purgedDocCount, nil
}

// Deletes all orphaned CouchDB attachments not used by any revisions.
func VacuumAttachments(bucket base.Bucket) (int, error) {
	return 0, base.HTTPErrorf(http.StatusNotImplemented, "Vacuum is temporarily out of order")
}

//////// SYNC FUNCTION:

// Sets the database context's sync function based on the JS code from config.
// Returns a boolean indicating whether the function is different from the saved one.
// If multiple gateway instances try to update the function at the same time (to the same new
// value) only one of them will get a changed=true result.
func (context *DatabaseContext) UpdateSyncFun(syncFun string) (changed bool, err error) {
	if syncFun == "" {
		context.ChannelMapper = nil
	} else if context.ChannelMapper != nil {
		_, err = context.ChannelMapper.SetFunction(syncFun)
	} else {
		context.ChannelMapper = channels.NewChannelMapper(syncFun)
	}
	if err != nil {
		base.Warnf(base.KeyAll, "Error setting sync function: %s", err)
		return
	}

	var syncData struct { // format of the sync-fn document
		Sync string
	}

	_, err = context.Bucket.Update(base.SyncDataKey, 0, func(currentValue []byte) ([]byte, *uint32, error) {
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
			return bytes, nil, err
		} else {
			return nil, nil, base.ErrUpdateCancel // value unchanged, no need to save
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
func (db *Database) UpdateAllDocChannels() (int, error) {

	base.Infof(base.KeyAll, "Recomputing document channels...")

	results, err := db.QueryResync()
	if err != nil {
		return 0, err
	}

	// We are about to alter documents without updating their sequence numbers, which would
	// really confuse the changeCache, so turn it off until we're done:
	db.changeCache.EnableChannelIndexing(false)
	defer db.changeCache.EnableChannelIndexing(true)

	err = db.changeCache.Clear()
	if err != nil {
		return 0, err
	}

	base.Infof(base.KeyAll, "Re-running sync function on all documents...")
	changeCount := 0
	docCount := 0

	var importRow QueryIdRow
	for results.Next(&importRow) {
		docid := importRow.Id
		key := realDocID(docid)

		docCount++
		documentUpdateFunc := func(doc *Document) (updatedDoc *Document, shouldUpdate bool, updatedExpiry *uint32, err error) {
			imported := false
			if !doc.HasValidSyncData() {
				// This is a document not known to the sync gateway. Ignore it:
				return nil, false, nil, base.ErrUpdateCancel
			} else {
				base.Debugf(base.KeyCRUD, "\tRe-syncing document %q", base.UD(docid))
			}

			// Run the sync fn over each current/leaf revision, in case there are conflicts:
			changed := 0
			doc.History.forEachLeaf(func(rev *RevInfo) {
				bodyBytes, _, err := db.get1xRevFromDoc(doc, rev.ID, false)
				if err != nil {
					base.Warnf(base.KeyAll, "Error getting rev from doc %s/%s %s", base.UD(docid), rev.ID, err)
				}
				var body Body
				if err := body.Unmarshal(bodyBytes); err != nil {
					base.Warnf(base.KeyAll, "Error unmarshalling body %s/%s for sync function %s", base.UD(docid), rev.ID, err)
					return
				}
				channels, access, roles, syncExpiry, _, err := db.getChannelsAndAccess(doc, body, rev.ID)
				if err != nil {
					// Probably the validator rejected the doc
					base.Warnf(base.KeyAll, "Error calling sync() on doc %q: %v", base.UD(docid), err)
					access = nil
					channels = nil
				}
				rev.Channels = channels

				if rev.ID == doc.CurrentRev {
					changedChannels, err := doc.updateChannels(channels)
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
			shouldUpdate = changed > 0 || imported
			return doc, shouldUpdate, updatedExpiry, nil
		}
		var err error
		if db.UseXattrs() {
			writeUpdateFunc := func(currentValue []byte, currentXattr []byte, cas uint64) (
				raw []byte, rawXattr []byte, deleteDoc bool, expiry *uint32, err error) {
				// There's no scenario where a doc should from non-deleted to deleted during UpdateAllDocChannels processing,
				// so deleteDoc is always returned as false.
				if currentValue == nil || len(currentValue) == 0 {
					return nil, nil, deleteDoc, nil, base.ErrUpdateCancel
				}
				doc, err := unmarshalDocumentWithXattr(docid, currentValue, currentXattr, cas, DocUnmarshalAll)
				if err != nil {
					return nil, nil, deleteDoc, nil, err
				}

				updatedDoc, shouldUpdate, updatedExpiry, err := documentUpdateFunc(doc)
				if err != nil {
					return nil, nil, deleteDoc, nil, err
				}
				if shouldUpdate {
					base.Infof(base.KeyAccess, "Saving updated channels and access grants of %q", base.UD(docid))
					if updatedExpiry != nil {
						updatedDoc.UpdateExpiry(*updatedExpiry)
					}
					raw, rawXattr, err = updatedDoc.MarshalWithXattr()
					return raw, rawXattr, deleteDoc, updatedExpiry, err
				} else {
					return nil, nil, deleteDoc, nil, base.ErrUpdateCancel
				}
			}
			_, err = db.Bucket.WriteUpdateWithXattr(key, base.SyncXattrName, 0, nil, writeUpdateFunc)
		} else {
			_, err = db.Bucket.Update(key, 0, func(currentValue []byte) ([]byte, *uint32, error) {
				// Be careful: this block can be invoked multiple times if there are races!
				if currentValue == nil {
					return nil, nil, base.ErrUpdateCancel // someone deleted it?!
				}
				doc, err := unmarshalDocument(docid, currentValue)
				if err != nil {
					return nil, nil, err
				}
				updatedDoc, shouldUpdate, updatedExpiry, err := documentUpdateFunc(doc)
				if err != nil {
					return nil, nil, err
				}
				if shouldUpdate {
					base.Infof(base.KeyAccess, "Saving updated channels and access grants of %q", base.UD(docid))
					if updatedExpiry != nil {
						updatedDoc.UpdateExpiry(*updatedExpiry)
					}
					updatedBytes, marshalErr := base.JSONMarshal(updatedDoc)
					return updatedBytes, updatedExpiry, marshalErr
				} else {
					return nil, nil, base.ErrUpdateCancel
				}
			})
		}
		if err == nil {
			changeCount++
		} else if err != base.ErrUpdateCancel {
			base.Warnf(base.KeyAll, "Error updating doc %q: %v", base.UD(docid), err)
		}
	}

	// Close query results
	closeErr := results.Close()
	if closeErr != nil {
		return 0, closeErr
	}

	base.Infof(base.KeyAll, "Finished re-running sync function; %d/%d docs changed", changeCount, docCount)

	if changeCount > 0 {
		// Now invalidate channel cache of all users/roles:
		base.Infof(base.KeyAll, "Invalidating channel caches of users/roles...")
		users, roles, _ := db.AllPrincipalIDs()
		for _, name := range users {
			db.invalUserChannels(name)
		}
		for _, name := range roles {
			db.invalRoleChannels(name)
		}
	}
	return changeCount, nil
}

func (db *Database) invalUserRoles(username string) {
	authr := db.Authenticator()
	if user, _ := authr.GetUser(username); user != nil {
		if err := authr.InvalidateRoles(user); err != nil {
			base.Warnf(base.KeyAll, "Error invalidating roles for user %s: %v", base.UD(username), err)
		}
	}
}

func (db *Database) invalUserChannels(username string) {
	authr := db.Authenticator()
	if user, _ := authr.GetUser(username); user != nil {
		if err := authr.InvalidateChannels(user); err != nil {
			base.Warnf(base.KeyAll, "Error invalidating channels for user %s: %v", base.UD(username), err)
		}
	}
}

func (db *Database) invalRoleChannels(rolename string) {
	authr := db.Authenticator()
	if role, _ := authr.GetRole(rolename); role != nil {
		if err := authr.InvalidateChannels(role); err != nil {
			base.Warnf(base.KeyAll, "Error invalidating channels for role %s: %v", base.UD(rolename), err)
		}
	}
}

func (db *Database) invalUserOrRoleChannels(name string) {

	principalName, isRole := channels.AccessNameToPrincipalName(name)
	if isRole {
		db.invalRoleChannels(principalName)
	} else {
		db.invalUserChannels(principalName)
	}
}

func (context *DatabaseContext) GetUserViewsEnabled() bool {
	if context.Options.UnsupportedOptions.UserViews.Enabled != nil {
		return *context.Options.UnsupportedOptions.UserViews.Enabled
	}
	return false
}

func (context *DatabaseContext) UseXattrs() bool {
	return context.Options.EnableXattr
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

	context.Options.UnsupportedOptions.UserViews.Enabled = &value
}

// For test usage
func (context *DatabaseContext) FlushRevisionCacheForTest() {

	context.revisionCache = NewRevisionCache(
		context.Options.RevisionCacheOptions,
		context,
		context.DbStats.StatsCache(),
	)

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
	return context.Options.UnsupportedOptions.APIEndpoints.EnableCouchbaseBucketFlush
}

//////// SEQUENCE ALLOCATION:

func (context *DatabaseContext) LastSequence() (uint64, error) {
	return context.sequences.lastSequence()
}
