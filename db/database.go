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
	"encoding/json"
	"errors"
	"expvar"
	"fmt"
	"net/http"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/couchbase/go-couchbase"
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
	DefaultRevsLimit     = 1000
	DefaultPurgeInterval = 30               // Default metadata purge interval, in days.  Used if server's purge interval is unavailable
	KSyncKeyPrefix       = "_sync:"         // All special/internal documents the gateway creates have this prefix in their keys.
	kSyncDataKey         = "_sync:syncdata" // Key used to store sync function
	KSyncXattrName       = "_sync"          // Name of XATTR used to store sync metadata
)

// Basic description of a database. Shared between all Database objects on the same database.
// This object is thread-safe so it can be shared between HTTP handlers.
type DatabaseContext struct {
	Name               string                  // Database name
	Bucket             base.Bucket             // Storage
	BucketSpec         base.BucketSpec         // The BucketSpec
	BucketLock         sync.RWMutex            // Control Access to the underlying bucket object
	mutationListener   changeListener          // Listens on server mutation feed (TAP or DCP)
	sequences          *sequenceAllocator      // Source of new sequence numbers
	ChannelMapper      *channels.ChannelMapper // Runs JS 'sync' function
	StartTime          time.Time               // Timestamp when context was instantiated
	ChangesClientStats Statistics              // Tracks stats of # of changes connections
	RevsLimit          uint32                  // Max depth a document's revision tree can grow to
	autoImport         bool                    // Add sync data to new untracked couchbase server docs?  (Xattr mode specific)
	Shadower           *Shadower               // Tracks an external Couchbase bucket
	revisionCache      *RevisionCache          // Cache of recently-accessed doc revisions
	changeCache        ChangeIndex             //
	EventMgr           *EventManager           // Manages notification events
	AllowEmptyPassword bool                    // Allow empty passwords?  Defaults to false
	SequenceHasher     *sequenceHasher         // Used to generate and resolve hash values for vector clock sequences
	SequenceType       SequenceType            // Type of sequences used for this DB (integer or vector clock)
	Options            DatabaseContextOptions  // Database Context Options
	AccessLock         sync.RWMutex            // Allows DB offline to block until synchronous calls have completed
	State              uint32                  // The runtime state of the DB from a service perspective
	ExitChanges        chan struct{}           // Active _changes feeds on the DB will close when this channel is closed
	OIDCProviders      auth.OIDCProviderMap    // OIDC clients
	PurgeInterval      int                     // Metadata purge interval, in hours
}

type DatabaseContextOptions struct {
	CacheOptions              *CacheOptions
	IndexOptions              *ChannelIndexOptions
	SequenceHashOptions       *SequenceHashOptions
	RevisionCacheCapacity     uint32
	OldRevExpirySeconds       uint32
	AdminInterface            *string
	UnsupportedOptions        UnsupportedOptions
	TrackDocs                 bool // Whether doc tracking channel should be created (used for autoImport, shadowing)
	OIDCOptions               *auth.OIDCOptions
	DBOnlineCallback          DBOnlineCallback // Callback function to take the DB back online
	ImportOptions             ImportOptions
	EnableXattr               bool   // Use xattr for _sync
	LocalDocExpirySecs        uint32 // The _local doc expiry time in seconds
	SessionCookieName         string // Pass-through DbConfig.SessionCookieName
	AllowConflicts            *bool  // False forbids creating conflicts
	SendWWWAuthenticateHeader *bool  // False disables setting of 'WWW-Authenticate' header
	UseViews                  bool   // Force use of views
}

type OidcTestProviderOptions struct {
	Enabled         bool `json:"enabled,omitempty"`           // Whether the oidc_test_provider endpoints should be exposed on the public API
	UnsignedIDToken bool `json:"unsigned_id_token,omitempty"` // Whether the internal test provider returns a signed ID token on a refresh request.  Used to simulate Azure behaviour
}

type UserViewsOptions struct {
	Enabled *bool `json:"enabled,omitempty"` // Whether pass-through view query is supported through public API
}

type APIEndpoints struct {

	// This setting is only needed for testing purposes.  In the Couchbase Lite unit tests that run in "integration mode"
	// against a running Sync Gateway, the tests need to be able to flush the data in between tests to start with a clean DB.
	EnableCouchbaseBucketFlush bool `json:"enable_couchbase_bucket_flush,omitempty"` // Whether Couchbase buckets can be flushed via Admin REST API
}

type UnsupportedOptions struct {
	UserViews         UserViewsOptions        `json:"user_views,omitempty"`          // Config settings for user views
	OidcTestProvider  OidcTestProviderOptions `json:"oidc_test_provider,omitempty"`  // Config settings for OIDC Provider
	APIEndpoints      APIEndpoints            `json:"api_endpoints,omitempty"`       // Config settings for API endpoints
	LeakyBucketConfig *base.LeakyBucketConfig  `json:"leaky_bucket_config,omitempty"` // Config settings for leaky bucket
}

// Options associated with the import of documents not written by Sync Gateway
type ImportOptions struct {
	ImportFilter *ImportFilterFunction // Opt-in filter for document import
}

// Represents a simulated CouchDB database. A new instance is created for each HTTP request,
// so this struct does not have to be thread-safe.
type Database struct {
	*DatabaseContext
	user auth.User
}

var dbExpvars = expvar.NewMap("syncGateway_db")

func ValidateDatabaseName(dbName string) error {
	// http://wiki.apache.org/couchdb/HTTP_database_API#Naming_and_Addressing
	if match, _ := regexp.MatchString(`^[a-z][-a-z0-9_$()+/]*$`, dbName); !match {
		return base.HTTPErrorf(http.StatusBadRequest,
			"Illegal database name: %s", dbName)
	}
	return nil
}

// Helper function to open a Couchbase connection and return a specific bucket.
func ConnectToBucket(spec base.BucketSpec, callback sgbucket.BucketNotifyFn) (bucket base.Bucket, err error) {

	//start a retry loop to connect to the bucket backing off double the delay each time
	worker := func() (shouldRetry bool, err error, value interface{}) {
		bucket, err = base.GetBucket(spec, callback)

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

	context := &DatabaseContext{
		Name:       dbName,
		Bucket:     bucket,
		StartTime:  time.Now(),
		RevsLimit:  DefaultRevsLimit,
		autoImport: autoImport,
		Options:    options,
	}
	context.revisionCache = NewRevisionCache(options.RevisionCacheCapacity, context.revCacheLoader)

	context.EventMgr = NewEventManager()

	var err error
	context.sequences, err = newSequenceAllocator(bucket)
	if err != nil {
		return nil, err
	}

	if options.IndexOptions == nil {
		// In-memory channel cache
		context.SequenceType = IntSequenceType
		context.changeCache = &changeCache{}
	} else {
		// KV channel index
		context.SequenceType = ClockSequenceType
		context.changeCache = &kvChangeIndex{}
		context.SequenceHasher, err = NewSequenceHasher(options.SequenceHashOptions)
		if err != nil {
			return nil, err
		}
	}

	// Callback that is invoked whenever a set of channels is changed in the ChangeCache
	notifyChange := func(changedChannels base.Set) {
		context.mutationListener.Notify(changedChannels)
	}

	// Initialize the ChangeCache.  Will be locked and unusable until .Start() is called (SG #3558)
	context.changeCache.Init(
		context,
		notifyChange,
		options.CacheOptions,
		options.IndexOptions,
	)

	// Set the DB Context notifyChange callback to call back the changecache DocChanged callback
	context.SetOnChangeCallback(context.changeCache.DocChanged)

	// Initialize the tap Listener for notify handling
	context.mutationListener.Init(bucket.GetName())

	// If this is an xattr import node, resume DCP feed where we left off.  Otherwise only listen for new changes (FeedNoBackfill)
	feedMode := uint64(sgbucket.FeedNoBackfill)
	if context.UseXattrs() && context.autoImport {
		feedMode = sgbucket.FeedResume
	}

	// If not using channel index or using channel index and tracking docs, start the tap feed
	if options.IndexOptions == nil || options.TrackDocs {
		base.Infof(base.KeyDCP, "Starting mutation feed on bucket %v due to either channel cache mode or doc tracking (auto-import/bucketshadow)", base.MD(bucket.GetName()))

		err = context.mutationListener.Start(bucket, options.TrackDocs, feedMode, func(bucket string, err error) {

			msgFormat := "%v dropped Mutation Feed (TAP/DCP) due to error: %v, taking offline"
			base.Warnf(base.KeyAll, msgFormat, base.UD(bucket), err)
			errTakeDbOffline := context.TakeDbOffline(fmt.Sprintf(msgFormat, bucket, err))
			if errTakeDbOffline == nil {

				//start a retry loop to pick up tap feed again backing off double the delay each time
				worker := func() (shouldRetry bool, err error, value interface{}) {
					//If DB is going online via an admin request Bucket will be nil
					if context.Bucket != nil {
						err = context.Bucket.Refresh()
					} else {
						err = base.HTTPErrorf(http.StatusPreconditionFailed, "Database %q, bucket is not available", dbName)
						return false, err, nil
					}
					return err != nil, err, nil
				}

				sleeper := base.CreateDoublingSleeperFunc(
					20, //MaxNumRetries
					5,  //InitialRetrySleepTimeMS
				)

				description := fmt.Sprintf("Attempt reconnect to lost Mutation (TAP/DCP) Feed for : %v", context.Name)
				err, _ := base.RetryLoop(description, worker, sleeper)

				if err == nil {
					base.Infof(base.KeyCRUD, "Connection to Mutation (TAP/DCP) feed for %v re-established, bringing DB back online", base.UD(context.Name))

					// The 10 second wait was introduced because the bucket was not fully initialised
					// after the return of the retry loop.
					timer := time.NewTimer(time.Duration(10) * time.Second)
					<-timer.C

					if options.DBOnlineCallback != nil {
						options.DBOnlineCallback(context)
					}
				}
			}

			// If errTakeDbOffline is non-nil, it can be safely ignored because:
			// - The only known error state for context.TakeDbOffline is if the current db state wasn't online
			// - That code would hit an error if the dropped tap feed triggered TakeDbOffline, but the db was already non-online
			// - In that case (some other event, potentially an admin action, took the DB offline), and so there is no reason to do the auto-reconnect processing

			// TODO: invoke the same callback function from there as well, to pick up the auto-online handling

		})

		// Check if there is an error starting the DCP feed
		if err != nil {
			context.changeCache = nil
			return nil, err
		}

		// Unlock change cache
		err := context.changeCache.Start()
		if err != nil {
			return nil, err
		}

	}

	// Load providers into provider map.  Does basic validation on the provider definition, and identifies the default provider.
	if options.OIDCOptions != nil {
		context.OIDCProviders = make(auth.OIDCProviderMap)

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
			if _, ok := context.OIDCProviders[provider.Issuer]; ok {
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

			context.OIDCProviders[name] = provider
		}
		if len(context.OIDCProviders) == 0 {
			return nil, errors.New("OpenID Connect defined in config, but no valid OpenID Connect providers specified.")
		}

	}

	// watchDocChanges is used for bucket shadowing
	if !context.UseXattrs() {
		go context.watchDocChanges()
	} else {
		// Set the purge interval for tombstone compaction
		context.PurgeInterval = DefaultPurgeInterval
		gocbBucket, ok := base.AsGoCBBucket(bucket)
		if ok {
			serverPurgeInterval, err := gocbBucket.GetMetadataPurgeInterval()
			if err != nil {
				base.Warnf(base.KeyAll, "Unable to retrieve server's metadata purge interval - will use default value. %s", err)
			} else if serverPurgeInterval > 0 {
				context.PurgeInterval = serverPurgeInterval
			}
		}
		base.Infof(base.KeyAll, "Using metadata purge interval of %.2f days for tombstone compaction.", float64(context.PurgeInterval)/24)

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

	return context, nil
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
	since := SequenceID{}
	since.SeqType = context.SequenceType
	since.SequenceHasher = context.SequenceHasher
	if context.SequenceType == ClockSequenceType {
		since.Clock = base.NewSequenceClockImpl()
	}
	return since
}

func (context *DatabaseContext) SetOnChangeCallback(callback DocChangedFunc) {
	context.mutationListener.OnDocChanged = callback
}

func (context *DatabaseContext) GetStableClock() (clock base.SequenceClock, err error) {
	staleOk := false
	return context.changeCache.GetStableClock(staleOk)
}

// Utility function to support cache testing from outside db package
func (context *DatabaseContext) GetChangeIndex() ChangeIndex {
	return context.changeCache
}

func (context *DatabaseContext) writeSequences() bool {
	return context.UseGlobalSequence()
}

func (context *DatabaseContext) UseGlobalSequence() bool {
	return context.SequenceType != ClockSequenceType
}

func (context *DatabaseContext) TapListener() changeListener {
	return context.mutationListener
}

func (context *DatabaseContext) Close() {
	context.BucketLock.Lock()
	defer context.BucketLock.Unlock()

	context.mutationListener.Stop()
	context.changeCache.Stop()
	context.Shadower.Stop()
	context.Bucket.Close()
	context.Bucket = nil
}

func (context *DatabaseContext) IsClosed() bool {
	context.BucketLock.Lock()
	defer context.BucketLock.Unlock()
	return context.Bucket == nil
}

// For testing only!
func (context *DatabaseContext) RestartListener() error {
	context.mutationListener.Stop()
	// Delay needed to properly stop
	time.Sleep(2 * time.Second)
	context.mutationListener.Init(context.Bucket.GetName())
	feedMode := uint64(sgbucket.FeedNoBackfill)
	if context.UseXattrs() && context.autoImport {
		feedMode = sgbucket.FeedResume
	}

	if err := context.mutationListener.Start(context.Bucket, context.Options.TrackDocs, feedMode, nil); err != nil {
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
	return removeObsoleteIndexes(context.Bucket, previewOnly, context.UseXattrs())
}

// Trigger terminate check handling for connected continuous replications.
// TODO: The underlying code (NotifyCheckForTermination) doesn't actually leverage the specific username - should be refactored
//    to remove
func (context *DatabaseContext) NotifyTerminatedChanges(username string) {
	context.mutationListener.NotifyCheckForTermination(base.SetOf(auth.UserKeyPrefix + username))
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
	// Authenticators are lightweight & stateless, so it's OK to return a new one every time
	authenticator := auth.NewAuthenticator(context.Bucket, context)
	if context.Options.SessionCookieName != "" {
		authenticator.SetSessionCookieName(context.Options.SessionCookieName)
	}
	return authenticator
}

// Makes a Database object given its name and bucket.
func GetDatabase(context *DatabaseContext, user auth.User) (*Database, error) {
	return &Database{context, user}, nil
}

func CreateDatabase(context *DatabaseContext) (*Database, error) {
	return &Database{context, nil}, nil
}

func (db *Database) SameAs(otherdb *Database) bool {
	return db != nil && otherdb != nil &&
		db.Bucket == otherdb.Bucket
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

type IDAndRev struct {
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

type ForEachDocIDFunc func(id IDAndRev, channels []string) bool

// Iterates over all documents in the database, calling the callback function on each
func (db *Database) ForEachDocID(callback ForEachDocIDFunc, resultsOpts ForEachDocIDOptions) error {

	results, err := db.QueryAllDocs(resultsOpts.Startkey, resultsOpts.Endkey)
	if err != nil {
		return err
	}

	db.processForEachDocIDResults(callback, resultsOpts.Limit, results)
	return results.Close()
}

// Iterate over the results of an AllDocs query, performing ForEachDocID handling for each row
func (db *Database) processForEachDocIDResults(callback ForEachDocIDFunc, limit uint64, results sgbucket.QueryResultIterator) {

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

		if callback(IDAndRev{docid, revid, seq}, channels) {
			count++
		}
		//We have to apply limit check after callback has been called
		//to account for rows that are not in the current users channels
		if limit > 0 && count == limit {
			break
		}

	}
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
	lenUserKeyPrefix := len(auth.UserKeyPrefix)
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
			isUser = queryRow.Id[0:lenUserKeyPrefix] == auth.UserKeyPrefix
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

	// Compact should be a no-op if not running w/ xattrs
	if !db.UseXattrs() {
		return 0, nil
	}

	// Trigger view compaction for all tombstoned documents older than the purge interval
	purgeIntervalDuration := time.Duration(-db.PurgeInterval) * time.Hour
	purgeOlderThan := time.Now().Add(purgeIntervalDuration)
	results, err := db.QueryTombstones(purgeOlderThan)
	if err != nil {
		return 0, err
	}

	base.Infof(base.KeyAll, "Compacting purged tombstones for %s ...", base.UD(db.Name))
	purgeBody := Body{"_purged": true}
	count := 0

	var tombstonesRow QueryIdRow
	for results.Next(&tombstonesRow) {
		base.Infof(base.KeyCRUD, "\tDeleting %q", tombstonesRow.Id)
		// First, attempt to purge.
		purgeErr := db.Purge(tombstonesRow.Id)
		if purgeErr == nil {
			count++
		} else if base.IsKeyNotFoundError(db.Bucket, purgeErr) {
			// If key no longer exists, need to add and remove to trigger removal from view
			_, addErr := db.Bucket.Add(tombstonesRow.Id, 0, purgeBody)
			if addErr != nil {
				base.Warnf(base.KeyAll, "Error compacting key %s (add) - tombstone will not be compacted.  %v", base.UD(tombstonesRow.Id), addErr)
				continue
			}
			if delErr := db.Bucket.Delete(tombstonesRow.Id); delErr != nil {
				base.Warnf(base.KeyAll, "Error compacting key %s (delete) - tombstone will not be compacted.  %v", base.UD(tombstonesRow.Id), delErr)
			}
			count++
		} else {
			base.Warnf(base.KeyAll, "Error compacting key %s (purge) - tombstone will not be compacted.  %v", base.UD(tombstonesRow.Id), purgeErr)
		}
	}
	return count, nil
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

	err = context.Bucket.Update(kSyncDataKey, 0, func(currentValue []byte) ([]byte, *uint32, error) {
		// The first time opening a new db, currentValue will be nil. Don't treat this as a change.
		if currentValue != nil {
			parseErr := json.Unmarshal(currentValue, &syncData)
			if parseErr != nil || syncData.Sync != syncFun {
				changed = true
			}
		}
		if changed || currentValue == nil {
			syncData.Sync = syncFun
			bytes, err := json.Marshal(syncData)
			return bytes, nil, err
		} else {
			return nil, nil, couchbase.UpdateCancel // value unchanged, no need to save
		}
	})

	if err == couchbase.UpdateCancel {
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
		documentUpdateFunc := func(doc *document) (updatedDoc *document, shouldUpdate bool, updatedExpiry *uint32, err error) {
			imported := false
			if !doc.HasValidSyncData(db.writeSequences()) {
				// This is a document not known to the sync gateway. Ignore it:
				return nil, false, nil, couchbase.UpdateCancel
			} else {
				base.Infof(base.KeyCRUD, "\tRe-syncing document %q", base.UD(docid))
			}

			// Run the sync fn over each current/leaf revision, in case there are conflicts:
			changed := 0
			doc.History.forEachLeaf(func(rev *RevInfo) {
				body, _ := db.getRevFromDoc(doc, rev.ID, false)
				channels, access, roles, syncExpiry, _, err := db.getChannelsAndAccess(doc, body, rev.ID)
				if err != nil {
					// Probably the validator rejected the doc
					base.Warnf(base.KeyAll, "Error calling sync() on doc %q: %v", base.UD(docid), err)
					access = nil
					channels = nil
				}
				rev.Channels = channels

				if rev.ID == doc.CurrentRev {
					changed = len(doc.Access.updateAccess(doc, access)) +
						len(doc.RoleAccess.updateAccess(doc, roles)) +
						len(doc.updateChannels(channels))
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
					return nil, nil, deleteDoc, nil, errors.New("Cancel update")
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
					return nil, nil, deleteDoc, nil, errors.New("Cancel update")
				}
			}
			_, err = db.Bucket.WriteUpdateWithXattr(key, KSyncXattrName, 0, nil, writeUpdateFunc)
		} else {
			err = db.Bucket.Update(key, 0, func(currentValue []byte) ([]byte, *uint32, error) {
				// Be careful: this block can be invoked multiple times if there are races!
				if currentValue == nil {
					return nil, nil, couchbase.UpdateCancel // someone deleted it?!
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
					updatedBytes, marshalErr := json.Marshal(updatedDoc)
					return updatedBytes, updatedExpiry, marshalErr
				} else {
					return nil, nil, couchbase.UpdateCancel
				}
			})
		}
		if err == nil {
			changeCount++
		} else if err != couchbase.UpdateCancel {
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

// Helper method for API unit test retrieval of index bucket
func (context *DatabaseContext) GetIndexBucket() base.Bucket {
	if kvChangeIndex, ok := context.changeCache.(*kvChangeIndex); ok {
		return kvChangeIndex.reader.indexReadBucket
	} else {
		return nil
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
func (context *DatabaseContext) FlushRevisionCache() {
	context.revisionCache = NewRevisionCache(context.Options.RevisionCacheCapacity, context.revCacheLoader)
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

func (context *DatabaseContext) ReserveSequences(numToReserve uint64) error {
	return context.sequences.reserveSequences(numToReserve)
}
