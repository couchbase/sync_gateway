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
	"time"

	"github.com/couchbase/go-couchbase"

	"sync"
	"sync/atomic"

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

// Basic description of a database. Shared between all Database objects on the same database.
// This object is thread-safe so it can be shared between HTTP handlers.
type DatabaseContext struct {
	Name               string                  // Database name
	Bucket             base.Bucket             // Storage
	BucketSpec         base.BucketSpec         // The BucketSpec
	BucketLock         sync.RWMutex            // Control Access to the underlying bucket object
	tapListener        changeListener          // Listens on server Tap feed
	sequences          *sequenceAllocator      // Source of new sequence numbers
	ChannelMapper      *channels.ChannelMapper // Runs JS 'sync' function
	StartTime          time.Time               // Timestamp when context was instantiated
	ChangesClientStats Statistics              // Tracks stats of # of changes connections
	RevsLimit          uint32                  // Max depth a document's revision tree can grow to
	autoImport         bool                    // Add sync data to new untracked docs?
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
}

type DatabaseContextOptions struct {
	CacheOptions          *CacheOptions
	IndexOptions          *ChangeIndexOptions
	SequenceHashOptions   *SequenceHashOptions
	RevisionCacheCapacity uint32
	AdminInterface        *string
	UnsupportedOptions    *UnsupportedOptions
	TrackDocs             bool // Whether doc tracking channel should be created (used for autoImport, shadowing)
	OIDCOptions           *auth.OIDCOptions
}

type OidcTestProviderOptions struct {
	Enabled         bool `json:"enabled,omitempty"`           // Whether the oidc_test_provider endpoints should be exposed on the public API
	UnsignedIDToken bool `json:"unsigned_id_token,omitempty"` // Whether the internal test provider returns a signed ID token on a refresh request.  Used to simulate Azure behaviour
}

type UnsupportedOptions struct {
	EnableUserViews  bool
	OidcTestProvider OidcTestProviderOptions
}

const DefaultRevsLimit = 1000

// Represents a simulated CouchDB database. A new instance is created for each HTTP request,
// so this struct does not have to be thread-safe.
type Database struct {
	*DatabaseContext
	user auth.User
}

// All special/internal documents the gateway creates have this prefix in their keys.
const KSyncKeyPrefix = "_sync:"

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
func ConnectToBucket(spec base.BucketSpec, callback func(bucket string, err error)) (bucket base.Bucket, err error) {
	//start a retry loop to connect to the bucket backing off double the delay each time
	worker := func() (shouldRetry bool, err error, value interface{}) {
		bucket, err = base.GetBucket(spec, callback)
		return err != nil, err, bucket
	}

	sleeper := base.CreateDoublingSleeperFunc(
		13, //MaxNumRetries approx 40 seconds total retry duration
		5,  //InitialRetrySleepTimeMS
	)

	description := fmt.Sprintf("Attempt to connect to bucket : %v", spec.BucketName)
	err, ibucket := base.RetryLoop(description, worker, sleeper)

	if err != nil {
		err = base.HTTPErrorf(http.StatusBadGateway,
			" Unable to connect to Couchbase Server (connection refused). Please ensure it is running and reachable at the configured host and port.  Detailed error: %s", err)
	} else {
		bucket, _ := ibucket.(base.Bucket)
		err = installViews(bucket)
	}
	return
}

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
	context.revisionCache = NewRevisionCache(int(options.RevisionCacheCapacity), context.revCacheLoader)

	context.EventMgr = NewEventManager()

	var err error
	context.sequences, err = newSequenceAllocator(bucket)
	if err != nil {
		return nil, err
	}
	lastSeq, err := context.sequences.lastSequence()
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

	context.changeCache.Init(context, SequenceID{Seq: lastSeq}, func(changedChannels base.Set) {
		context.tapListener.Notify(changedChannels)
	}, options.CacheOptions, options.IndexOptions)
	context.SetOnChangeCallback(context.changeCache.DocChanged)

	// Initialize the tap Listener for notify handling
	context.tapListener.Init(bucket.GetName())

	// If not using channel index or using channel index and tracking docs, start the tap feed
	if options.IndexOptions == nil || options.TrackDocs {
		if err = context.tapListener.Start(bucket, options.TrackDocs, func(bucket string, err error) {
			context.TakeDbOffline("Lost TAP Feed")
		}); err != nil {
			return nil, err
		}
	}

	// Load providers into provider map.  Does basic validation on the provider definition, and identifies the default provider.
	if options.OIDCOptions != nil {
		context.OIDCProviders = make(auth.OIDCProviderMap)

		for name, provider := range options.OIDCOptions.Providers {
			if provider.Issuer == "" || provider.ClientID == nil {
				base.Warn("Issuer and ClientID required for OIDC Provider - skipping provider %q", name)
				continue
			}

			if provider.ValidationKey == nil {
				base.Warn("Validation Key not defined in config for provider %q - auth code flow will not be supported for this provider", name)
			}

			if strings.Contains(name, "_") {
				return nil, fmt.Errorf("OpenID Connect provider names cannot contain underscore:%s", name)
			}
			provider.Name = name
			if _, ok := context.OIDCProviders[provider.Issuer]; ok {
				base.Warn("Multiple OIDC providers defined for issuer %v", provider.Issuer)
				return nil, fmt.Errorf("Multiple OIDC providers defined for issuer %v", provider.Issuer)
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

	go context.watchDocChanges()
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
		return nil, fmt.Errorf("No provider found for provider name %q", providerName)
	}
}

func (context *DatabaseContext) SetOnChangeCallback(callback DocChangedFunc) {
	context.tapListener.OnDocChanged = callback
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
	return context.tapListener
}

func (context *DatabaseContext) Close() {
	context.BucketLock.Lock()
	defer context.BucketLock.Unlock()

	context.tapListener.Stop()
	context.changeCache.Stop()
	context.Shadower.Stop()
	context.Bucket.Close()
	context.Bucket = nil
}

func (context *DatabaseContext) IsClosed() bool {
	return context.Bucket == nil
}

// For testing only!
func (context *DatabaseContext) RestartListener() error {
	context.tapListener.Stop()
	// Delay needed to properly stop
	time.Sleep(2 * time.Second)
	context.tapListener.Init(context.Bucket.GetName())
	if err := context.tapListener.Start(context.Bucket, context.Options.TrackDocs, nil); err != nil {
		return err
	}
	return nil
}

func (context *DatabaseContext) NotifyUser(username string) {
	context.tapListener.NotifyCheckForTermination(base.SetOf(auth.UserKeyPrefix + username))
}

func (dc *DatabaseContext) TakeDbOffline(reason string) error {
	base.LogTo("CRUD", "Taking Database : %v, offline", dc.Name)
	dbState := atomic.LoadUint32(&dc.State)
	//If the DB is already trasitioning to: offline or is offline silently return
	if dbState == DBOffline || dbState == DBResyncing || dbState == DBStopping {
		return nil
	}

	if atomic.CompareAndSwapUint32(&dc.State, DBOnline, DBStopping) {

		//notify all active _changes feeds to close
		close(dc.ExitChanges)

		base.LogTo("CRUD", "Waiting for all active calls to complete on Database : %v", dc.Name)
		//Block until all current calls have returned, including _changes feeds
		dc.AccessLock.Lock()
		defer dc.AccessLock.Unlock()

		base.LogTo("CRUD", "Database : %v, is offline", dc.Name)
		//set DB state to Offline
		atomic.StoreUint32(&dc.State, DBOffline)

		if dc.EventMgr.HasHandlerForEvent(DBStateChange) {
			dc.EventMgr.RaiseDBStateChangeEvent(dc.Name, "offline", reason, *dc.Options.AdminInterface)
		}

		return nil
	} else {
		base.LogTo("CRUD", "Unable to take Database offline, database must be in Online state")
		return base.HTTPErrorf(http.StatusServiceUnavailable, "Unable to take Database offline, database must be in Online state")
	}
}

func (context *DatabaseContext) Authenticator() *auth.Authenticator {
	// Authenticators are lightweight & stateless, so it's OK to return a new one every time
	return auth.NewAuthenticator(context.Bucket, context)
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

// The number of documents in the database.
func (db *Database) DocCount() int {
	vres, err := db.queryAllDocs(true)
	if err != nil {
		return -1
	}
	if len(vres.Rows) == 0 {
		return 0
	}
	return int(vres.Rows[0].Value.(float64))
}

func installViews(bucket base.Bucket) error {
	// View for finding every Couchbase doc (used when deleting a database)
	// Key is docid; value is null
	allbits_map := `function (doc, meta) {
                      emit(meta.id, null); }`
	// View for _all_docs
	// Key is docid; value is [revid, sequence]
	alldocs_map := `function (doc, meta) {
                     var sync = doc._sync;
                     if (sync === undefined || meta.id.substring(0,6) == "_sync:")
                       return;
                     if ((sync.flags & 1) || sync.deleted)
                       return;
                     var channels = sync.channels;
                     var channelNames = [];
                     for (ch in channels) {
                     	if (channels[ch] == null)
                     		channelNames.push(ch);
                     }
                     emit(meta.id, {r:sync.rev, s:sync.sequence, c:channelNames}); }`
	// View for importing unknown docs
	// Key is [existing?, docid] where 'existing?' is false for unknown docs
	import_map := `function (doc, meta) {
                     if(meta.id.substring(0,6) != "_sync:") {
                       var exists = (doc["_sync"] !== undefined);
                       emit([exists, meta.id], null); } }`
	// View for compaction -- finds all revision docs
	// Key and value are ignored.
	oldrevs_map := `function (doc, meta) {
                     var sync = doc._sync;
                     if (meta.id.substring(0,10) == "_sync:rev:")
	                     emit("",null); }`

	// Sessions view - used for session delete
	// Key is username; value is docid
	sessions_map := `function (doc, meta) {
                     	var prefix = meta.id.substring(0,%d);
                     	if (prefix == %q)
                     		emit(doc.username, meta.id);}`
	sessions_map = fmt.Sprintf(sessions_map, len(auth.SessionKeyPrefix), auth.SessionKeyPrefix)

	// All-principals view
	// Key is name; value is true for user, false for role
	principals_map := `function (doc, meta) {
							 var prefix = meta.id.substring(0,11);
							 var isUser = (prefix == %q);
							 if (isUser || prefix == %q)
			                     emit(meta.id.substring(%d), isUser); }`
	principals_map = fmt.Sprintf(principals_map, auth.UserKeyPrefix, auth.RoleKeyPrefix,
		len(auth.UserKeyPrefix))
	// By-channels view.
	// Key is [channelname, sequence]; value is [docid, revid, flag?]
	// where flag is true for doc deletion, false for removed from channel, missing otherwise
	channels_map := `function (doc, meta) {
	                    var sync = doc._sync;
	                    if (sync === undefined || meta.id.substring(0,6) == "_sync:")
	                        return;
						var sequence = sync.sequence;
	                    if (sequence === undefined)
	                        return;
	                    var value = {rev:sync.rev};
	                    if (sync.flags) {
	                    	value.flags = sync.flags
	                    } else if (sync.deleted) {
	                    	value.flags = %d // channels.Deleted
	                    }
	                    if (%v) // EnableStarChannelLog
							emit(["*", sequence], value);
						var channels = sync.channels;
						if (channels) {
							for (var name in channels) {
								removed = channels[name];
								if (!removed)
									emit([name, sequence], value);
								else {
									var flags = removed.del ? %d : %d; // channels.Removed/Deleted
									emit([name, removed.seq], {rev:removed.rev, flags: flags});
								}
							}
						}
					}`
	channels_map = fmt.Sprintf(channels_map, channels.Deleted, EnableStarChannelLog,
		channels.Removed|channels.Deleted, channels.Removed)
	// Channel access view, used by ComputeChannelsForPrincipal()
	// Key is username; value is dictionary channelName->firstSequence (compatible with TimedSet)
	access_map := `function (doc, meta) {
	                    var sync = doc._sync;
	                    if (sync === undefined || meta.id.substring(0,6) == "_sync:")
	                        return;
	                    var access = sync.access;
	                    if (access) {
	                        for (var name in access) {
	                            emit(name, access[name]);
	                        }
	                    }
	               }`

	// Vbucket sequence version of channel access view, used by ComputeChannelsForPrincipal()
	// Key is username; value is dictionary channelName->firstSequence (compatible with TimedSet)

	access_vbSeq_map := `function (doc, meta) {
		                    var sync = doc._sync;
		                    if (sync === undefined || meta.id.substring(0,6) == "_sync:")
		                        return;
		                    var access = sync.access;
		                    if (access) {
		                        for (var name in access) {
		                        	// Build a timed set based on vb and vbseq of this revision
		                        	var value = {};
		                        	for (var channel in access[name]) {
		                        		var timedSetWithVbucket = {};
				                        timedSetWithVbucket["vb"] = parseInt(meta.vb, 10);
				                        timedSetWithVbucket["seq"] = parseInt(meta.seq, 10);
				                        value[channel] = timedSetWithVbucket;
			                        }
		                            emit(name, value)
		                        }

		                    }
		               }`

	// Role access view, used by ComputeRolesForUser()
	// Key is username; value is array of role names
	roleAccess_map := `function (doc, meta) {
	                    var sync = doc._sync;
	                    if (sync === undefined || meta.id.substring(0,6) == "_sync:")
	                        return;
	                    var access = sync.role_access;
	                    if (access) {
	                        for (var name in access) {
	                            emit(name, access[name]);
	                        }
	                    }
	               }`

	// Vbucket sequence version of role access view, used by ComputeRolesForUser()
	// Key is username; value is dictionary channelName->firstSequence (compatible with TimedSet)

	roleAccess_vbSeq_map := `function (doc, meta) {
		                    var sync = doc._sync;
		                    if (sync === undefined || meta.id.substring(0,6) == "_sync:")
		                        return;
		                    var access = sync.role_access;
		                    if (access) {
		                        for (var name in access) {
		                        	// Build a timed set based on vb and vbseq of this revision
		                        	var value = {};
		                        	for (var role in access[name]) {
		                        		var timedSetWithVbucket = {};
				                        timedSetWithVbucket["vb"] = parseInt(meta.vb, 10);
				                        timedSetWithVbucket["seq"] = parseInt(meta.seq, 10);
				                        value[role] = timedSetWithVbucket;
			                        }
		                            emit(name, value)
		                        }

		                    }
		               }`

	designDocMap := map[string]sgbucket.DesignDoc{}

	designDocMap[DesignDocSyncGateway] = sgbucket.DesignDoc{
		Views: sgbucket.ViewMap{
			ViewPrincipals:      sgbucket.ViewDef{Map: principals_map},
			ViewChannels:        sgbucket.ViewDef{Map: channels_map},
			ViewAccess:          sgbucket.ViewDef{Map: access_map},
			ViewAccessVbSeq:     sgbucket.ViewDef{Map: access_vbSeq_map},
			ViewRoleAccess:      sgbucket.ViewDef{Map: roleAccess_map},
			ViewRoleAccessVbSeq: sgbucket.ViewDef{Map: roleAccess_vbSeq_map},
		},
	}

	designDocMap[DesignDocSyncHousekeeping] = sgbucket.DesignDoc{
		Views: sgbucket.ViewMap{
			ViewAllBits:  sgbucket.ViewDef{Map: allbits_map},
			ViewAllDocs:  sgbucket.ViewDef{Map: alldocs_map, Reduce: "_count"},
			ViewImport:   sgbucket.ViewDef{Map: import_map, Reduce: "_count"},
			ViewOldRevs:  sgbucket.ViewDef{Map: oldrevs_map, Reduce: "_count"},
			ViewSessions: sgbucket.ViewDef{Map: sessions_map},
		},
	}

	sleeper := base.CreateDoublingSleeperFunc(
		11, //MaxNumRetries approx 10 seconds total retry duration
		5,  //InitialRetrySleepTimeMS
	)

	// add all design docs from map into bucket
	for designDocName, designDoc := range designDocMap {

		//start a retry loop to put design document backing off double the delay each time
		worker := func() (shouldRetry bool, err error, value interface{}) {
			err = bucket.PutDDoc(designDocName, designDoc)
			if err != nil {
				base.Warn("Error installing Couchbase design doc: %v", err)
			}
			return err != nil, err, nil
		}

		description := fmt.Sprintf("Attempt to install Couchbase design doc bucket : %v", designDocName)
		err, _ := base.RetryLoop(description, worker, sleeper)

		if err != nil {
			return err
		}
	}

	return nil
}

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
	type viewRow struct {
		Key   string
		Value struct {
			RevID    string   `json:"r"`
			Sequence uint64   `json:"s"`
			Channels []string `json:"c"`
		}
	}
	var vres struct {
		Rows []viewRow
	}
	opts := Body{"stale": false, "reduce": false}

	if resultsOpts.Startkey != "" {
		opts["startkey"] = resultsOpts.Startkey
	}

	if resultsOpts.Endkey != "" {
		opts["endkey"] = resultsOpts.Endkey
	}

	err := db.Bucket.ViewCustom(DesignDocSyncHousekeeping, ViewAllDocs, opts, &vres)
	if err != nil {
		base.Warn("all_docs got error: %v", err)
		return err
	}

	count := uint64(0)
	for _, row := range vres.Rows {
		if callback(IDAndRev{row.Key, row.Value.RevID, row.Value.Sequence}, row.Value.Channels) {
			count++
		}
		//We have to apply limit check after callback has been called
		//to account for rows that are not in the current users channels
		if resultsOpts.Limit > 0 && count == resultsOpts.Limit {
			break
		}
	}

	return nil
}

// Returns the IDs of all users and roles
func (db *DatabaseContext) AllPrincipalIDs() (users, roles []string, err error) {
	vres, err := db.Bucket.View(DesignDocSyncGateway, ViewPrincipals, Body{"stale": false})
	if err != nil {
		return
	}
	users = []string{}
	roles = []string{}
	for _, row := range vres.Rows {
		name := row.Key.(string)
		if name != "" {
			if row.Value.(bool) {
				users = append(users, name)
			} else {
				roles = append(roles, name)
			}
		}
	}
	return
}

func (db *Database) queryAllDocs(reduce bool) (sgbucket.ViewResult, error) {
	opts := Body{"stale": false, "reduce": reduce}
	vres, err := db.Bucket.View(DesignDocSyncHousekeeping, ViewAllDocs, opts)
	if err != nil {
		base.Warn("all_docs got error: %v", err)
	}
	return vres, err
}

//////// HOUSEKEEPING:

// Deletes all documents in the database
func (db *Database) DeleteAllDocs(docType string) error {
	opts := Body{"stale": false}
	if docType != "" {
		opts["startkey"] = "_sync:" + docType + ":"
		opts["endkey"] = "_sync:" + docType + "~"
		opts["inclusive_end"] = false
	}
	vres, err := db.Bucket.View(DesignDocSyncHousekeeping, ViewAllBits, opts)
	if err != nil {
		base.Warn("all_bits view returned %v", err)
		return err
	}

	//FIX: Is there a way to do this in one operation?
	base.Logf("Deleting %d %q documents of %q ...", len(vres.Rows), docType, db.Name)
	for _, row := range vres.Rows {
		base.LogTo("CRUD", "\tDeleting %q", row.ID)
		if err := db.Bucket.Delete(row.ID); err != nil {
			base.Warn("Error deleting %q: %v", row.ID, err)
		}
	}
	return nil
}

// Deletes all session documents for a user
func (db *DatabaseContext) DeleteUserSessions(userName string) error {
	opts := Body{"stale": false}
	opts["startkey"] = userName
	opts["endkey"] = userName
	vres, err := db.Bucket.View(DesignDocSyncHousekeeping, ViewSessions, opts)
	if err != nil {
		base.Warn("sessions view returned %v", err)
		return err
	}

	for _, row := range vres.Rows {
		docId := row.Value.(string)
		base.LogTo("CRUD", "\tDeleting %q", docId)
		if err := db.Bucket.Delete(docId); err != nil {
			base.Warn("Error deleting %q: %v", row.ID, err)
		}
	}
	return nil
}

// Deletes old revisions that have been moved to individual docs
func (db *Database) Compact() (int, error) {
	opts := Body{"stale": false, "reduce": false}
	vres, err := db.Bucket.View(DesignDocSyncHousekeeping, ViewOldRevs, opts)
	if err != nil {
		base.Warn("old_revs view returned %v", err)
		return 0, err
	}

	//FIX: Is there a way to do this in one operation?
	base.Logf("Compacting away %d old revs of %q ...", len(vres.Rows), db.Name)
	count := 0
	for _, row := range vres.Rows {
		base.LogTo("CRUD", "\tDeleting %q", row.ID)
		if err := db.Bucket.Delete(row.ID); err != nil {
			base.Warn("Error deleting %q: %v", row.ID, err)
		} else {
			count++
		}
	}
	return count, nil
}

// Deletes all orphaned CouchDB attachments not used by any revisions.
func VacuumAttachments(bucket base.Bucket) (int, error) {
	return 0, base.HTTPErrorf(http.StatusNotImplemented, "Vacuum is temporarily out of order")
}

//////// SYNC FUNCTION:

const kSyncDataKey = "_sync:syncdata"

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
		base.Warn("Error setting sync function: %s", err)
		return
	}

	var syncData struct { // format of the sync-fn document
		Sync string
	}

	err = context.Bucket.Update(kSyncDataKey, 0, func(currentValue []byte) ([]byte, error) {
		// The first time opening a new db, currentValue will be nil. Don't treat this as a change.
		if currentValue != nil {
			parseErr := json.Unmarshal(currentValue, &syncData)
			if parseErr != nil || syncData.Sync != syncFun {
				changed = true
			}
		}
		if changed || currentValue == nil {
			syncData.Sync = syncFun
			return json.Marshal(syncData)
		} else {
			return nil, couchbase.UpdateCancel // value unchanged, no need to save
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
func (db *Database) UpdateAllDocChannels(doCurrentDocs bool, doImportDocs bool) (int, error) {
	if doCurrentDocs {
		base.Log("Recomputing document channels...")
	}
	if doImportDocs {
		base.Log("Importing documents...")
	} else if !doCurrentDocs {
		return 0, nil // no-op if neither option is set
	}
	options := Body{"stale": false, "reduce": false}
	if !doCurrentDocs {
		options["endkey"] = []interface{}{true}
		options["endkey_inclusive"] = false
	} else if !doImportDocs {
		options["startkey"] = []interface{}{true}
	}
	vres, err := db.Bucket.View(DesignDocSyncHousekeeping, ViewImport, options)
	if err != nil {
		return 0, err
	}

	// We are about to alter documents without updating their sequence numbers, which would
	// really confuse the changeCache, so turn it off until we're done:
	db.changeCache.EnableChannelIndexing(false)
	defer db.changeCache.EnableChannelIndexing(true)
	db.changeCache.Clear()

	base.Logf("Re-running sync function on all %d documents...", len(vres.Rows))
	changeCount := 0
	for _, row := range vres.Rows {
		rowKey := row.Key.([]interface{})
		docid := rowKey[1].(string)
		key := realDocID(docid)
		//base.Log("\tupdating %q", docid)
		err := db.Bucket.Update(key, 0, func(currentValue []byte) ([]byte, error) {
			// Be careful: this block can be invoked multiple times if there are races!
			if currentValue == nil {
				return nil, couchbase.UpdateCancel // someone deleted it?!
			}
			doc, err := unmarshalDocument(docid, currentValue)
			if err != nil {
				return nil, err
			}

			imported := false
			if !doc.HasValidSyncData(db.writeSequences()) {
				// This is a document not known to the sync gateway. Ignore or import it:
				if !doImportDocs {
					return nil, couchbase.UpdateCancel
				}
				imported = true
				if err = db.initializeSyncData(doc); err != nil {
					return nil, err
				}
				base.LogTo("CRUD", "\tImporting document %q --> rev %q", docid, doc.CurrentRev)
			} else {
				if !doCurrentDocs {
					return nil, couchbase.UpdateCancel
				}
				base.LogTo("CRUD", "\tRe-syncing document %q", docid)
			}

			// Run the sync fn over each current/leaf revision, in case there are conflicts:
			changed := 0
			doc.History.forEachLeaf(func(rev *RevInfo) {
				body, _ := db.getRevFromDoc(doc, rev.ID, false)
				channels, access, roles, _, err := db.getChannelsAndAccess(doc, body, rev.ID)
				if err != nil {
					// Probably the validator rejected the doc
					base.Warn("Error calling sync() on doc %q: %v", docid, err)
					access = nil
					channels = nil
				}
				rev.Channels = channels

				if rev.ID == doc.CurrentRev {
					changed = len(doc.Access.updateAccess(doc, access)) +
						len(doc.RoleAccess.updateAccess(doc, roles)) +
						len(doc.updateChannels(channels))
				}
			})

			if changed > 0 || imported {
				base.LogTo("Access", "Saving updated channels and access grants of %q", docid)
				return json.Marshal(doc)
			} else {
				return nil, couchbase.UpdateCancel
			}
		})
		if err == nil {
			changeCount++
		} else if err != couchbase.UpdateCancel {
			base.Warn("Error updating doc %q: %v", docid, err)
		}
	}
	base.Logf("Finished re-running sync function; %d docs changed", changeCount)

	if changeCount > 0 {
		// Now invalidate channel cache of all users/roles:
		base.Log("Invalidating channel caches of users/roles...")
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
			base.Warn("Error invalidating roles for user %s: %v", username, err)
		}
	}
}

func (db *Database) invalUserChannels(username string) {
	authr := db.Authenticator()
	if user, _ := authr.GetUser(username); user != nil {
		if err := authr.InvalidateChannels(user); err != nil {
			base.Warn("Error invalidating channels for user %s: %v", username, err)
		}
	}
}

func (db *Database) invalRoleChannels(rolename string) {
	authr := db.Authenticator()
	if role, _ := authr.GetRole(rolename); role != nil {
		if err := authr.InvalidateChannels(role); err != nil {
			base.Warn("Error invalidating channels for role %s: %v", rolename, err)
		}
	}
}

func (db *Database) invalUserOrRoleChannels(name string) {
	if strings.HasPrefix(name, "role:") {
		db.invalRoleChannels(name[5:])
	} else {
		db.invalUserChannels(name)
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
	if context.Options.UnsupportedOptions != nil {
		return context.Options.UnsupportedOptions.EnableUserViews
	}
	return false
}

func (context *DatabaseContext) SetUserViewsEnabled(value bool) {
	context.Options.UnsupportedOptions.EnableUserViews = value
}

//////// SEQUENCE ALLOCATION:

func (context *DatabaseContext) LastSequence() (uint64, error) {
	return context.sequences.lastSequence()
}

func (context *DatabaseContext) ReserveSequences(numToReserve uint64) error {
	return context.sequences.reserveSequences(numToReserve)
}
