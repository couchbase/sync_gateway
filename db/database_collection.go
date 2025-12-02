// Copyright 2022-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package db

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"time"

	"github.com/couchbase/sync_gateway/auth"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"
	"github.com/google/uuid"
)

// DatabaseCollection provides a representation of a single collection of a database.
type DatabaseCollection struct {
	dataStore            base.DataStore          // Storage
	revisionCache        collectionRevisionCache // Collection reference to cache of recently-accessed document revisions
	collectionStats      *base.CollectionStats   // pointer to the collection stats (to avoid map lookups when used)
	dbCtx                *DatabaseContext        // pointer to database context to allow passthrough of functions
	ChannelMapper        *channels.ChannelMapper // Collection's sync function
	importFilterFunction *ImportFilterFunction   // collections import options
	Name                 string
	ScopeName            string
}

// DatabaseCollectionWithUser represents CouchDB database. A new instance is created for each request,
// so this struct does not have to be thread-safe.
type DatabaseCollectionWithUser struct {
	*DatabaseCollection
	user auth.User
}

// newDatabaseCollection returns a collection which inherits values from the database but is specific to a given DataStore.
func newDatabaseCollection(ctx context.Context, dbContext *DatabaseContext, dataStore base.DataStore, stats *base.CollectionStats) (*DatabaseCollection, error) {
	dbCollection := &DatabaseCollection{
		dataStore:       dataStore,
		dbCtx:           dbContext,
		collectionStats: stats,
		revisionCache:   newCollectionRevisionCache(&dbContext.revisionCache, dataStore.GetCollectionID()),
		ScopeName:       dataStore.ScopeName(),
		Name:            dataStore.CollectionName(),
	}

	return dbCollection, nil
}

// AllowConflicts allows different revisions of a single document to be pushed. This is controlled at the database level.
func (c *DatabaseCollection) AllowConflicts() bool {
	if c.dbCtx.Options.AllowConflicts != nil {
		return *c.dbCtx.Options.AllowConflicts
	}
	return base.DefaultAllowConflicts
}

type GetRawDocOpts struct {
	IncludeDoc bool // If true, the document body will be returned as well as the metadata.
	Redact     bool
	RedactSalt string
}

// GetRawDoc returns the persisted version of a document, including its SG-related xattrs.
func (c *DatabaseCollection) GetRawDoc(ctx context.Context, docID string, opts *GetRawDocOpts) (docBody json.RawMessage, xattrs map[string]json.RawMessage, err error) {
	if opts == nil {
		// default
		opts = &GetRawDocOpts{
			IncludeDoc: true,
		}
	}

	if opts.Redact && opts.RedactSalt == "" {
		// generate a random salt if one is not provided
		opts.RedactSalt = uuid.New().String()
	} else if !opts.Redact && opts.RedactSalt != "" {
		return nil, nil, fmt.Errorf("RedactSalt provided to GetRawDoc but Redact is not enabled")
	}

	// collect the set of xattrs to fetch that Sync Gateway is interested in.
	xattrKeys := slices.Clone(base.SyncGatewayRawDocXattrs)
	userXattrKey := c.dbCtx.Options.UserXattrKey
	if userXattrKey != "" {
		// we'll redact this later after fetching - it's still useful to know it was present even if we can't see the contents.
		xattrKeys = append(xattrKeys, userXattrKey)
	}

	var xattrValuesBytes map[string][]byte
	if opts.IncludeDoc {
		docBody, xattrValuesBytes, _, err = c.dataStore.GetWithXattrs(ctx, docID, xattrKeys)
	} else {
		xattrValuesBytes, _, err = c.dataStore.GetXattrs(ctx, docID, xattrKeys)
	}
	if err != nil {
		return nil, nil, err
	}

	// stamp all requested xattrKeys and populate with values where appropriate (ensures null values are present)
	xattrs = make(map[string]json.RawMessage, len(xattrValuesBytes))
	for _, k := range xattrKeys {
		if opts.Redact {
			switch k {
			case base.SyncXattrName:
				redactedV, err := RedactRawSyncData(xattrValuesBytes[k], opts.RedactSalt)
				if err != nil {
					return nil, nil, fmt.Errorf("couldn't redact sync data: %w", err)
				}
				xattrs[k] = redactedV
			case base.GlobalXattrName:
				redactedV, err := RedactRawGlobalSyncData(xattrValuesBytes[k], opts.RedactSalt)
				if err != nil {
					return nil, nil, fmt.Errorf("couldn't redact global sync data: %w", err)
				}
				xattrs[k] = redactedV
			case userXattrKey:
				// include the key but not the value so we can see _something_ was present.
				xattrs[k] = []byte(`"redacted"`)
			default:
				// no redaction for this key
				xattrs[k] = xattrValuesBytes[k]
			}
		} else {
			// redaction disabled
			xattrs[k] = xattrValuesBytes[k]
		}
	}

	return docBody, xattrs, nil
}

func (c *DatabaseCollection) GetCollectionDatastore() base.DataStore {
	return c.dataStore
}

// allPrincipalIDs returns the IDs of all users and roles, including deleted Roles
func (c *DatabaseCollection) allPrincipalIDs(ctx context.Context) (users, roles []string, err error) {
	return c.dbCtx.AllPrincipalIDs(ctx)
}

// Authenticator returns authentication options associated with the collection's database.
func (c *DatabaseCollection) Authenticator(ctx context.Context) *auth.Authenticator {
	return c.dbCtx.Authenticator(ctx)
}

// activeChannels tracks active replications by channel. This is a database level property.
func (c *DatabaseCollection) activeChannels() *channels.ActiveChannels {
	return c.dbCtx.activeChannels
}

// backupOldRev creates a temporary copy of a old revision body when resolving conflicts. This is controlled at the database level.
func (c *DatabaseCollection) backupOldRev() bool {
	return c.dbCtx.Options.ImportOptions.BackupOldRev

}

// bucketName returns the name of the bucket this collection is stored in.
func (c *DatabaseCollection) bucketName() string {
	return c.dataStore.GetName()

}

// changeCache returns the change cache for the database.
func (c *DatabaseCollection) changeCache() *changeCache {
	return &c.dbCtx.changeCache
}

// channelQueryLimit returns the pagination for the number of channels returned in a query. This is a database level property.
func (c *DatabaseCollection) channelQueryLimit() int {
	return c.dbCtx.Options.CacheOptions.ChannelQueryLimit
}

// clusterUUID returns a couchbase server UUID. If running with walrus, return an empty string.
func (c *DatabaseCollection) serverUUID() string {
	return c.dbCtx.ServerUUID
}

// DbStats are stats that correspond to database level collections.
func (c *DatabaseCollection) dbStats() *base.DbStats {
	return c.dbCtx.DbStats
}

// deltaSyncEnabled returns true if delta sync is enabled. This is controlled at the database level.
func (c *DatabaseCollection) deltaSyncEnabled() bool {
	return c.dbCtx.Options.DeltaSyncOptions.Enabled
}

// deltaSyncRevMaxAgeSeconds is the number of seconds that old revisions will be in memory. This is controlled at database level.
func (c *DatabaseCollection) deltaSyncRevMaxAgeSeconds() uint32 {
	return c.dbCtx.Options.DeltaSyncOptions.RevMaxAgeSeconds
}

// storeLegacyRevTreeData returns true if legacy revision tree pointer data is stored. This is controlled at the database level.
func (c *DatabaseCollection) storeLegacyRevTreeData() bool {
	return base.ValDefault(c.dbCtx.Options.StoreLegacyRevTreeData, DefaultStoreLegacyRevTreeData)
}

// eventMgr manages nofication events. This is controlled at database level.
func (c *DatabaseCollection) eventMgr() *EventManager {
	return c.dbCtx.EventMgr
}

// exitChanges will close active _changes feeds on the DB. This is a database level close.
func (c *DatabaseCollection) exitChanges() chan struct{} {
	return c.dbCtx.ExitChanges
}

// GetCollectionID returns a collectionID. If couchbase server does not return collections, it will return base.DefaultCollectionID, like the default collection for a Couchbase Server that does support collections.
func (c *DatabaseCollection) GetCollectionID() uint32 {
	return c.dataStore.GetCollectionID()
}

// GetRevisionCacheForTest allow accessing a copy of revision cache.
func (c *DatabaseCollection) GetRevisionCacheForTest() *collectionRevisionCache {
	return &c.revisionCache
}

// FlushChannelCache flush support. Currently test-only - added for unit test access from rest package
func (c *DatabaseCollection) FlushChannelCache(ctx context.Context) error {
	base.InfofCtx(ctx, base.KeyCache, "Flushing channel cache")
	return c.dbCtx.changeCache.Clear(ctx)
}

// ForceAPIForbiddenErrors returns true if we return 403 vs empty docs. This is controlled at the database level.
func (c *DatabaseCollection) ForceAPIForbiddenErrors() bool {
	return c.dbCtx.Options.UnsupportedOptions != nil && c.dbCtx.Options.UnsupportedOptions.ForceAPIForbiddenErrors
}

// ImportFilter returns the import filter.
func (c *DatabaseCollection) importFilter() *ImportFilterFunction {
	return c.importFilterFunction
}

// IsClosed returns true if the underlying collection has been closed.
func (c *DatabaseCollection) IsClosed() bool {
	return c.dataStore == nil
}

// IsDefaultCollection returns true if collection is _default._default.
func (c *DatabaseCollection) IsDefaultCollection() bool {
	return base.IsDefaultCollection(c.ScopeName, c.Name)
}

// isGuestReadOnly returns true if the guest user can only perform read operations. This is controlled at the database level.
func (c *DatabaseCollection) isGuestReadOnly() bool {
	return c.dbCtx.Options.UnsupportedOptions != nil && c.dbCtx.Options.UnsupportedOptions.GuestReadOnly
}

// LastSequence returns the highest sequence number allocated for this collection.
func (c *DatabaseCollection) LastSequence(ctx context.Context) (uint64, error) {
	return c.dbCtx.sequences.lastSequence(ctx)
}

// localDocExpirySecs returns the expiry for docs tracking Couchbase Lite replication state. This is controlled at the database level.
func (c *DatabaseCollection) localDocExpirySecs() uint32 {
	return c.dbCtx.Options.LocalDocExpirySecs
}

// mutationListener returns mutation level for the database.
func (c *DatabaseCollection) mutationListener() *changeListener {
	return &c.dbCtx.mutationListener
}

// oldRevExpirySeconds is the number of seconds before old revisions are removed from Couchbase server. This is controlled at a database level.
func (c *DatabaseCollection) oldRevExpirySeconds() uint32 {
	return c.dbCtx.Options.OldRevExpirySeconds
}

// ReloadUser the User object, in case its persistent properties have been changed. This code does not lock and is not safe to call from concurrent goroutines.
func (c *DatabaseCollectionWithUser) ReloadUser(ctx context.Context) error {
	if c.user == nil {
		return nil
	}
	user, err := c.Authenticator(ctx).GetUser(c.user.Name())
	if err != nil {
		return err
	}
	if user == nil {
		return fmt.Errorf("User not found during reload")
	}
	c.user = user
	return nil
}

// RemoveFromChangeCache removes select documents from all channel caches and returns the number of documents removed.
func (c *DatabaseCollection) RemoveFromChangeCache(ctx context.Context, docIDs []string, startTime time.Time) int {
	return c.dbCtx.changeCache.Remove(ctx, c.GetCollectionID(), docIDs, startTime)
}

// revsLimit is the max depth a document's revision tree can grow to. This is controlled at a database level.
func (c *DatabaseCollection) revsLimit() uint32 {
	return c.dbCtx.RevsLimit
}

// sequences returns the sequence generator for a collection.
func (c *DatabaseCollection) sequences() *sequenceAllocator {
	return c.dbCtx.sequences
}

// slowQueryWarningThreshold is the duration of N1QL query to log as slow. This is controlled at a database level.
func (c *DatabaseCollection) slowQueryWarningThreshold() time.Duration {
	return c.dbCtx.Options.SlowQueryWarningThreshold
}

// unsupportedOptions returns options that are potentially unstable. This is controlled at a database level.
func (c *DatabaseCollection) unsupportedOptions() *UnsupportedOptions {
	return c.dbCtx.Options.UnsupportedOptions
}

// syncGlobalSyncAndUserXattrKeys returns the xattr keys for the user and sync xattrs.
func (c *DatabaseCollection) syncGlobalSyncAndUserXattrKeys() []string {
	xattrKeys := []string{base.SyncXattrName, base.VvXattrName, base.GlobalXattrName}
	userXattrKey := c.userXattrKey()
	if userXattrKey != "" {
		xattrKeys = append(xattrKeys, userXattrKey)
	}
	return xattrKeys
}

// syncGlobalSyncMouAndUserXattrKeys returns the xattr keys for the user, mou and sync xattrs.
func (c *DatabaseCollection) syncGlobalSyncMouAndUserXattrKeys() []string {
	xattrKeys := []string{base.SyncXattrName, base.VvXattrName,
		base.MouXattrName, base.GlobalXattrName}
	userXattrKey := c.userXattrKey()
	if userXattrKey != "" {
		xattrKeys = append(xattrKeys, userXattrKey)
	}
	return xattrKeys
}

// syncGlobalSyncMouRevSeqNoAndUserXattrKeys returns the xattr keys for the user, mou, revSeqNo and sync xattrs.
func (c *DatabaseCollection) syncGlobalSyncMouRevSeqNoAndUserXattrKeys() []string {
	xattrKeys := []string{base.SyncXattrName, base.VvXattrName, base.VirtualXattrRevSeqNo}
	if c.useMou() {
		xattrKeys = append(xattrKeys, base.MouXattrName, base.GlobalXattrName)
	}
	userXattrKey := c.userXattrKey()
	if userXattrKey != "" {
		xattrKeys = append(xattrKeys, userXattrKey)
	}
	return xattrKeys
}

// Returns the xattr key that will be accessible from the sync function. This is controlled at a database level.
func (c *DatabaseCollection) userXattrKey() string {
	return c.dbCtx.Options.UserXattrKey
}

// UseXattrs specifies whether the collection stores metadata in xattars or inline. This is controlled at a database level.
func (c *DatabaseCollection) UseXattrs() bool {
	return c.dbCtx.Options.EnableXattr
}

// numIndexPartitions returns the number of partitions for the collection's indexes. This is controlled at a database level.
func (c *DatabaseCollection) numIndexPartitions() uint32 {
	return c.dbCtx.numIndexPartitions()
}

// User will return the user object.
func (c *DatabaseCollectionWithUser) User() auth.User {
	return c.user
}

// useViews will return whether the bucket is configured using views.
func (c *DatabaseCollection) useViews() bool {
	return c.dbCtx.Options.UseViews
}

// Sets the collection's sync function based on the JS code from config.
// Returns a boolean indicating whether the function is different from the saved one.
// If multiple gateway instances try to update the function at the same time (to the same new
// value) only one of them will get a changed=true result.
func (c *DatabaseCollection) UpdateSyncFun(ctx context.Context, syncFun string) (changed bool, err error) {
	if syncFun == "" {
		c.ChannelMapper = nil
	} else if c.ChannelMapper != nil {
		_, err = c.ChannelMapper.SetFunction(syncFun)
	} else {
		c.ChannelMapper = channels.NewChannelMapper(ctx, syncFun, c.dbCtx.Options.JavascriptTimeout)
	}
	if err != nil {
		base.WarnfCtx(ctx, "Error setting sync function: %s", err)
		return
	}

	var syncData struct { // format of the sync-fn document
		Sync string
	}

	syncFunctionDocID := base.CollectionSyncFunctionKeyWithGroupID(c.dbCtx.Options.GroupID, c.ScopeName, c.Name)
	_, err = c.dbCtx.MetadataStore.Update(syncFunctionDocID, 0, func(currentValue []byte) ([]byte, *uint32, bool, error) {
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

// DatabaseCollection helper methods for channel and role invalidation - invoke the multi-collection version on
// the databaseContext for a single collection.
// invalUserOrRoleChannels invalidates a user or role's channels for collection c
func (c *DatabaseCollection) invalUserOrRoleChannels(ctx context.Context, name string, invalSeq uint64) {
	principalName, isRole := channels.AccessNameToPrincipalName(name)
	if isRole {
		c.invalRoleChannels(ctx, principalName, invalSeq)
	} else {
		c.invalUserChannels(ctx, principalName, invalSeq)
	}
}

// invalRoleChannels invalidates a user's computed channels for collection c
func (c *DatabaseCollection) invalUserChannels(ctx context.Context, username string, invalSeq uint64) {
	c.dbCtx.invalUserChannels(ctx, username, base.ScopeAndCollectionNames{c.ScopeAndCollectionName()}, invalSeq)
}

// invalRoleChannels invalidates a role's computed channels for collection c
func (c *DatabaseCollection) invalRoleChannels(ctx context.Context, rolename string, invalSeq uint64) {
	c.dbCtx.invalRoleChannels(ctx, rolename, base.ScopeAndCollectionNames{c.ScopeAndCollectionName()}, invalSeq)
}

func (c *DatabaseCollection) useMou() bool {
	return c.dbCtx.UseMou()
}

func (c *DatabaseCollection) ScopeAndCollectionName() base.ScopeAndCollectionName {
	return base.NewScopeAndCollectionName(c.ScopeName, c.Name)
}

// AddCollectionContext adds the collection name to a context.
func (c *DatabaseCollection) AddCollectionContext(ctx context.Context) context.Context {
	return base.CollectionLogCtx(ctx, c.ScopeName, c.Name)
}
