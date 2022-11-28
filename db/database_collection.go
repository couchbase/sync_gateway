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
	"fmt"
	"time"

	"github.com/couchbase/sync_gateway/auth"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"
)

// DatabaseCollection provides a representation of a single collection of a database.
type DatabaseCollection struct {
	dataStore     base.DataStore   // Storage
	revisionCache RevisionCache    // Cache of recently-accessed doc revisions
	changeCache   *changeCache     // Cache of recently-access channels
	dbCtx         *DatabaseContext // pointer to database context to allow passthrough of functions
}

// DatabaseCollectionWithUser represents CouchDB database. A new instance is created for each request,
// so this struct does not have to be thread-safe.
type DatabaseCollectionWithUser struct {
	*DatabaseCollection
	user auth.User
}

// AllowConflicts allows different revisions of a single document to be pushed. This is controlled at the database level.
func (c *DatabaseCollection) AllowConflicts() bool {
	if c.dbCtx.Options.AllowConflicts != nil {
		return *c.dbCtx.Options.AllowConflicts
	}
	return base.DefaultAllowConflicts
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

// channelMapper runs the javascript sync function. This is currently at the database level.
func (c *DatabaseCollection) channelMapper() *channels.ChannelMapper {
	return c.dbCtx.ChannelMapper
}

// channelQueryLimit returns the pagination for the number of channels returned in a query. This is a database level property.
func (c *DatabaseCollection) channelQueryLimit() int {
	return c.dbCtx.Options.CacheOptions.ChannelQueryLimit
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
	// FIXME (bbrks) - will not work with wrappers
	collection, err := base.AsCollection(c.dataStore)
	if err != nil {
		return base.DefaultCollectionID
	}
	return collection.GetCollectionID()
}

// GetRevisionCacheForTest allow accessing a copy of revision cache.
func (c *DatabaseCollection) GetRevisionCacheForTest() RevisionCache {
	return c.revisionCache
}

// groupID return the GroupID defined at a database level.
func (c *DatabaseCollection) groupID() string {
	return c.dbCtx.Options.GroupID
}

// FlushChannelCache flush support. Currently test-only - added for unit test access from rest package
func (c *DatabaseCollection) FlushChannelCache(ctx context.Context) error {
	base.InfofCtx(ctx, base.KeyCache, "Flushing channel cache")
	return c.changeCache.Clear()
}

// FlushRevisionCacheForTest creates a new revision cache. This is currently at the database level. Only use this in test code.
func (c *DatabaseCollection) FlushRevisionCacheForTest() {
	c.revisionCache = NewRevisionCache(
		c.dbCtx.Options.RevisionCacheOptions,
		c,
		c.dbStats().Cache(),
	)

}

// ForceAPIForbiddenErrors returns true if we return 403 vs empty docs. This is controlled at the database level.
func (c *DatabaseCollection) ForceAPIForbiddenErrors() bool {
	return c.dbCtx.Options.UnsupportedOptions != nil && c.dbCtx.Options.UnsupportedOptions.ForceAPIForbiddenErrors
}

// importFilter returns the sync function.
func (c *DatabaseCollection) importFilter() *ImportFilterFunction {
	return c.dbCtx.Options.ImportOptions.ImportFilter
}

// IsClosed returns true if the underlying collection has been closed.
func (c *DatabaseCollection) IsClosed() bool {
	return c.dataStore == nil
}

// isGuestReadOnly returns true if the guest user can only perform read operations. This is controlled at the database level.
func (c *DatabaseCollection) isGuestReadOnly() bool {
	return c.dbCtx.Options.UnsupportedOptions != nil && c.dbCtx.Options.UnsupportedOptions.GuestReadOnly
}

// LastSequence returns the highest sequence number allocated for this collection.
func (c *DatabaseCollection) LastSequence() (uint64, error) {
	return c.dbCtx.sequences.lastSequence()
}

// localDocExpirySecs returns the expiry for docs tracking Couchbase Lite replication state. This is controlled at the database level.
func (c *DatabaseCollection) localDocExpirySecs() uint32 {
	return c.dbCtx.Options.LocalDocExpirySecs
}

// mutationListener returns mutation level for the database.
func (c *DatabaseCollection) mutationListener() *changeListener {
	return &c.dbCtx.mutationListener
}

// Name returns the name of the collection. If couchbase server is not aware of collections, it will return _default.
func (c *DatabaseCollection) Name() string {
	collection, err := base.AsCollection(c.dataStore)
	if err != nil {
		return base.DefaultCollection
	}
	return collection.CollectionName()

}

// oldRevExpirySeconds is the number of seconds before old revisions are removed from Couchbase server. This is controlled at a database level.
func (c *DatabaseCollection) oldRevExpirySeconds() uint32 {
	return c.dbCtx.Options.OldRevExpirySeconds
}

// queryPaginationLimit limits the size of large queries. This is is controlled at a database level.
func (c *DatabaseCollection) queryPaginationLimit() int {
	return c.dbCtx.Options.QueryPaginationLimit
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

// Name returns the name of the scope the collection is in. If couchbase server is not aware of collections, it will return _default.
func (c *DatabaseCollection) ScopeName() string {
	collection, err := base.AsCollection(c.dataStore)
	if err != nil {
		return base.DefaultScope
	}
	return collection.ScopeName()
}

// RemoveFromChangeCache removes select documents from all channel caches and returns the number of documents removed.
func (c *DatabaseCollection) RemoveFromChangeCache(docIDs []string, startTime time.Time) int {
	return c.changeCache.Remove(c.GetCollectionID(), docIDs, startTime)
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

// Returns the xattr key that will be accessible from the sync function. This is controlled at a database level.
func (c *DatabaseCollection) userXattrKey() string {
	return c.dbCtx.Options.UserXattrKey
}

// UseXattrs specifies whether the collection stores metadata in xattars or inline. This is controlled at a database level.
func (c *DatabaseCollection) UseXattrs() bool {
	return c.dbCtx.Options.EnableXattr
}

// User will return the user object.
func (c *DatabaseCollectionWithUser) User() auth.User {
	return c.user
}

// useViews will return whether the bucket is configured using views.
func (c *DatabaseCollection) useViews() bool {
	return c.dbCtx.Options.UseViews
}
