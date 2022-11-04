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

	"github.com/couchbase/sync_gateway/auth"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"
)

// DatabaseCollection provides a representation of a single collection of a database.
type DatabaseCollection struct {
	Bucket        base.Bucket      // Storage
	revisionCache RevisionCache    // Cache of recently-accessed doc revisions
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

// Authenticator returns authentication options associated with the collection's database.
func (c *DatabaseCollection) Authenticator(ctx context.Context) *auth.Authenticator {
	return c.dbCtx.Authenticator(ctx)
}

// backupOldRev creates a temporary copy of a old revision body when resolving conflicts. This is controlled at the database level.
func (c *DatabaseCollection) backupOldRev() bool {
	return c.dbCtx.Options.ImportOptions.BackupOldRev

}

// changeCache returns the set of channels in memory. This is currently at the database level.
func (c *DatabaseCollection) changeCache() *changeCache {
	return c.dbCtx.changeCache
}

// channelMapper runs the javascript sync function. The is currently at the database level.
func (c *DatabaseCollection) channelMapper() *channels.ChannelMapper {
	return c.dbCtx.ChannelMapper
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

// GetReivisonCacheForTest allow accessing a copy of revision cache.
func (context *DatabaseCollection) GetRevisionCacheForTest() RevisionCache {
	return context.revisionCache
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

// Name returns the name of the collection. If couchbase server is not aware of collections, it will return _default.
func (c *DatabaseCollection) Name() string {
	collection, err := base.AsCollection(c.Bucket)
	if err != nil {
		return base.DefaultCollection
	}
	return collection.Name()

}

// oldRevExpirySeconds is the number of seconds before old revisions are removed from Couchbase server. This is controlled at a database level.
func (c *DatabaseCollection) oldRevExpirySeconds() uint32 {
	return c.dbCtx.Options.OldRevExpirySeconds
}

// Name returns the name of the scope the collection is in. If couchbase server is not aware of collections, it will return _default.
func (c *DatabaseCollection) ScopeName() string {
	collection, err := base.AsCollection(c.Bucket)
	if err != nil {
		return base.DefaultScope
	}
	return collection.ScopeName()
}

// revsLimit is the max depth a document's revision tree can grow to. This is controlled at a database level.
func (c *DatabaseCollection) revsLimit() uint32 {
	return c.dbCtx.RevsLimit
}

// sequences returns the sequence generator for a collection.
func (c *DatabaseCollection) sequences() *sequenceAllocator {
	return c.dbCtx.sequences
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
