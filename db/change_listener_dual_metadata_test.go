// Copyright 2026-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package db

import (
	"testing"

	"github.com/couchbase/sync_gateway/base"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCachingFeedCollections_DualMetadataStore verifies that when the metadata store is a
// *base.MetadataStore, the caching feed subscribes to both the primary (_system._mobile)
// and fallback (_default._default) datastores so that _sync:* mutations in either
// collection are observed during the metadata migration window.
func TestCachingFeedCollections_DualMetadataStore(t *testing.T) {
	ctx := base.TestCtx(t)
	bucket := base.GetTestBucket(t)
	defer bucket.Close(ctx)

	primary := bucket.GetMobileSystemDataStore() // skips if backing store does not support system collections
	fallback := bucket.DefaultDataStore(ctx)
	ms := base.NewMetadataStore(primary, fallback)

	got := cachingFeedCollections(ms, nil)

	require.Contains(t, got, base.SystemScope, "expected %s scope to be present", base.SystemScope)
	assert.Contains(t, got[base.SystemScope], base.SystemCollectionMobile)

	require.Contains(t, got, base.DefaultScope, "expected %s scope to be present", base.DefaultScope)
	assert.Contains(t, got[base.DefaultScope], base.DefaultCollection)
}

// TestCachingFeedCollectionsDualMetadataStoreMigrationComplete verifies that once the dual
// metadata store reports migration complete, the caching feed subscribes only to the primary
// (_system._mobile) and omits the fallback (_default._default). This prevents the DCP feed Start
// from failing when a customer has dropped _default._default after migration completed.
func TestCachingFeedCollectionsDualMetadataStoreMigrationComplete(t *testing.T) {
	ctx := base.TestCtx(t)
	bucket := base.GetTestBucket(t)
	defer bucket.Close(ctx)

	primary := bucket.GetMobileSystemDataStore() // skips if backing store does not support system collections
	fallback := bucket.DefaultDataStore(ctx)
	ms := base.NewMetadataStore(primary, fallback)
	ms.SetMigrationComplete()

	got := cachingFeedCollections(ms, nil)

	require.Contains(t, got, base.SystemScope, "expected %s scope to be present", base.SystemScope)
	assert.Contains(t, got[base.SystemScope], base.SystemCollectionMobile)

	// The fallback (_default._default) must not be subscribed at all once migration is complete, so
	// the _default scope must be absent from the result entirely.
	assert.NotContains(t, got, base.DefaultScope, "fallback _default._default must not be subscribed after migration completes")
}

// TestCachingFeedCollections_SingleMetadataStore verifies that when the metadata store is a
// plain DataStore (not the dual wrapper), the caching feed subscribes only to that store's
// collection — preserving the pre-CBG-5223 behaviour.
func TestCachingFeedCollections_SingleMetadataStore(t *testing.T) {
	ctx := base.TestCtx(t)
	bucket := base.GetTestBucket(t)
	defer bucket.Close(ctx)

	metadataStore := bucket.DefaultDataStore(ctx)

	got := cachingFeedCollections(metadataStore, nil)

	require.Contains(t, got, base.DefaultScope)
	assert.Contains(t, got[base.DefaultScope], base.DefaultCollection)
	// Should not have added any other scope (notably _system).
	assert.Len(t, got, 1, "expected exactly one scope in single-metadata-store case, got %v", got)
}

// TestCachingFeedCollections_UserScopesIncluded verifies that user-data scopes/collections
// are appended to the set regardless of whether the metadata store is a dual wrapper.
func TestCachingFeedCollections_UserScopesIncluded(t *testing.T) {
	ctx := base.TestCtx(t)
	bucket := base.GetTestBucket(t)
	defer bucket.Close(ctx)

	primary := bucket.GetMobileSystemDataStore()
	fallback := bucket.DefaultDataStore(ctx)
	ms := base.NewMetadataStore(primary, fallback)

	scopes := map[string]Scope{
		"myScope": {
			Collections: map[string]*DatabaseCollection{
				"collA": nil,
				"collB": nil,
			},
		},
	}

	got := cachingFeedCollections(ms, scopes)

	require.Contains(t, got, base.SystemScope)
	assert.Contains(t, got[base.SystemScope], base.SystemCollectionMobile)
	require.Contains(t, got, base.DefaultScope)
	assert.Contains(t, got[base.DefaultScope], base.DefaultCollection)
	require.Contains(t, got, "myScope")
	assert.Contains(t, got["myScope"], "collA")
	assert.Contains(t, got["myScope"], "collB")
}
