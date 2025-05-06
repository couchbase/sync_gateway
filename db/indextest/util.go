// Copyright 2024-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package indextest

import (
	"context"
	"testing"

	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db"
	"github.com/stretchr/testify/require"
)

func getDatabaseContextOptions(useLegacySyncDocsIndex bool) db.DatabaseContextOptions {
	defaultCacheOptions := db.DefaultCacheOptions()

	return db.DatabaseContextOptions{
		CacheOptions:           &defaultCacheOptions,
		UseLegacySyncDocsIndex: useLegacySyncDocsIndex,
	}
}

// testIndexCreationOptions are used for db.indextest package to create database like rest.DatabaseInitManager would
type testIndexCreationOptions struct {
	numPartitions                uint32
	useLegacySyncDocsIndex       bool
	useXattrs                    bool
	forceSingleDefaultCollection bool
	numCollections               int // if specified forces the number of collections to be created
}

// setupIndexes initializes the indexes for a database. This is normally done by the rest.DatabaseInitManager
func setupIndexes(t *testing.T, bucket base.Bucket, createOpts testIndexCreationOptions) {
	testBucket, ok := bucket.(*base.TestBucket)
	require.True(t, ok)

	hasOnlyDefaultDataStore := len(testBucket.GetNonDefaultDatastoreNames()) == 0 || createOpts.forceSingleDefaultCollection

	options := db.InitializeIndexOptions{
		NumReplicas:         0,
		LegacySyncDocsIndex: createOpts.useLegacySyncDocsIndex,
		UseXattrs:           createOpts.useXattrs,
		NumPartitions:       createOpts.numPartitions,
	}
	if hasOnlyDefaultDataStore {
		options.MetadataIndexes = db.IndexesAll
	} else {
		options.MetadataIndexes = db.IndexesMetadataOnly
	}
	ctx := base.TestCtx(t)
	initializeCollectionIndexes(ctx, t, testBucket, bucket.DefaultDataStore(), options)
	if hasOnlyDefaultDataStore {
		return
	}
	options.MetadataIndexes = db.IndexesWithoutMetadata

	numCollections := 1
	if createOpts.numCollections != 0 {
		numCollections = createOpts.numCollections
	}
	for collectionIndex := range numCollections {
		dataStore, err := testBucket.GetNamedDataStore(collectionIndex)
		require.NoError(t, err)

		initializeCollectionIndexes(ctx, t, testBucket, dataStore, options)
	}
}

// initializeCollectionIndexes initializes the indexes for a data store like rest.DatabaseInitManager's DatabaseInitWorker
func initializeCollectionIndexes(ctx context.Context, t *testing.T, testBucket base.Bucket, dsName sgbucket.DataStoreName, options db.InitializeIndexOptions) {
	gocbBucket, err := base.AsGocbV2Bucket(testBucket)
	require.NoError(t, err)

	n1qlStore, err := base.NewClusterOnlyN1QLStore(gocbBucket.GetCluster(), gocbBucket.BucketName(), dsName.ScopeName(), dsName.CollectionName())
	require.NoError(t, err)

	ctx = base.CollectionLogCtx(ctx, dsName.ScopeName(), dsName.CollectionName())
	require.NoError(t, db.InitializeIndexes(ctx, n1qlStore, options))
}

func requireCoveredQuery(t *testing.T, database *db.Database, statement string, isCovered bool) {
	n1QLStore, ok := base.AsN1QLStore(database.MetadataStore)
	require.True(t, ok)
	plan, explainErr := n1QLStore.ExplainQuery(base.TestCtx(t), statement, nil)
	require.NoError(t, explainErr, "Error generating explain for %+v", statement)

	covered := db.IsCovered(plan)
	planJSON, err := base.JSONMarshal(plan)
	require.NoError(t, err)
	require.Equal(t, isCovered, covered, "query covered by index; expectedToBeCovered: %t, Plan: %s", isCovered, planJSON)
}

// setupIndexAndDB creates the indexes for a database like rest.DatabaseInitManager and creates an online test database. The bucket and database will be cleaned up by testing.T.Cleanup.
func setupIndexAndDB(t *testing.T, opts testIndexCreationOptions) *db.Database {
	bucket := base.GetTestBucket(t)
	ctx := base.TestCtx(t)
	t.Cleanup(func() { bucket.Close(ctx) })
	setupIndexes(t, bucket, opts)
	dbOptions := getDatabaseContextOptions(opts.useLegacySyncDocsIndex)
	numCollections := 1
	if opts.numCollections != 0 {
		numCollections = opts.numCollections
	}
	if opts.forceSingleDefaultCollection {
		dbOptions.Scopes = db.GetScopesOptionsDefaultCollectionOnly(t)
	} else {
		dbOptions.Scopes = db.GetScopesOptions(t, bucket, numCollections)
	}
	dbOptions.EnableXattr = opts.useXattrs

	database, ctx := db.CreateTestDatabase(t, bucket, dbOptions)

	t.Cleanup(func() { database.Close(ctx) })
	return database
}
