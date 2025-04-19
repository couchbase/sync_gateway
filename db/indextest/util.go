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

// setupN1QLStore initializes the indexes for a database. This is normally done by the rest package
func setupN1QLStore(ctx context.Context, t *testing.T, bucket base.Bucket, useLegacySyncDocsIndex bool, numPartitions uint32) {
	testBucket, ok := bucket.(*base.TestBucket)
	require.True(t, ok)

	hasOnlyDefaultDataStore := len(testBucket.GetNonDefaultDatastoreNames()) == 0

	options := db.InitializeIndexOptions{
		NumReplicas:         0,
		LegacySyncDocsIndex: useLegacySyncDocsIndex,
		UseXattrs:           base.TestUseXattrs(),
		NumPartitions:       numPartitions,
	}
	if hasOnlyDefaultDataStore {
		options.MetadataIndexes = db.IndexesAll
	} else {
		options.MetadataIndexes = db.IndexesMetadataOnly
	}
	initializeIndexes(ctx, t, testBucket, bucket.DefaultDataStore(), options)
	if hasOnlyDefaultDataStore {
		return
	}
	options = db.InitializeIndexOptions{
		NumReplicas:         0,
		LegacySyncDocsIndex: useLegacySyncDocsIndex,
		UseXattrs:           base.TestUseXattrs(),
		MetadataIndexes:     db.IndexesWithoutMetadata,
		NumPartitions:       numPartitions,
	}

	dataStore, err := testBucket.GetNamedDataStore(0)
	require.NoError(t, err)

	initializeIndexes(ctx, t, testBucket, dataStore, options)
}

// initializeIndexes initializes the indexes for a data store like rest.DatabaseInitManager's DatabaseInitWorker
func initializeIndexes(ctx context.Context, t *testing.T, testBucket base.Bucket, dsName sgbucket.DataStoreName, options db.InitializeIndexOptions) {
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
