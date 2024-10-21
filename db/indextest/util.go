package indextest

import (
	"context"
	"testing"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db"
	"github.com/stretchr/testify/require"
)

func getDatabaseContextOptions(isServerless bool) db.DatabaseContextOptions {
	defaultCacheOptions := db.DefaultCacheOptions()

	return db.DatabaseContextOptions{
		CacheOptions: &defaultCacheOptions,
		Serverless:   isServerless,
	}
}

// setupN1QLStore initializes the indexes for a database. This is normally done by the rest package
func setupN1QLStore(ctx context.Context, t *testing.T, bucket base.Bucket, isServerless, useN1qlCluster bool) {
	testBucket, ok := bucket.(*base.TestBucket)
	require.True(t, ok)

	hasOnlyDefaultDataStore := len(testBucket.GetNonDefaultDatastoreNames()) == 0

	defaultDataStore := bucket.DefaultDataStore()
	defaultN1QLStore, ok := base.AsN1QLStore(defaultDataStore)
	require.True(t, ok, "Unable to get n1QLStore for defaultDataStore")
	options := db.InitializeIndexOptions{
		NumReplicas: 0,
		Serverless:  isServerless,
		UseXattrs:   base.TestUseXattrs(),
	}
	if hasOnlyDefaultDataStore {
		options.MetadataIndexes = db.IndexesAll
	} else {
		options.MetadataIndexes = db.IndexesMetadataOnly
	}
	ctx = base.CollectionLogCtx(ctx, defaultDataStore.ScopeName(), defaultDataStore.CollectionName())
	require.NoError(t, db.InitializeIndexes(ctx, defaultN1QLStore, options))
	if hasOnlyDefaultDataStore {
		return
	}
	options = db.InitializeIndexOptions{
		NumReplicas:     0,
		Serverless:      isServerless,
		UseXattrs:       base.TestUseXattrs(),
		MetadataIndexes: db.IndexesWithoutMetadata,
	}

	dataStore, err := testBucket.GetNamedDataStore(0)
	require.NoError(t, err)

	var n1qlStore base.N1QLStore
	if useN1qlCluster {
		gocbBucket, err := base.AsGocbV2Bucket(testBucket)
		require.NoError(t, err)

		n1qlStore, err = base.NewClusterOnlyN1QLStore(gocbBucket.GetCluster(), gocbBucket.BucketName(), dataStore.ScopeName(), dataStore.CollectionName())
		require.NoError(t, err)
	} else {
		var ok bool
		n1qlStore, ok = base.AsN1QLStore(dataStore)
		require.True(t, ok)
	}

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
