/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package indextest

import (
	"context"
	"fmt"
	"strings"
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

func setupTestDBForBucketWithOptions(t testing.TB, dbcOptions db.DatabaseContextOptions) (*db.Database, context.Context) {
	ctx := base.TestCtx(t)
	tBucket := base.GetTestBucket(t)
	AddOptionsFromEnvironmentVariables(&dbcOptions)
	dbCtx, err := db.NewDatabaseContext(ctx, "db", tBucket, false, dbcOptions)
	require.NoError(t, err, "Couldn't create context for database 'db'")
	db, err := db.CreateDatabase(dbCtx)
	require.NoError(t, err, "Couldn't create database 'db'")
	ctx = db.AddDatabaseLogContext(ctx)
	return db, ctx
}

type resetN1QLStoreFn func(n1QLStore base.N1QLStore, isServerless bool) error

func setupN1QLStore(bucket base.Bucket, isServerless bool) (base.N1QLStore, resetN1QLStoreFn, error) {
	n1QLStore, ok := base.AsN1QLStore(bucket)
	if !ok {
		return nil, nil, fmt.Errorf("Unable to get n1QLStore for testBucket")
	}

	if err := db.InitializeIndexes(n1QLStore, base.TestUseXattrs(), 0, false, isServerless); err != nil {
		return nil, nil, err
	}

	return n1QLStore, clearIndexes, nil
}

var clearIndexes resetN1QLStoreFn = func(n1QLStore base.N1QLStore, isServerless bool) error {
	indexes := db.GetIndexesName(isServerless, base.TestUseXattrs())
	for _, index := range indexes {
		err := n1QLStore.DropIndex(index)
		if err != nil && strings.Contains(err.Error(), "Index not exist") {
			return err
		}
	}
	return nil
}

// If certain environment variables are set, for example to turn on XATTR support, then update
// the DatabaseContextOptions accordingly
func AddOptionsFromEnvironmentVariables(dbcOptions *db.DatabaseContextOptions) {
	if base.TestUseXattrs() {
		dbcOptions.EnableXattr = true
	}

	if base.TestsDisableGSI() {
		dbcOptions.UseViews = true
	}
}
