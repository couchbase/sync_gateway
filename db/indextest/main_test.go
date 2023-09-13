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
	"errors"
	"fmt"
	"testing"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db"
)

func TestMain(m *testing.M) {
	// these tests are only meant to be be run against Couchbase Server with GSI
	if base.UnitTestUrlIsWalrus() || base.TestsDisableGSI() {
		return
	}
	ctx := context.Background() // start of test process
	tbpOptions := base.TestBucketPoolOptions{MemWatermarkThresholdMB: 2048}
	base.TestBucketPoolMain(ctx, m, primaryIndexReadier, primaryIndexInit, tbpOptions)
}

// primaryIndexInit is run synchronously only once per-bucket to create a primary index.
var primaryIndexInit base.TBPBucketInitFunc = func(ctx context.Context, b base.Bucket, tbp *base.TestBucketPool) error {
	tbp.Logf(ctx, "Starting bucket init function")

	dataStores, err := b.ListDataStores()
	if err != nil {
		return err
	}

	for _, dataStoreName := range dataStores {
		dataStore, err := b.NamedDataStore(dataStoreName)
		if err != nil {
			return err
		}

		n1qlStore, ok := base.AsN1QLStore(dataStore)
		if !ok {
			return fmt.Errorf("bucket %T was not a N1QL store", b)
		}

		tbp.Logf(ctx, "dropping existing bucket indexes")
		if err := base.DropAllIndexes(ctx, n1qlStore); err != nil {
			tbp.Logf(ctx, "Failed to drop bucket indexes: %v", err)
			return err
		}

		err = n1qlStore.CreatePrimaryIndex(ctx, base.PrimaryIndexName, nil)
		if err != nil {
			return err
		}
		tbp.Logf(ctx, "finished creating SG bucket indexes")
	}
	return nil
}

// primaryIndexReadier empties the bucket using the primary index. It is run asynchronously as soon as a test is finished with a bucket.
var primaryIndexReadier base.TBPBucketReadierFunc = func(ctx context.Context, b base.Bucket, tbp *base.TestBucketPool) error {
	tbp.Logf(ctx, "emptying bucket via N1QL primary index")
	if err := base.N1QLBucketEmptierFunc(ctx, b, tbp); err != nil {
		return err
	}

	dataStores, err := b.ListDataStores()
	if err != nil {
		return err
	}
	for _, dataStoreName := range dataStores {
		dataStore, err := b.NamedDataStore(dataStoreName)
		if err != nil {
			return err
		}
		dsName, ok := base.AsDataStoreName(dataStore)
		if !ok {
			err := fmt.Errorf("Could not determine datastore name from datastore: %+v", dataStore)
			tbp.Logf(ctx, "%s", err)
			return err
		}
		tbp.Logf(ctx, "dropping existing bucket indexes")

		if err := db.EmptyPrimaryIndex(ctx, dataStore); err != nil {
			return err
		}
		n1qlStore, ok := base.AsN1QLStore(dataStore)
		if !ok {
			return errors.New("attempting to empty indexes with non-N1QL store")
		}
		// assert no lost indexes
		indexes, err := n1qlStore.GetIndexes()
		if err != nil {
			return err
		}
		if len(indexes) != 1 && indexes[0] != base.PrimaryIndexName {
			return fmt.Errorf("expected only primary index to be present, found: %v", indexes)
		}
		tbp.Logf(ctx, "waiting for empty bucket indexes %s.%s.%s", b.GetName(), dsName.ScopeName(), dsName.CollectionName())
		// wait for primary index to be empty
		if err := db.WaitForPrimaryIndexEmpty(ctx, n1qlStore); err != nil {
			tbp.Logf(ctx, "waitForPrimaryIndexEmpty returned an error: %v", err)
			return err
		}
		tbp.Logf(ctx, "bucket primary index empty")
	}
	return nil
}
