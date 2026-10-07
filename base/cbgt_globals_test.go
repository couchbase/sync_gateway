// Copyright 2026-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package base

import (
	"crypto/x509"
	"fmt"
	"testing"

	"github.com/couchbase/cbgt"
	"github.com/couchbase/sync_gateway/testing/assert"
	"github.com/couchbase/sync_gateway/testing/require"
)

// TestCbgtGlobalsSameDbNameOnTwoBuckets registers two cbgt managers for databases with the same name on different
// buckets, and makes sure that unregistering one leaves the lookups for the other intact.
func TestCbgtGlobalsSameDbNameOnTwoBuckets(t *testing.T) {
	TestRequiresCbgt(t)
	globals := newCbgtGlobalData()
	const dbName = "db"
	const destKey = dbName + "_import"

	type registration struct {
		mgr        *cbgt.Manager
		bucket     *GocbV2Bucket
		bucketName string
		username   string
	}
	regs := make([]registration, 0, 2)
	for _, bucket := range getTwoGocbV2Buckets(t) {
		regs = append(regs, registration{mgr: &cbgt.Manager{}, bucket: bucket, bucketName: bucket.GetName(), username: "user" + bucket.GetName()})
	}
	for _, reg := range regs {
		globals.registerManager(reg.mgr, cbgtManagerData{
			creds:       cbgtCreds{username: reg.username},
			bucket:      reg.bucket,
			dbName:      dbName,
			destKey:     destKey,
			destFactory: func(func()) (cbgt.Dest, error) { return nil, fmt.Errorf("dest for %s", reg.bucketName) },
		})
	}

	requireRegistered := func(t *testing.T, reg registration) {
		creds, found := globals.getManagerCredentials(reg.mgr)
		require.True(t, found)
		assert.Equal(t, reg.username, creds.username)

		creds, found = globals.getDBCredentials(reg.bucketName, dbName)
		require.True(t, found)
		assert.Equal(t, reg.username, creds.username)

		bucket, found := globals.getBucket(reg.bucketName)
		require.True(t, found)
		assert.Same(t, reg.bucket, bucket)

		destFactory, found := globals.getDestFactory(reg.mgr, destKey)
		require.True(t, found)
		_, err := destFactory(nil)
		assert.EqualError(t, err, "dest for "+reg.bucketName)
		assert.True(t, globals.hasDestKey(destKey))
	}
	requireUnregistered := func(t *testing.T, reg registration) {
		_, found := globals.getManagerCredentials(reg.mgr)
		assert.False(t, found)
		_, found = globals.getDBCredentials(reg.bucketName, dbName)
		assert.False(t, found)
		_, found = globals.getBucket(reg.bucketName)
		assert.False(t, found)
		_, found = globals.getDestFactory(reg.mgr, destKey)
		assert.False(t, found)
	}

	for _, reg := range regs {
		requireRegistered(t, reg)
	}

	globals.unregisterManager(regs[0].mgr)
	requireUnregistered(t, regs[0])
	requireRegistered(t, regs[1])

	globals.unregisterManager(regs[1].mgr)
	requireUnregistered(t, regs[1])
	assert.False(t, globals.hasDestKey(destKey))
}

// TestCbgtRootCAsProviderSameDbNameOnTwoBuckets makes sure that cbgtRootCAsProvider returns the root certificates of
// the database on the requested bucket when another bucket has a database with the same name.
func TestCbgtRootCAsProviderSameDbNameOnTwoBuckets(t *testing.T) {
	TestRequiresCbgt(t)
	const dbName = "db"
	sourceParams, err := JSONMarshal(SGFeedSourceParams{DbName: dbName})
	require.NoError(t, err)

	certPools := make(map[string]*x509.CertPool)
	for _, bucket := range getTwoGocbV2Buckets(t) {
		certPool := x509.NewCertPool()
		certPools[bucket.GetName()] = certPool
		mgr := &cbgt.Manager{}
		cbgtGlobals.registerManager(mgr, cbgtManagerData{
			creds:  cbgtCreds{useTLS: true, certPool: certPool},
			bucket: bucket,
			dbName: dbName,
		})
		t.Cleanup(func() { cbgtGlobals.unregisterManager(mgr) })
	}

	for bucketName, certPool := range certPools {
		certPoolFn := cbgtRootCAsProvider(bucketName, "", string(sourceParams))
		require.NotNil(t, certPoolFn, "bucket %s", bucketName)
		assert.Same(t, certPool, certPoolFn(), "bucket %s", bucketName)
	}
	require.Nil(t, cbgtRootCAsProvider(t.Name()+"otherBucket", "", string(sourceParams)))
}

// getTwoGocbV2Buckets returns two distinct test buckets, which are closed at the end of the test.
func getTwoGocbV2Buckets(t *testing.T) []*GocbV2Bucket {
	ctx := TestCtx(t)
	buckets := make([]*GocbV2Bucket, 0, 2)
	for range 2 {
		bucket := GetTestBucket(t)
		t.Cleanup(func() { bucket.Close(ctx) })
		gocbBucket, err := AsGocbV2Bucket(bucket)
		require.NoError(t, err)
		buckets = append(buckets, gocbBucket)
	}
	return buckets
}
