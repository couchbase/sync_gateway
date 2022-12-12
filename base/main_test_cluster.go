// Copyright 2022-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package base

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/couchbase/gocb/v2"
)

// tbpCluster defines the required test bucket pool cluster operations
type tbpCluster interface {
	getBucketNames() ([]string, error)
	insertBucket(name string, quotaMB int) error
	removeBucket(name string) error
	openTestBucket(name tbpBucketName, waitUntilReady time.Duration) (Bucket, error)
	supportsCollections() (bool, error)
	close() error
}

type clusterLogFunc func(ctx context.Context, format string, args ...interface{})

// newTestCluster returns a cluster based on the driver used by the defaultBucketSpec.  Accepts a clusterLogFunc to support
// cluster logging within a test bucket pool context
func newTestCluster(server string, logger clusterLogFunc) tbpCluster {
	return newTestClusterV2(server, logger)
}

// tbpClusterV2 implements the tbpCluster interface for a gocb v2 cluster
type tbpClusterV2 struct {
	logger clusterLogFunc
	server string

	// cluster can be used to perform cluster-level operations (but not bucket-level operations)
	cluster *gocb.Cluster
}

var _ tbpCluster = &tbpClusterV2{}

func newTestClusterV2(server string, logger clusterLogFunc) *tbpClusterV2 {
	tbpCluster := &tbpClusterV2{}
	tbpCluster.logger = logger
	tbpCluster.cluster = initV2Cluster(server)
	tbpCluster.server = server
	return tbpCluster
}

// initV2Cluster makes cluster connection.  Callers must close.
func initV2Cluster(server string) *gocb.Cluster {

	testClusterTimeout := 10 * time.Second
	spec := BucketSpec{
		Server:          server,
		TLSSkipVerify:   true,
		BucketOpTimeout: &testClusterTimeout,
	}

	connStr, err := spec.GetGoCBConnString(nil)
	if err != nil {
		FatalfCtx(context.TODO(), "error getting connection string: %v", err)
	}

	securityConfig, err := GoCBv2SecurityConfig(&spec.TLSSkipVerify, spec.CACertPath)
	if err != nil {
		FatalfCtx(context.TODO(), "Couldn't initialize cluster security config: %v", err)
	}

	authenticatorConfig, authErr := GoCBv2Authenticator(TestClusterUsername(), TestClusterPassword(), spec.Certpath, spec.Keypath)
	if authErr != nil {
		FatalfCtx(context.TODO(), "Couldn't initialize cluster authenticator config: %v", authErr)
	}

	timeoutsConfig := GoCBv2TimeoutsConfig(spec.BucketOpTimeout, StdlibDurationPtr(spec.GetViewQueryTimeout()))

	clusterOptions := gocb.ClusterOptions{
		Authenticator:  authenticatorConfig,
		SecurityConfig: securityConfig,
		TimeoutsConfig: timeoutsConfig,
	}

	cluster, err := gocb.Connect(connStr, clusterOptions)
	if err != nil {
		FatalfCtx(context.TODO(), "Couldn't connect to %q: %v", server, err)
	}
	const clusterReadyTimeout = 90 * time.Second
	err = cluster.WaitUntilReady(clusterReadyTimeout, nil)
	if err != nil {
		FatalfCtx(context.TODO(), "Cluster not ready after %ds: %v", int(clusterReadyTimeout.Seconds()), err)
	}
	return cluster
}

func (c *tbpClusterV2) getBucketNames() ([]string, error) {

	bucketSettings, err := c.cluster.Buckets().GetAllBuckets(nil)
	if err != nil {
		return nil, fmt.Errorf("couldn't get buckets from cluster: %w", err)
	}

	var names []string
	for name, _ := range bucketSettings {
		names = append(names, name)
	}

	return names, nil
}

func (c *tbpClusterV2) insertBucket(name string, quotaMB int) error {

	settings := gocb.CreateBucketSettings{
		BucketSettings: gocb.BucketSettings{
			Name:         name,
			RAMQuotaMB:   uint64(quotaMB),
			BucketType:   gocb.CouchbaseBucketType,
			FlushEnabled: true,
			NumReplicas:  0,
		},
	}

	options := &gocb.CreateBucketOptions{
		Timeout: 10 * time.Second,
	}
	return c.cluster.Buckets().CreateBucket(settings, options)
}

func (c *tbpClusterV2) removeBucket(name string) error {
	return c.cluster.Buckets().DropBucket(name, nil)
}

// openTestBucket opens the bucket of the given name for the gocb cluster in the given TestBucketPool.
func (c *tbpClusterV2) openTestBucket(testBucketName tbpBucketName, waitUntilReady time.Duration) (Bucket, error) {

	bucketCluster := initV2Cluster(c.server)

	// bucketSpec := getTestBucketSpec(testBucketName, usingNamedCollections)
	bucketSpec := getTestBucketSpec(testBucketName)

	bucketFromSpec, err := GetGocbV2BucketFromCluster(bucketCluster, bucketSpec, waitUntilReady, false)
	if err != nil {
		return nil, err
	}

	return bucketFromSpec, nil
}

func (c *tbpClusterV2) close() error {
	// no close operations needed
	if c.cluster != nil {
		if err := c.cluster.Close(nil); err != nil {
			c.logger(context.Background(), "Couldn't close cluster connection: %v", err)
			return err
		}
	}
	return nil
}

func (c *tbpClusterV2) getMinClusterCompatVersion() int {
	nodesMeta, err := c.cluster.Internal().GetNodesMetadata(nil)
	if err != nil {
		FatalfCtx(context.Background(), "TEST: failed to fetch nodes metadata: %v", err)
	}
	if len(nodesMeta) < 1 {
		panic("invalid NodesMetadata: no nodes")
	}
	return nodesMeta[0].ClusterCompatibility
}

func (c *tbpClusterV2) supportsCollections() (bool, error) {
	major, _, err := getClusterVersion(c.cluster)
	if err != nil {
		return false, err
	}
	return major >= 7, nil
}

// dropAllScopesAndCollections attempts to drop *all* non-_default scopes and collections from the bucket associated with the collection, except those used by the test bucket pool. Intended for test usage only.
func dropAllScopesAndCollections(bucket *gocb.Bucket) error {
	cm := bucket.Collections()
	scopes, err := cm.GetAllScopes(nil)
	if err != nil {
		if httpErr, ok := err.(gocb.HTTPError); ok && httpErr.StatusCode == 404 {
			return ErrCollectionsUnsupported
		}
		WarnfCtx(context.TODO(), "Error getting scopes on bucket %s: %v  Will retry.", MD(bucket.Name()).Redact(), err)
		return err
	}

	// For each non-default scope, drop them.
	// For each collection within the default scope, drop them.
	for _, scope := range scopes {
		if scope.Name != DefaultScope && !strings.HasPrefix(scope.Name, tbpScopePrefix) {
			scopeName := fmt.Sprintf("scope %s on bucket %s", MD(scope).Redact(), MD(bucket.Name()).Redact())
			TracefCtx(context.TODO(), KeyAll, "Dropping %s", scopeName)
			if err := cm.DropScope(scope.Name, nil); err != nil {
				WarnfCtx(context.TODO(), "Error dropping %s: %v  Will retry.", scopeName, err)
				return err
			}
			continue
		}

		// can't delete _default scope - but we can delete the non-_default collections within it
		for _, collection := range scope.Collections {
			if collection.Name != DefaultCollection && !strings.HasPrefix(collection.Name, tbpCollectionPrefix) {
				collectionName := fmt.Sprintf("collection %s in scope %s on bucket %s", MD(collection.Name).Redact(), MD(scope).Redact(), MD(bucket.Name()).Redact())
				TracefCtx(context.TODO(), KeyAll, "Dropping %s", collectionName)
				if err := cm.DropCollection(collection, nil); err != nil {
					WarnfCtx(context.TODO(), "Error dropping %s: %v  Will retry.", collectionName, err)
					return err
				}
			}
		}

	}
	return nil
}
