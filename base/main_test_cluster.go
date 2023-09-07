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
	insertBucket(ctx context.Context, name string, quotaMB int) error
	removeBucket(name string) error
	openTestBucket(ctx context.Context, name tbpBucketName, waitUntilReady time.Duration) (Bucket, error)
	supportsCollections() (bool, error)
	supportsMobileRBAC() (bool, error)
	isServerEnterprise() (bool, error)
	close(context.Context) error
}

type clusterLogFunc func(ctx context.Context, format string, args ...interface{})

// newTestCluster returns a cluster based on the driver used by the defaultBucketSpec.  Accepts a clusterLogFunc to support
// cluster logging within a test bucket pool context
func newTestCluster(ctx context.Context, server string, logger clusterLogFunc) tbpCluster {
	return newTestClusterV2(ctx, server, logger)
}

// tbpClusterV2 implements the tbpCluster interface for a gocb v2 cluster
type tbpClusterV2 struct {
	logger clusterLogFunc
	server string

	// cluster can be used to perform cluster-level operations (but not bucket-level operations)
	cluster *gocb.Cluster
}

var _ tbpCluster = &tbpClusterV2{}

func newTestClusterV2(ctx context.Context, server string, logger clusterLogFunc) *tbpClusterV2 {
	tbpCluster := &tbpClusterV2{}
	tbpCluster.logger = logger
	tbpCluster.cluster = initV2Cluster(ctx, server)
	tbpCluster.server = server
	return tbpCluster
}

// initV2Cluster makes cluster connection.  Callers must close.
func initV2Cluster(ctx context.Context, server string) *gocb.Cluster {

	testClusterTimeout := 10 * time.Second
	spec := BucketSpec{
		Server:          server,
		TLSSkipVerify:   true,
		BucketOpTimeout: &testClusterTimeout,
	}

	connStr, err := spec.GetGoCBConnString(nil)
	if err != nil {
		FatalfCtx(ctx, "error getting connection string: %v", err)
	}

	securityConfig, err := GoCBv2SecurityConfig(ctx, &spec.TLSSkipVerify, spec.CACertPath)
	if err != nil {
		FatalfCtx(ctx, "Couldn't initialize cluster security config: %v", err)
	}

	authenticatorConfig, authErr := GoCBv2Authenticator(TestClusterUsername(), TestClusterPassword(), spec.Certpath, spec.Keypath)
	if authErr != nil {
		FatalfCtx(ctx, "Couldn't initialize cluster authenticator config: %v", authErr)
	}

	timeoutsConfig := GoCBv2TimeoutsConfig(spec.BucketOpTimeout, StdlibDurationPtr(spec.GetViewQueryTimeout()))

	clusterOptions := gocb.ClusterOptions{
		Authenticator:  authenticatorConfig,
		SecurityConfig: securityConfig,
		TimeoutsConfig: timeoutsConfig,
	}

	cluster, err := gocb.Connect(connStr, clusterOptions)
	if err != nil {
		FatalfCtx(ctx, "Couldn't connect to %q: %v", server, err)
	}
	const clusterReadyTimeout = 90 * time.Second
	err = cluster.WaitUntilReady(clusterReadyTimeout, nil)
	if err != nil {
		FatalfCtx(ctx, "Cluster not ready after %ds: %v", int(clusterReadyTimeout.Seconds()), err)
	}
	return cluster
}

// isServerEnterprise returns true if the connected returns true if the connected couchbase server
// instance is Enterprise edition And false for Community edition
func (c *tbpClusterV2) isServerEnterprise() (bool, error) {
	metadata, err := c.cluster.Internal().GetNodesMetadata(&gocb.GetNodesMetadataOptions{})
	if err != nil {
		return false, err
	}

	if strings.Contains(metadata[0].Version, "enterprise") {
		return true, nil
	}
	return false, nil
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

func (c *tbpClusterV2) insertBucket(ctx context.Context, name string, quotaMB int) error {

	settings := gocb.CreateBucketSettings{
		BucketSettings: gocb.BucketSettings{
			Name:         name,
			RAMQuotaMB:   uint64(quotaMB),
			BucketType:   gocb.CouchbaseBucketType,
			FlushEnabled: true,
			NumReplicas:  tbpNumReplicas(ctx),
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
func (c *tbpClusterV2) openTestBucket(ctx context.Context, testBucketName tbpBucketName, waitUntilReady time.Duration) (Bucket, error) {

	bucketCluster := initV2Cluster(ctx, c.server)

	// bucketSpec := getTestBucketSpec(testBucketName, usingNamedCollections)
	bucketSpec := getTestBucketSpec(testBucketName)

	bucketFromSpec, err := GetGocbV2BucketFromCluster(ctx, bucketCluster, bucketSpec, waitUntilReady, false)
	if err != nil {
		return nil, err
	}

	return bucketFromSpec, nil
}

func (c *tbpClusterV2) close(ctx context.Context) error {
	// no close operations needed
	if c.cluster != nil {
		if err := c.cluster.Close(nil); err != nil {
			c.logger(ctx, "Couldn't close cluster connection: %v", err)
			return err
		}
	}
	return nil
}

func (c *tbpClusterV2) getMinClusterCompatVersion(ctx context.Context) int {
	nodesMeta, err := c.cluster.Internal().GetNodesMetadata(nil)
	if err != nil {
		FatalfCtx(ctx, "TEST: failed to fetch nodes metadata: %v", err)
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

// supportsMobileRBAC is true if running couchbase server with all Sync Gateway roles
func (c *tbpClusterV2) supportsMobileRBAC() (bool, error) {
	isEE, err := c.isServerEnterprise()
	if err != nil {
		return false, err
	}
	// mobile RBAC is only supported on EE
	if !isEE {
		return false, nil
	}
	// mobile RBAC is only supported on 7.1+
	major, minor, err := getClusterVersion(c.cluster)
	if err != nil {
		return false, err
	}
	return major >= 7 && minor >= 1, nil
}
