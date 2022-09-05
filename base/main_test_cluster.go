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
	"errors"
	"fmt"
	"time"

	"github.com/couchbase/gocb/v2"
	gocbv1 "gopkg.in/couchbase/gocb.v1"
)

// tbpCluster defines the required test bucket pool cluster operations
type tbpCluster interface {
	getBucketNames() ([]string, error)
	insertBucket(name string, quotaMB int) error
	removeBucket(name string) error
	openTestBucket(name tbpBucketName, waitUntilReadySeconds int) (Bucket, error)
	close() error
}

type clusterLogFunc func(ctx context.Context, format string, args ...interface{})

// newTestCluster returns a cluster based on the driver used by the defaultBucketSpec.  Accepts a clusterLogFunc to support
// cluster logging within a test bucket pool context
func newTestCluster(server string, logger clusterLogFunc) tbpCluster {
	if tbpDefaultBucketSpec.CouchbaseDriver == GoCBv2 {
		return newTestClusterV2(server, logger)
	} else {
		return newTestClusterV1(server, logger)
	}

}

var _ tbpCluster = &tbpClusterV1{}

// tbpClusterV1 implements the tbpCluster interface for a gocb v1 cluster
type tbpClusterV1 struct {
	cluster    *gocbv1.Cluster
	clusterMgr *gocbv1.ClusterManager
	logger     clusterLogFunc
}

func newTestClusterV1(server string, logger clusterLogFunc) *tbpClusterV1 {
	tbpCluster := &tbpClusterV1{}
	tbpCluster.cluster = initV1Cluster(server)
	tbpCluster.clusterMgr = tbpCluster.cluster.Manager(TestClusterUsername(), TestClusterPassword())
	tbpCluster.logger = logger
	return tbpCluster
}

// tbpCluster returns an authenticated gocb Cluster for the given server URL.
func initV1Cluster(server string) *gocbv1.Cluster {
	spec := BucketSpec{
		Server: server,
	}

	connStr, err := spec.GetGoCBConnString(nil)
	if err != nil {
		FatalfCtx(context.TODO(), "error getting connection string: %v", err)
	}

	cluster, err := gocbv1.Connect(connStr)
	if err != nil {
		FatalfCtx(context.TODO(), "Couldn't connect to %q: %v", server, err)
	}

	err = cluster.Authenticate(gocbv1.PasswordAuthenticator{
		Username: TestClusterUsername(),
		Password: TestClusterPassword(),
	})
	if err != nil {
		FatalfCtx(context.TODO(), "Couldn't authenticate with %q: %v", server, err)
	}

	return cluster
}

func (c *tbpClusterV1) getBucketNames() ([]string, error) {
	buckets, err := c.clusterMgr.GetBuckets()
	if err != nil {
		// special handling for gocb's empty non-nil error if we send this request with invalid credentials
		if err.Error() == "" {
			err = errors.New("couldn't get buckets from cluster, check authentication credentials")
		}
		return nil, err
	}

	var names []string
	for _, b := range buckets {
		names = append(names, b.Name)
	}

	return names, nil
}

func (c *tbpClusterV1) insertBucket(name string, quotaMB int) error {
	return c.clusterMgr.InsertBucket(&gocbv1.BucketSettings{
		Name:          name,
		Quota:         quotaMB,
		Type:          gocbv1.Couchbase,
		FlushEnabled:  true,
		IndexReplicas: false,
		Replicas:      0,
	})
}

func (c *tbpClusterV1) removeBucket(name string) error {
	return c.clusterMgr.RemoveBucket(name)
}

// openTestBucket opens the bucket of the given name for the gocb cluster in the given TestBucketPool.
func (c *tbpClusterV1) openTestBucket(testBucketName tbpBucketName, waitUntilReadySeconds int) (Bucket, error) {

	sleeper := CreateSleeperFunc(waitUntilReadySeconds, 1000)
	ctx := bucketNameCtx(context.Background(), string(testBucketName))

	bucketSpec := getBucketSpec(testBucketName)

	waitForNewBucketWorker := func() (shouldRetry bool, err error, value interface{}) {
		gocbBucket, err := GetCouchbaseBucketGoCBFromAuthenticatedCluster(c.cluster, bucketSpec, "")
		if err != nil {
			c.logger(ctx, "Retrying OpenBucket")
			return true, err, nil
		}
		return false, nil, gocbBucket
	}

	c.logger(ctx, "Opening bucket")
	err, val := RetryLoop("waitForNewBucket", waitForNewBucketWorker, sleeper)

	gocbBucket, _ := val.(*CouchbaseBucketGoCB)

	return gocbBucket, err
}

func (c *tbpClusterV1) close() error {
	if c.cluster != nil {
		if err := c.cluster.Close(); err != nil {
			c.logger(context.Background(), "Couldn't close cluster connection: %v", err)
			return err
		}
	}
	return nil
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
		CouchbaseDriver: TestClusterDriver(),
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
	err = cluster.WaitUntilReady(1*time.Minute, nil)
	if err != nil {
		FatalfCtx(context.TODO(), "Cluster not ready: %v", err)
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
func (c *tbpClusterV2) openTestBucket(testBucketName tbpBucketName, waitUntilReadySeconds int) (Bucket, error) {

	bucketCluster := initV2Cluster(c.server)
	bucketSpec := getBucketSpec(testBucketName)

	return GetCollectionFromCluster(bucketCluster, bucketSpec, waitUntilReadySeconds)
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
