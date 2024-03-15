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

// firstServerVersionToSupportMobileXDCR this is the first server version to support Mobile XDCR feature
var firstServerVersionToSupportMobileXDCR = &ComparableBuildVersion{
	epoch: 0,
	major: 7,
	minor: 6,
	patch: 1,
}

type clusterLogFunc func(ctx context.Context, format string, args ...interface{})

// tbpCluster represents a gocb v2 cluster
type tbpCluster struct {
	logger       clusterLogFunc
	server       string // server address to connect to cluster
	connstr      string // connection string used to connect to the cluster
	supportsHLV  bool   // Flag to indicate cluster supports Mobile XDCR
	majorVersion int
	minorVersion int
	version      string
	// cluster can be used to perform cluster-level operations (but not bucket-level operations)
	cluster *gocb.Cluster
}

// newTestCluster returns a cluster based on the driver used by the defaultBucketSpec.
func newTestCluster(ctx context.Context, server string, tbp *TestBucketPool) *tbpCluster {
	tbpCluster := &tbpCluster{
		logger: tbp.Logf,
		server: server,
	}
	tbpCluster.cluster, tbpCluster.connstr = getGocbClusterForTest(ctx, server)
	metadata, err := tbpCluster.cluster.Internal().GetNodesMetadata(&gocb.GetNodesMetadataOptions{})
	if err != nil {
		tbp.Fatalf(ctx, "Couldn't get cluster metadata: %v", err)
	}
	tbpCluster.version = metadata[0].Version
	tbpCluster.majorVersion, tbpCluster.minorVersion, err = getClusterVersion(tbpCluster.cluster)
	if err != nil {
		tbp.Fatalf(ctx, "Couldn't get cluster version: %v", err)
	}
	return tbpCluster
}

// getGocbClusterForTest makes cluster connection. Callers must close. Returns the cluster and the connection string used to connect.
func getGocbClusterForTest(ctx context.Context, server string) (*gocb.Cluster, string) {

	testClusterTimeout := 10 * time.Second
	spec := BucketSpec{
		Server:          server,
		TLSSkipVerify:   true,
		BucketOpTimeout: &testClusterTimeout,
	}
	connStr, err := spec.GetGoCBConnString()
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
	return cluster, connStr
}

// isServerEnterprise returns true if the connected returns true if the connected couchbase server
// instance is Enterprise edition And false for Community edition
func (c *tbpCluster) isServerEnterprise() (bool, error) {
	if strings.Contains(c.version, "enterprise") {
		return true, nil
	}
	return false, nil
}

func (c *tbpCluster) getBucketNames() ([]string, error) {

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

func (c *tbpCluster) insertBucket(ctx context.Context, name string, quotaMB int) error {

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

func (c *tbpCluster) removeBucket(name string) error {
	return c.cluster.Buckets().DropBucket(name, nil)
}

// openTestBucket opens the bucket of the given name for the gocb cluster in the given TestBucketPool.
func (c *tbpCluster) openTestBucket(ctx context.Context, testBucketName tbpBucketName, waitUntilReady time.Duration) (Bucket, error) {

	bucketCluster, connstr := getGocbClusterForTest(ctx, c.server)

	bucketSpec := getTestBucketSpec(testBucketName)

	bucketFromSpec, err := GetGocbV2BucketFromCluster(ctx, bucketCluster, bucketSpec, connstr, waitUntilReady, false)
	if err != nil {
		return nil, err
	}

	// add whether bucket is mobile XDCR ready to bucket object
	if c.supportsHLV {
		bucketFromSpec.supportsHLV = true
	}

	return bucketFromSpec, nil
}

func (c *tbpCluster) close(ctx context.Context) error {
	// no close operations needed
	if c.cluster != nil {
		if err := c.cluster.Close(nil); err != nil {
			c.logger(ctx, "Couldn't close cluster connection: %v", err)
			return err
		}
	}
	return nil
}

func (c *tbpCluster) supportsCollections() (bool, error) {
	major, _, err := getClusterVersion(c.cluster)
	if err != nil {
		return false, err
	}
	return major >= 7, nil
}

// supportsMobileRBAC is true if running couchbase server with all Sync Gateway roles
func (c *tbpCluster) supportsMobileRBAC() (bool, error) {
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

// mobileXDCRCompatible checks if a cluster is mobile XDCR compatible, a cluster must be enterprise edition AND > 7.6.1
func (c *tbpCluster) mobileXDCRCompatible(ctx context.Context) (bool, error) {
	enterprise, err := c.isServerEnterprise()
	if err != nil {
		return false, err
	}
	if !enterprise {
		return false, nil
	}

	// take server version, server version will be the first 5 character of version string
	// in the form of x.x.x
	vrs := c.version[:5]

	// convert the above string into a comparable string
	version, err := NewComparableBuildVersionFromString(vrs)
	if err != nil {
		return false, err
	}

	if !version.Less(firstServerVersionToSupportMobileXDCR) {
		c.supportsHLV = true
		return true, nil
	}
	c.logger(ctx, "cluster does not support mobile XDCR")

	return false, nil
}
