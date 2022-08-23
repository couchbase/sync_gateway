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
}

var _ tbpCluster = &tbpClusterV2{}

func newTestClusterV2(server string, logger clusterLogFunc) *tbpClusterV2 {
	tbpCluster := &tbpClusterV2{}
	tbpCluster.logger = logger
	tbpCluster.server = server
	return tbpCluster
}

// getCluster makes cluster connection.  Callers must close.
func getCluster(server string) *gocb.Cluster {

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
	err = cluster.WaitUntilReady(15*time.Second, nil)
	if err != nil {
		FatalfCtx(context.TODO(), "Cluster not ready: %v", err)
	}

	return cluster
}

func (c *tbpClusterV2) getBucketNames() ([]string, error) {

	cluster := getCluster(c.server)
	defer c.closeCluster(cluster)

	manager := cluster.Buckets()

	bucketSettings, err := manager.GetAllBuckets(nil)
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

	cluster := getCluster(c.server)
	defer c.closeCluster(cluster)
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
	return cluster.Buckets().CreateBucket(settings, options)
}

func (c *tbpClusterV2) removeBucket(name string) error {
	cluster := getCluster(c.server)
	defer c.closeCluster(cluster)

	return cluster.Buckets().DropBucket(name, nil)
}

// openTestBucket opens the bucket of the given name for the gocb cluster in the given TestBucketPool.
func (c *tbpClusterV2) openTestBucket(testBucketName tbpBucketName, waitUntilReadySeconds int) (Bucket, error) {

	cluster := getCluster(c.server)
	bucketSpec := getBucketSpec(testBucketName)

	// Create scope and collection on server, first drop any scope and collect in the bucket and then create
	// new scope + collection
	bucket := cluster.Bucket(bucketSpec.BucketName)
	err := bucket.WaitUntilReady(time.Duration(waitUntilReadySeconds*4)*time.Second, nil)
	if err != nil {
		panic(fmt.Sprintf("here %+v", err))
		return nil, err
	}
	DebugfCtx(context.TODO(), KeySGTest, "Got bucket %s", testBucketName)

	if err := DropAllScopesAndCollections(bucket); err != nil && !errors.Is(err, ErrCollectionsUnsupported) {
		return nil, err
	}

	specScope, specCollection := bucketSpecScopeAndCollection(bucketSpec)
	if specScope != "" || specCollection != "" {
		err := CreateBucketScopesAndCollections(context.TODO(), bucketSpec,
			map[string][]string{
				specScope: []string{specCollection},
			})
		if err != nil {
			return nil, err
		}
	}

	// Return a Collection object

	return GetCollectionFromCluster(cluster, bucketSpec, waitUntilReadySeconds)
}

func (c *tbpClusterV2) close() error {
	// no close operations needed
	return nil
}

func (c *tbpClusterV2) closeCluster(cluster *gocb.Cluster) {
	if err := cluster.Close(nil); err != nil {
		c.logger(context.Background(), "Couldn't close cluster connection: %v", err)
	}
}

func (c *tbpClusterV2) getMinClusterCompatVersion() int {
	cluster := getCluster(c.server)
	defer c.closeCluster(cluster)

	nodesMeta, err := cluster.Internal().GetNodesMetadata(nil)
	if err != nil {
		FatalfCtx(context.Background(), "TEST: failed to fetch nodes metadata: %v", err)
	}
	if len(nodesMeta) < 1 {
		panic("invalid NodesMetadata: no nodes")
	}
	return nodesMeta[0].ClusterCompatibility
}

// DropAllScopesAndCollections attempts to drop *all* non-_default scopes and collections from the bucket associated with the collection.  Intended for test usage only.
func DropAllScopesAndCollections(bucket *gocb.Bucket) error {
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
		if scope.Name != DefaultScope {
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
			if collection.Name != DefaultCollection {
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
