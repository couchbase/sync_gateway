/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

// TODO: Move/rename this to bucket_gocb.go after review!

package base

import (
	"context"
	"errors"
	"expvar"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/couchbase/gocb/v2"
	"github.com/couchbase/gocbcore/v10"
	sgbucket "github.com/couchbase/sg-bucket"
	pkgerrors "github.com/pkg/errors"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
)

// GetGoCBv2Bucket opens a connection to the Couchbase cluster and returns a *GocbV2Bucket for the specified BucketSpec.
func GetGoCBv2Bucket(ctx context.Context, spec BucketSpec) (*GocbV2Bucket, error) {

	connString, err := spec.GetGoCBConnString()
	if err != nil {
		WarnfCtx(ctx, "Unable to parse server value: %s error: %v", SD(spec.Server), err)
		return nil, err
	}

	securityConfig, err := GoCBv2SecurityConfig(ctx, &spec.TLSSkipVerify, spec.CACertPath)
	if err != nil {
		return nil, err
	}

	authenticator, err := spec.GocbAuthenticator()
	if err != nil {
		return nil, err
	}

	if _, ok := authenticator.(gocb.CertificateAuthenticator); ok {
		InfofCtx(ctx, KeyAuth, "Using cert authentication for bucket %s on %s", MD(spec.BucketName), MD(spec.Server))
	} else {
		InfofCtx(ctx, KeyAuth, "Using credential authentication for bucket %s on %s", MD(spec.BucketName), MD(spec.Server))
	}

	timeoutsConfig := GoCBv2TimeoutsConfig(spec.BucketOpTimeout, StdlibDurationPtr(spec.GetViewQueryTimeout()))
	InfofCtx(ctx, KeyAll, "Setting query timeouts for bucket %s to %v", spec.BucketName, timeoutsConfig.QueryTimeout)

	clusterOptions := gocb.ClusterOptions{
		Authenticator:  authenticator,
		SecurityConfig: securityConfig,
		TimeoutsConfig: timeoutsConfig,
		RetryStrategy:  gocb.NewBestEffortRetryStrategy(nil),
	}

	cluster, err := gocb.Connect(connString, clusterOptions)
	if err != nil {
		InfofCtx(ctx, KeyAuth, "Unable to connect to cluster: %v", err)
		return nil, err
	}

	err = cluster.WaitUntilReady(time.Second*5, &gocb.WaitUntilReadyOptions{
		DesiredState:  gocb.ClusterStateOnline,
		ServiceTypes:  []gocb.ServiceType{gocb.ServiceTypeManagement},
		RetryStrategy: &goCBv2FailFastRetryStrategy{},
	})

	if err != nil {
		_ = cluster.Close(nil)
		if errors.Is(err, gocb.ErrAuthenticationFailure) {
			return nil, ErrAuthError
		}
		WarnfCtx(ctx, "Error waiting for cluster to be ready: %v", err)
		return nil, err
	}

	return GetGocbV2BucketFromCluster(ctx, cluster, spec, connString, time.Second*30, true)

}

// getClusterVersion returns major and minor versions of connected cluster
func getClusterVersion(cluster *gocb.Cluster) (int, int, error) {
	// Query node meta to find cluster compat version
	nodesMetadata, err := cluster.Internal().GetNodesMetadata(&gocb.GetNodesMetadataOptions{})
	if err != nil || len(nodesMetadata) == 0 {
		return 0, 0, fmt.Errorf("unable to get server cluster compatibility for %d nodes: %w", len(nodesMetadata), err)
	}
	// Safe to get first node as there will always be at least one node in the list and cluster compat is uniform across all nodes.
	clusterCompatMajor, clusterCompatMinor := decodeClusterVersion(nodesMetadata[0].ClusterCompatibility)
	return clusterCompatMajor, clusterCompatMinor, nil
}

// GetGocbV2BucketFromCluster returns a gocb.Bucket from an existing gocb.Cluster
func GetGocbV2BucketFromCluster(ctx context.Context, cluster *gocb.Cluster, spec BucketSpec, connstr string, waitUntilReady time.Duration, failFast bool) (*GocbV2Bucket, error) {

	// Connect to bucket
	bucket := cluster.Bucket(spec.BucketName)

	var retryStrategy gocb.RetryStrategy
	if failFast {
		retryStrategy = &goCBv2FailFastRetryStrategy{}
	} else {
		retryStrategy = gocb.NewBestEffortRetryStrategy(nil)
	}
	err := bucket.WaitUntilReady(waitUntilReady, &gocb.WaitUntilReadyOptions{
		RetryStrategy: retryStrategy,
	})
	if err != nil {
		_ = cluster.Close(&gocb.ClusterCloseOptions{})
		if errors.Is(err, gocb.ErrAuthenticationFailure) {
			return nil, ErrAuthError
		}
		WarnfCtx(ctx, "Error waiting for bucket to be ready: %v", err)
		return nil, err
	}
	clusterCompatMajor, clusterCompatMinor, err := getClusterVersion(cluster)
	if err != nil {
		_ = cluster.Close(&gocb.ClusterCloseOptions{})
		return nil, err
	}

	gocbv2Bucket := &GocbV2Bucket{
		bucket:                    bucket,
		cluster:                   cluster,
		Spec:                      spec,
		clusterCompatMajorVersion: uint64(clusterCompatMajor),
		clusterCompatMinorVersion: uint64(clusterCompatMinor),
	}

	// Set limits for concurrent query and kv ops
	maxConcurrentQueryOps := MaxConcurrentQueryOps
	if spec.MaxConcurrentQueryOps != nil {
		maxConcurrentQueryOps = *spec.MaxConcurrentQueryOps
	}

	queryNodeCount, err := gocbv2Bucket.QueryEpsCount()
	if err != nil || queryNodeCount == 0 {
		queryNodeCount = 1
	}

	if maxConcurrentQueryOps > DefaultHttpMaxIdleConnsPerHost*queryNodeCount {
		maxConcurrentQueryOps = DefaultHttpMaxIdleConnsPerHost * queryNodeCount
		InfofCtx(ctx, KeyAll, "Setting max_concurrent_query_ops to %d based on query node count (%d)", maxConcurrentQueryOps, queryNodeCount)
	}

	gocbv2Bucket.queryOps = make(chan struct{}, maxConcurrentQueryOps)

	// gocb v2 has a queue size of 2048 per pool per server node.
	// SG conservatively limits to 1000 per pool per node, to handle imbalanced
	// request distribution between server nodes.
	nodeCount := 1
	mgmtEps, mgmtEpsErr := gocbv2Bucket.MgmtEps()
	if mgmtEpsErr != nil && len(mgmtEps) > 0 {
		nodeCount = len(mgmtEps)
	}

	numPools, err := getIntFromConnStr(connstr, kvPoolSizeKey)
	if err != nil {
		WarnfCtx(ctx, "Error getting kv pool size from connection string: %v", err)
		_ = cluster.Close(&gocb.ClusterCloseOptions{})
		return nil, err
	}
	gocbv2Bucket.kvOps = make(chan struct{}, MaxConcurrentSingleOps*nodeCount*(*numPools))

	// Query to see if mobile XDCR bucket setting is set and store on bucket object
	err = gocbv2Bucket.queryHLVBucketSetting(ctx)
	if err != nil {
		return nil, err
	}

	return gocbv2Bucket, nil
}

type GocbV2Bucket struct {
	bucket                                               *gocb.Bucket  // bucket connection - used by scope/collection operations
	cluster                                              *gocb.Cluster // cluster connection - required for N1QL operations
	Spec                                                 BucketSpec    // Spec is a copy of the BucketSpec for DCP usage
	queryOps                                             chan struct{} // Manages max concurrent query ops
	kvOps                                                chan struct{} // Manages max concurrent kv ops
	clusterCompatMajorVersion, clusterCompatMinorVersion uint64        // E.g: 6 and 0 for 6.0.3
	supportsHLV                                          bool          // Flag to indicate with bucket supports mobile XDCR
}

var (
	_ sgbucket.BucketStore            = &GocbV2Bucket{}
	_ CouchbaseBucketStore            = &GocbV2Bucket{}
	_ sgbucket.DynamicDataStoreBucket = &GocbV2Bucket{}
)

func AsGocbV2Bucket(bucket Bucket) (*GocbV2Bucket, error) {
	baseBucket := GetBaseBucket(bucket)
	if gocbv2Bucket, ok := baseBucket.(*GocbV2Bucket); ok {
		return gocbv2Bucket, nil
	}
	return nil, fmt.Errorf("bucket is not a gocb bucket (type %T)", baseBucket)
}

func (b *GocbV2Bucket) GetName() string {
	return b.bucket.Name()
}

func (b *GocbV2Bucket) UUID() (string, error) {
	config, configErr := b.getConfigSnapshot()
	if configErr != nil {
		return "", fmt.Errorf("Unable to determine bucket UUID for collection %v: %w", b.GetName(), configErr)
	}
	return config.BucketUUID(), nil
}

// GetCluster returns an open cluster object
func (b *GocbV2Bucket) GetCluster() *gocb.Cluster {
	return b.cluster
}

func (b *GocbV2Bucket) Close(ctx context.Context) {
	if err := b.cluster.Close(nil); err != nil {
		WarnfCtx(ctx, "Error closing cluster for bucket %s: %v", MD(b.BucketName()), err)
	}
}

func (b *GocbV2Bucket) IsSupported(feature sgbucket.BucketStoreFeature) bool {
	switch feature {
	case sgbucket.BucketStoreFeatureSubdocOperations, sgbucket.BucketStoreFeatureXattrs, sgbucket.BucketStoreFeatureCrc32cMacroExpansion:
		// Available on all supported server versions
		return true
	case sgbucket.BucketStoreFeatureN1ql:
		agent, err := b.getGoCBAgent()
		if err != nil {
			return false
		}
		return len(agent.N1qlEps()) > 0
	case sgbucket.BucketStoreFeatureCreateDeletedWithXattr:
		status, err := b.bucket.Internal().CapabilityStatus(gocb.CapabilityCreateAsDeleted)
		if err != nil {
			return false
		}
		return status == gocb.CapabilityStatusSupported
	case sgbucket.BucketStoreFeaturePreserveExpiry, sgbucket.BucketStoreFeatureCollections:
		// TODO: Change to capability check when GOCBC-1218 merged
		return isMinimumVersion(b.clusterCompatMajorVersion, b.clusterCompatMinorVersion, 7, 0)
	case sgbucket.BucketStoreFeatureSystemCollections:
		return isMinimumVersion(b.clusterCompatMajorVersion, b.clusterCompatMinorVersion, 7, 6)
	case sgbucket.BucketStoreFeatureMobileXDCR:
		return b.supportsHLV
	default:
		return false
	}
}

// queryHLVBucketSetting sends request to server to check for enableCrossClusterVersioning bucket setting
func (b *GocbV2Bucket) queryHLVBucketSetting(ctx context.Context) error {
	url := fmt.Sprintf("/pools/default/buckets/%s", b.GetName())
	output, statusCode, err := mgmtRequest(ctx, b, http.MethodGet, url, nil)
	if err != nil || statusCode != http.StatusOK {
		return fmt.Errorf("error executing query for mobile XDCR bucket setting, status code: %d error: %v", statusCode, err)
	}

	type bucket struct {
		SupportsHLV *bool `json:"enableCrossClusterVersioning,omitempty"`
	}
	var bucketSettings bucket
	err = JSONUnmarshal(output, &bucketSettings)
	if err != nil {
		return err
	}
	if bucketSettings.SupportsHLV != nil {
		b.supportsHLV = *bucketSettings.SupportsHLV
	}
	return nil
}

func (b *GocbV2Bucket) StartDCPFeed(ctx context.Context, args sgbucket.FeedArguments, callback sgbucket.FeedEventCallbackFunc, dbStats *expvar.Map) error {
	groupID := ""
	return StartGocbDCPFeed(ctx, b, b.Spec.BucketName, args, callback, dbStats, DCPMetadataStoreInMemory, groupID)
}

func (b *GocbV2Bucket) StartTapFeed(args sgbucket.FeedArguments, dbStats *expvar.Map) (sgbucket.MutationFeed, error) {
	return nil, errors.New("StartTapFeed not implemented")
}

func (b *GocbV2Bucket) GetStatsVbSeqno(maxVbno uint16, useAbsHighSeqNo bool) (uuids map[uint16]uint64, highSeqnos map[uint16]uint64, seqErr error) {

	agent, agentErr := b.getGoCBAgent()
	if agentErr != nil {
		return nil, nil, agentErr
	}

	statsOptions := gocbcore.StatsOptions{
		Key:      "vbucket-seqno",
		Deadline: b.getBucketOpDeadline(),
	}

	statsResult := &gocbcore.StatsResult{}
	wg := sync.WaitGroup{}
	wg.Add(1)
	statsCallback := func(result *gocbcore.StatsResult, err error) {
		defer wg.Done()
		if err != nil {
			seqErr = err
			return
		}
		statsResult = result
	}

	_, err := agent.Stats(statsOptions, statsCallback)
	if err != nil {
		wg.Done()
		return nil, nil, err
	}
	wg.Wait()

	// Convert gocbcore StatsResult to generic map of maps for use by GetStatsVbSeqno
	genericStats := make(map[string]map[string]string)
	for server, serverStats := range statsResult.Servers {
		genericServerStats := make(map[string]string)
		for k, v := range serverStats.Stats {
			genericServerStats[k] = v
		}
		genericStats[server] = genericServerStats
	}

	return GetStatsVbSeqno(genericStats, maxVbno, useAbsHighSeqNo)
}

func (b *GocbV2Bucket) GetMaxVbno() (uint16, error) {

	config, configErr := b.getConfigSnapshot()
	if configErr != nil {
		return 0, fmt.Errorf("Unable to determine vbucket count: %w", configErr)
	}

	vbNo, err := config.NumVbuckets()
	if err != nil {
		return 0, fmt.Errorf("Unable to determine vbucket count: %w", err)
	}

	return uint16(vbNo), nil
}

func (b *GocbV2Bucket) getConfigSnapshot() (*gocbcore.ConfigSnapshot, error) {
	agent, err := b.getGoCBAgent()
	if err != nil {
		return nil, fmt.Errorf("no gocbcore.Agent: %w", err)
	}

	config, configErr := agent.ConfigSnapshot()
	if configErr != nil {
		return nil, fmt.Errorf("no gocbcore.Agent config snapshot: %w", configErr)
	}
	return config, nil
}

func (b *GocbV2Bucket) IsError(err error, errorType sgbucket.DataStoreErrorType) bool {
	if err == nil {
		return false
	}
	switch errorType {
	case sgbucket.KeyNotFoundError:
		return errors.Is(err, gocb.ErrDocumentNotFound)
	default:
		return false
	}
}

func (b *GocbV2Bucket) GetSpec() BucketSpec {
	return b.Spec
}

// This flushes the *entire* bucket associated with the collection (not just the collection).  Intended for test usage only.
func (b *GocbV2Bucket) Flush(ctx context.Context) error {

	bucketManager := b.cluster.Buckets()

	workerFlush := func() (shouldRetry bool, err error, value interface{}) {
		if err := bucketManager.FlushBucket(b.GetName(), nil); err != nil {
			WarnfCtx(ctx, "Error flushing bucket %s: %v  Will retry.", MD(b.GetName()).Redact(), err)
			return true, err, nil
		}

		return false, nil, nil
	}

	err, _ := RetryLoop(ctx, "EmptyTestBucket", workerFlush, CreateDoublingSleeperFunc(12, 10))
	if err != nil {
		return err
	}

	// Wait until the bucket item count is 0, since flush is asynchronous
	worker := func() (shouldRetry bool, err error, value interface{}) {
		itemCount, err := b.BucketItemCount(ctx)
		if err != nil {
			return false, err, nil
		}

		if itemCount == 0 {
			// bucket flushed, we're done
			return false, nil, nil
		}

		// Retry
		return true, nil, nil

	}

	// Kick off retry loop
	err, _ = RetryLoop(ctx, "Wait until bucket has 0 items after flush", worker, CreateMaxDoublingSleeperFunc(25, 100, 10000))
	if err != nil {
		return pkgerrors.Wrapf(err, "Error during Wait until bucket %s has 0 items after flush", MD(b.GetName()).Redact())
	}

	return nil

}

// BucketItemCount first tries to retrieve an accurate bucket count via N1QL,
// but falls back to the REST API if that cannot be done (when there's no index to count all items in a bucket)
func (b *GocbV2Bucket) BucketItemCount(ctx context.Context) (itemCount int, err error) {
	dataStoreNames, err := b.ListDataStores()
	if err != nil {
		return 0, err
	}

	for _, dsn := range dataStoreNames {
		ds, err := b.NamedDataStore(dsn)
		if err != nil {
			return 0, err
		}
		ns, ok := AsN1QLStore(ds)
		if !ok {
			return 0, fmt.Errorf("DataStore %v %T is not a N1QLStore", ds.GetName(), ds)
		}
		itemCount, err = QueryBucketItemCount(ctx, ns)
		if err == nil {
			return itemCount, nil
		}
	}

	// TODO: implement APIBucketItemCount for collections as part of CouchbaseBucketStore refactoring.  Until then, give flush a moment to finish
	time.Sleep(1 * time.Second)
	// itemCount, err = bucket.APIBucketItemCount()
	return 0, err
}

func (b *GocbV2Bucket) MgmtEps() (url []string, err error) {
	agent, err := b.getGoCBAgent()
	if err != nil {
		return url, err
	}
	mgmtEps := agent.MgmtEps()
	if len(mgmtEps) == 0 {
		return nil, fmt.Errorf("No available Couchbase Server nodes")
	}
	return mgmtEps, nil
}

func (b *GocbV2Bucket) QueryEpsCount() (int, error) {
	agent, err := b.getGoCBAgent()
	if err != nil {
		return 0, err
	}

	return len(agent.N1qlEps()), nil
}

// Gets the metadata purge interval for the bucket.  First checks for a bucket-specific value.  If not
// found, retrieves the cluster-wide value.
func (b *GocbV2Bucket) MetadataPurgeInterval(ctx context.Context) (time.Duration, error) {
	return getMetadataPurgeInterval(ctx, b)
}

func (b *GocbV2Bucket) MaxTTL(ctx context.Context) (int, error) {
	return getMaxTTL(ctx, b)
}

func (b *GocbV2Bucket) HttpClient(ctx context.Context) *http.Client {
	agent, err := b.getGoCBAgent()
	if err != nil {
		WarnfCtx(ctx, "Unable to obtain gocbcore.Agent while retrieving httpClient:%v", err)
		return nil
	}
	return agent.HTTPClient()
}

func (b *GocbV2Bucket) BucketName() string {
	// TODO: Consider removing this method and swap for GetName()/Name()?
	return b.GetName()
}

func (b *GocbV2Bucket) MgmtRequest(ctx context.Context, method, uri, contentType string, body io.Reader) (*http.Response, error) {
	if contentType == "" && body != nil {
		return nil, errors.New("Content-type must be specified for non-null body.")
	}

	mgmtEp, err := GoCBBucketMgmtEndpoint(b)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest(method, mgmtEp+uri, body)
	if err != nil {
		return nil, err
	}

	if contentType != "" {
		req.Header.Add("Content-Type", contentType)
	}

	if b.Spec.Auth != nil {
		username, password, _ := b.Spec.Auth.GetCredentials()
		req.SetBasicAuth(username, password)
	}
	return b.HttpClient(ctx).Do(req)
}

// This prevents Sync Gateway from overflowing gocb's pipeline
func (b *GocbV2Bucket) waitForAvailKvOp() {
	b.kvOps <- struct{}{}
}

func (b *GocbV2Bucket) releaseKvOp() {
	<-b.kvOps
}

// GetGoCBAgent returns the underlying agent from gocbcore
func (b *GocbV2Bucket) getGoCBAgent() (*gocbcore.Agent, error) {
	return b.bucket.Internal().IORouter()
}

// GetBucketOpDeadline returns a deadline for use in gocbcore calls
func (b *GocbV2Bucket) getBucketOpDeadline() time.Time {
	opTimeout := DefaultGocbV2OperationTimeout
	configOpTimeout := b.Spec.BucketOpTimeout
	if configOpTimeout != nil {
		opTimeout = *configOpTimeout
	}
	return time.Now().Add(opTimeout)
}

func (b *GocbV2Bucket) GetCollectionManifest() (gocbcore.Manifest, error) {
	agent, err := b.bucket.Internal().IORouter()
	if err != nil {
		return gocbcore.Manifest{}, fmt.Errorf("failed to get gocbcore agent: %w", err)
	}
	result := make(chan any) // either a CollectionsManifest or error
	_, err = agent.GetCollectionManifest(gocbcore.GetCollectionManifestOptions{
		Deadline: b.getBucketOpDeadline(),
	}, func(res *gocbcore.GetCollectionManifestResult, err error) {
		defer close(result)
		if err != nil {
			result <- err
			return
		}
		var manifest gocbcore.Manifest
		err = JSONUnmarshal(res.Manifest, &manifest)
		if err != nil {
			result <- fmt.Errorf("failed to parse collection manifest: %w", err)
			return
		}
		result <- manifest
	})
	if err != nil {
		return gocbcore.Manifest{}, fmt.Errorf("failed to execute GetCollectionManifest: %w", err)
	}
	returned := <-result
	if err, ok := returned.(error); ok && err != nil {
		return gocbcore.Manifest{}, err
	}
	rv := returned.(gocbcore.Manifest)
	return rv, nil
}

func GetIDForCollection(manifest gocbcore.Manifest, scopeName, collectionName string) (uint32, bool) {
	for _, scope := range manifest.Scopes {
		if scope.Name != scopeName {
			continue
		}
		for _, coll := range scope.Collections {
			if coll.Name == collectionName {
				return coll.UID, true
			}
		}
	}
	return 0, false
}

// waitForAvailQueryOp prevents Sync Gateway from having too many concurrent
// queries against Couchbase Server
func (b *GocbV2Bucket) waitForAvailQueryOp() {
	b.queryOps <- struct{}{}
}

func (b *GocbV2Bucket) releaseQueryOp() {
	<-b.queryOps
}

func (b *GocbV2Bucket) ListDataStores() ([]sgbucket.DataStoreName, error) {
	if !b.IsSupported(sgbucket.BucketStoreFeatureCollections) {
		return []sgbucket.DataStoreName{ScopeAndCollectionName{Scope: DefaultScope, Collection: DefaultCollection}}, nil
	}
	scopes, err := b.bucket.Collections().GetAllScopes(nil)
	if err != nil {
		return nil, err
	}
	collections := make([]sgbucket.DataStoreName, 0)
	for _, s := range scopes {
		// clients using system scopes should know what they're called,
		// and we don't want to accidentally iterate over other system collections
		if s.Name == SystemScope {
			continue
		}
		for _, c := range s.Collections {
			collections = append(collections, ScopeAndCollectionName{Scope: s.Name, Collection: c.Name})
		}
	}
	return collections, nil
}

func (b *GocbV2Bucket) DropDataStore(name sgbucket.DataStoreName) error {
	return b.bucket.Collections().DropCollection(gocb.CollectionSpec{Name: name.CollectionName(), ScopeName: name.ScopeName()}, nil)
}

func (b *GocbV2Bucket) CreateDataStore(ctx context.Context, name sgbucket.DataStoreName) error {
	// create scope first (if it doesn't already exist)
	if name.ScopeName() != DefaultScope {
		err := b.bucket.Collections().CreateScope(name.ScopeName(), nil)
		if err != nil && !errors.Is(err, gocb.ErrScopeExists) {
			return err
		}
	}
	err := b.bucket.Collections().CreateCollection(gocb.CollectionSpec{Name: name.CollectionName(), ScopeName: name.ScopeName()}, nil)
	if err != nil {
		return err
	}
	// Can't use Collection.Exists since we can't get a collection until the collection exists on CBS
	gocbCollection := b.bucket.Scope(name.ScopeName()).Collection(name.CollectionName())
	return WaitForNoError(ctx, func() error {
		_, err := gocbCollection.Exists("fakedocid", nil)
		return err
	})
}

// DefaultDataStore returns the default collection for the bucket.
func (b *GocbV2Bucket) DefaultDataStore() sgbucket.DataStore {
	return &Collection{
		Bucket:     b,
		Collection: b.bucket.DefaultCollection(),
	}
}

// NamedDataStore returns a collection on a bucket within the given scope and collection.
func (b *GocbV2Bucket) NamedDataStore(name sgbucket.DataStoreName) (sgbucket.DataStore, error) {
	c, err := NewCollection(
		b,
		b.bucket.Scope(name.ScopeName()).Collection(name.CollectionName()))
	if err != nil {
		if errors.Is(err, gocb.ErrCollectionNotFound) || errors.Is(err, gocb.ErrScopeNotFound) {
			return nil, ErrAuthError
		}
		return nil, err
	}
	return c, nil
}

// ServerMetrics returns all the metrics for couchbase server.
func (b *GocbV2Bucket) ServerMetrics(ctx context.Context) (map[string]*dto.MetricFamily, error) {
	url := "/metrics/"
	resp, err := b.MgmtRequest(ctx, http.MethodGet, url, "application/x-www-form-urlencoded", nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()

	output, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("Could not read body from %s", url)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("Could not get metrics from %s. %s %s -> (%d) %s", b.GetName(), http.MethodGet, url, resp.StatusCode, output)
	}

	// filter duplicates from couchbase server or TextToMetricFamilies will fail MB-43772
	lines := map[string]struct{}{}
	filteredOutput := []string{}
	for _, line := range strings.Split(string(output), "\n") {
		_, ok := lines[line]
		if ok {
			continue
		}
		lines[line] = struct{}{}
		filteredOutput = append(filteredOutput, line)
	}
	filteredOutput = append(filteredOutput, "")
	var parser expfmt.TextParser
	mf, err := parser.TextToMetricFamilies(strings.NewReader(strings.Join(filteredOutput, "\n")))
	if err != nil {
		return nil, err
	}

	return mf, nil
}

func GetCollectionID(dataStore DataStore) uint32 {
	switch c := dataStore.(type) {
	case WrappingDatastore:
		return GetCollectionID(c.GetUnderlyingDataStore())
	case sgbucket.Collection:
		return c.GetCollectionID()
	default:
		return DefaultCollectionID
	}
}
