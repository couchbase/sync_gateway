//  Copyright 2013-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package base

import (
	"bytes"
	"context"
	"errors"
	"expvar"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/couchbase/gocb/v2"
	"github.com/couchbase/gocbcore/v10"
	sgbucket "github.com/couchbase/sg-bucket"
	pkgerrors "github.com/pkg/errors"
)

const (
	MaxConcurrentSingleOps = 1000 // Max 1000 concurrent single bucket ops
	MaxConcurrentBulkOps   = 35   // Max 35 concurrent bulk ops
	MaxConcurrentQueryOps  = 1000 // Max concurrent query ops
	MaxBulkBatchSize       = 100  // Maximum number of ops per bulk call

	// Causes the write op to block until the change has been replicated to numNodesReplicateTo many nodes.
	// In our case, we only want to block until it's durable on the node we're writing to, so this is set to 0.
	numNodesReplicateTo = uint(0)

	// Causes the write op to block until the change has been persisted (made durable -- written to disk) on
	// numNodesPersistTo.  In our case, we only want to block until it's durable on the node we're writing to,
	// so this is set to 1
	numNodesPersistTo = uint(1)

	// CRC-32 checksum represents the body hash of "Deleted" document.
	DeleteCrc32c = "0x00000000"
)

// GetGoCBv2Bucket returns a *GocbV2Bucket for the specified bucket.
func GetGoCBv2Bucket(spec BucketSpec) (*GocbV2Bucket, error) {

	logCtx := context.TODO()
	connString, err := spec.GetGoCBConnString(nil)
	if err != nil {
		WarnfCtx(logCtx, "Unable to parse server value: %s error: %v", SD(spec.Server), err)
		return nil, err
	}

	securityConfig, err := GoCBv2SecurityConfig(&spec.TLSSkipVerify, spec.CACertPath)
	if err != nil {
		return nil, err
	}

	authenticator, err := spec.GocbAuthenticator()
	if err != nil {
		return nil, err
	}

	if _, ok := authenticator.(gocb.CertificateAuthenticator); ok {
		InfofCtx(logCtx, KeyAuth, "Using cert authentication for bucket %s on %s", MD(spec.BucketName), MD(spec.Server))
	} else {
		InfofCtx(logCtx, KeyAuth, "Using credential authentication for bucket %s on %s", MD(spec.BucketName), MD(spec.Server))
	}

	timeoutsConfig := GoCBv2TimeoutsConfig(spec.BucketOpTimeout, StdlibDurationPtr(spec.GetViewQueryTimeout()))
	InfofCtx(logCtx, KeyAll, "Setting query timeouts for bucket %s to %v", spec.BucketName, timeoutsConfig.QueryTimeout)

	clusterOptions := gocb.ClusterOptions{
		Authenticator:  authenticator,
		SecurityConfig: securityConfig,
		TimeoutsConfig: timeoutsConfig,
		RetryStrategy:  gocb.NewBestEffortRetryStrategy(nil),
	}

	if spec.KvPoolSize > 0 {
		// TODO: Equivalent of kvPoolSize in gocb v2?
	}

	cluster, err := gocb.Connect(connString, clusterOptions)
	if err != nil {
		InfofCtx(logCtx, KeyAuth, "Unable to connect to cluster: %v", err)
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
		WarnfCtx(context.TODO(), "Error waiting for cluster to be ready: %v", err)
		return nil, err
	}

	return GetGocbV2BucketFromCluster(cluster, spec, time.Second*30, true)

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

func GetGocbV2BucketFromCluster(cluster *gocb.Cluster, spec BucketSpec, waitUntilReady time.Duration, failFast bool) (*GocbV2Bucket, error) {

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
		WarnfCtx(context.TODO(), "Error waiting for bucket to be ready: %v", err)
		return nil, err
	}
	clusterCompatMajor, clusterCompatMinor, err := getClusterVersion(cluster)
	if err != nil {
		_ = cluster.Close(&gocb.ClusterCloseOptions{})
		return nil, fmt.Errorf("%s", err)
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
		InfofCtx(context.TODO(), KeyAll, "Setting max_concurrent_query_ops to %d based on query node count (%d)", maxConcurrentQueryOps, queryNodeCount)
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
	numPools := 1
	if spec.KvPoolSize > 0 {
		numPools = spec.KvPoolSize
	}
	gocbv2Bucket.kvOps = make(chan struct{}, MaxConcurrentSingleOps*nodeCount*numPools)

	return gocbv2Bucket, nil
}

type GocbV2Bucket struct {
	bucket                                               *gocb.Bucket  // bucket connection - used by scope/collection operations
	cluster                                              *gocb.Cluster // cluster connection - required for N1QL operations
	Spec                                                 BucketSpec    // Spec is a copy of the BucketSpec for DCP usage
	queryOps                                             chan struct{} // Manages max concurrent query ops
	kvOps                                                chan struct{} // Manages max concurrent kv ops
	clusterCompatMajorVersion, clusterCompatMinorVersion uint64        // E.g: 6 and 0 for 6.0.3
}

var (
	_ sgbucket.BucketStore = &GocbV2Bucket{}
	_ CouchbaseBucketStore = &GocbV2Bucket{}
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

func (b *GocbV2Bucket) Close() {
	if err := b.cluster.Close(nil); err != nil {

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
	default:
		return false
	}
}

func (b *GocbV2Bucket) ListDataStores() ([]sgbucket.DataStoreName, error) {
	scopes, err := b.bucket.Collections().GetAllScopes(nil)
	if err != nil {
		return nil, err
	}
	collections := make([]sgbucket.DataStoreName, 0)
	for _, s := range scopes {
		for _, c := range s.Collections {
			collections = append(collections, ScopeAndCollectionName{s.Name, c.Name})
		}
	}
	return collections, nil
}

func (b *GocbV2Bucket) DropDataStore(name sgbucket.DataStoreName) error {
	return b.bucket.Collections().DropCollection(gocb.CollectionSpec{Name: name.CollectionName(), ScopeName: name.ScopeName()}, nil)
}

func (b *GocbV2Bucket) CreateDataStore(name sgbucket.DataStoreName) error {
	// create scope first (if it doesn't already exist)
	err := b.bucket.Collections().CreateScope(name.ScopeName(), nil)
	if err != nil && !errors.Is(err, gocb.ErrScopeExists) {
		return err
	}
	return b.bucket.Collections().CreateCollection(gocb.CollectionSpec{Name: name.CollectionName(), ScopeName: name.ScopeName()}, nil)
}

func (bucket *GocbV2Bucket) StartDCPFeed(args sgbucket.FeedArguments, callback sgbucket.FeedEventCallbackFunc, dbStats *expvar.Map) error {
	groupID := ""
	return StartGocbDCPFeed(bucket, bucket.Spec.BucketName, args, callback, dbStats, DCPMetadataStoreInMemory, groupID)
}

func (bucket *GocbV2Bucket) StartTapFeed(args sgbucket.FeedArguments, dbStats *expvar.Map) (sgbucket.MutationFeed, error) {
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

// GetBucketOpDeadline returns a deadline for use in gocbcore calls
func (b *GocbV2Bucket) getBucketOpDeadline() time.Time {
	opTimeout := DefaultGocbV2OperationTimeout
	configOpTimeout := b.Spec.BucketOpTimeout
	if configOpTimeout != nil {
		opTimeout = *configOpTimeout
	}
	return time.Now().Add(opTimeout)
}

// This prevents Sync Gateway from overflowing gocb's pipeline
func (b *GocbV2Bucket) waitForAvailKvOp() {
	b.kvOps <- struct{}{}
}

func (b *GocbV2Bucket) releaseKvOp() {
	<-b.kvOps
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

// DefaultDataStore returns the default collection for the bucket.
func (b *GocbV2Bucket) DefaultDataStore() sgbucket.DataStore {
	return &Collection{
		Bucket:     b,
		Collection: b.bucket.DefaultCollection(),
	}
}

// NamedDataStore returns a collection on a bucket within the given scope and collection.
func (b *GocbV2Bucket) NamedDataStore(name sgbucket.DataStoreName) sgbucket.DataStore {
	return &Collection{
		Bucket:     b,
		Collection: b.bucket.Scope(name.ScopeName()).Collection(name.CollectionName()),
	}
}

func (b *GocbV2Bucket) BucketName() string {
	// TODO: Consider removing this method and swap for GetName()/Name()?
	return b.GetName()
}

func (b *GocbV2Bucket) mgmtRequest(method, uri, contentType string, body io.Reader) (*http.Response, error) {
	if contentType == "" && body != nil {
		// TODO: CBG-1948
		panic("Content-type must be specified for non-null body.")
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

	return b.HttpClient().Do(req)
}

// This flushes the *entire* bucket associated with the collection (not just the collection).  Intended for test usage only.
func (b *GocbV2Bucket) Flush() error {

	bucketManager := b.cluster.Buckets()

	workerFlush := func() (shouldRetry bool, err error, value interface{}) {
		if err := bucketManager.FlushBucket(b.GetName(), nil); err != nil {
			WarnfCtx(context.TODO(), "Error flushing bucket %s: %v  Will retry.", MD(b.GetName()).Redact(), err)
			return true, err, nil
		}

		return false, nil, nil
	}

	err, _ := RetryLoop("EmptyTestBucket", workerFlush, CreateDoublingSleeperFunc(12, 10))
	if err != nil {
		return err
	}

	// Wait until the bucket item count is 0, since flush is asynchronous
	worker := func() (shouldRetry bool, err error, value interface{}) {
		itemCount, err := b.BucketItemCount()
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
	err, _ = RetryLoop("Wait until bucket has 0 items after flush", worker, CreateMaxDoublingSleeperFunc(25, 100, 10000))
	if err != nil {
		return pkgerrors.Wrapf(err, "Error during Wait until bucket %s has 0 items after flush", MD(b.GetName()).Redact())
	}

	return nil

}

// BucketItemCount first tries to retrieve an accurate bucket count via N1QL,
// but falls back to the REST API if that cannot be done (when there's no index to count all items in a bucket)
func (b *GocbV2Bucket) BucketItemCount() (itemCount int, err error) {
	ns, ok := AsN1QLStore(b.DefaultDataStore())
	if !ok {
		return 0, fmt.Errorf("bucket %T is not a N1QLStore", b)
	}
	itemCount, err = QueryBucketItemCount(ns)
	if err == nil {
		return itemCount, nil
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

// GetGoCBAgent returns the underlying agent from gocbcore
func (b *GocbV2Bucket) getGoCBAgent() (*gocbcore.Agent, error) {
	return b.bucket.Internal().IORouter()
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
func (b *GocbV2Bucket) MetadataPurgeInterval() (time.Duration, error) {
	return getMetadataPurgeInterval(b)
}

func (b *GocbV2Bucket) ServerUUID() (uuid string, err error) {
	return getServerUUID(b)
}

func (b *GocbV2Bucket) MaxTTL() (int, error) {
	return getMaxTTL(b)
}

func (b *GocbV2Bucket) HttpClient() *http.Client {
	agent, err := b.getGoCBAgent()
	if err != nil {
		WarnfCtx(context.TODO(), "Unable to obtain gocbcore.Agent while retrieving httpClient:%v", err)
		return nil
	}
	return agent.HTTPClient()
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

// waitForAvailQueryOp prevents Sync Gateway from having too many concurrent
// queries against Couchbase Server
func (b *GocbV2Bucket) waitForAvailQueryOp() {
	b.queryOps <- struct{}{}
}

func (b *GocbV2Bucket) releaseQueryOp() {
	<-b.queryOps
}

func createBatchesEntries(batchSize uint, entries []*sgbucket.BulkSetEntry) [][]*sgbucket.BulkSetEntry {
	// boundary checking
	if len(entries) == 0 {
		WarnfCtx(context.Background(), "createBatchesEnrties called with empty entries")
		return [][]*sgbucket.BulkSetEntry{}
	}
	if batchSize == 0 {
		WarnfCtx(context.Background(), "createBatchesEntries called with invalid batchSize")
		result := [][]*sgbucket.BulkSetEntry{}
		return append(result, entries)
	}

	batches := [][]*sgbucket.BulkSetEntry{}
	batch := []*sgbucket.BulkSetEntry{}

	for idxEntry, entry := range entries {

		batch = append(batch, entry)

		isBatchFull := uint(len(batch)) == batchSize
		isLastEntry := idxEntry == (len(entries) - 1)

		if isBatchFull || isLastEntry {
			// this batch is full, add it to batches and start a new batch
			batches = append(batches, batch)
			batch = []*sgbucket.BulkSetEntry{}
		}

	}
	return batches

}

func createBatchesKeys(batchSize uint, keys []string) [][]string {

	// boundary checking
	if len(keys) == 0 {
		WarnfCtx(context.Background(), "createBatchesKeys called with empty keys")
		return [][]string{}
	}
	if batchSize == 0 {
		WarnfCtx(context.Background(), "createBatchesKeys called with invalid batchSize")
		result := [][]string{}
		return append(result, keys)
	}

	batches := [][]string{}
	batch := []string{}

	for idxKey, key := range keys {

		batch = append(batch, key)

		isBatchFull := uint(len(batch)) == batchSize
		isLastKey := idxKey == (len(keys) - 1)

		if isBatchFull || isLastKey {
			// this batch is full, add it to batches and start a new batch
			batches = append(batches, batch)
			batch = []string{}
		}

	}
	return batches

}

// If the error is a net/url.Error and the error message is:
//
//	net/http: request canceled while waiting for connection
//
// Then it means that the view request timed out, most likely due to the fact that it's a stale=false query and
// it's rebuilding the index.  In that case, it's desirable to return a more informative error than the
// underlying net/url.Error. See https://github.com/couchbase/sync_gateway/issues/2639
func isGoCBQueryTimeoutError(err error) bool {

	if err == nil {
		return false
	}

	// If it's not a *url.Error, then it's not a viewtimeout error
	netUrlError, ok := pkgerrors.Cause(err).(*url.Error)
	if !ok {
		return false
	}

	// If it's a *url.Error and contains the "request canceled" substring, then it's a viewtimeout error.
	return strings.Contains(netUrlError.Error(), "request canceled")

}

// putDDocForTombstones uses the provided client and endpoints to create a design doc with index_xattr_on_deleted_docs=true
func putDDocForTombstones(name string, payload []byte, capiEps []string, client *http.Client, username string, password string) error {

	// From gocb.Bucket.getViewEp() - pick view endpoint at random
	if len(capiEps) == 0 {
		return errors.New("No available view nodes.")
	}
	viewEp := capiEps[rand.Intn(len(capiEps))]

	// Based on implementation in gocb.BucketManager.UpsertDesignDocument
	uri := fmt.Sprintf("/_design/%s", name)
	body := bytes.NewReader(payload)

	// Build the HTTP request
	req, err := http.NewRequest("PUT", viewEp+uri, body)
	if err != nil {
		return err
	}
	req.Header.Add("Content-Type", "application/json")
	req.SetBasicAuth(username, password)

	resp, err := client.Do(req)
	if err != nil {
		return err
	}

	defer ensureBodyClosed(resp.Body)
	if resp.StatusCode != 201 {
		data, err := io.ReadAll(resp.Body)
		if err != nil {
			return err
		}
		return fmt.Errorf("Client error: %s", string(data))
	}

	return nil

}

// QueryBucketItemCount uses a request plus query to get the number of items in a bucket, as the REST API can be slow to update its value.
// Requires a primary index on the bucket.
func QueryBucketItemCount(n1qlStore N1QLStore) (itemCount int, err error) {
	statement := fmt.Sprintf("SELECT COUNT(1) AS count FROM %s", KeyspaceQueryToken)
	r, err := n1qlStore.Query(statement, nil, RequestPlus, true)
	if err != nil {
		return -1, err
	}
	var val struct {
		Count int `json:"count"`
	}
	err = r.One(&val)
	if err != nil {
		return -1, err
	}
	return val.Count, nil
}

func isMinimumVersion(major, minor, requiredMajor, requiredMinor uint64) bool {
	if major < requiredMajor {
		return false
	}

	if major == requiredMajor && minor < requiredMinor {
		return false
	}

	return true
}

func normalizeIntToUint(value interface{}) (uint, error) {
	switch typeValue := value.(type) {
	case int:
		return uint(typeValue), nil
	case uint64:
		return uint(typeValue), nil
	case string:
		i, err := strconv.Atoi(typeValue)
		return uint(i), err
	default:
		return uint(0), fmt.Errorf("Unable to convert %v (%T) -> uint.", value, value)
	}
}

func asBool(value interface{}) bool {

	switch typeValue := value.(type) {
	case string:
		parsedVal, err := strconv.ParseBool(typeValue)
		if err != nil {
			WarnfCtx(context.Background(), "asBool called with unknown value: %v.  defaulting to false", typeValue)
			return false
		}
		return parsedVal
	case bool:
		return typeValue
	default:
		WarnfCtx(context.Background(), "asBool called with unknown type: %T.  defaulting to false", typeValue)
		return false
	}

}

// AsLeakyBucket tries to return the given bucket as a LeakyBucket.
func AsLeakyBucket(bucket Bucket) (*LeakyBucket, bool) {

	var underlyingBucket Bucket
	switch typedBucket := bucket.(type) {
	case *LeakyBucket:
		return typedBucket, true
	case *TestBucket:
		underlyingBucket = typedBucket.Bucket
	default:
		// bail out for unrecognised/unsupported buckets
		return nil, false
	}

	return AsLeakyBucket(underlyingBucket)
}

func GoCBBucketMgmtEndpoints(bucket CouchbaseBucketStore) (url []string, err error) {
	return bucket.MgmtEps()
}

// Get one of the management endpoints.  It will be a string such as http://couchbase
func GoCBBucketMgmtEndpoint(bucket CouchbaseBucketStore) (url string, err error) {
	mgmtEps, err := bucket.MgmtEps()
	if err != nil {
		return "", err
	}
	bucketEp := mgmtEps[rand.Intn(len(mgmtEps))]
	return bucketEp, nil
}
