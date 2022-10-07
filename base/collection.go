/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package base

import (
	"context"
	"errors"
	"expvar"
	"fmt"
	"io"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/couchbase/gocb/v2"
	"github.com/couchbase/gocbcore/v10"
	sgbucket "github.com/couchbase/sg-bucket"
	pkgerrors "github.com/pkg/errors"
)

var ErrCollectionsUnsupported = errors.New("collections not supported")

const DefaultCollectionID = 0x0

var _ sgbucket.KVStore = &Collection{}
var _ CouchbaseStore = &Collection{}

// Connect to the default collection for the specified bucket
func GetCouchbaseCollection(spec BucketSpec) (*Collection, error) {

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

	return GetCollectionFromCluster(cluster, spec, 30)

}

// bucketSpecScopeAndCollection returns a scope and collection for the given bucket spec.
func bucketSpecScopeAndCollection(spec BucketSpec) (scope string, collection string) {
	scope, collection = DefaultScope, DefaultCollection
	if spec.Scope != nil {
		scope = *spec.Scope
	}
	if spec.Collection != nil {
		collection = *spec.Collection
	}
	return scope, collection
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

func GetCollectionFromCluster(cluster *gocb.Cluster, spec BucketSpec, waitUntilReadySeconds int) (*Collection, error) {

	// Connect to bucket
	bucket := cluster.Bucket(spec.BucketName)
	err := bucket.WaitUntilReady(time.Duration(waitUntilReadySeconds)*time.Second, nil)
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

	specScope, specCollection := bucketSpecScopeAndCollection(spec)
	c := cluster.Bucket(spec.BucketName).Scope(specScope).Collection(specCollection)

	collection := &Collection{
		Collection:                c,
		Spec:                      spec,
		cluster:                   cluster,
		clusterCompatMajorVersion: uint64(clusterCompatMajor),
		clusterCompatMinorVersion: uint64(clusterCompatMinor),
	}

	// Set limits for concurrent query and kv ops
	maxConcurrentQueryOps := MaxConcurrentQueryOps
	if spec.MaxConcurrentQueryOps != nil {
		maxConcurrentQueryOps = *spec.MaxConcurrentQueryOps
	}

	queryNodeCount, err := collection.QueryEpsCount()
	if err != nil || queryNodeCount == 0 {
		queryNodeCount = 1
	}

	if maxConcurrentQueryOps > DefaultHttpMaxIdleConnsPerHost*queryNodeCount {
		maxConcurrentQueryOps = DefaultHttpMaxIdleConnsPerHost * queryNodeCount
		InfofCtx(context.TODO(), KeyAll, "Setting max_concurrent_query_ops to %d based on query node count (%d)", maxConcurrentQueryOps, queryNodeCount)
	}

	collection.queryOps = make(chan struct{}, maxConcurrentQueryOps)

	// gocb v2 has a queue size of 2048 per pool per server node.
	// SG conservatively limits to 1000 per pool per node, to handle imbalanced
	// request distribution between server nodes.
	nodeCount := 1
	mgmtEps, mgmtEpsErr := collection.MgmtEps()
	if mgmtEpsErr != nil && len(mgmtEps) > 0 {
		nodeCount = len(mgmtEps)
	}
	numPools := 1
	if spec.KvPoolSize > 0 {
		numPools = spec.KvPoolSize
	}
	collection.kvOps = make(chan struct{}, MaxConcurrentSingleOps*nodeCount*numPools)

	return collection, nil
}

type Collection struct {
	*gocb.Collection               // underlying gocb Collection
	Spec             BucketSpec    // keep a copy of the BucketSpec for DCP usage
	cluster          *gocb.Cluster // Associated cluster - required for N1QL operations
	queryOps         chan struct{} // Manages max concurrent query ops
	kvOps            chan struct{} // Manages max concurrent kv ops
	collectionID     atomic.Value  // cached copy of collectionID

	clusterCompatMajorVersion, clusterCompatMinorVersion uint64 // E.g: 6 and 0 for 6.0.3
}

// DataStore
func (c *Collection) GetName() string {
	// Returning bucket name until full collection support is implemented
	return c.Collection.Bucket().Name()
}

func (c *Collection) UUID() (string, error) {
	config, configErr := c.getConfigSnapshot()
	if configErr != nil {
		return "", fmt.Errorf("Unable to determine bucket UUID: %w", configErr)
	}
	return config.BucketUUID(), nil
}

// GetCluster returns an open cluster object
func (c *Collection) GetCluster() *gocb.Cluster {
	return c.cluster
}

func (c *Collection) Close() {
	if c.cluster != nil {
		if err := c.cluster.Close(nil); err != nil {
			WarnfCtx(context.TODO(), "Error closing collection cluster: %v", err)
		}
	}
	return
}

func (c *Collection) IsSupported(feature sgbucket.DataStoreFeature) bool {

	switch feature {
	case sgbucket.DataStoreFeatureSubdocOperations, sgbucket.DataStoreFeatureXattrs, sgbucket.DataStoreFeatureCrc32cMacroExpansion:
		// Available on all supported server versions
		return true
	case sgbucket.DataStoreFeatureN1ql:
		agent, err := c.getGoCBAgent()
		if err != nil {
			return false
		}
		return len(agent.N1qlEps()) > 0
	case sgbucket.DataStoreFeatureCreateDeletedWithXattr:
		status, err := c.Bucket().Internal().CapabilityStatus(gocb.CapabilityCreateAsDeleted)
		if err != nil {
			return false
		}
		return status == gocb.CapabilityStatusSupported
	case sgbucket.DataStoreFeaturePreserveExpiry, sgbucket.DataStoreFeatureCollections:
		// TODO: Change to capability check when GOCBC-1218 merged
		return isMinimumVersion(c.clusterCompatMajorVersion, c.clusterCompatMinorVersion, 7, 0)
	default:
		return false
	}
}

// KV store

func (c *Collection) Get(k string, rv interface{}) (cas uint64, err error) {

	c.waitForAvailKvOp()
	defer c.releaseKvOp()

	getOptions := &gocb.GetOptions{
		Transcoder: NewSGJSONTranscoder(),
	}
	getResult, err := c.Collection.Get(k, getOptions)
	if err != nil {
		return 0, err
	}
	err = getResult.Content(rv)
	return uint64(getResult.Cas()), err
}

func (c *Collection) GetRaw(k string) (rv []byte, cas uint64, err error) {
	c.waitForAvailKvOp()
	defer c.releaseKvOp()

	getOptions := &gocb.GetOptions{
		Transcoder: NewSGRawTranscoder(),
	}
	getRawResult, getErr := c.Collection.Get(k, getOptions)
	if getErr != nil {
		return nil, 0, getErr
	}

	err = getRawResult.Content(&rv)
	return rv, uint64(getRawResult.Cas()), err
}

func (c *Collection) GetAndTouchRaw(k string, exp uint32) (rv []byte, cas uint64, err error) {
	c.waitForAvailKvOp()
	defer c.releaseKvOp()

	getAndTouchOptions := &gocb.GetAndTouchOptions{
		Transcoder: NewSGRawTranscoder(),
	}
	getAndTouchRawResult, getErr := c.Collection.GetAndTouch(k, CbsExpiryToDuration(exp), getAndTouchOptions)
	if getErr != nil {
		return nil, 0, getErr
	}

	err = getAndTouchRawResult.Content(&rv)
	return rv, uint64(getAndTouchRawResult.Cas()), err
}

func (c *Collection) Touch(k string, exp uint32) (cas uint64, err error) {
	c.waitForAvailKvOp()
	defer c.releaseKvOp()

	result, err := c.Collection.Touch(k, CbsExpiryToDuration(exp), nil)
	if err != nil {
		return 0, err
	}
	return uint64(result.Cas()), nil
}

func (c *Collection) Add(k string, exp uint32, v interface{}) (added bool, err error) {
	c.waitForAvailKvOp()
	defer c.releaseKvOp()

	opts := &gocb.InsertOptions{
		Expiry:     CbsExpiryToDuration(exp),
		Transcoder: NewSGJSONTranscoder(),
	}
	_, gocbErr := c.Collection.Insert(k, v, opts)
	if gocbErr != nil {
		// Check key exists handling
		if errors.Is(gocbErr, gocb.ErrDocumentExists) {
			return false, nil
		}
		err = pkgerrors.WithStack(gocbErr)
	}
	return err == nil, err
}

func (c *Collection) AddRaw(k string, exp uint32, v []byte) (added bool, err error) {
	c.waitForAvailKvOp()
	defer c.releaseKvOp()

	opts := &gocb.InsertOptions{
		Expiry:     CbsExpiryToDuration(exp),
		Transcoder: NewSGRawTranscoder(),
	}
	_, gocbErr := c.Collection.Insert(k, v, opts)
	if gocbErr != nil {
		// Check key exists handling
		if errors.Is(gocbErr, gocb.ErrDocumentExists) {
			return false, nil
		}
		err = pkgerrors.WithStack(gocbErr)
	}
	return err == nil, err
}

func (c *Collection) Set(k string, exp uint32, opts *sgbucket.UpsertOptions, v interface{}) error {
	c.waitForAvailKvOp()
	defer c.releaseKvOp()

	goCBUpsertOptions := &gocb.UpsertOptions{
		Expiry:     CbsExpiryToDuration(exp),
		Transcoder: NewSGJSONTranscoder(),
	}
	fillUpsertOptions(goCBUpsertOptions, opts)

	if _, ok := v.([]byte); ok {
		goCBUpsertOptions.Transcoder = gocb.NewRawJSONTranscoder()
	}

	_, err := c.Collection.Upsert(k, v, goCBUpsertOptions)
	return err
}

func (c *Collection) SetRaw(k string, exp uint32, opts *sgbucket.UpsertOptions, v []byte) error {
	c.waitForAvailKvOp()
	defer c.releaseKvOp()

	goCBUpsertOptions := &gocb.UpsertOptions{
		Expiry:     CbsExpiryToDuration(exp),
		Transcoder: NewSGRawTranscoder(),
	}
	fillUpsertOptions(goCBUpsertOptions, opts)

	_, err := c.Collection.Upsert(k, v, goCBUpsertOptions)
	return err
}

func (c *Collection) WriteCas(k string, flags int, exp uint32, cas uint64, v interface{}, opt sgbucket.WriteOptions) (casOut uint64, err error) {
	c.waitForAvailKvOp()
	defer c.releaseKvOp()

	var result *gocb.MutationResult
	if cas == 0 {
		insertOpts := &gocb.InsertOptions{
			Expiry:     CbsExpiryToDuration(exp),
			Transcoder: NewSGJSONTranscoder(),
		}
		if opt == sgbucket.Raw {
			insertOpts.Transcoder = gocb.NewRawBinaryTranscoder()
		}
		result, err = c.Collection.Insert(k, v, insertOpts)
	} else {
		replaceOpts := &gocb.ReplaceOptions{
			Cas:        gocb.Cas(cas),
			Expiry:     CbsExpiryToDuration(exp),
			Transcoder: NewSGJSONTranscoder(),
		}
		if opt == sgbucket.Raw {
			replaceOpts.Transcoder = gocb.NewRawBinaryTranscoder()
		}
		result, err = c.Collection.Replace(k, v, replaceOpts)
	}
	if err != nil {
		return 0, err
	}
	return uint64(result.Cas()), nil
}

func (c *Collection) Delete(k string) error {
	_, err := c.Remove(k, 0)
	return err
}

func (c *Collection) Remove(k string, cas uint64) (casOut uint64, err error) {
	c.waitForAvailKvOp()
	defer c.releaseKvOp()

	result, errRemove := c.Collection.Remove(k, &gocb.RemoveOptions{Cas: gocb.Cas(cas)})
	if errRemove == nil && result != nil {
		casOut = uint64(result.Cas())
	}
	return casOut, errRemove
}

func (c *Collection) Update(k string, exp uint32, callback sgbucket.UpdateFunc) (casOut uint64, err error) {
	for {
		var value []byte
		var err error
		var callbackExpiry *uint32

		// Load the existing value.
		getOptions := &gocb.GetOptions{
			Transcoder: gocb.NewRawJSONTranscoder(),
		}

		var cas uint64

		c.waitForAvailKvOp()
		getResult, err := c.Collection.Get(k, getOptions)
		c.releaseKvOp()

		if err != nil {
			if !errors.Is(err, gocb.ErrDocumentNotFound) {
				// Unexpected error, abort
				return cas, err
			}
			cas = 0 // Key not found error
		} else {
			cas = uint64(getResult.Cas())
			err = getResult.Content(&value)
			if err != nil {
				return 0, err
			}
		}

		// Invoke callback to get updated value
		var isDelete bool
		value, callbackExpiry, isDelete, err = callback(value)
		if err != nil {
			return cas, err
		}

		if callbackExpiry != nil {
			exp = *callbackExpiry
		}

		var casGoCB gocb.Cas
		var result *gocb.MutationResult
		casRetry := false

		c.waitForAvailKvOp()
		if cas == 0 {
			// If the Get fails, the cas will be 0 and so call Insert().
			// If we get an error on the insert, due to a race, this will
			// go back through the cas loop
			insertOpts := &gocb.InsertOptions{
				Transcoder: gocb.NewRawJSONTranscoder(),
				Expiry:     CbsExpiryToDuration(exp),
			}
			result, err = c.Collection.Insert(k, value, insertOpts)
			if err == nil {
				casGoCB = result.Cas()
			} else if errors.Is(err, gocb.ErrDocumentExists) {
				casRetry = true
			}
		} else {
			if value == nil && isDelete {
				removeOptions := &gocb.RemoveOptions{
					Cas: gocb.Cas(cas),
				}
				result, err = c.Collection.Remove(k, removeOptions)
				if err == nil {
					casGoCB = result.Cas()
				} else if errors.Is(err, gocb.ErrCasMismatch) {
					casRetry = true
				}
			} else {
				// Otherwise, attempt to do a replace.  won't succeed if
				// updated underneath us
				replaceOptions := &gocb.ReplaceOptions{
					Transcoder: gocb.NewRawJSONTranscoder(),
					Cas:        gocb.Cas(cas),
					Expiry:     CbsExpiryToDuration(exp),
				}
				result, err = c.Collection.Replace(k, value, replaceOptions)
				if err == nil {
					casGoCB = result.Cas()
				} else if errors.Is(err, gocb.ErrCasMismatch) {
					casRetry = true
				}
			}
		}
		c.releaseKvOp()

		if casRetry {
			// retry on cas failure
		} else {
			// err will be nil if successful
			return uint64(casGoCB), err
		}
	}
}

func (c *Collection) Incr(k string, amt, def uint64, exp uint32) (uint64, error) {
	c.waitForAvailKvOp()
	defer c.releaseKvOp()
	if amt == 0 {
		return 0, errors.New("amt passed to Incr must be non-zero")
	}
	incrOptions := gocb.IncrementOptions{
		Initial: int64(def),
		Delta:   amt,
		Expiry:  CbsExpiryToDuration(exp),
	}
	incrResult, err := c.Collection.Binary().Increment(k, &incrOptions)
	if err != nil {
		return 0, err
	}

	return incrResult.Content(), nil
}

func (c *Collection) StartDCPFeed(args sgbucket.FeedArguments, callback sgbucket.FeedEventCallbackFunc, dbStats *expvar.Map) error {
	groupID := ""
	return StartGocbDCPFeed(c, c.Spec.BucketName, args, callback, dbStats, DCPMetadataStoreInMemory, groupID)
}
func (c *Collection) StartTapFeed(args sgbucket.FeedArguments, dbStats *expvar.Map) (sgbucket.MutationFeed, error) {
	return nil, errors.New("StartTapFeed not implemented")
}
func (c *Collection) Dump() {
	return
}

// CouchbaseStore

func (c *Collection) GetStatsVbSeqno(maxVbno uint16, useAbsHighSeqNo bool) (uuids map[uint16]uint64, highSeqnos map[uint16]uint64, seqErr error) {

	agent, agentErr := c.getGoCBAgent()
	if agentErr != nil {
		return nil, nil, agentErr
	}

	statsOptions := gocbcore.StatsOptions{
		Key:      "vbucket-seqno",
		Deadline: c.getBucketOpDeadline(),
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

func (c *Collection) GetMaxVbno() (uint16, error) {

	config, configErr := c.getConfigSnapshot()
	if configErr != nil {
		return 0, fmt.Errorf("Unable to determine vbucket count: %w", configErr)
	}

	vbNo, err := config.NumVbuckets()
	if err != nil {
		return 0, fmt.Errorf("Unable to determine vbucket count: %w", err)
	}

	return uint16(vbNo), nil
}

func (c *Collection) getConfigSnapshot() (*gocbcore.ConfigSnapshot, error) {
	agent, err := c.getGoCBAgent()
	if err != nil {
		return nil, fmt.Errorf("no gocbcore.Agent: %w", err)
	}

	config, configErr := agent.ConfigSnapshot()
	if configErr != nil {
		return nil, fmt.Errorf("no gocbcore.Agent config snapshot: %w", configErr)
	}
	return config, nil
}

func (c *Collection) IsError(err error, errorType sgbucket.DataStoreErrorType) bool {
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

// Recoverable errors or timeouts trigger retry for gocb v2 read operations
func (c *Collection) isRecoverableReadError(err error) bool {

	if err == nil {
		return false
	}

	if isGoCBTimeoutError(err) {
		return true
	}

	if errors.Is(err, gocb.ErrTemporaryFailure) || errors.Is(err, gocb.ErrOverload) {
		return true
	}
	return false
}

// Recoverable errors trigger retry for gocb v2 write operations
func (c *Collection) isRecoverableWriteError(err error) bool {

	if err == nil {
		return false
	}

	// TODO: CBG-1142 Handle SyncWriteInProgress errors --> Currently gocbv2 retries this internally and returns a
	//  timeout with KV_SYNC_WRITE_IN_PROGRESS as its reason. Decision on whether to handle inside gocb or retry here
	if errors.Is(err, gocb.ErrTemporaryFailure) || errors.Is(err, gocb.ErrOverload) {
		return true
	}
	return false
}

// This flushes the *entire* bucket associated with the collection (not just the collection).  Intended for test usage only.
func (c *Collection) Flush() error {

	bucketManager := c.cluster.Buckets()

	workerFlush := func() (shouldRetry bool, err error, value interface{}) {
		if err := bucketManager.FlushBucket(c.Bucket().Name(), nil); err != nil {
			WarnfCtx(context.TODO(), "Error flushing bucket %s: %v  Will retry.", MD(c.Bucket().Name()).Redact(), err)
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
		itemCount, err := c.BucketItemCount()
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
		return pkgerrors.Wrapf(err, "Error during Wait until bucket %s has 0 items after flush", MD(c.Bucket().Name()).Redact())
	}

	return nil

}

// BucketItemCount first tries to retrieve an accurate bucket count via N1QL,
// but falls back to the REST API if that cannot be done (when there's no index to count all items in a bucket)
func (c *Collection) BucketItemCount() (itemCount int, err error) {
	itemCount, err = QueryBucketItemCount(c)
	if err == nil {
		return itemCount, nil
	}

	// TODO: implement APIBucketItemCount for collections as part of CouchbaseStore refactoring.  Until then, give flush a moment to finish
	time.Sleep(1 * time.Second)
	// itemCount, err = bucket.APIBucketItemCount()
	return 0, err
}

func (c *Collection) MgmtEps() (url []string, err error) {

	agent, err := c.getGoCBAgent()
	if err != nil {
		return url, err
	}
	mgmtEps := agent.MgmtEps()
	if len(mgmtEps) == 0 {
		return nil, fmt.Errorf("No available Couchbase Server nodes")
	}
	return mgmtEps, nil
}

func (c *Collection) QueryEpsCount() (int, error) {
	agent, err := c.getGoCBAgent()
	if err != nil {
		return 0, err
	}

	return len(agent.N1qlEps()), nil
}

// Gets the metadata purge interval for the bucket.  First checks for a bucket-specific value.  If not
// found, retrieves the cluster-wide value.
func (c *Collection) MetadataPurgeInterval() (time.Duration, error) {
	return getMetadataPurgeInterval(c)
}

func (c *Collection) ServerUUID() (uuid string, err error) {
	return getServerUUID(c)
}

func (c *Collection) MaxTTL() (int, error) {
	return getMaxTTL(c)
}

func (c *Collection) HttpClient() *http.Client {
	agent, err := c.getGoCBAgent()
	if err != nil {
		WarnfCtx(context.TODO(), "Unable to obtain gocbcore.Agent while retrieving httpClient:%v", err)
		return nil
	}
	return agent.HTTPClient()
}

// GetExpiry requires a full document retrieval in order to obtain the expiry, which is reasonable for
// current use cases (on-demand import).  If there's a need for expiry as part of normal get, this shouldn't be
// used - an enhanced version of Get() should be implemented to avoid two ops
func (c *Collection) GetExpiry(k string) (expiry uint32, getMetaError error) {
	agent, err := c.getGoCBAgent()
	if err != nil {
		WarnfCtx(context.TODO(), "Unable to obtain gocbcore.Agent while retrieving expiry:%v", err)
		return 0, err
	}

	getMetaOptions := gocbcore.GetMetaOptions{
		Key:      []byte(k),
		Deadline: c.getBucketOpDeadline(),
	}
	if c.IsSupported(sgbucket.DataStoreFeatureCollections) {
		collectionID, err := c.GetCollectionID()
		if err != nil {
			return 0, err
		}
		getMetaOptions.CollectionID = collectionID
	}

	wg := sync.WaitGroup{}
	wg.Add(1)

	getMetaCallback := func(result *gocbcore.GetMetaResult, err error) {
		defer wg.Done()
		if err != nil {
			getMetaError = err
			return
		}
		expiry = result.Expiry
	}

	_, err = agent.GetMeta(getMetaOptions, getMetaCallback)
	if err != nil {
		wg.Done()
		return 0, err
	}
	wg.Wait()

	return expiry, getMetaError
}

func (c *Collection) BucketName() string {
	return c.Bucket().Name()
}

func getTranscoder(value interface{}) gocb.Transcoder {
	switch value.(type) {
	case []byte, *[]byte:
		return gocb.NewRawJSONTranscoder()
	default:
		return nil
	}
}

func (c *Collection) mgmtRequest(method, uri, contentType string, body io.Reader) (*http.Response, error) {
	if contentType == "" && body != nil {
		// TODO: CBG-1948
		panic("Content-type must be specified for non-null body.")
	}

	mgmtEp, err := GoCBBucketMgmtEndpoint(c)
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

	if c.Spec.Auth != nil {
		username, password, _ := c.Spec.Auth.GetCredentials()
		req.SetBasicAuth(username, password)
	}

	return c.HttpClient().Do(req)
}

// This prevents Sync Gateway from overflowing gocb's pipeline
func (c *Collection) waitForAvailKvOp() {
	c.kvOps <- struct{}{}
}

func (c *Collection) releaseKvOp() {
	<-c.kvOps
}

// SGJsonTranscoder reads and writes JSON, with relaxed datatype restrictions on decode, and
// embedded support for writing raw JSON on encode
type SGJSONTranscoder struct {
}

func NewSGJSONTranscoder() *SGJSONTranscoder {
	return &SGJSONTranscoder{}
}

// SGJSONTranscoder supports reading BinaryType documents as JSON, for backward
// compatibility with legacy Sync Gateway data
func (t *SGJSONTranscoder) Decode(bytes []byte, flags uint32, out interface{}) error {
	valueType, compression := gocbcore.DecodeCommonFlags(flags)

	// Make sure compression is disabled
	if compression != gocbcore.NoCompression {
		return errors.New("unexpected value compression")
	}
	// Type-based decoding
	if valueType == gocbcore.BinaryType {
		switch typedOut := out.(type) {
		case *[]byte:
			*typedOut = bytes
			return nil
		case *interface{}:
			*typedOut = bytes
			return nil
		case *string:
			*typedOut = string(bytes)
			return nil
		default:
			return errors.New("you must encode raw JSON data in a byte array or string")
		}
	} else if valueType == gocbcore.StringType {
		return gocb.NewRawStringTranscoder().Decode(bytes, flags, out)
	} else if valueType == gocbcore.JSONType {
		switch out.(type) {
		case []byte, *[]byte:
			return gocb.NewRawJSONTranscoder().Decode(bytes, flags, out)
		default:
			return gocb.NewJSONTranscoder().Decode(bytes, flags, out)
		}
	}

	return errors.New("unexpected expectedFlags value")
}

// SGJSONTranscoder.Encode supports writing JSON as either raw bytes or an unmarshalled interface
func (t *SGJSONTranscoder) Encode(value interface{}) ([]byte, uint32, error) {
	switch value.(type) {
	case []byte, *[]byte:
		return gocb.NewRawJSONTranscoder().Encode(value)
	default:
		return gocb.NewJSONTranscoder().Encode(value)
	}
}

// SGBinaryTranscoder uses the appropriate raw transcoder for the data type.  Provides backward compatibility
// for pre-3.0 documents intended to be binary but written with JSON datatype, and vice versa
type SGRawTranscoder struct {
}

// NewRawBinaryTranscoder returns a new RawBinaryTranscoder.
func NewSGRawTranscoder() *SGRawTranscoder {
	return &SGRawTranscoder{}
}

// Decode applies raw binary transcoding behaviour to decode into a Go type.
func (t *SGRawTranscoder) Decode(bytes []byte, flags uint32, out interface{}) error {
	valueType, compression := gocbcore.DecodeCommonFlags(flags)

	// Make sure compression is disabled
	if compression != gocbcore.NoCompression {
		return errors.New("unexpected value compression")
	}
	// Normal types of decoding
	switch valueType {
	case gocbcore.BinaryType:
		return gocb.NewRawBinaryTranscoder().Decode(bytes, flags, out)
	case gocbcore.StringType:
		return gocb.NewRawStringTranscoder().Decode(bytes, flags, out)
	case gocbcore.JSONType:
		return gocb.NewRawJSONTranscoder().Decode(bytes, flags, out)
	default:
		return errors.New("unexpected expectedFlags value")
	}
}

// Encode applies raw binary transcoding behaviour to encode a Go type.
func (t *SGRawTranscoder) Encode(value interface{}) ([]byte, uint32, error) {
	return gocb.NewRawBinaryTranscoder().Encode(value)

}

// GetGoCBAgent returns the underlying agent from gocbcore
func (c *Collection) getGoCBAgent() (*gocbcore.Agent, error) {
	return c.Bucket().Internal().IORouter()

}

// GetBucketOpDeadline returns a deadline for use in gocbcore calls
func (c *Collection) getBucketOpDeadline() time.Time {
	opTimeout := DefaultGocbV2OperationTimeout
	configOpTimeout := c.Spec.BucketOpTimeout
	if configOpTimeout != nil {
		opTimeout = *configOpTimeout
	}
	return time.Now().Add(opTimeout)
}

// GetCollectionID returns the gocbcore CollectionID for the current collection
func (c *Collection) GetCollectionID() (uint32, error) {
	if !c.IsSupported(sgbucket.DataStoreFeatureCollections) {
		return 0, fmt.Errorf("Couchbase server does not support collections")
	}
	// default collection has a known ID
	if c.IsDefaultScopeCollection() {
		return DefaultCollectionID, nil
	}
	// return cached value if present
	collectionIDAtomic := c.collectionID.Load()
	if collectionIDAtomic != nil {
		collectionID, ok := collectionIDAtomic.(uint32)
		if !ok {
			return 0, fmt.Errorf("Expected Collection.collectionID to be uint32: %T", collectionID)
		}
		return collectionID, nil
	}

	agent, err := c.getGoCBAgent()
	if err != nil {
		return 0, err
	}
	var collectionID uint32
	scope := c.ScopeName()
	collection := c.Name()
	wg := sync.WaitGroup{}
	wg.Add(1)
	var callbackErr error
	callbackFunc := func(res *gocbcore.GetCollectionIDResult, getCollectionErr error) {
		defer wg.Done()
		if getCollectionErr != nil {
			callbackErr = getCollectionErr
			return
		}
		if res == nil {
			callbackErr = fmt.Errorf("getCollectionID not retrieved for %s.%s", scope, collection)
			return
		}

		collectionID = res.CollectionID
	}
	_, err = agent.GetCollectionID(scope,
		collection,
		gocbcore.GetCollectionIDOptions{
			Deadline: c.getBucketOpDeadline(),
		},
		callbackFunc)

	if err != nil {
		wg.Done()
		return 0, fmt.Errorf("GetCollectionID for %s.%s, err: %w", scope, collection, err)
	}
	wg.Wait()
	if callbackErr != nil {
		return 0, fmt.Errorf("GetCollectionID for %s.%s, err: %w", scope, collection, callbackErr)
	}
	// cache value for future use
	c.collectionID.Store(collectionID)
	return collectionID, nil
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

func (c *Collection) GetCollectionManifest() (gocbcore.Manifest, error) {
	c.waitForAvailKvOp()
	defer c.releaseKvOp()
	agent, err := c.Collection.Bucket().Internal().IORouter()
	if err != nil {
		return gocbcore.Manifest{}, fmt.Errorf("failed to get gocbcore agent: %w", err)
	}
	result := make(chan any) // either a CollectionsManifest or error
	_, err = agent.GetCollectionManifest(gocbcore.GetCollectionManifestOptions{
		Deadline: c.getBucketOpDeadline(),
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

// asCollection tries to return the given bucket as a Collection.
func AsCollection(bucket Bucket) (*Collection, error) {
	var underlyingBucket Bucket
	switch typedBucket := bucket.(type) {
	case *Collection:
		return typedBucket, nil
	case *LoggingBucket:
		underlyingBucket = typedBucket.GetUnderlyingBucket()
	case *LeakyBucket:
		underlyingBucket = typedBucket.GetUnderlyingBucket()
	case *TestBucket:
		underlyingBucket = typedBucket.Bucket
	default:
		return nil, fmt.Errorf("bucket has unrecognized type %T", bucket)
	}

	return AsCollection(underlyingBucket)
}
