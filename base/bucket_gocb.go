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
	"io/ioutil"
	"math/rand"
	"net/http"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/couchbase/gocbcore/v10/memd"
	sgbucket "github.com/couchbase/sg-bucket"
	pkgerrors "github.com/pkg/errors"
	"gopkg.in/couchbase/gocb.v1"
	"gopkg.in/couchbase/gocbcore.v7"
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

	// Can be removed and usages swapped out once this is present in gocb
	SubdocDocFlagCreateAsDeleted = gocb.SubdocDocFlag(gocbcore.SubdocDocFlag(0x08))
)

var recoverableGocbV1Errors = map[string]struct{}{
	gocbcore.ErrOverload.Error(): {},
	gocbcore.ErrBusy.Error():     {},
	gocbcore.ErrTmpFail.Error():  {},
}

// Implementation of sgbucket.Bucket that talks to a Couchbase server and uses gocb v1
type CouchbaseBucketGoCB struct {
	*gocb.Bucket               // the underlying gocb bucket
	Spec         BucketSpec    // keep a copy of the BucketSpec for DCP usage
	singleOps    chan struct{} // Manages max concurrent single ops (per kv node)
	bulkOps      chan struct{} // Manages max concurrent bulk ops (per kv node)
	queryOps     chan struct{} // Manages max concurrent view / query ops (per kv node)

	clusterCompatMajorVersion, clusterCompatMinorVersion uint64 // E.g: 6 and 0 for 6.0.3
}

var _ sgbucket.KVStore = &CouchbaseBucketGoCB{}
var _ CouchbaseStore = &CouchbaseBucketGoCB{}

// Creates a Bucket that talks to a real live Couchbase server.
func GetCouchbaseBucketGoCB(spec BucketSpec) (bucket *CouchbaseBucketGoCB, err error) {
	logCtx := context.TODO()
	connString, err := spec.GetGoCBConnString()
	if err != nil {
		WarnfCtx(logCtx, "Unable to parse server value: %s error: %v", SD(spec.Server), err)
		return nil, err
	}

	cluster, err := gocb.Connect(connString)
	if err != nil {
		InfofCtx(logCtx, KeyAuth, "gocb connect returned error: %v", err)
		return nil, err
	}

	bucketPassword := ""
	// Check for client cert (x.509) authentication
	if spec.Certpath != "" {
		InfofCtx(logCtx, KeyAuth, "Attempting cert authentication against bucket %s on %s", MD(spec.BucketName), MD(spec.Server))
		certAuthErr := cluster.Authenticate(gocb.CertAuthenticator{})
		if certAuthErr != nil {
			InfofCtx(logCtx, KeyAuth, "Error Attempting certificate authentication %s", certAuthErr)
			return nil, pkgerrors.WithStack(certAuthErr)
		}
	} else if spec.Auth != nil {
		InfofCtx(logCtx, KeyAuth, "Attempting credential authentication against bucket %s on %s", MD(spec.BucketName), MD(spec.Server))
		user, pass, _ := spec.Auth.GetCredentials()
		authErr := cluster.Authenticate(gocb.PasswordAuthenticator{
			Username: user,
			Password: pass,
		})
		// If RBAC authentication fails, revert to non-RBAC authentication by including the password to OpenBucket
		if authErr != nil {
			WarnfCtx(logCtx, "RBAC authentication against bucket %s as user %s failed - will re-attempt w/ bucketname, password", MD(spec.BucketName), UD(user))
			bucketPassword = pass
		}
	}

	return GetCouchbaseBucketGoCBFromAuthenticatedCluster(cluster, spec, bucketPassword)
}

func GetCouchbaseBucketGoCBFromAuthenticatedCluster(cluster *gocb.Cluster, spec BucketSpec, bucketPassword string) (bucket *CouchbaseBucketGoCB, err error) {
	logCtx := context.TODO()
	goCBBucket, err := cluster.OpenBucket(spec.BucketName, bucketPassword)
	if err != nil {
		InfofCtx(logCtx, KeyAll, "Error opening bucket %s: %v", spec.BucketName, err)
		if pkgerrors.Cause(err) == gocb.ErrAuthError {
			return nil, ErrAuthError
		}
		return nil, pkgerrors.WithStack(err)
	}
	InfofCtx(logCtx, KeyAll, "Successfully opened bucket %s", spec.BucketName)

	// Query node meta to find cluster compat version
	user, pass, _ := spec.Auth.GetCredentials()
	nodesMetadata, err := cluster.Manager(user, pass).Internal().GetNodesMetadata()
	if err != nil || len(nodesMetadata) == 0 {
		_ = goCBBucket.Close()
		return nil, fmt.Errorf("Unable to get server cluster compatibility for %d nodes: %w", len(nodesMetadata), err)
	}
	// Safe to get first node as there will always be at least one node in the list and cluster compat is uniform across all nodes.
	clusterCompatMajor, clusterCompatMinor := decodeClusterVersion(nodesMetadata[0].ClusterCompatibility)

	// Set the GoCB opTimeout which controls how long blocking GoCB ops remain blocked before
	// returning an "operation timed out" error.  Defaults to 2.5 seconds.  (SG #3508)
	if spec.BucketOpTimeout != nil {
		goCBBucket.SetOperationTimeout(*spec.BucketOpTimeout)
		// Update the bulk op timeout to preserve the 1:4 ratio between op timeouts and bulk op timeouts.
		goCBBucket.SetBulkOperationTimeout(*spec.BucketOpTimeout * 4)
	}

	if spec.CouchbaseDriver == GoCBCustomSGTranscoder {
		// Set transcoder to SGTranscoder to avoid cases where it tries to write docs as []byte without setting
		// the proper doctype flag and then later read them as JSON, which fails because it gets back a []byte
		// initially this was using SGTranscoder for all GoCB buckets, but due to
		// https://github.com/couchbase/sync_gateway/pull/2416#issuecomment-288882896
		// it's only being set for data buckets
		goCBBucket.SetTranscoder(SGTranscoder{})
	}

	spec.MaxNumRetries = 10
	spec.InitialRetrySleepTimeMS = 5

	// Identify number of nodes to use as a multiplier for MaxConcurrentOps, since gocb maintains one pipeline per data node.
	// TODO: We don't currently have a process to monitor cluster changes behind a gocb bucket.  When that's available, should
	//       consider the ability to modify this as the cluster changes.
	nodeCount := 1
	mgmtEps := goCBBucket.IoRouter().MgmtEps()
	if mgmtEps != nil && len(mgmtEps) > 0 {
		nodeCount = len(mgmtEps)
	}

	// Scale gocb pipeline size with KV pool size
	numPools := 1
	if spec.KvPoolSize > 0 {
		numPools = spec.KvPoolSize
	}

	// Define channels to limit the number of concurrent single and bulk operations,
	// to avoid gocb queue overflow issues
	singleOpsQueue := make(chan struct{}, MaxConcurrentSingleOps*nodeCount*numPools)
	bucketOpsQueue := make(chan struct{}, MaxConcurrentBulkOps*nodeCount*numPools)

	maxConcurrentQueryOps := MaxConcurrentQueryOps
	if spec.MaxConcurrentQueryOps != nil {
		maxConcurrentQueryOps = *spec.MaxConcurrentQueryOps
	}

	queryNodeCount := len(goCBBucket.IoRouter().N1qlEps())
	if queryNodeCount == 0 {
		queryNodeCount = 1
	}

	if maxConcurrentQueryOps > DefaultHttpMaxIdleConnsPerHost*queryNodeCount {
		maxConcurrentQueryOps = DefaultHttpMaxIdleConnsPerHost * queryNodeCount
		InfofCtx(logCtx, KeyAll, "Setting max_concurrent_query_ops to %d based on query node count (%d)", maxConcurrentQueryOps, queryNodeCount)
	}

	viewOpsQueue := make(chan struct{}, maxConcurrentQueryOps)

	bucket = &CouchbaseBucketGoCB{
		Bucket:                    goCBBucket,
		Spec:                      spec,
		singleOps:                 singleOpsQueue,
		bulkOps:                   bucketOpsQueue,
		queryOps:                  viewOpsQueue,
		clusterCompatMajorVersion: uint64(clusterCompatMajor),
		clusterCompatMinorVersion: uint64(clusterCompatMinor),
	}

	bucket.Bucket.SetViewTimeout(bucket.Spec.GetViewQueryTimeout())
	bucket.Bucket.SetN1qlTimeout(bucket.Spec.GetViewQueryTimeout())

	InfofCtx(logCtx, KeyAll, "Set query timeouts for bucket %s to cluster:%v, bucket:%v", spec.BucketName, cluster.N1qlTimeout(), bucket.N1qlTimeout())
	return bucket, err
}

func (bucket *CouchbaseBucketGoCB) GetBucketCredentials() (username, password string) {
	if bucket.Spec.Auth != nil {
		username, password, _ = bucket.Spec.Auth.GetCredentials()
	}
	return username, password
}

// Gets the metadata purge interval for the bucket.  First checks for a bucket-specific value.  If not
// found, retrieves the cluster-wide value.
func (bucket *CouchbaseBucketGoCB) MetadataPurgeInterval() (time.Duration, error) {
	return getMetadataPurgeInterval(bucket)
}

// Get the Server UUID of the bucket, this is also known as the Cluster UUID
func (bucket *CouchbaseBucketGoCB) ServerUUID() (uuid string, err error) {
	return getServerUUID(bucket)
}

// Gets the bucket max TTL, or 0 if no TTL was set.  Sync gateway should fail to bring the DB online if this is non-zero,
// since it's not meant to operate against buckets that auto-delete data.
func (bucket *CouchbaseBucketGoCB) MaxTTL() (int, error) {
	return getMaxTTL(bucket)
}

// mgmtRequest is a re-implementation of gocb's mgmtRequest
// TODO: Request gocb to either:
// - Make a public version of mgmtRequest for us to use
// - Or add all of the neccesary APIs we need to use
func (bucket *CouchbaseBucketGoCB) mgmtRequest(method, uri, contentType string, body io.Reader) (*http.Response, error) {
	if contentType == "" && body != nil {
		// TODO: CBG-1948
		panic("Content-type must be specified for non-null body.")
	}

	mgmtEp, err := GoCBBucketMgmtEndpoint(bucket)
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

	username, password := bucket.GetBucketCredentials()
	if username != "" || password != "" {
		req.SetBasicAuth(username, password)
	}

	return bucket.IoRouter().HttpClient().Do(req)
}

func (bucket *CouchbaseBucketGoCB) GetName() string {
	return bucket.Spec.BucketName
}

func (bucket *CouchbaseBucketGoCB) GetRaw(k string) (rv []byte, cas uint64, err error) {

	var returnVal []byte
	cas, err = bucket.Get(k, &returnVal)
	if returnVal == nil {
		return nil, cas, err
	}

	return returnVal, cas, err
}

func (bucket *CouchbaseBucketGoCB) Get(k string, rv interface{}) (cas uint64, err error) {

	bucket.singleOps <- struct{}{}
	defer func() {
		<-bucket.singleOps
	}()
	worker := func() (shouldRetry bool, err error, value uint64) {
		casGoCB, err := bucket.Bucket.Get(k, rv)
		shouldRetry = bucket.isRecoverableReadError(err)
		return shouldRetry, err, uint64(casGoCB)
	}

	// Kick off retry loop
	err, cas = RetryLoopCas("Get", worker, bucket.Spec.RetrySleeper())
	if err != nil {
		err = pkgerrors.Wrapf(err, "Error during Get %s", UD(k).Redact())
	}

	return cas, err

}

// Retry up to the retry limit, then return.  Does not retry items if they had CAS failures,
// and it's up to the caller to handle those.
func (bucket *CouchbaseBucketGoCB) SetBulk(entries []*sgbucket.BulkSetEntry) (err error) {

	// Create the RetryWorker for BulkSet op
	worker := bucket.newSetBulkRetryWorker(entries)

	// Kick off retry loop
	err, _ = RetryLoop("SetBulk", worker, bucket.Spec.RetrySleeper())
	if err != nil {
		return pkgerrors.Wrapf(err, "Error performing SetBulk with %v entries", len(entries))
	}

	return nil

}

func (bucket *CouchbaseBucketGoCB) newSetBulkRetryWorker(entries []*sgbucket.BulkSetEntry) RetryWorker {

	worker := func() (shouldRetry bool, err error, value interface{}) {

		retryEntries := []*sgbucket.BulkSetEntry{}

		// break up into batches
		entryBatches := createBatchesEntries(MaxBulkBatchSize, entries)

		for _, entryBatch := range entryBatches {
			err, retryEntriesForBatch := bucket.processBulkSetEntriesBatch(
				entryBatch,
			)
			retryEntries = append(retryEntries, retryEntriesForBatch...)
			if err != nil {
				return false, err, nil
			}
		}

		// if there are no keys to retry, then we're done.
		if len(retryEntries) == 0 {
			return false, nil, nil
		}

		// otherwise, retry the entries that need to be retried
		entries = retryEntries

		// return true to signal that this function needs to be retried
		return true, nil, nil

	}

	return worker

}

func (bucket *CouchbaseBucketGoCB) processBulkSetEntriesBatch(entries []*sgbucket.BulkSetEntry) (error, []*sgbucket.BulkSetEntry) {

	retryEntries := []*sgbucket.BulkSetEntry{}
	var items []gocb.BulkOp

	for _, entry := range entries {

		switch entry.Cas {
		case 0:
			// if no CAS val, treat it as an insert (similar to WriteCas())
			item := &gocb.InsertOp{
				Key:   entry.Key,
				Value: entry.Value,
			}
			items = append(items, item)
		default:
			// otherwise, treat it as a replace
			item := &gocb.ReplaceOp{
				Key:   entry.Key,
				Value: entry.Value,
				Cas:   gocb.Cas(entry.Cas),
			}
			items = append(items, item)
		}

	}

	// Do the underlying bulk operation
	if err := bucket.Do(items); err != nil {
		return err, retryEntries
	}

	for index, item := range items {
		entry := entries[index]
		switch item := item.(type) {
		case *gocb.InsertOp:
			entry.Cas = uint64(item.Cas)
			entry.Error = item.Err
			if bucket.isRecoverableWriteError(item.Err) {
				retryEntries = append(retryEntries, entry)
			}
		case *gocb.ReplaceOp:
			entry.Cas = uint64(item.Cas)
			entry.Error = item.Err
			if bucket.isRecoverableWriteError(item.Err) {
				retryEntries = append(retryEntries, entry)
			}
		}
	}

	return nil, retryEntries

}

// Retrieve keys in bulk for increased efficiency.  If any keys are not found, they
// will not be returned, and so the size of the map may be less than the size of the
// keys slice, and no error will be returned in that case since it's an expected
// situation.
//
// If there is an "overall error" calling the underlying GoCB bulk operation, then
// that error will be returned.
//
// If there are errors on individual keys -- aside from "not found" errors -- such as
// QueueOverflow errors that can be retried successfully, they will be retried
// with a backoff loop.
func (bucket *CouchbaseBucketGoCB) GetBulkRaw(keys []string) (map[string][]byte, error) {

	// Create a RetryWorker for the GetBulkRaw operation
	worker := bucket.newGetBulkRawRetryWorker(keys)

	// Kick off retry loop
	err, result := RetryLoop("GetBulkRaw", worker, bucket.Spec.RetrySleeper())
	if err != nil {
		err = pkgerrors.Wrapf(err, "Error during GetBulkRaw with %v keys", len(keys))
	}

	// If the RetryLoop returns a nil result, convert to an empty map.
	if result == nil {
		return map[string][]byte{}, err
	}

	// Type assertion of result into a map
	resultMap, ok := result.(map[string][]byte)
	if !ok {
		return nil, RedactErrorf("Error doing type assertion of %v into a map", UD(result))
	}

	return resultMap, err

}

// Retrieve keys in bulk for increased efficiency.  If any keys are not found, they
// will not be returned, and so the size of the map may be less than the size of the
// keys slice, and no error will be returned in that case since it's an expected
// situation.
//
// If there is an "overall error" calling the underlying GoCB bulk operation, then
// that error will be returned.
//
// If there are errors on individual keys -- aside from "not found" errors -- such as
// QueueOverflow errors that can be retried successfully, they will be retried
// with a backoff loop.
func (bucket *CouchbaseBucketGoCB) GetBulkCounters(keys []string) (map[string]uint64, error) {

	// Create a RetryWorker for the GetBulkRaw operation
	worker := bucket.newGetBulkCountersRetryWorker(keys)

	// Kick off retry loop
	err, result := RetryLoop("GetBulkRaw", worker, bucket.Spec.RetrySleeper())
	if err != nil {
		err = pkgerrors.Wrapf(err, "Error during GetBulkRaw with %v keys", len(keys))
	}

	// If the RetryLoop returns a nil result, convert to an empty map.
	if result == nil {
		return map[string]uint64{}, err
	}

	// Type assertion of result into a map
	resultMap, ok := result.(map[string]uint64)
	if !ok {
		return nil, RedactErrorf("Error doing type assertion of %v into a map", UD(result))
	}

	return resultMap, err

}

func (bucket *CouchbaseBucketGoCB) newGetBulkRawRetryWorker(keys []string) RetryWorker {

	// resultAccumulator scoped in closure, will accumulate results across multiple worker invocations
	resultAccumulator := make(map[string][]byte, len(keys))

	// pendingKeys scoped in closure, represents set of keys that still need to be attempted or re-attempted
	pendingKeys := keys

	worker := func() (shouldRetry bool, err error, value interface{}) {

		retryKeys := []string{}
		keyBatches := createBatchesKeys(MaxBulkBatchSize, pendingKeys)
		for _, keyBatch := range keyBatches {

			// process batch and add successful results to resultAccumulator
			// and recoverable (non "Not Found") errors to retryKeys
			err := bucket.processGetRawBatch(keyBatch, resultAccumulator, retryKeys)
			if err != nil {
				return false, err, nil
			}

		}

		// if there are no keys to retry, then we're done.
		if len(retryKeys) == 0 {
			return false, nil, resultAccumulator
		}

		// otherwise, retry the keys the need to be retried
		keys = retryKeys

		// return true to signal that this function needs to be retried
		return true, nil, nil

	}

	return worker

}

func (bucket *CouchbaseBucketGoCB) newGetBulkCountersRetryWorker(keys []string) RetryWorker {

	// resultAccumulator scoped in closure, will accumulate results across multiple worker invocations
	resultAccumulator := make(map[string]uint64, len(keys))

	// pendingKeys scoped in closure, represents set of keys that still need to be attempted or re-attempted
	pendingKeys := keys

	worker := func() (shouldRetry bool, err error, value interface{}) {

		retryKeys := []string{}
		keyBatches := createBatchesKeys(MaxBulkBatchSize, pendingKeys)
		for _, keyBatch := range keyBatches {

			// process batch and add successful results to resultAccumulator
			// and recoverable (non "Not Found") errors to retryKeys
			err := bucket.processGetCountersBatch(keyBatch, resultAccumulator, retryKeys)
			if err != nil {
				return false, err, nil
			}

		}

		// if there are no keys to retry, then we're done.
		if len(retryKeys) == 0 {
			return false, nil, resultAccumulator
		}

		// otherwise, retry the keys the need to be retried
		keys = retryKeys

		// return true to signal that this function needs to be retried
		return true, nil, nil

	}

	return worker

}

func (bucket *CouchbaseBucketGoCB) processGetRawBatch(keys []string, resultAccumulator map[string][]byte, retryKeys []string) error {

	var items []gocb.BulkOp
	for _, key := range keys {
		var value []byte
		item := &gocb.GetOp{Key: key, Value: &value}
		items = append(items, item)
	}
	err := bucket.Do(items)
	if err != nil {
		return err
	}

	for _, item := range items {
		getOp, ok := item.(*gocb.GetOp)
		if !ok {
			continue
		}
		// Ignore any ops with errors.
		// NOTE: some of the errors are misleading:
		// https://issues.couchbase.com/browse/GOCBC-64
		if getOp.Err == nil {
			byteValue, ok := getOp.Value.(*[]byte)
			if ok {
				resultAccumulator[getOp.Key] = *byteValue
			} else {
				WarnfCtx(context.TODO(), "Skipping GetBulkRaw result - unable to cast to []byte.  Type: %v", reflect.TypeOf(getOp.Value))
			}
		} else {
			// if it's a recoverable error, then throw it in retry collection.
			if bucket.isRecoverableReadError(getOp.Err) {
				retryKeys = append(retryKeys, getOp.Key)
			}
		}

	}

	return nil
}

func (bucket *CouchbaseBucketGoCB) processGetCountersBatch(keys []string, resultAccumulator map[string]uint64, retryKeys []string) error {

	var items []gocb.BulkOp
	for _, key := range keys {
		var value uint64
		item := &gocb.GetOp{Key: key, Value: &value}
		items = append(items, item)
	}
	err := bucket.Do(items)
	if err != nil {
		return err
	}

	for _, item := range items {
		getOp, ok := item.(*gocb.GetOp)
		if !ok {
			continue
		}
		// Ignore any ops with errors.
		// NOTE: some of the errors are misleading:
		// https://issues.couchbase.com/browse/GOCBC-64
		if getOp.Err == nil {
			intValue, ok := getOp.Value.(*uint64)
			if ok {
				resultAccumulator[getOp.Key] = *intValue
			} else {
				WarnfCtx(context.Background(), "Skipping GetBulkCounter result - unable to cast to []byte.  Type: %v", reflect.TypeOf(getOp.Value))
			}
		} else {
			// if it's a recoverable error, then throw it in retry collection.
			if bucket.isRecoverableReadError(getOp.Err) {
				retryKeys = append(retryKeys, getOp.Key)
			}
		}

	}

	return nil
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

// Set this to true to cause isRecoverableGoCBError() to return true
// on the next call.  Only useful for unit tests.
var doSingleFakeRecoverableGOCBError = false

// Recoverable errors or timeouts trigger retry for gocb v1 read operations
func (bucket *CouchbaseBucketGoCB) isRecoverableReadError(err error) bool {

	if err == nil {
		return false
	}

	if isGoCBTimeoutError(err) {
		return true
	}

	_, ok := recoverableGocbV1Errors[pkgerrors.Cause(err).Error()]

	return ok
}

// Recoverable errors trigger retry for gocb v1 write operations
func (bucket *CouchbaseBucketGoCB) isRecoverableWriteError(err error) bool {

	if err == nil {
		return false
	}

	_, ok := recoverableGocbV1Errors[pkgerrors.Cause(err).Error()]
	if ok {
		return ok
	}

	// In some circumstances we are unable to supply errors in the recoverable error map so need to check the error
	// codes
	if isKVError(err, memd.StatusSyncWriteInProgress) {
		return true
	}

	return false
}

func isGoCBTimeoutError(err error) bool {
	return pkgerrors.Cause(err) == gocb.ErrTimeout
}

// If the error is a net/url.Error and the error message is:
// 		net/http: request canceled while waiting for connection
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

func (bucket *CouchbaseBucketGoCB) GetAndTouchRaw(k string, exp uint32) (rv []byte, cas uint64, err error) {

	bucket.singleOps <- struct{}{}
	defer func() {
		<-bucket.singleOps
	}()

	var returnVal []byte
	worker := func() (shouldRetry bool, err error, value uint64) {
		casGoCB, err := bucket.Bucket.GetAndTouch(k, exp, &returnVal)
		shouldRetry = bucket.isRecoverableReadError(err)
		return shouldRetry, err, uint64(casGoCB)

	}

	// Kick off retry loop
	err, cas = RetryLoopCas("GetAndTouchRaw", worker, bucket.Spec.MaxRetrySleeper(1000))
	if err != nil {
		err = pkgerrors.Wrapf(err, fmt.Sprintf("Error during GetAndTouchRaw with key %v", UD(k).Redact()))
	}

	// If returnVal was never set to anything, return nil or else type assertion below will panic
	if returnVal == nil {
		return nil, cas, err
	}

	return returnVal, cas, err

}

func (bucket *CouchbaseBucketGoCB) Touch(k string, exp uint32) (cas uint64, err error) {

	bucket.singleOps <- struct{}{}
	defer func() {
		<-bucket.singleOps
	}()

	worker := func() (shouldRetry bool, err error, value uint64) {
		casGoCB, err := bucket.Bucket.Touch(k, 0, exp)
		shouldRetry = bucket.isRecoverableWriteError(err)
		return shouldRetry, err, uint64(casGoCB)

	}

	// Kick off retry loop
	err, cas = RetryLoopCas("Touch", worker, bucket.Spec.MaxRetrySleeper(1000))
	if err != nil {
		err = pkgerrors.Wrapf(err, "Error during Touch for key %v", UD(k).Redact())
	}

	return cas, err

}

func (bucket *CouchbaseBucketGoCB) Add(k string, exp uint32, v interface{}) (added bool, err error) {
	bucket.singleOps <- struct{}{}
	defer func() {
		<-bucket.singleOps
	}()

	worker := func() (shouldRetry bool, err error, value interface{}) {

		_, err = bucket.Bucket.Insert(k, v, exp)
		if bucket.isRecoverableWriteError(err) {
			return true, err, nil
		}

		return false, err, nil

	}
	err, _ = RetryLoop("CouchbaseBucketGoCB Add()", worker, bucket.Spec.RetrySleeper())

	if err != nil && err == gocb.ErrKeyExists {
		return false, nil
	}

	return err == nil, err
}

// GoCB AddRaw writes as BinaryDocument, which results in the document having the
// binary doc common flag set.  Callers that want to write JSON documents as raw bytes should
// pass v as []byte to the stanard bucket.Add
func (bucket *CouchbaseBucketGoCB) AddRaw(k string, exp uint32, v []byte) (added bool, err error) {
	bucket.singleOps <- struct{}{}
	defer func() {
		<-bucket.singleOps
	}()

	worker := func() (shouldRetry bool, err error, value interface{}) {

		_, err = bucket.Bucket.Insert(k, bucket.FormatBinaryDocument(v), exp)
		if bucket.isRecoverableWriteError(err) {
			return true, err, nil
		}

		return false, err, nil

	}
	err, _ = RetryLoop("CouchbaseBucketGoCB AddRaw()", worker, bucket.Spec.RetrySleeper())

	if err != nil {
		if err == gocb.ErrKeyExists {
			return false, nil
		}
		err = pkgerrors.WithStack(err)
	}

	return err == nil, err

}

func (bucket *CouchbaseBucketGoCB) Append(k string, data []byte) error {
	_, err := bucket.Bucket.Append(k, string(data))
	return err
}

func (bucket *CouchbaseBucketGoCB) Set(k string, exp uint32, _ *sgbucket.UpsertOptions, v interface{}) error {

	bucket.singleOps <- struct{}{}
	defer func() {
		<-bucket.singleOps
	}()

	worker := func() (shouldRetry bool, err error, value interface{}) {

		_, err = bucket.Bucket.Upsert(k, v, exp)
		if bucket.isRecoverableWriteError(err) {
			return true, err, nil
		}

		return false, err, nil

	}
	err, _ := RetryLoop("CouchbaseBucketGoCB Set()", worker, bucket.Spec.RetrySleeper())
	if err != nil {
		err = pkgerrors.WithStack(err)
	}
	return err

}

func (bucket *CouchbaseBucketGoCB) SetRaw(k string, exp uint32, _ *sgbucket.UpsertOptions, v []byte) error {

	bucket.singleOps <- struct{}{}
	defer func() {
		<-bucket.singleOps
	}()

	worker := func() (shouldRetry bool, err error, value interface{}) {

		_, err = bucket.Bucket.Upsert(k, bucket.FormatBinaryDocument(v), exp)
		if bucket.isRecoverableWriteError(err) {
			return true, err, nil
		}

		return false, err, nil

	}
	err, _ := RetryLoop("CouchbaseBucketGoCB SetRaw()", worker, bucket.Spec.RetrySleeper())

	return err
}

func (bucket *CouchbaseBucketGoCB) Delete(k string) error {

	worker := func() (shouldRetry bool, err error, value interface{}) {

		_, err = bucket.Remove(k, 0)
		if bucket.isRecoverableWriteError(err) {
			return true, err, nil
		}

		return false, err, nil

	}
	err, _ := RetryLoop("CouchbaseBucketGoCB Delete()", worker, bucket.Spec.RetrySleeper())

	return err

}

func (bucket *CouchbaseBucketGoCB) Remove(k string, cas uint64) (casOut uint64, err error) {

	bucket.singleOps <- struct{}{}
	defer func() {
		<-bucket.singleOps
	}()

	worker := func() (shouldRetry bool, err error, value interface{}) {

		newCas, errRemove := bucket.Bucket.Remove(k, gocb.Cas(cas))
		if bucket.isRecoverableWriteError(errRemove) {
			return true, errRemove, newCas
		}

		return false, errRemove, newCas

	}
	err, newCasVal := RetryLoop("CouchbaseBucketGoCB Remove()", worker, bucket.Spec.RetrySleeper())
	if newCasVal != nil {
		casOut = uint64(newCasVal.(gocb.Cas))
	}

	if err != nil {
		return casOut, err
	}

	return casOut, nil

}

func (bucket *CouchbaseBucketGoCB) Write(k string, flags int, exp uint32, v interface{}, opt sgbucket.WriteOptions) error {
	// TODO: CBG-1948
	PanicfCtx(context.TODO(), "Unimplemented method: Write()")
	return nil
}

func (bucket *CouchbaseBucketGoCB) WriteCas(k string, flags int, exp uint32, cas uint64, v interface{}, opt sgbucket.WriteOptions) (casOut uint64, err error) {

	bucket.singleOps <- struct{}{}
	defer func() {
		<-bucket.singleOps
	}()

	// we only support the sgbucket.Raw WriteOption at this point
	if opt != 0 && opt != sgbucket.Raw {
		// TODO: CBG-1948
		PanicfCtx(context.TODO(), "WriteOption must be empty or sgbucket.Raw")
	}

	// also, flags must be 0, since that is not supported by gocb
	if flags != 0 {
		// TODO: CBG-1948
		PanicfCtx(context.TODO(), "flags must be 0")
	}

	worker := func() (shouldRetry bool, err error, value uint64) {

		if cas == 0 {
			// Try to insert the value into the bucket
			newCas, err := bucket.Bucket.Insert(k, v, exp)
			shouldRetry = bucket.isRecoverableWriteError(err)
			return shouldRetry, err, uint64(newCas)
		}

		// Otherwise, replace existing value
		newCas, err := bucket.Bucket.Replace(k, v, gocb.Cas(cas), exp)
		shouldRetry = bucket.isRecoverableWriteError(err)
		return shouldRetry, err, uint64(newCas)

	}

	// Kick off retry loop
	err, cas = RetryLoopCas("WriteCas", worker, bucket.Spec.RetrySleeper())
	if err != nil {
		err = pkgerrors.Wrapf(err, "WriteCas with key %v", k)
	}

	return cas, err

}

func (bucket *CouchbaseBucketGoCB) Update(k string, exp uint32, callback sgbucket.UpdateFunc) (casOut uint64, err error) {

	retryAttempts := 0
	for {
		var value []byte
		var err error
		var cas uint64

		// Load the existing value.
		value, cas, err = bucket.GetRaw(k)
		if err != nil {
			if !bucket.IsKeyNotFoundError(err) {
				// Unexpected error, abort
				return cas, err
			}
			cas = 0 // Key not found error
		}

		// Invoke callback to get updated value
		var callbackExpiry *uint32
		var isDelete bool
		value, callbackExpiry, isDelete, err = callback(value)
		if err != nil {
			return cas, err
		}
		if callbackExpiry != nil {
			exp = *callbackExpiry
		}

		var casGoCB gocb.Cas
		if cas == 0 {
			// Ignore delete if document doesn't exist
			if isDelete {
				return 0, nil
			}
			// If the Get fails, the cas will be 0 and so call Insert().
			// If we get an error on the insert, due to a race, this will
			// go back through the cas loop
			casGoCB, err = bucket.Bucket.Insert(k, value, exp)
		} else {
			if value == nil && isDelete {
				casGoCB, err = bucket.Bucket.Remove(k, gocb.Cas(cas))
			} else {
				// Otherwise, attempt to do a replace.  won't succeed if
				// updated underneath us
				casGoCB, err = bucket.Bucket.Replace(k, value, gocb.Cas(cas), exp)
			}
		}

		if pkgerrors.Cause(err) == gocb.ErrKeyExists {
			// retry on cas failure
		} else if bucket.isRecoverableWriteError(err) {
			// retry on recoverable failure, up to MaxNumRetries
			if retryAttempts >= bucket.Spec.MaxNumRetries {
				return 0, pkgerrors.Wrapf(err, "WriteUpdate retry loop aborted after %d attempts", retryAttempts)
			}
		} else {
			// err will be nil if successful
			return uint64(casGoCB), err
		}

		retryAttempts++
		DebugfCtx(context.TODO(), KeyCRUD, "CAS Update RetryLoop retrying for doc %q, attempt %d", UD(k), retryAttempts)
	}
}

// Increment the atomic counter k by amt, where amt must be a non-zero delta for the counter.
func (bucket *CouchbaseBucketGoCB) Incr(k string, amt, def uint64, exp uint32) (uint64, error) {
	if amt == 0 {
		return 0, errors.New("amt passed to Incr must be non-zero")
	}

	// This is an actual incr, not just counter retrieval.
	bucket.singleOps <- struct{}{}
	defer func() {
		<-bucket.singleOps
	}()

	worker := func() (shouldRetry bool, err error, value uint64) {

		result, _, err := bucket.Counter(k, int64(amt), int64(def), exp)
		shouldRetry = bucket.isRecoverableWriteError(err)
		return shouldRetry, err, result

	}

	// Kick off retry loop.  Using RetryLoopCas here to return a strongly typed
	// incr result (NOT CAS)
	err, val := RetryLoopCas("Incr with key", worker, bucket.Spec.RetrySleeper())
	if err != nil {
		err = pkgerrors.Wrapf(err, "Error during Incr with key: %v", UD(k).Redact())
	}

	return val, err

}

func (bucket *CouchbaseBucketGoCB) GetDDocs() (ddocs map[string]sgbucket.DesignDoc, err error) {
	bucketManager, err := bucket.getBucketManager()
	if err != nil {
		return nil, err
	}

	gocbDDocs, err := bucketManager.GetDesignDocuments()
	if err != nil {
		return nil, err
	}

	result := make(map[string]*gocb.DesignDocument, len(gocbDDocs))
	for _, ddoc := range gocbDDocs {
		result[ddoc.Name] = ddoc
	}

	resultBytes, err := JSONMarshal(result)
	if err != nil {
		return nil, err
	}

	// Deserialize []byte into "into" empty interface
	if err := JSONUnmarshal(resultBytes, &ddocs); err != nil {
		return nil, err
	}

	return ddocs, nil
}

func (bucket *CouchbaseBucketGoCB) GetDDoc(docname string) (ddoc sgbucket.DesignDoc, err error) {

	bucketManager, err := bucket.getBucketManager()
	if err != nil {
		return ddoc, err
	}

	// TODO: Retry here for recoverable gocb errors?
	designDocPointer, err := bucketManager.GetDesignDocument(docname)
	if err != nil {
		// GoCB doesn't provide an easy way to distinguish what the cause of the error was, so
		// resort to a string pattern match for "not_found" and propagate a 404 error in that case.
		if strings.Contains(err.Error(), "not_found") {
			return ddoc, ErrNotFound
		}
		return ddoc, err
	}

	// Serialize/deserialize to convert to sgbucket.DesignDoc
	designDocBytes, err := JSONMarshal(designDocPointer)
	if err != nil {
		return ddoc, err
	}

	err = JSONUnmarshal(designDocBytes, &ddoc)
	return ddoc, err

}

// Get bucket manager.  Relies on existing auth settings for bucket.
func (bucket *CouchbaseBucketGoCB) getBucketManager() (*gocb.BucketManager, error) {

	username, password := bucket.GetBucketCredentials()

	manager := bucket.Bucket.Manager(username, password)
	if manager == nil {
		return nil, RedactErrorf("Unable to obtain manager for bucket %s", MD(bucket.GetName()))
	}
	return manager, nil
}

func (bucket *CouchbaseBucketGoCB) PutDDoc(docname string, sgDesignDoc *sgbucket.DesignDoc) error {

	manager, err := bucket.getBucketManager()
	if err != nil {
		return err
	}

	gocbDesignDoc := &gocb.DesignDocument{
		Name:  docname,
		Views: make(map[string]gocb.View),
	}

	for viewName, view := range sgDesignDoc.Views {
		gocbView := gocb.View{
			Map:    view.Map,
			Reduce: view.Reduce,
		}
		gocbDesignDoc.Views[viewName] = gocbView
	}

	if sgDesignDoc.Options != nil && sgDesignDoc.Options.IndexXattrOnTombstones {
		return bucket.putDDocForTombstones(gocbDesignDoc)
	}

	// Retry for all errors (The view service sporadically returns 500 status codes with Erlang errors (for unknown reasons) - E.g: 500 {"error":"case_clause","reason":"false"})
	var worker RetryWorker = func() (bool, error, interface{}) {
		err := manager.UpsertDesignDocument(gocbDesignDoc)
		if err != nil {
			WarnfCtx(context.Background(), "Got error from UpsertDesignDocument: %v - Retrying...", err)
			return true, err, nil
		}
		return false, nil, nil
	}

	err, _ = RetryLoop("PutDDocRetryLoop", worker, CreateSleeperFunc(5, 100))
	return err

}

type XattrEnabledDesignDoc struct {
	*gocb.DesignDocument
	IndexXattrOnTombstones bool `json:"index_xattr_on_deleted_docs,omitempty"`
}

// For the view engine to index tombstones, we need to set an explicit property in the design doc.
//      see https://issues.couchbase.com/browse/MB-24616
// This design doc property isn't exposed via the SDK (it's an internal-only property), so we need to
// jump through some hoops to create the design doc.  Follows same approach used internally by gocb.
func (bucket *CouchbaseBucketGoCB) putDDocForTombstones(ddoc *gocb.DesignDocument) error {

	goCBClient := bucket.Bucket.IoRouter()

	username, password := bucket.GetBucketCredentials()

	httpClient := goCBClient.HttpClient()
	xattrEnabledDesignDoc := XattrEnabledDesignDoc{
		DesignDocument:         ddoc,
		IndexXattrOnTombstones: true,
	}
	data, err := JSONMarshal(&xattrEnabledDesignDoc)
	if err != nil {
		return err
	}

	return putDDocForTombstones(ddoc.Name, data, goCBClient.CapiEps(), httpClient, username, password)
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
		data, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return err
		}
		return fmt.Errorf("Client error: %s", string(data))
	}

	return nil

}

func (bucket *CouchbaseBucketGoCB) IsKeyNotFoundError(err error) bool {
	return pkgerrors.Cause(err) == gocb.ErrKeyNotFound
}

func (bucket *CouchbaseBucketGoCB) IsError(err error, errorType sgbucket.DataStoreErrorType) bool {
	if err == nil {
		return false
	}
	switch errorType {
	case sgbucket.KeyNotFoundError:
		return pkgerrors.Cause(err) == gocb.ErrKeyNotFound
	default:
		return false
	}
}

func (bucket *CouchbaseBucketGoCB) DeleteDDoc(docname string) error {

	manager, err := bucket.getBucketManager()
	if err != nil {
		return err
	}

	return manager.RemoveDesignDocument(docname)

}

func (bucket *CouchbaseBucketGoCB) View(ddoc, name string, params map[string]interface{}) (sgbucket.ViewResult, error) {

	// Block until there is an available concurrent view op, release on function exit
	bucket.waitForAvailQueryOp()
	defer bucket.releaseQueryOp()

	viewResult := sgbucket.ViewResult{}
	viewResult.Rows = sgbucket.ViewRows{}

	viewQuery := gocb.NewViewQuery(ddoc, name)

	// convert params map to these params
	if err := applyViewQueryOptions(viewQuery, params); err != nil {
		return viewResult, err
	}

	goCbViewResult, err := bucket.ExecuteViewQuery(viewQuery)

	// If it's a view timeout error, return an error message specific to that.
	if isGoCBQueryTimeoutError(err) {
		return viewResult, ErrViewTimeoutError
	}

	// If it's any other error, return it as-is
	if err != nil {
		return viewResult, pkgerrors.WithStack(err)
	}

	if goCbViewResult != nil {

		viewResult.TotalRows = getTotalRows(goCbViewResult)

		for {

			viewRow := sgbucket.ViewRow{}
			if gotRow := goCbViewResult.Next(&viewRow); gotRow == false {
				break
			}

			viewResult.Rows = append(viewResult.Rows, &viewRow)

		}

		// Any error processing view results is returned on Close.  If Close() returns errors, it most likely means
		// that there were "partial errors" (see SG issue #2702).  If there were multiple partial errors, Close()
		// returns a gocb.MultiError, but if there was only a single partial error, it will be a gocb.viewErr
		err := goCbViewResult.Close()
		if err != nil {
			switch v := err.(type) {
			case *gocb.MultiError:
				for _, multiErr := range v.Errors {
					viewErr := sgbucket.ViewError{
						// Since we only have the error interface, just add the Error() string to the Reason field
						Reason: multiErr.Error(),
					}
					viewResult.Errors = append(viewResult.Errors, viewErr)
				}
			default:
				viewErr := sgbucket.ViewError{
					Reason: v.Error(),
				}
				viewResult.Errors = append(viewResult.Errors, viewErr)
			}
		}

	}

	// Indicate the view response contained partial errors so consumers can determine
	// if the result is valid to their particular use-case (see SG issue #2383)
	if len(viewResult.Errors) > 0 {
		return viewResult, ErrPartialViewErrors
	}

	return viewResult, nil

}

func (bucket CouchbaseBucketGoCB) ViewQuery(ddoc, name string, params map[string]interface{}) (sgbucket.QueryResultIterator, error) {

	bucket.waitForAvailQueryOp()
	defer bucket.releaseQueryOp()

	viewQuery := gocb.NewViewQuery(ddoc, name)

	// convert params map to these params
	if err := applyViewQueryOptions(viewQuery, params); err != nil {
		return nil, err
	}

	goCbViewResult, err := bucket.ExecuteViewQuery(viewQuery)

	// If it's a view timeout error, return an error message specific to that.
	if isGoCBQueryTimeoutError(err) {
		return nil, ErrViewTimeoutError
	}

	// If it's any other error, return it as-is
	if err != nil {
		return nil, pkgerrors.WithStack(err)
	}

	return goCbViewResult, nil

}

func getTotalRows(goCbViewResult gocb.ViewResults) int {
	viewResultMetrics, gotTotalRows := goCbViewResult.(gocb.ViewResultMetrics)
	if !gotTotalRows {
		// Should never happen
		WarnfCtx(context.Background(), "Unable to type assert goCbViewResult -> gocb.ViewResultMetrics.  The total rows count will be missing.")
		return -1
	}
	return viewResultMetrics.TotalRows()
}

// This is a "better-than-nothing" version of Refresh().
// See https://forums.couchbase.com/t/equivalent-of-go-couchbase-bucket-refresh/12498/2
func (bucket *CouchbaseBucketGoCB) Refresh() error {

	// If it's possible to call GetCouchbaseBucketGoCB without error, consider it "refreshed" and return a nil error which will cause the reconnect
	// loop to stop.  otherwise, return an error which will cause it to keep retrying
	// This fixes: https://github.com/couchbase/sync_gateway/issues/2423#issuecomment-294651245
	bucketGoCb, err := GetCouchbaseBucketGoCB(bucket.Spec)
	if bucketGoCb != nil {
		bucketGoCb.Close()
	}

	return err

}

// GoCB (and Server 5.0.0) don't support the TapFeed. For legacy support, start a DCP feed and stream over a single channel
func (bucket *CouchbaseBucketGoCB) StartTapFeed(args sgbucket.FeedArguments, dbStats *expvar.Map) (sgbucket.MutationFeed, error) {

	InfofCtx(context.TODO(), KeyDCP, "Using DCP to generate TAP-like stream")
	// Create the feed channel that will be passed back to the caller
	eventFeed := make(chan sgbucket.FeedEvent, 10)
	terminator := make(chan bool)

	//  Create a new SimpleFeed for Close() support
	feed := &SimpleFeed{
		eventFeed:  eventFeed,
		terminator: terminator,
	}
	args.Terminator = terminator

	callback := func(dcpFeedEvent sgbucket.FeedEvent) bool {
		eventFeed <- dcpFeedEvent
		// TAP feed should not persist checkpoints
		return false
	}

	err := bucket.StartDCPFeed(args, callback, dbStats)
	return feed, err
}

func (bucket *CouchbaseBucketGoCB) StartDCPFeed(args sgbucket.FeedArguments, callback sgbucket.FeedEventCallbackFunc, dbStats *expvar.Map) error {

	// TODO: Evaluate whether to use cbgt for non-sharded caching feed, as a way to push concurrency upstream
	return StartDCPFeed(bucket, bucket.Spec, args, callback, dbStats)

}

func (bucket *CouchbaseBucketGoCB) GetStatsVbSeqno(maxVbno uint16, useAbsHighSeqNo bool) (uuids map[uint16]uint64, highSeqnos map[uint16]uint64, seqErr error) {

	worker := func() (shouldRetry bool, err error, value interface{}) {
		stats, err := bucket.Stats("vbucket-seqno")
		shouldRetry = bucket.isRecoverableReadError(err)
		return shouldRetry, err, stats
	}

	// Kick off retry loop
	err, result := RetryLoop("getStatsVbSeqno", worker, bucket.Spec.RetrySleeper())
	if err != nil {
		return uuids, highSeqnos, err
	}

	// If the retry loop returned a nil result, return error
	if result == nil {
		return uuids, highSeqnos, errors.New("Nil response returned from bucket.Stats call")
	}

	// Type assertion of result
	resultStats, ok := result.(gocb.ServerStats)
	if !ok {
		return uuids, highSeqnos, fmt.Errorf("GetStatsVbSeqno: Error doing type assertion of response (type:%T) to ServerStats", result)
	}

	return GetStatsVbSeqno(resultStats, maxVbno, useAbsHighSeqNo)
}

func (bucket *CouchbaseBucketGoCB) Dump() {
	WarnfCtx(context.Background(), "CouchbaseBucketGoCB: Unimplemented method: Dump()")
}

func (bucket *CouchbaseBucketGoCB) VBHash(docID string) uint32 {
	numVbuckets := bucket.Bucket.IoRouter().NumVbuckets()
	return VBHash(docID, numVbuckets)
}

func (bucket *CouchbaseBucketGoCB) GetMaxVbno() (uint16, error) {

	if bucket.Bucket.IoRouter() != nil {
		return uint16(bucket.Bucket.IoRouter().NumVbuckets()), nil
	}
	return 0, fmt.Errorf("Unable to determine vbucket count")
}

func (bucket *CouchbaseBucketGoCB) UUID() (string, error) {
	return bucket.Bucket.IoRouter().BucketUUID(), nil
}

func (bucket *CouchbaseBucketGoCB) Close() {
	if err := bucket.Bucket.Close(); err != nil {
		WarnfCtx(context.Background(), "Error closing GoCB bucket: %v.", err)
		return
	}
}

// This flushes the bucket.
func (bucket *CouchbaseBucketGoCB) Flush() error {

	bucketManager, err := bucket.getBucketManager()
	if err != nil {
		return err
	}

	workerFlush := func() (shouldRetry bool, err error, value interface{}) {
		err = bucketManager.Flush()
		if err != nil {
			WarnfCtx(context.Background(), "Error flushing bucket %s: %v  Will retry.", MD(bucket.Spec.BucketName).Redact(), err)
			shouldRetry = true
		}
		return shouldRetry, err, nil
	}

	err, _ = RetryLoop("EmptyTestBucket", workerFlush, CreateDoublingSleeperFunc(12, 10))
	if err != nil {
		return err
	}

	// Wait until the bucket item count is 0, since flush is asynchronous
	worker := func() (shouldRetry bool, err error, value interface{}) {
		itemCount, err := bucket.BucketItemCount()
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
		return pkgerrors.Wrapf(err, "Error during Wait until bucket %s has 0 items after flush", MD(bucket.Spec.BucketName).Redact())
	}

	return nil

}

// BucketItemCount first tries to retrieve an accurate bucket count via N1QL,
// but falls back to the REST API if that cannot be done (when there's no index to count all items in a bucket)
func (bucket *CouchbaseBucketGoCB) BucketItemCount() (itemCount int, err error) {
	itemCount, err = QueryBucketItemCount(bucket)
	if err == nil {
		return itemCount, nil
	}

	itemCount, err = bucket.APIBucketItemCount()
	return itemCount, err
}

// Get the number of items in the bucket.
// GOCB doesn't currently offer a way to do this, and so this is a workaround to go directly
// to Couchbase Server REST API.
func (bucket *CouchbaseBucketGoCB) APIBucketItemCount() (itemCount int, err error) {
	uri := fmt.Sprintf("/pools/default/buckets/%s", bucket.Name())
	resp, err := bucket.mgmtRequest(http.MethodGet, uri, "application/json", nil)
	if err != nil {
		return -1, err
	}

	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != 200 {
		_, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return -1, err
		}
		return -1, pkgerrors.Wrapf(err, "Error trying to find number of items in bucket")
	}

	respJson := map[string]interface{}{}
	decoder := JSONDecoder(resp.Body)
	if err := decoder.Decode(&respJson); err != nil {
		return -1, err
	}

	basicStats := respJson["basicStats"].(map[string]interface{})
	itemCountRaw := basicStats["itemCount"]
	itemCountFloat := itemCountRaw.(float64)

	return int(itemCountFloat), nil
}

// QueryBucketItemCount uses a request plus query to get the number of items in a bucket, as the REST API can be slow to update its value.
// Requires a primary index on the bucket.
func QueryBucketItemCount(n1qlStore N1QLStore) (itemCount int, err error) {
	statement := fmt.Sprintf("SELECT COUNT(1) AS count FROM `%s`", KeyspaceQueryToken)
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

func (bucket *CouchbaseBucketGoCB) getExpirySingleAttempt(k string) (expiry uint32, getMetaError error) {

	bucket.singleOps <- struct{}{}
	defer func() {
		<-bucket.singleOps
	}()

	agent := bucket.IoRouter()

	wg := sync.WaitGroup{}
	wg.Add(1)

	getMetaCallback := func(value []byte, flags uint32, cas gocbcore.Cas, exp uint32, seq gocbcore.SeqNo, dataType uint8, deleted uint32, err error) {
		defer wg.Done()
		if err != nil {
			getMetaError = err
			return
		}
		expiry = exp
	}

	_, _ = agent.GetMeta([]byte(k), getMetaCallback)

	wg.Wait()

	if getMetaError != nil {
		getMetaError = pkgerrors.WithStack(getMetaError)
	}

	return expiry, getMetaError

}

func (bucket *CouchbaseBucketGoCB) GetExpiry(k string) (expiry uint32, getMetaError error) {

	worker := func() (shouldRetry bool, err error, value interface{}) {
		expirySingleAttempt, err := bucket.getExpirySingleAttempt(k)
		shouldRetry = bucket.isRecoverableReadError(err)
		return shouldRetry, err, uint32(expirySingleAttempt)
	}

	// Kick off retry loop
	err, result := RetryLoop("GetExpiry", worker, bucket.Spec.RetrySleeper())
	if err != nil {
		err = pkgerrors.Wrapf(err, "Error during GetExpiry for key: %v", UD(k).Redact())
	}

	// If the retry loop returned a nil result, set to 0 to prevent type assertion on nil error
	if result == nil {
		result = uint32(0)
	}

	// Type assertion of result
	expiry, ok := result.(uint32)
	if !ok {
		return 0, RedactErrorf("Get: Error doing type assertion of %v into a uint32,  Key: %v", result, UD(k))
	}

	return expiry, err

}

// Formats binary document to the style expected by the transcoder.  GoCBCustomSGTranscoder
// expects binary documents to be wrapped in BinaryDocument (this supports writing JSON as raw bytes).
// The default goCB transcoder doesn't require additional formatting (assumes all incoming []byte should
// be stored as binary docs.)
func (bucket *CouchbaseBucketGoCB) FormatBinaryDocument(input []byte) interface{} {
	if bucket.Spec.CouchbaseDriver == GoCBCustomSGTranscoder {
		return BinaryDocument(input)
	} else {
		return input
	}
}

func (bucket *CouchbaseBucketGoCB) IsSupported(feature sgbucket.DataStoreFeature) bool {

	major := bucket.clusterCompatMajorVersion
	minor := bucket.clusterCompatMinorVersion
	switch feature {
	case sgbucket.DataStoreFeatureSubdocOperations:
		return isMinimumVersion(major, minor, 4, 5)
	case sgbucket.DataStoreFeatureXattrs:
		return isMinimumVersion(major, minor, 5, 0)
	case sgbucket.DataStoreFeatureN1ql:
		numberOfN1qlNodes := len(bucket.IoRouter().N1qlEps())
		return numberOfN1qlNodes > 0
	// Crc32c macro expansion is used to avoid conflicting with the Couchbase Eventing module, which also uses XATTRS.
	// Since Couchbase Eventing was introduced in Couchbase Server 5.5, the Crc32c macro expansion only needs to be done on 5.5 or later.
	case sgbucket.DataStoreFeatureCrc32cMacroExpansion:
		return isMinimumVersion(major, minor, 5, 5)
	case sgbucket.DataStoreFeatureCreateDeletedWithXattr:
		return isMinimumVersion(major, minor, 6, 6)
	default:
		return false
	}
}

// SubdocInsert inserts a new property into the document body.  Returns error if the property already exists
func (bucket *CouchbaseBucketGoCB) SubdocInsert(docID string, fieldPath string, cas uint64, value interface{}) error {
	_, err := bucket.MutateIn(docID, gocb.Cas(cas), 0).
		Insert(fieldPath, value, false).
		Execute()
	if err == nil {
		return nil
	}

	if err == gocb.ErrKeyNotFound {
		return ErrNotFound
	}

	subdocMutateErr, ok := pkgerrors.Cause(err).(gocbcore.SubDocMutateError)
	if ok && subdocMutateErr.Err == gocb.ErrSubDocPathExists {
		return ErrAlreadyExists
	}

	return err
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

// Applies the viewquery options as specified in the params map to the viewQuery object,
// for example stale=false, etc.
func applyViewQueryOptions(viewQuery *gocb.ViewQuery, params map[string]interface{}) error {

	for optionName, optionValue := range params {
		switch optionName {
		case ViewQueryParamStale:
			optionAsStale := asStale(optionValue)
			viewQuery.Stale(optionAsStale)
		case ViewQueryParamReduce:
			viewQuery.Reduce(asBool(optionValue))
		case ViewQueryParamLimit:
			uintVal, err := normalizeIntToUint(optionValue)
			if err != nil {
				WarnfCtx(context.Background(), "ViewQueryParamLimit error: %v", err)
			}
			viewQuery.Limit(uintVal)
		case ViewQueryParamDescending:
			if asBool(optionValue) == true {
				viewQuery.Order(gocb.Descending)
			}
		case ViewQueryParamSkip:
			uintVal, err := normalizeIntToUint(optionValue)
			if err != nil {
				WarnfCtx(context.Background(), "ViewQueryParamSkip error: %v", err)
			}
			viewQuery.Skip(uintVal)
		case ViewQueryParamGroup:
			viewQuery.Group(asBool(optionValue))
		case ViewQueryParamGroupLevel:
			uintVal, err := normalizeIntToUint(optionValue)
			if err != nil {
				WarnfCtx(context.Background(), "ViewQueryParamGroupLevel error: %v", err)
			}
			viewQuery.GroupLevel(uintVal)
		case ViewQueryParamKey:
			viewQuery.Key(optionValue)
		case ViewQueryParamKeys:
			keys, err := ConvertToEmptyInterfaceSlice(optionValue)
			if err != nil {
				return err
			}
			viewQuery.Keys(keys)
		case ViewQueryParamStartKey, ViewQueryParamEndKey, ViewQueryParamInclusiveEnd, ViewQueryParamStartKeyDocId, ViewQueryParamEndKeyDocId:
			// These are dealt with outside of this case statement to build ranges
		case ViewQueryParamIncludeDocs:
			// Ignored -- see https://forums.couchbase.com/t/do-the-viewquery-options-omit-include-docs-on-purpose/12399
		default:
			return fmt.Errorf("Unexpected view query param: %v.  This will be ignored", optionName)
		}

	}

	// Range: startkey, endkey, inclusiveend
	var startKey, endKey interface{}
	if _, ok := params[ViewQueryParamStartKey]; ok {
		startKey = params[ViewQueryParamStartKey]
	}
	if _, ok := params[ViewQueryParamEndKey]; ok {
		endKey = params[ViewQueryParamEndKey]
	}

	// Default value of inclusiveEnd in Couchbase Server is true (if not specified)
	inclusiveEnd := true
	if _, ok := params[ViewQueryParamInclusiveEnd]; ok {
		inclusiveEnd = asBool(params[ViewQueryParamInclusiveEnd])
	}
	viewQuery.Range(startKey, endKey, inclusiveEnd)

	// IdRange: startKeyDocId, endKeyDocId
	startKeyDocId := ""
	endKeyDocId := ""
	if _, ok := params[ViewQueryParamStartKeyDocId]; ok {
		startKeyDocId = params[ViewQueryParamStartKeyDocId].(string)
	}
	if _, ok := params[ViewQueryParamEndKeyDocId]; ok {
		endKeyDocId = params[ViewQueryParamEndKeyDocId].(string)
	}
	viewQuery.IdRange(startKeyDocId, endKeyDocId)

	return nil

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

func asStale(value interface{}) gocb.StaleMode {

	switch typeValue := value.(type) {
	case string:
		if typeValue == "ok" {
			return gocb.None
		}
		if typeValue == "update_after" {
			return gocb.After
		}
		parsedVal, err := strconv.ParseBool(typeValue)
		if err != nil {
			WarnfCtx(context.Background(), "asStale called with unknown value: %v.  defaulting to stale=false", typeValue)
			return gocb.Before
		}
		if parsedVal {
			return gocb.None
		} else {
			return gocb.Before
		}
	case bool:
		if typeValue {
			return gocb.None
		} else {
			return gocb.Before
		}
	default:
		WarnfCtx(context.Background(), "asBool called with unknown type: %T.  defaulting to false", typeValue)
		return gocb.Before
	}

}

// This prevents Sync Gateway from having too many outstanding concurrent queries against Couchbase Server
func (bucket *CouchbaseBucketGoCB) waitForAvailQueryOp() {
	bucket.queryOps <- struct{}{}
}

func (bucket *CouchbaseBucketGoCB) releaseQueryOp() {
	<-bucket.queryOps
}

func (bucket *CouchbaseBucketGoCB) OverrideClusterCompatVersion(clusterCompatMajorVersion, clusterCompatMinorVersion uint64) {
	bucket.clusterCompatMajorVersion = clusterCompatMajorVersion
	bucket.clusterCompatMinorVersion = clusterCompatMinorVersion
}

// AsGoCBBucket tries to return the given bucket as a GoCBBucket.
func AsGoCBBucket(bucket Bucket) (*CouchbaseBucketGoCB, bool) {

	var underlyingBucket Bucket
	switch typedBucket := bucket.(type) {
	case *CouchbaseBucketGoCB:
		return typedBucket, true
	case *LoggingBucket:
		underlyingBucket = typedBucket.GetUnderlyingBucket()
	case *LeakyBucket:
		underlyingBucket = typedBucket.GetUnderlyingBucket()
	case *TestBucket:
		underlyingBucket = typedBucket.Bucket
	default:
		// bail out for unrecognised/unsupported buckets
		return nil, false
	}

	return AsGoCBBucket(underlyingBucket)
}

// AsLeakyBucket tries to return the given bucket as a LeakyBucket.
func AsLeakyBucket(bucket Bucket) (*LeakyBucket, bool) {

	var underlyingBucket Bucket
	switch typedBucket := bucket.(type) {
	case *LeakyBucket:
		return typedBucket, true
	case *LoggingBucket:
		underlyingBucket = typedBucket.GetUnderlyingBucket()
	case *TestBucket:
		underlyingBucket = typedBucket.Bucket
	default:
		// bail out for unrecognised/unsupported buckets
		return nil, false
	}

	return AsLeakyBucket(underlyingBucket)
}

func (bucket *CouchbaseBucketGoCB) MgmtEps() (url []string, err error) {
	mgmtEps := bucket.Bucket.IoRouter().MgmtEps()
	if len(mgmtEps) == 0 {
		return nil, fmt.Errorf("No available Couchbase Server nodes")
	}
	return mgmtEps, nil
}

func (bucket *CouchbaseBucketGoCB) HttpClient() *http.Client {
	return bucket.Bucket.IoRouter().HttpClient()
}

func (bucket *CouchbaseBucketGoCB) BucketName() string {
	return bucket.Bucket.Name()
}

func GoCBBucketMgmtEndpoints(bucket CouchbaseStore) (url []string, err error) {
	return bucket.MgmtEps()
}

// Get one of the management endpoints.  It will be a string such as http://couchbase
func GoCBBucketMgmtEndpoint(bucket CouchbaseStore) (url string, err error) {
	mgmtEps, err := bucket.MgmtEps()
	if err != nil {
		return "", err
	}
	bucketEp := mgmtEps[rand.Intn(len(mgmtEps))]
	return bucketEp, nil
}
