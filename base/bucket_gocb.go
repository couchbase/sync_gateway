//  Copyright (c) 2013 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package base

import (
	"bytes"
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

	sgbucket "github.com/couchbase/sg-bucket"
	pkgerrors "github.com/pkg/errors"
	"gopkg.in/couchbase/gocb.v1"
	"gopkg.in/couchbase/gocbcore.v7"
)

const (
	MaxConcurrentSingleOps = 1000 // Max 1000 concurrent single bucket ops
	MaxConcurrentBulkOps   = 35   // Max 35 concurrent bulk ops
	MaxConcurrentViewOps   = 100  // Max concurrent view ops
	MaxBulkBatchSize       = 100  // Maximum number of ops per bulk call

	// Causes the write op to block until the change has been replicated to numNodesReplicateTo many nodes.
	// In our case, we only want to block until it's durable on the node we're writing to, so this is set to 0.
	numNodesReplicateTo = uint(0)

	// Causes the write op to block until the change has been persisted (made durable -- written to disk) on
	// numNodesPersistTo.  In our case, we only want to block until it's durable on the node we're writing to,
	// so this is set to 1
	numNodesPersistTo = uint(1)

	xattrMacroCas         = "cas"
	xattrMacroValueCrc32c = "value_crc32c"

	// CRC-32 checksum represents the body hash of "Deleted" document.
	DeleteCrc32c = "0x00"

	// Can be removed and usages swapped out once this is present in gocb
	SubdocDocFlagCreateAsDeleted = gocb.SubdocDocFlag(gocbcore.SubdocDocFlag(0x08))
)

var recoverableGoCBErrors = map[string]struct{}{
	gocbcore.ErrOverload.Error(): {},
	gocbcore.ErrBusy.Error():     {},
	gocbcore.ErrTmpFail.Error():  {},
}

// Implementation of sgbucket.Bucket that talks to a Couchbase server and uses gocb
type CouchbaseBucketGoCB struct {
	*gocb.Bucket               // the underlying gocb bucket
	Spec         BucketSpec    // keep a copy of the BucketSpec for DCP usage
	singleOps    chan struct{} // Manages max concurrent single ops (per kv node)
	bulkOps      chan struct{} // Manages max concurrent bulk ops (per kv node)
	viewOps      chan struct{} // Manages max concurrent view ops (per kv node)

	clusterCompatMajorVersion, clusterCompatMinorVersion uint64 // E.g: 6 and 0 for 6.0.3
}

// Creates a Bucket that talks to a real live Couchbase server.
func GetCouchbaseBucketGoCB(spec BucketSpec) (bucket *CouchbaseBucketGoCB, err error) {
	connString, err := spec.GetGoCBConnString()
	if err != nil {
		Warnf("Unable to parse server value: %s error: %v", SD(spec.Server), err)
		return nil, err
	}

	cluster, err := gocb.Connect(connString)
	if err != nil {
		Infof(KeyAuth, "gocb connect returned error: %v", err)
		return nil, err
	}

	bucketPassword := ""
	// Check for client cert (x.509) authentication
	if spec.Certpath != "" {
		Infof(KeyAuth, "Attempting cert authentication against bucket %s on %s", MD(spec.BucketName), MD(spec.Server))
		certAuthErr := cluster.Authenticate(gocb.CertAuthenticator{})
		if certAuthErr != nil {
			Infof(KeyAuth, "Error Attempting certificate authentication %s", certAuthErr)
			return nil, pkgerrors.WithStack(certAuthErr)
		}
	} else if spec.Auth != nil {
		Infof(KeyAuth, "Attempting credential authentication against bucket %s on %s", MD(spec.BucketName), MD(spec.Server))
		user, pass, _ := spec.Auth.GetCredentials()
		authErr := cluster.Authenticate(gocb.PasswordAuthenticator{
			Username: user,
			Password: pass,
		})
		// If RBAC authentication fails, revert to non-RBAC authentication by including the password to OpenBucket
		if authErr != nil {
			Warnf("RBAC authentication against bucket %s as user %s failed - will re-attempt w/ bucketname, password", MD(spec.BucketName), UD(user))
			bucketPassword = pass
		}
	}

	return GetCouchbaseBucketGoCBFromAuthenticatedCluster(cluster, spec, bucketPassword)
}

func GetCouchbaseBucketGoCBFromAuthenticatedCluster(cluster *gocb.Cluster, spec BucketSpec, bucketPassword string) (bucket *CouchbaseBucketGoCB, err error) {
	goCBBucket, err := cluster.OpenBucket(spec.BucketName, bucketPassword)
	if err != nil {
		Infof(KeyAll, "Error opening bucket %s: %v", spec.BucketName, err)
		return nil, pkgerrors.WithStack(err)
	}
	Infof(KeyAll, "Successfully opened bucket %s", spec.BucketName)

	// Query node meta to find cluster compat version
	user, pass, _ := spec.Auth.GetCredentials()
	nodesMetadata, err := cluster.Manager(user, pass).Internal().GetNodesMetadata()
	if err != nil || len(nodesMetadata) == 0 {
		_ = goCBBucket.Close()
		return nil, pkgerrors.Wrapf(err, "Unable to get server cluster compatibility for %d nodes", len(nodesMetadata))
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
	viewOpsQueue := make(chan struct{}, MaxConcurrentViewOps*nodeCount)

	bucket = &CouchbaseBucketGoCB{
		Bucket:                    goCBBucket,
		Spec:                      spec,
		singleOps:                 singleOpsQueue,
		bulkOps:                   bucketOpsQueue,
		viewOps:                   viewOpsQueue,
		clusterCompatMajorVersion: uint64(clusterCompatMajor),
		clusterCompatMinorVersion: uint64(clusterCompatMinor),
	}

	bucket.Bucket.SetViewTimeout(bucket.Spec.GetViewQueryTimeout())
	bucket.Bucket.SetN1qlTimeout(bucket.Spec.GetViewQueryTimeout())

	Infof(KeyAll, "Set query timeouts for bucket %s to cluster:%v, bucket:%v", spec.BucketName, cluster.N1qlTimeout(), bucket.N1qlTimeout())
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
func (bucket *CouchbaseBucketGoCB) GetMetadataPurgeInterval() (time.Duration, error) {

	// Bucket-specific settings
	uri := fmt.Sprintf("/pools/default/buckets/%s", bucket.Name())
	bucketPurgeInterval, err := bucket.retrievePurgeInterval(uri)
	if bucketPurgeInterval > 0 || err != nil {
		return bucketPurgeInterval, err
	}

	// Cluster-wide settings
	uri = fmt.Sprintf("/settings/autoCompaction")
	clusterPurgeInterval, err := bucket.retrievePurgeInterval(uri)
	if clusterPurgeInterval > 0 || err != nil {
		return clusterPurgeInterval, err
	}

	return 0, nil

}

// Helper function to retrieve a Metadata Purge Interval from server and convert to hours.  Works for any uri
// that returns 'purgeInterval' as a root-level property (which includes the two server endpoints for
// bucket and server purge intervals).
func (bucket *CouchbaseBucketGoCB) retrievePurgeInterval(uri string) (time.Duration, error) {

	// Both of the purge interval endpoints (cluster and bucket) return purgeInterval in the same way
	var purgeResponse struct {
		PurgeInterval float64 `json:"purgeInterval,omitempty"`
	}

	resp, err := bucket.mgmtRequest(http.MethodGet, uri, "application/json", nil)
	if err != nil {
		return 0, err
	}

	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode == http.StatusForbidden {
		Warnf("403 Forbidden attempting to access %s.  Bucket user must have Bucket Full Access and Bucket Admin roles to retrieve metadata purge interval.", UD(uri))
	} else if resp.StatusCode != http.StatusOK {
		return 0, errors.New(resp.Status)
	}

	respBytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return 0, err
	}

	if err := JSONUnmarshal(respBytes, &purgeResponse); err != nil {
		return 0, err
	}

	// Server purge interval is a float value, in days.  Round up to hours
	purgeIntervalHours := int(purgeResponse.PurgeInterval*24 + 0.5)
	return time.Duration(purgeIntervalHours) * time.Hour, nil
}

// Get the Server UUID of the bucket, this is also known as the Cluster UUID
func (bucket *CouchbaseBucketGoCB) GetServerUUID() (uuid string, err error) {
	resp, err := bucket.mgmtRequest(http.MethodGet, "/pools", "application/json", nil)
	if err != nil {
		return "", err
	}

	defer func() { _ = resp.Body.Close() }()

	respBytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}

	var responseJson struct {
		ServerUUID string `json:"uuid"`
	}

	if err := JSONUnmarshal(respBytes, &responseJson); err != nil {
		return "", err
	}

	return responseJson.ServerUUID, nil
}

// Gets the bucket max TTL, or 0 if no TTL was set.  Sync gateway should fail to bring the DB online if this is non-zero,
// since it's not meant to operate against buckets that auto-delete data.
func (bucket *CouchbaseBucketGoCB) GetMaxTTL() (int, error) {
	var bucketResponseWithMaxTTL struct {
		MaxTTLSeconds int `json:"maxTTL,omitempty"`
	}

	uri := fmt.Sprintf("/pools/default/buckets/%s", bucket.Spec.BucketName)
	resp, err := bucket.mgmtRequest(http.MethodGet, uri, "application/json", nil)
	if err != nil {
		return -1, err
	}

	defer func() { _ = resp.Body.Close() }()

	respBytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return -1, err
	}

	if err := JSONUnmarshal(respBytes, &bucketResponseWithMaxTTL); err != nil {
		return -1, err
	}

	return bucketResponseWithMaxTTL.MaxTTLSeconds, nil
}

// mgmtRequest is a re-implementation of gocb's mgmtRequest
// TODO: Request gocb to either:
// - Make a public version of mgmtRequest for us to use
// - Or add all of the neccesary APIs we need to use
func (bucket *CouchbaseBucketGoCB) mgmtRequest(method, uri, contentType string, body io.Reader) (*http.Response, error) {
	if contentType == "" && body != nil {
		panic("Content-type must be specified for non-null body.")
	}

	mgmtEp, err := GoCBBucketMgmtEndpoint(bucket.Bucket)
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
		shouldRetry = isRecoverableReadError(err)
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
			if isRecoverableWriteError(item.Err) {
				retryEntries = append(retryEntries, entry)
			}
		case *gocb.ReplaceOp:
			entry.Cas = uint64(item.Cas)
			entry.Error = item.Err
			if isRecoverableWriteError(item.Err) {
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
				Warnf("Skipping GetBulkRaw result - unable to cast to []byte.  Type: %v", reflect.TypeOf(getOp.Value))
			}
		} else {
			// if it's a recoverable error, then throw it in retry collection.
			if isRecoverableReadError(getOp.Err) {
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
				Warnf("Skipping GetBulkCounter result - unable to cast to []byte.  Type: %v", reflect.TypeOf(getOp.Value))
			}
		} else {
			// if it's a recoverable error, then throw it in retry collection.
			if isRecoverableReadError(getOp.Err) {
				retryKeys = append(retryKeys, getOp.Key)
			}
		}

	}

	return nil
}

func createBatchesEntries(batchSize uint, entries []*sgbucket.BulkSetEntry) [][]*sgbucket.BulkSetEntry {
	// boundary checking
	if len(entries) == 0 {
		Warnf("createBatchesEnrties called with empty entries")
		return [][]*sgbucket.BulkSetEntry{}
	}
	if batchSize == 0 {
		Warnf("createBatchesEntries called with invalid batchSize")
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
		Warnf("createBatchesKeys called with empty keys")
		return [][]string{}
	}
	if batchSize == 0 {
		Warnf("createBatchesKeys called with invalid batchSize")
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

// Recoverable errors or timeouts trigger retry for gocb read operations
func isRecoverableReadError(err error) bool {

	if err == nil {
		return false
	}

	if isGoCBTimeoutError(err) {
		return true
	}

	_, ok := recoverableGoCBErrors[pkgerrors.Cause(err).Error()]

	return ok
}

// Recoverable errors trigger retry for gocb write operations
func isRecoverableWriteError(err error) bool {

	if err == nil {
		return false
	}

	_, ok := recoverableGoCBErrors[pkgerrors.Cause(err).Error()]

	return ok
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
		shouldRetry = isRecoverableReadError(err)
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
		shouldRetry = isRecoverableWriteError(err)
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
		if isRecoverableWriteError(err) {
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
		if isRecoverableWriteError(err) {
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

func (bucket *CouchbaseBucketGoCB) Set(k string, exp uint32, v interface{}) error {

	bucket.singleOps <- struct{}{}
	defer func() {
		<-bucket.singleOps
	}()

	worker := func() (shouldRetry bool, err error, value interface{}) {

		_, err = bucket.Bucket.Upsert(k, v, exp)
		if isRecoverableWriteError(err) {
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

func (bucket *CouchbaseBucketGoCB) SetRaw(k string, exp uint32, v []byte) error {

	bucket.singleOps <- struct{}{}
	defer func() {
		<-bucket.singleOps
	}()

	worker := func() (shouldRetry bool, err error, value interface{}) {

		_, err = bucket.Bucket.Upsert(k, bucket.FormatBinaryDocument(v), exp)
		if isRecoverableWriteError(err) {
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
		if isRecoverableWriteError(err) {
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
		if isRecoverableWriteError(errRemove) {
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
	Panicf("Unimplemented method: Write()")
	return nil
}

func (bucket *CouchbaseBucketGoCB) WriteCas(k string, flags int, exp uint32, cas uint64, v interface{}, opt sgbucket.WriteOptions) (casOut uint64, err error) {

	bucket.singleOps <- struct{}{}
	defer func() {
		<-bucket.singleOps
	}()

	// we only support the sgbucket.Raw WriteOption at this point
	if opt != 0 && opt != sgbucket.Raw {
		Panicf("WriteOption must be empty or sgbucket.Raw")
	}

	// also, flags must be 0, since that is not supported by gocb
	if flags != 0 {
		Panicf("flags must be 0")
	}

	worker := func() (shouldRetry bool, err error, value uint64) {

		if cas == 0 {
			// Try to insert the value into the bucket
			newCas, err := bucket.Bucket.Insert(k, v, exp)
			shouldRetry = isRecoverableWriteError(err)
			return shouldRetry, err, uint64(newCas)
		}

		// Otherwise, replace existing value
		newCas, err := bucket.Bucket.Replace(k, v, gocb.Cas(cas), exp)
		shouldRetry = isRecoverableWriteError(err)
		return shouldRetry, err, uint64(newCas)

	}

	// Kick off retry loop
	err, cas = RetryLoopCas("WriteCas", worker, bucket.Spec.RetrySleeper())
	if err != nil {
		err = pkgerrors.Wrapf(err, "WriteCas with key %v", k)
	}

	return cas, err

}

// CAS-safe write of a document and it's associated named xattr
func (bucket *CouchbaseBucketGoCB) WriteCasWithXattr(k string, xattrKey string, exp uint32, cas uint64, v interface{}, xv interface{}) (casOut uint64, err error) {

	// WriteCasWithXattr always stamps the xattr with the new cas using macro expansion, into a top-level property called 'cas'.
	// This is the only use case for macro expansion today - if more cases turn up, should change the sg-bucket API to handle this more generically.
	xattrCasProperty := fmt.Sprintf("%s.%s", xattrKey, xattrMacroCas)
	xattrBodyHashProperty := fmt.Sprintf("%s.%s", xattrKey, xattrMacroValueCrc32c)

	crc32cMacroExpansionSupported := bucket.IsSupported(sgbucket.DataStoreFeatureCrc32cMacroExpansion)
	if err != nil {
		return 0, err
	}

	bucket.singleOps <- struct{}{}
	defer func() {
		<-bucket.singleOps
	}()

	worker := func() (shouldRetry bool, err error, value uint64) {

		// cas=0 specifies an insert
		if cas == 0 {
			// TODO: Once that's fixed, need to wrap w/ retry handling

			mutateInBuilder := bucket.Bucket.MutateInEx(k, gocb.SubdocDocFlagReplaceDoc, 0, exp).
				UpsertEx(xattrKey, xv, gocb.SubdocFlagXattr).                                                // Update the xattr
				UpsertEx(xattrCasProperty, "${Mutation.CAS}", gocb.SubdocFlagXattr|gocb.SubdocFlagUseMacros) // Stamp the cas on the xattr
			if crc32cMacroExpansionSupported {
				mutateInBuilder.UpsertEx(xattrBodyHashProperty, "${Mutation.value_crc32c}", gocb.SubdocFlagXattr|gocb.SubdocFlagUseMacros) // Stamp the body hash on the xattr
			}
			mutateInBuilder.UpsertEx("", v, gocb.SubdocFlagNone) // Update the document body
			docFragment, err := mutateInBuilder.Execute()

			if err != nil {
				shouldRetry = isRecoverableWriteError(err)
				return shouldRetry, err, uint64(0)
			}
			return false, nil, uint64(docFragment.Cas())
		}

		casOut = 0
		// Otherwise, replace existing value
		if v != nil {
			// Have value and xattr value - update both
			mutateInBuilder := bucket.Bucket.MutateInEx(k, gocb.SubdocDocFlagMkDoc, gocb.Cas(cas), exp).
				UpsertEx(xattrKey, xv, gocb.SubdocFlagXattr).                                                // Update the xattr
				UpsertEx(xattrCasProperty, "${Mutation.CAS}", gocb.SubdocFlagXattr|gocb.SubdocFlagUseMacros) // Stamp the cas on the xattr
			if crc32cMacroExpansionSupported {
				mutateInBuilder.UpsertEx(xattrBodyHashProperty, "${Mutation.value_crc32c}", gocb.SubdocFlagXattr|gocb.SubdocFlagUseMacros) // Stamp the body hash on the xattr
			}
			mutateInBuilder.UpsertEx("", v, gocb.SubdocFlagNone) // Update the document body
			docFragment, err := mutateInBuilder.Execute()

			if err != nil {
				shouldRetry = isRecoverableWriteError(err)
				return shouldRetry, err, uint64(0)
			}
			casOut = uint64(docFragment.Cas())
		} else {
			// Update xattr only
			mutateInBuilder := bucket.Bucket.MutateInEx(k, gocb.SubdocDocFlagAccessDeleted, gocb.Cas(cas), exp).
				UpsertEx(xattrKey, xv, gocb.SubdocFlagXattr).                                                // Update the xattr
				UpsertEx(xattrCasProperty, "${Mutation.CAS}", gocb.SubdocFlagXattr|gocb.SubdocFlagUseMacros) // Stamp the cas on the xattr
			if crc32cMacroExpansionSupported {
				mutateInBuilder.UpsertEx(xattrBodyHashProperty, "${Mutation.value_crc32c}", gocb.SubdocFlagXattr|gocb.SubdocFlagUseMacros) // Stamp the body hash on the xattr
			}
			docFragment, err := mutateInBuilder.Execute()

			if err != nil {
				shouldRetry = isRecoverableWriteError(err)
				return shouldRetry, err, uint64(0)
			}
			casOut = uint64(docFragment.Cas())
		}

		return false, nil, casOut
	}

	// Kick off retry loop
	err, cas = RetryLoopCas("WriteCasWithXattr", worker, bucket.Spec.RetrySleeper())
	if err != nil {
		err = pkgerrors.Wrapf(err, "WriteCasWithXattr with key %v", UD(k).Redact())
	}

	return cas, err
}

// CAS-safe update of a document's xattr (only).  Deletes the document body if deleteBody is true.
func (bucket *CouchbaseBucketGoCB) UpdateXattr(k string, xattrKey string, exp uint32, cas uint64, xv interface{}, deleteBody, isDelete bool) (casOut uint64, err error) {

	// WriteCasWithXattr always stamps the xattr with the new cas using macro expansion, into a top-level property called 'cas'.
	// This is the only use case for macro expansion today - if more cases turn up, should change the sg-bucket API to handle this more generically.
	xattrCasProperty := fmt.Sprintf("%s.%s", xattrKey, xattrMacroCas)
	xattrBodyHashProperty := fmt.Sprintf("%s.%s", xattrKey, xattrMacroValueCrc32c)

	var makeDocCalled bool

	supportsTombstoneCreation := bucket.IsSupported(sgbucket.DataStoreFeatureCreateDeletedWithXattr)
	worker := func() (shouldRetry bool, err error, value uint64) {

		var mutateFlag gocb.SubdocDocFlag
		if deleteBody {
			// Since the body exists, we don't need to set a SubdocDocFlag
			mutateFlag = gocb.SubdocDocFlagNone
		} else {
			if cas == 0 {
				// If the doc doesn't exist, set SubdocDocFlagMkDoc to allow us to write the xattr
				if supportsTombstoneCreation && isDelete {
					mutateFlag = SubdocDocFlagCreateAsDeleted | gocb.SubdocDocFlagAccessDeleted | gocb.SubdocDocFlagReplaceDoc
				} else {
					makeDocCalled = true
					mutateFlag = gocb.SubdocDocFlagMkDoc
				}
			} else {
				// Since the body _may_ not exist, we need to set SubdocDocFlagAccessDeleted
				mutateFlag = gocb.SubdocDocFlagAccessDeleted
			}
		}

		builder := bucket.Bucket.MutateInEx(k, mutateFlag, gocb.Cas(cas), exp).
			UpsertEx(xattrKey, xv, gocb.SubdocFlagXattr).                                                // Update the xattr
			UpsertEx(xattrCasProperty, "${Mutation.CAS}", gocb.SubdocFlagXattr|gocb.SubdocFlagUseMacros) // Stamp the cas on the xattr

		if bucket.IsSupported(sgbucket.DataStoreFeatureCrc32cMacroExpansion) {
			// Stamp the body hash on the xattr
			if isDelete {
				builder.UpsertEx(xattrBodyHashProperty, DeleteCrc32c, gocb.SubdocFlagXattr)
			} else {
				builder.UpsertEx(xattrBodyHashProperty, "${Mutation.value_crc32c}", gocb.SubdocFlagXattr|gocb.SubdocFlagUseMacros)
			}
		}
		if deleteBody {
			builder.RemoveEx("", gocb.SubdocFlagNone) // Delete the document body
		}
		docFragment, removeErr := builder.Execute()

		if removeErr != nil {
			shouldRetry = isRecoverableWriteError(removeErr)
			return shouldRetry, removeErr, uint64(0)
		}
		return false, nil, uint64(docFragment.Cas())
	}

	// Kick off retry loop
	err, cas = RetryLoopCas("UpdateXattr", worker, bucket.Spec.RetrySleeper())
	if err != nil {
		err = pkgerrors.Wrapf(err, "Error during UpdateXattr with key %v", UD(k).Redact())
		return cas, err
	}

	// In the case where the SubdocDocFlagCreateAsDeleted is not available and we are performing the creation of a
	// tombstoned document we need to perform this second operation. This is due to the fact that SubdocDocFlagMkDoc
	// will have been used above instead which will create an empty body {} which we then need to delete here. If there
	// is a CAS mismatch we exit the operation as this means there has been a subsequent update to the body.
	if isDelete && !supportsTombstoneCreation && makeDocCalled {
		worker := func() (shouldRetry bool, err error, value uint64) {
			builder := bucket.Bucket.MutateInEx(k, gocb.SubdocDocFlagNone, gocb.Cas(cas), exp).
				UpsertEx(xattrBodyHashProperty, DeleteCrc32c, gocb.SubdocFlagXattr).
				UpsertEx(xattrCasProperty, "${Mutation.CAS}", gocb.SubdocFlagXattr|gocb.SubdocFlagUseMacros).
				RemoveEx("", gocb.SubdocFlagNone)
			docFragment, removeErr := builder.Execute()
			if removeErr != nil {

				// If there is a cas mismatch the body has since been updated and so we don't need to bother removing
				// body in this operation
				if IsCasMismatch(removeErr) {
					return false, nil, cas
				}

				shouldRetry = isRecoverableWriteError(removeErr)
				return shouldRetry, removeErr, uint64(0)
			}
			return false, nil, uint64(docFragment.Cas())
		}

		err, cas = RetryLoopCas("UpdateXattrDeleteBodySecondOp", worker, bucket.Spec.RetrySleeper())
		if err != nil {
			err = pkgerrors.Wrapf(err, "Error during UpdateXattr delete op with key %v", UD(k).Redact())
			return cas, err
		}

	}

	return cas, err
}

// Retrieve a document and it's associated named xattr
func (bucket *CouchbaseBucketGoCB) GetWithXattr(k string, xattrKey string, userXattrKey string, rv interface{}, xv interface{}, uxv interface{}) (cas uint64, err error) {

	// Until we get a fix for https://issues.couchbase.com/browse/MB-23522, need to disable the singleOp handling because of the potential for a nested call to bucket.Get
	/*
		bucket.singleOps <- struct{}{}
		gocbExpvars.Add("SingleOps", 1)
		defer func() {
			<-bucket.singleOps
			gocbExpvars.Add("SingleOps", -1)
		}()
	*/
	worker := func() (shouldRetry bool, err error, value uint64) {

		// First, attempt to get the document and xattr in one shot. We can't set SubdocDocFlagAccessDeleted when attempting
		// to retrieve the full doc body, so need to retry that scenario below.
		res, lookupErr := bucket.Bucket.LookupInEx(k, gocb.SubdocDocFlagAccessDeleted).
			GetEx(xattrKey, gocb.SubdocFlagXattr). // Get the xattr
			GetEx("", gocb.SubdocFlagNone).        // Get the document body
			Execute()

		// There are two 'partial success' error codes:
		//   ErrSubDocBadMulti - one of the subdoc operations failed.  Occurs when doc exists but xattr does not
		//   ErrSubDocMultiPathFailureDeleted - one of the subdoc operations failed, and the doc is deleted.  Occurs when xattr exists but doc is deleted (tombstone)
		switch lookupErr {
		case nil, gocbcore.ErrSubDocBadMulti:
			// Attempt to retrieve the document body, if present
			docContentErr := res.Content("", rv)
			if docContentErr != nil {
				Debugf(KeyCRUD, "No document body found for key=%s, xattrKey=%s: %v", UD(k), UD(xattrKey), docContentErr)
			}
			// Attempt to retrieve the xattr, if present
			xattrContentErr := res.Content(xattrKey, xv)
			if xattrContentErr != nil {
				Debugf(KeyCRUD, "No xattr content found for key=%s, xattrKey=%s: %v", UD(k), UD(xattrKey), xattrContentErr)
			}
			cas = uint64(res.Cas())

		case gocbcore.ErrSubDocMultiPathFailureDeleted:
			//   ErrSubDocMultiPathFailureDeleted - one of the subdoc operations failed, and the doc is deleted.  Occurs when xattr may exist but doc is deleted (tombstone)
			xattrContentErr := res.Content(xattrKey, xv)
			cas = uint64(res.Cas())
			if xattrContentErr != nil {
				// No doc, no xattr means the doc isn't found
				Debugf(KeyCRUD, "No xattr content found for key=%s, xattrKey=%s: %v", UD(k), UD(xattrKey), xattrContentErr)
				return false, gocb.ErrKeyNotFound, cas
			}
			return false, nil, cas

		default:
			shouldRetry = isRecoverableReadError(lookupErr)
			return shouldRetry, lookupErr, uint64(0)
		}

		// TODO: We may be able to improve in the future by having this secondary op as part of the first. At present
		// there is no support to obtain more than one xattr in a single operation however MB-28041 is filed for this.
		if userXattrKey != "" {
			userXattrCas, err := bucket.GetXattr(k, userXattrKey, uxv)
			switch pkgerrors.Cause(err) {

			case gocb.ErrKeyNotFound:
				// If key not found it has been deleted in between the first op and this op.
				return false, err, userXattrCas

			case gocb.ErrSubDocBadMulti:
				// Xattr doesn't exist, can skip

			case nil:
				if cas != userXattrCas {
					return true, errors.New("cas mismatch between user xattr and document body"), uint64(0)
				}
			default:
				// Unknown error occurred
				// Shouldn't retry as any recoverable error will have been retried already in GetXattr
				return false, err, uint64(0)
			}
		}

		return false, nil, cas

	}

	// Kick off retry loop
	err, cas = RetryLoopCas("GetWithXattr", worker, bucket.Spec.RetrySleeper())
	if err != nil {
		err = pkgerrors.Wrapf(err, "GetWithXattr %v", UD(k).Redact())
	}

	return cas, err

}

// Delete a document and it's associated named xattr.  Couchbase server will preserve system xattrs as part of the (CBS)
// tombstone when a document is deleted.  To remove the system xattr as well, an explicit subdoc delete operation is required.
// This is currently called only for Purge operations.
//
// The doc existing doc is expected to be in one of the following states:
//   - DocExists and XattrExists
//   - DocExists but NoXattr
//   - XattrExists but NoDoc
//   - NoDoc and NoXattr
// In all cases, the end state will be NoDoc and NoXattr.
// Expected errors:
//    - Temporary server overloaded errors, in which case the caller should retry
//    - If the doc is in the the NoDoc and NoXattr state, it will return a KeyNotFound error
func (bucket *CouchbaseBucketGoCB) DeleteWithXattr(k string, xattrKey string) error {

	// Delegate to internal method that can take a testing-related callback
	return bucket.deleteWithXattrInternal(k, xattrKey, nil)

}

func (bucket *CouchbaseBucketGoCB) GetXattr(k string, xattrKey string, xv interface{}) (casOut uint64, err error) {

	worker := func() (shouldRetry bool, err error, value interface{}) {
		res, lookupErr := bucket.Bucket.LookupInEx(k, gocb.SubdocDocFlagAccessDeleted).
			GetEx(xattrKey, gocb.SubdocFlagXattr).Execute()

		switch lookupErr {
		case nil:
			xattrContErr := res.Content(xattrKey, xv)
			if xattrContErr != nil {
				Debugf(KeyCRUD, "No xattr content found for key=%s, xattrKey=%s: %v", UD(k), UD(xattrKey), xattrContErr)
			}
			cas := uint64(res.Cas())
			return false, err, cas
		case gocbcore.ErrSubDocBadMulti:
			xattrErr := res.Content(xattrKey, xv)
			Debugf(KeyCRUD, "No xattr content found for key=%s, xattrKey=%s: %v", UD(k), UD(xattrKey), xattrErr)
			cas := uint64(res.Cas())
			return false, gocb.ErrSubDocBadMulti, cas
		case gocbcore.ErrKeyNotFound:
			Debugf(KeyCRUD, "No document found for key=%s", UD(k))
			return false, gocb.ErrKeyNotFound, 0
		case gocbcore.ErrSubDocMultiPathFailureDeleted, gocb.ErrSubDocSuccessDeleted:
			xattrContentErr := res.Content(xattrKey, xv)
			if xattrContentErr != nil {
				return false, gocbcore.ErrKeyNotFound, uint64(0)
			}
			cas := uint64(res.Cas())
			return false, nil, cas
		default:
			shouldRetry = isRecoverableReadError(lookupErr)
			return shouldRetry, lookupErr, uint64(0)
		}

	}

	err, result := RetryLoop("GetXattr", worker, bucket.Spec.RetrySleeper())
	if err != nil {
		err = pkgerrors.Wrapf(err, "GetXattr %s", UD(k).Redact())
	}

	if result == nil {
		return 0, err
	}

	cas, ok := result.(uint64)
	if !ok {
		return 0, RedactErrorf("GetXattr: Error doing type assertio of %v (%T) into uint64, Key %v", UD(result), result, UD(k))
	}

	return cas, err
}

// A function that will be called back after the first delete attempt but before second delete attempt
// to simulate the doc having changed state (artifiically injected race condition)
type deleteWithXattrRaceInjection func(bucket CouchbaseBucketGoCB, k string, xattrKey string)

func (bucket *CouchbaseBucketGoCB) deleteWithXattrInternal(k string, xattrKey string, callback deleteWithXattrRaceInjection) error {

	bucket.singleOps <- struct{}{}
	defer func() {
		<-bucket.singleOps
	}()

	Debugf(KeyCRUD, "DeleteWithXattr called with key: %v xattrKey: %v", UD(k), UD(xattrKey))

	// Try to delete body and xattrs in single op
	// NOTE: ongoing discussion w/ KV Engine team on whether this should handle cases where the body
	// doesn't exist (eg, a tombstoned xattr doc) by just ignoring the "delete body" mutation, rather
	// than current behavior of returning gocb.ErrKeyNotFound
	_, mutateErr := bucket.Bucket.MutateInEx(k, gocb.SubdocDocFlagNone, gocb.Cas(0), uint32(0)).
		RemoveEx(xattrKey, gocb.SubdocFlagXattr). // Remove the xattr
		RemoveEx("", gocb.SubdocFlagNone).        // Delete the document body
		Execute()

	// If no error, or it was just a ErrSubDocSuccessDeleted error, we're done.
	// ErrSubDocSuccessDeleted is a "success error" that means "operation was on a tombstoned document"
	if mutateErr == nil || mutateErr == gocbcore.ErrSubDocSuccessDeleted {
		Debugf(KeyCRUD, "No error or ErrSubDocSuccessDeleted.  We're done.")
		return nil
	}

	switch {
	case bucket.IsKeyNotFoundError(mutateErr):

		// Invoke the testing related callback.  This is a no-op in non-test contexts.
		if callback != nil {
			callback(*bucket, k, xattrKey)
		}

		// KeyNotFound indicates there is no doc body.  Try to delete only the xattr.
		return bucket.deleteDocXattrOnly(k, xattrKey, callback)

	case bucket.IsSubDocPathNotFound(mutateErr):

		// Invoke the testing related callback.  This is a no-op in non-test contexts.
		if callback != nil {
			callback(*bucket, k, xattrKey)
		}

		// KeyNotFound indicates there is no XATTR.  Try to delete only the body.
		return bucket.deleteDocBodyOnly(k, xattrKey, callback)

	default:

		// return error
		return mutateErr

	}

}

func (bucket *CouchbaseBucketGoCB) deleteDocXattrOnly(k string, xattrKey string, callback deleteWithXattrRaceInjection) error {

	//  Do get w/ xattr in order to get cas
	var retrievedVal map[string]interface{}
	var retrievedXattr map[string]interface{}
	getCas, err := bucket.GetWithXattr(k, xattrKey, "", &retrievedVal, &retrievedXattr, nil)
	if err != nil {
		return err
	}

	// If the doc body is non-empty at this point, then give up because it seems that a doc update has been
	// interleaved with the purge.  Return error to the caller and cancel the purge.
	if len(retrievedVal) != 0 {
		return fmt.Errorf("DeleteWithXattr was unable to delete the doc. Another update " +
			"was received which resurrected the doc by adding a new revision, in which case this delete operation is " +
			"considered as cancelled.")
	}

	// Cas-safe delete of just the XATTR.  Use SubdocDocFlagAccessDeleted since presumably the document body
	// has been deleted.
	_, mutateErrDeleteXattr := bucket.Bucket.MutateInEx(k, gocb.SubdocDocFlagAccessDeleted, gocb.Cas(getCas), uint32(0)).
		RemoveEx(xattrKey, gocb.SubdocFlagXattr). // Remove the xattr
		Execute()

	// If no error, or it was just a ErrSubDocSuccessDeleted error, we're done.
	// ErrSubDocSuccessDeleted is a "success error" that means "operation was on a tombstoned document"
	if mutateErrDeleteXattr == nil || mutateErrDeleteXattr == gocbcore.ErrSubDocSuccessDeleted {
		return nil
	}

	// If the cas-safe delete of XATTR fails, return an error to the caller.
	// This might happen if there was a concurrent update interleaved with the purge (someone resurrected doc)
	return pkgerrors.Wrapf(mutateErrDeleteXattr, "DeleteWithXattr was unable to delete the doc.  Another update "+
		"was received which resurrected the doc by adding a new revision, in which case this delete operation is "+
		"considered as cancelled. ")

}

func (bucket *CouchbaseBucketGoCB) deleteDocBodyOnly(k string, xattrKey string, callback deleteWithXattrRaceInjection) error {

	//  Do get in order to get cas
	var retrievedVal map[string]interface{}
	getCas, err := bucket.Get(k, &retrievedVal)
	if err != nil {
		return err
	}

	// Cas-safe delete of just the doc body
	_, mutateErrDeleteBody := bucket.Bucket.MutateInEx(k, gocb.SubdocDocFlagNone, gocb.Cas(getCas), uint32(0)).
		RemoveEx("", gocb.SubdocFlagNone). // Delete the document body
		Execute()

	// If no error, or it was just a ErrSubDocSuccessDeleted error, we're done.
	// ErrSubDocSuccessDeleted is a "success error" that means "operation was on a tombstoned document"
	if mutateErrDeleteBody == nil || mutateErrDeleteBody == gocbcore.ErrSubDocSuccessDeleted {
		return nil
	}

	// If the cas-safe delete of doc body fails, return an error to the caller.
	// This might happen if there was a concurrent update interleaved with the purge (someone resurrected doc)
	return pkgerrors.Wrapf(mutateErrDeleteBody, "DeleteWithXattr was unable to delete the doc.  It might be the case that another update "+
		"was received which resurrected the doc by adding a new revision, in which case this delete operation is "+
		"considred as cancelled.")

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
		} else if isRecoverableWriteError(err) {
			// retry on recoverable failure, up to MaxNumRetries
			if retryAttempts >= bucket.Spec.MaxNumRetries {
				return 0, pkgerrors.Wrapf(err, "WriteUpdate retry loop aborted after %d attempts", retryAttempts)
			}
		} else {
			// err will be nil if successful
			return uint64(casGoCB), err
		}

		retryAttempts++
		Debugf(KeyCRUD, "CAS Update RetryLoop retrying for doc %q, attempt %d", UD(k), retryAttempts)
	}
}

// WriteUpdateWithXattr retrieves the existing doc from the bucket, invokes the callback to update the document, then writes the new document to the bucket.  Will repeat this process on cas
// failure.  If previousValue/xattr/cas are provided, will use those on the first iteration instead of retrieving from the bucket.
func (bucket *CouchbaseBucketGoCB) WriteUpdateWithXattr(k string, xattrKey string, userXattrKey string, exp uint32, previous *sgbucket.BucketDocument, callback sgbucket.WriteUpdateWithXattrFunc) (casOut uint64, err error) {

	var value []byte
	var xattrValue []byte
	var userXattrValue []byte
	var cas uint64
	emptyCas := uint64(0)

	// If an existing value has been provided, use that as the initial value
	if previous != nil && previous.Cas > 0 {
		value = previous.Body
		xattrValue = previous.Xattr
		cas = previous.Cas
		userXattrValue = previous.UserXattr
	}

	for {
		var err error
		// If no existing value has been provided, retrieve the current value from the bucket
		if cas == 0 {
			// Load the existing value.
			cas, err = bucket.GetWithXattr(k, xattrKey, userXattrKey, &value, &xattrValue, &userXattrValue)

			if err != nil {
				if !bucket.IsKeyNotFoundError(err) {
					// Unexpected error, cancel writeupdate
					Debugf(KeyCRUD, "Retrieval of existing doc failed during WriteUpdateWithXattr for key=%s, xattrKey=%s: %v", UD(k), UD(xattrKey), err)
					return emptyCas, err
				}
				// Key not found - initialize values
				value = nil
				xattrValue = nil
			}
		}

		// Invoke callback to get updated value
		updatedValue, updatedXattrValue, isDelete, callbackExpiry, err := callback(value, xattrValue, userXattrValue, cas)

		// If it's an ErrCasFailureShouldRetry, then retry by going back through the for loop
		if err == ErrCasFailureShouldRetry {
			cas = 0 // force the call to GetWithXattr() to refresh
			continue
		}

		// On any other errors, abort the Write attempt
		if err != nil {
			return emptyCas, err
		}
		if callbackExpiry != nil {
			exp = *callbackExpiry
		}

		// Attempt to write the updated document to the bucket.  Mark body for deletion if previous body was non-empty
		deleteBody := value != nil
		casOut, writeErr := bucket.WriteWithXattr(k, xattrKey, exp, cas, updatedValue, updatedXattrValue, isDelete, deleteBody)

		switch pkgerrors.Cause(writeErr) {
		case nil:
			return casOut, nil
		case gocb.ErrKeyExists, gocb.ErrNotStored:
			// Retry on cas failure.  ErrNotStored is returned in some concurrent insert races that appear to be related
			// to the timing of concurrent xattr subdoc operations.  Treating as CAS failure as these will get the usual
			// conflict/duplicate handling on retry.
		default:
			// WriteWithXattr already handles retry on recoverable errors, so fail on any errors other than ErrKeyExists
			Warnf("Failed to update doc with xattr for key=%s, xattrKey=%s: %v", UD(k), UD(xattrKey), writeErr)
			return emptyCas, writeErr
		}

		// Reset value, xattr, cas for cas retry
		value = nil
		xattrValue = nil
		cas = 0

	}
}

// Single attempt to update a document and xattr.  Setting isDelete=true and value=nil will delete the document body.  Both
// update types (UpdateXattr, WriteCasWithXattr) include recoverable error retry.
func (bucket *CouchbaseBucketGoCB) WriteWithXattr(k string, xattrKey string, exp uint32, cas uint64, value []byte, xattrValue []byte, isDelete bool, deleteBody bool) (casOut uint64, err error) {
	// If this is a tombstone, we want to delete the document and update the xattr
	if isDelete {
		return bucket.UpdateXattr(k, xattrKey, exp, cas, xattrValue, deleteBody, isDelete)
	} else {
		// Not a delete - update the body and xattr
		return bucket.WriteCasWithXattr(k, xattrKey, exp, cas, value, xattrValue)
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
		shouldRetry = isRecoverableWriteError(err)
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
			Warnf("Got error from UpsertDesignDocument: %v - Retrying...", err)
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

	if resp.StatusCode != 201 {
		data, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return err
		}
		err = resp.Body.Close()
		if err != nil {
			Warnf("Failed to close socket: %v", err)
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

// Check if this is a SubDocPathNotFound error
// Pending question to see if there is an easier way: https://forums.couchbase.com/t/checking-for-errsubdocpathnotfound-errors/13492
func (bucket *CouchbaseBucketGoCB) IsSubDocPathNotFound(err error) bool {

	subdocMutateErr, ok := pkgerrors.Cause(err).(gocbcore.SubDocMutateError)
	if ok {
		return subdocMutateErr.Err == gocb.ErrSubDocPathNotFound
	}
	return false
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
	bucket.waitForAvailViewOp()
	defer bucket.releaseViewOp()

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

	bucket.waitForAvailViewOp()
	defer bucket.releaseViewOp()

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
		Warnf("Unable to type assert goCbViewResult -> gocb.ViewResultMetrics.  The total rows count will be missing.")
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

	Infof(KeyDCP, "Using DCP to generate TAP-like stream")
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
		shouldRetry = isRecoverableReadError(err)
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
	Warnf("CouchbaseBucketGoCB: Unimplemented method: Dump()")
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

func (bucket *CouchbaseBucketGoCB) CouchbaseServerVersion() (major uint64, minor uint64, micro string) {
	return bucket.clusterCompatMajorVersion, bucket.clusterCompatMinorVersion, ""
}

func (bucket *CouchbaseBucketGoCB) UUID() (string, error) {
	return bucket.Bucket.IoRouter().BucketUUID(), nil
}

func (bucket *CouchbaseBucketGoCB) Close() {
	if err := bucket.Bucket.Close(); err != nil {
		Warnf("Error closing GoCB bucket: %v.", err)
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
			Warnf("Error flushing bucket: %v  Will retry.", err)
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
	itemCount, err = bucket.QueryBucketItemCount()
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
func (bucket *CouchbaseBucketGoCB) QueryBucketItemCount() (itemCount int, err error) {
	statement := fmt.Sprintf("SELECT COUNT(1) AS count FROM `%s`", KeyspaceQueryToken)
	r, err := bucket.Query(statement, nil, RequestPlus, true)
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
		shouldRetry = isRecoverableReadError(err)
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
	major, minor, _ := bucket.CouchbaseServerVersion()
	switch feature {
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
				Warnf("ViewQueryParamLimit error: %v", err)
			}
			viewQuery.Limit(uintVal)
		case ViewQueryParamDescending:
			if asBool(optionValue) == true {
				viewQuery.Order(gocb.Descending)
			}
		case ViewQueryParamSkip:
			uintVal, err := normalizeIntToUint(optionValue)
			if err != nil {
				Warnf("ViewQueryParamSkip error: %v", err)
			}
			viewQuery.Skip(uintVal)
		case ViewQueryParamGroup:
			viewQuery.Group(asBool(optionValue))
		case ViewQueryParamGroupLevel:
			uintVal, err := normalizeIntToUint(optionValue)
			if err != nil {
				Warnf("ViewQueryParamGroupLevel error: %v", err)
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
			Warnf("asBool called with unknown value: %v.  defaulting to false", typeValue)
			return false
		}
		return parsedVal
	case bool:
		return typeValue
	default:
		Warnf("asBool called with unknown type: %T.  defaulting to false", typeValue)
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
			Warnf("asStale called with unknown value: %v.  defaulting to stale=false", typeValue)
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
		Warnf("asBool called with unknown type: %T.  defaulting to false", typeValue)
		return gocb.Before
	}

}

// This prevents Sync Gateway from having too many outstanding concurrent view queries against Couchbase Server
func (bucket *CouchbaseBucketGoCB) waitForAvailViewOp() {
	bucket.viewOps <- struct{}{}
}

func (bucket *CouchbaseBucketGoCB) releaseViewOp() {
	<-bucket.viewOps
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

// Get one of the management endpoints.  It will be a string such as http://couchbase
func GoCBBucketMgmtEndpoint(bucket *gocb.Bucket) (url string, err error) {
	mgmtEps := bucket.IoRouter().MgmtEps()
	if len(mgmtEps) == 0 {
		return "", fmt.Errorf("No available Couchbase Server nodes")
	}
	bucketEp := mgmtEps[rand.Intn(len(mgmtEps))]
	return bucketEp, nil
}
