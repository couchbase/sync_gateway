/*
Copyright 2018-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package base

import (
	"context"
	"strings"
	"time"

	sgbucket "github.com/couchbase/sg-bucket"

	pkgerrors "github.com/pkg/errors"
	"gopkg.in/couchbase/gocb.v1"
)

const KeyspaceQueryToken = "$_keyspace" // Token used for bucket name replacement in query statements
const MaxQueryRetries = 30              // Maximum query retries on indexer error
const IndexStateOnline = "online"       // bucket state value, as returned by SELECT FROM system:indexes.  Index has been created and built.
const IndexStateDeferred = "deferred"   // bucket state value, as returned by SELECT FROM system:indexes.  Index has been created but not built.
const IndexStatePending = "pending"     // bucket state value, as returned by SELECT FROM system:indexes.  Index has been created, build is in progress
const PrimaryIndexName = "#primary"

// IndexOptions used to build the 'with' clause
type N1qlIndexOptions struct {
	NumReplica      uint `json:"num_replica,omitempty"`          // Number of replicas
	IndexTombstones bool `json:"retain_deleted_xattr,omitempty"` // Whether system xattrs on tombstones should be indexed
	DeferBuild      bool `json:"defer_build,omitempty"`          // Whether to defer initial build of index (requires a subsequent BUILD INDEX invocation)
}

var _ N1QLStore = &CouchbaseBucketGoCB{}

// Keyspace for a bucket is bucket name
func (bucket *CouchbaseBucketGoCB) Keyspace() string {
	return bucket.GetName()
}

// Query accepts a parameterized statement,  optional list of params, and an optional flag to force adhoc query execution.
// Params specified using the $param notation in the statement are intended to be used w/ N1QL prepared statements, and will be
// passed through as params to n1ql.  e.g.:
//   SELECT _sync.sequence FROM $_keyspace WHERE _sync.sequence > $minSeq
// https://developer.couchbase.com/documentation/server/current/sdk/go/n1ql-queries-with-sdk.html for additional details.
// Will additionally replace all instances of KeyspaceQueryToken($_keyspace) in the statement
// with the bucket name.  'bucket' should not be included in params.
//
// If adhoc=true, prepared statement handling will be disabled.  Should only be set to true for queries that can't be prepared, e.g.:
//  SELECT _sync.channels.ABC.seq from $bucket
//
// Query retries on Indexer Errors, as these are normally transient
func (bucket *CouchbaseBucketGoCB) Query(statement string, params map[string]interface{}, consistency ConsistencyMode, adhoc bool) (results sgbucket.QueryResultIterator, err error) {
	logCtx := context.TODO()
	bucketStatement := strings.Replace(statement, KeyspaceQueryToken, bucket.GetName(), -1)
	n1qlQuery := gocb.NewN1qlQuery(bucketStatement)
	n1qlQuery = n1qlQuery.AdHoc(adhoc)
	n1qlQuery = n1qlQuery.Consistency(gocb.ConsistencyMode(consistency))

	waitTime := 10 * time.Millisecond
	for i := 1; i <= MaxQueryRetries; i++ {

		TracefCtx(logCtx, KeyQuery, "Executing N1QL query: %v", UD(n1qlQuery))
		queryResults, queryErr := bucket.runQuery(n1qlQuery, params)

		if queryErr == nil {
			return queryResults, queryErr
		}

		// Timeout error - return named error
		if isGoCBQueryTimeoutError(queryErr) {
			return queryResults, ErrViewTimeoutError
		}

		// Non-retry error - return
		if !isTransientIndexerError(queryErr) {
			WarnfCtx(logCtx, "Error when querying index using statement: [%s] parameters: [%+v] error:%v", UD(bucketStatement), UD(params), queryErr)
			return queryResults, pkgerrors.WithStack(queryErr)
		}

		// Indexer error - wait then retry
		err = queryErr
		WarnfCtx(logCtx, "Indexer error during query - retry %d/%d after %v.  Error: %v", i, MaxQueryRetries, waitTime, queryErr)
		time.Sleep(waitTime)

		waitTime = time.Duration(waitTime * 2)
	}

	WarnfCtx(logCtx, "Exceeded max retries for query when querying index using statement: [%s], err:%v", UD(bucketStatement), err)
	return nil, err
}

func (bucket *CouchbaseBucketGoCB) ExplainQuery(statement string, params map[string]interface{}) (plan map[string]interface{}, err error) {
	return ExplainQuery(bucket, statement, params)
}

func (bucket *CouchbaseBucketGoCB) CreateIndex(indexName string, expression string, filterExpression string, options *N1qlIndexOptions) error {
	return CreateIndex(bucket, indexName, expression, filterExpression, options)
}

// Issues a build command for any deferred sync gateway indexes associated with the bucket.
func (bucket *CouchbaseBucketGoCB) BuildDeferredIndexes(indexSet []string) error {
	return BuildDeferredIndexes(bucket, indexSet)
}

func (bucket *CouchbaseBucketGoCB) runQuery(n1qlQuery *gocb.N1qlQuery, params map[string]interface{}) (sgbucket.QueryResultIterator, error) {
	bucket.waitForAvailQueryOp()
	defer bucket.releaseQueryOp()

	return bucket.ExecuteN1qlQuery(n1qlQuery, params)
}

func (bucket *CouchbaseBucketGoCB) executeQuery(statement string) (sgbucket.QueryResultIterator, error) {
	n1qlQuery := gocb.NewN1qlQuery(statement)
	results, err := bucket.runQuery(n1qlQuery, nil)
	return results, err
}

func (bucket *CouchbaseBucketGoCB) executeStatement(statement string) error {
	results, err := bucket.executeQuery(statement)
	if err != nil {
		return err
	}
	return results.Close()
}

func (bucket *CouchbaseBucketGoCB) CreatePrimaryIndex(indexName string, options *N1qlIndexOptions) error {
	return CreatePrimaryIndex(bucket, indexName, options)
}

// Waits for index state to be online.  Waits no longer than provided timeout
func (bucket *CouchbaseBucketGoCB) WaitForIndexOnline(indexName string) error {
	return WaitForIndexOnline(bucket, indexName)
}

func (bucket *CouchbaseBucketGoCB) GetIndexMeta(indexName string) (exists bool, meta *IndexMeta, err error) {
	return GetIndexMeta(bucket, indexName)
}

// DropIndex drops the specified index from the current bucket.
func (bucket *CouchbaseBucketGoCB) DropIndex(indexName string) error {
	return DropIndex(bucket, indexName)
}

func (bucket *CouchbaseBucketGoCB) IsErrNoResults(err error) bool {
	return err == gocb.ErrNoResults
}

// Get a list of all index names in the bucket
func (bucket *CouchbaseBucketGoCB) getIndexes() (indexes []string, err error) {

	indexes = []string{}

	manager, err := bucket.getBucketManager()
	if err != nil {
		return indexes, err
	}

	indexInfo, err := manager.GetIndexes()
	if err != nil {
		return indexes, err
	}

	for _, indexInfo := range indexInfo {
		if indexInfo.Keyspace == bucket.GetName() {
			indexes = append(indexes, indexInfo.Name)
		}
	}

	return indexes, nil
}
