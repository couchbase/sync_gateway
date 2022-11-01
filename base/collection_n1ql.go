/*
Copyright 2021-Present Couchbase, Inc.

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
	"fmt"
	"strings"
	"time"

	"github.com/couchbase/gocb/v2"
	sgbucket "github.com/couchbase/sg-bucket"
	pkgerrors "github.com/pkg/errors"
)

var _ N1QLStore = &Collection{}

const waitTime = 5 * time.Second

// IsDefaultScopeCollection returns true if the given Collection is on the _default._default scope and collection.
func (c *Collection) IsDefaultScopeCollection() bool {
	return c.ScopeName() == DefaultScope && c.Name() == DefaultCollection
}

// EscapedKeyspace returns the escaped fully-qualified identifier for the keyspace (e.g. `bucket`.`scope`.`collection`)
func (c *Collection) EscapedKeyspace() string {
	if !c.IsSupported(sgbucket.DataStoreFeatureCollections) {
		return fmt.Sprintf("`%s`", c.BucketName())
	}
	return fmt.Sprintf("`%s`.`%s`.`%s`", c.BucketName(), c.ScopeName(), c.Name())
}

// IndexMetaBucketID returns the value of bucket_id for the system:indexes table for the collection.
func (c *Collection) IndexMetaBucketID() string {
	if c.IsDefaultScopeCollection() {
		return ""
	}
	return c.BucketName()
}

// IndexMetaScopeID returns the value of scope_id for the system:indexes table for the collection.
func (c *Collection) IndexMetaScopeID() string {
	if c.IsDefaultScopeCollection() {
		return ""
	}
	return c.ScopeName()
}

// IndexMetaKeyspaceID returns the value of keyspace_id for the system:indexes table for the collection.
func (c *Collection) IndexMetaKeyspaceID() string {
	if c.IsDefaultScopeCollection() {
		return c.BucketName()
	}
	return c.Name()
}

func (c *Collection) Query(statement string, params map[string]interface{}, consistency ConsistencyMode, adhoc bool) (resultsIterator sgbucket.QueryResultIterator, err error) {
	logCtx := context.TODO()

	keyspaceStatement := strings.Replace(statement, KeyspaceQueryToken, c.EscapedKeyspace(), -1)

	n1qlOptions := &gocb.QueryOptions{
		ScanConsistency: gocb.QueryScanConsistency(consistency),
		Adhoc:           adhoc,
		NamedParameters: params,
	}

	waitTime := 10 * time.Millisecond
	for i := 1; i <= MaxQueryRetries; i++ {
		TracefCtx(logCtx, KeyQuery, "Executing N1QL query: %v - %+v", UD(keyspaceStatement), UD(params))
		queryResults, queryErr := c.runQuery(keyspaceStatement, n1qlOptions)
		if queryErr == nil {
			resultsIterator := &gocbRawIterator{
				rawResult:                  queryResults.Raw(),
				concurrentQueryOpLimitChan: c.queryOps,
			}
			return resultsIterator, queryErr
		}

		// Timeout error - return named error
		if errors.Is(queryErr, gocb.ErrTimeout) {
			return resultsIterator, ErrViewTimeoutError
		}

		// Non-retry error - return
		if !isTransientIndexerError(queryErr) {
			WarnfCtx(logCtx, "Error when querying index using statement: [%s] parameters: [%+v] error:%v", UD(keyspaceStatement), UD(params), queryErr)
			return resultsIterator, pkgerrors.WithStack(queryErr)
		}

		// Indexer error - wait then retry
		err = queryErr
		WarnfCtx(logCtx, "Indexer error during query - retry %d/%d after %v.  Error: %v", i, MaxQueryRetries, waitTime, queryErr)
		time.Sleep(waitTime)

		waitTime = waitTime * 2
	}

	WarnfCtx(logCtx, "Exceeded max retries for query when querying index using statement: [%s] parameters: [%+v], err:%v", UD(keyspaceStatement), UD(params), err)
	return nil, err
}

func (c *Collection) ExplainQuery(statement string, params map[string]interface{}) (plan map[string]interface{}, err error) {
	return ExplainQuery(c, statement, params)
}

func (c *Collection) CreateIndex(indexName string, expression string, filterExpression string, options *N1qlIndexOptions) error {
	return CreateIndex(c, indexName, expression, filterExpression, options)
}

func (c *Collection) CreatePrimaryIndex(indexName string, options *N1qlIndexOptions) error {
	return CreatePrimaryIndex(c, indexName, options)
}

// WaitForIndexOnline takes set of indexes and watches them till they're online.
func (c *Collection) WaitForIndexesOnline(indexNames []string, watchPrimary bool) error {
	logCtx := context.TODO()
	mgr := c.cluster.QueryIndexes()
	maxNumAttempts := 150 * len(indexNames)
	retrySleeper := CreateMaxDoublingSleeperFunc(maxNumAttempts, 100, 5000)
	retryCount := 0

	indexOption := gocb.GetAllQueryIndexesOptions{
		ScopeName:      c.ScopeName(),
		CollectionName: c.Name(),
	}

	for {
		onlineIndexCount := 0
		var onlineIndexList []string
		currIndexes, err := mgr.GetAllIndexes(c.BucketName(), &indexOption)
		if err != nil {
			return err
		}

		for i := 0; i < len(currIndexes); i++ {
			if currIndexes[i].State == IndexStateOnline {
				onlineIndexList = append(onlineIndexList, currIndexes[i].Name)
				InfofCtx(logCtx, KeyAll, "Index %s is online", MD(currIndexes[i].Name))
			}
		}

		for _, currVal := range onlineIndexList {
			for _, listVal := range indexNames {
				if currVal == listVal {
					onlineIndexCount++
				}
			}
		}
		if onlineIndexCount == len(indexNames) {
			return nil
		}
		retryCount++
		shouldContinue, sleepMs := retrySleeper(retryCount)
		if !shouldContinue {
			return err
		}
		InfofCtx(logCtx, KeyAll, "Indexes for bucket %s not ready - retrying...", MD(c.BucketName()))
		time.Sleep(time.Millisecond * time.Duration(sleepMs))
	}
}

func (c *Collection) GetIndexMeta(indexName string) (exists bool, meta *IndexMeta, err error) {
	return GetIndexMeta(c, indexName)
}

// DropIndex drops the specified index from the current bucket.
func (c *Collection) DropIndex(indexName string) error {
	return DropIndex(c, indexName)
}

// Issues a build command for any deferred sync gateway indexes associated with the bucket.
func (c *Collection) BuildDeferredIndexes(indexSet []string) error {
	bucket, err := GetBucket(c.Spec)
	if err != nil {
		return err
	}
	return BuildDeferredIndexes(bucket, c, indexSet)
}

func (c *Collection) runQuery(statement string, n1qlOptions *gocb.QueryOptions) (*gocb.QueryResult, error) {
	c.waitForAvailQueryOp()

	if n1qlOptions == nil {
		n1qlOptions = &gocb.QueryOptions{}
	}
	queryResults, err := c.cluster.Query(statement, n1qlOptions)
	// In the event that we get an error during query we should release a view op as Close() will not be called.
	if err != nil {
		c.releaseQueryOp()
	}

	return queryResults, err
}

func (c *Collection) executeQuery(statement string) (sgbucket.QueryResultIterator, error) {
	queryResults, queryErr := c.runQuery(statement, nil)
	if queryErr != nil {
		return nil, queryErr
	}

	resultsIterator := &gocbRawIterator{
		rawResult:                  queryResults.Raw(),
		concurrentQueryOpLimitChan: c.queryOps,
	}
	return resultsIterator, nil
}

func (c *Collection) executeStatement(statement string) error {
	queryResults, queryErr := c.runQuery(statement, nil)
	if queryErr != nil {
		return queryErr
	}

	// Drain results to return any non-query errors
	for queryResults.Next() {
	}
	closeErr := queryResults.Close()
	c.releaseQueryOp()
	if closeErr != nil {
		return closeErr
	}
	return queryResults.Err()
}

func (c *Collection) IsErrNoResults(err error) bool {
	return err == gocb.ErrNoResult
}

func (c *Collection) getIndexes() (indexes []string, err error) {

	indexes = []string{}
	var opts *gocb.GetAllQueryIndexesOptions
	if c.IsSupported(sgbucket.DataStoreFeatureCollections) {
		opts = &gocb.GetAllQueryIndexesOptions{
			ScopeName:      c.ScopeName(),
			CollectionName: c.Name(),
		}
	}
	indexInfo, err := c.cluster.QueryIndexes().GetAllIndexes(c.BucketName(), opts)
	if err != nil {
		return indexes, err
	}

	for _, indexInfo := range indexInfo {
		indexes = append(indexes, indexInfo.Name)
	}
	return indexes, nil
}

// waitUntilQueryServiceReady will wait for the specified duration until the query service is available.
func (c *Collection) waitUntilQueryServiceReady(timeout time.Duration) error {
	return c.cluster.WaitUntilReady(timeout,
		&gocb.WaitUntilReadyOptions{ServiceTypes: []gocb.ServiceType{gocb.ServiceTypeQuery}},
	)
}
