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

// IsDefaultScopeCollection returns true if the given Collection is on the _default._default scope and collection.
func (c *Collection) IsDefaultScopeCollection() bool {
	return IsDefaultCollection(c.ScopeName(), c.CollectionName())
}

func IsDefaultCollection(scope, collection string) bool {
	// check collection first to early exit non-default collection
	return collection == DefaultCollection && scope == DefaultScope
}

// EscapedKeyspace returns the escaped fully-qualified identifier for the keyspace (e.g. `bucket`.`scope`.`collection`)
func (c *Collection) EscapedKeyspace() string {
	if !c.IsSupported(sgbucket.BucketStoreFeatureCollections) {
		return fmt.Sprintf("`%s`", c.BucketName())
	}
	return fmt.Sprintf("`%s`.`%s`.`%s`", c.BucketName(), c.ScopeName(), c.CollectionName())
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

func (c *Collection) BucketName() string {
	return c.Bucket.GetName()
}

// IndexMetaKeyspaceID returns the value of keyspace_id for the system:indexes table for the collection.
func (c *Collection) IndexMetaKeyspaceID() string {
	if c.IsDefaultScopeCollection() {
		return c.BucketName()
	}
	return c.CollectionName()
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
		queryResults, queryErr := c.Bucket.runQuery(keyspaceStatement, n1qlOptions)
		if queryErr == nil {
			resultsIterator := &gocbRawIterator{
				rawResult:                  queryResults.Raw(),
				concurrentQueryOpLimitChan: c.Bucket.queryOps,
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

// WaitForIndexesOnline takes set of indexes and watches them till they're online.
func (c *Collection) WaitForIndexesOnline(indexNames []string, failfast bool) error {
	ctx := BucketAndCollectionCtx(context.Background(), c.Bucket, c.CollectionName())
	mgr := c.Bucket.cluster.QueryIndexes()
	maxNumAttempts := 180
	if failfast {
		maxNumAttempts = 1
	}
	retrySleeper := CreateMaxDoublingSleeperFunc(maxNumAttempts, 100, 5000)
	retryCount := 0

	onlineIndexes := make(map[string]bool)

	indexOption := gocb.GetAllQueryIndexesOptions{
		ScopeName:      c.ScopeName(),
		CollectionName: c.CollectionName(),
		RetryStrategy:  &goCBv2FailFastRetryStrategy{},
	}

	for {
		watchedOnlineIndexCount := 0
		currIndexes, err := mgr.GetAllIndexes(c.BucketName(), &indexOption)
		if err != nil {
			return err
		}
		// check each of the current indexes state, add to map once finished to make sure each index online is only being logged once
		for i := 0; i < len(currIndexes); i++ {
			if currIndexes[i].State == IndexStateOnline {
				if !onlineIndexes[currIndexes[i].Name] {
					InfofCtx(ctx, KeyAll, "Index %s is online", MD(currIndexes[i].Name))
					onlineIndexes[currIndexes[i].Name] = true
				}
			}
		}
		// check online index against indexes we watch to have online, increase counter as each comes online
		for _, listVal := range indexNames {
			if onlineIndexes[listVal] {
				watchedOnlineIndexCount++
			}
		}

		if watchedOnlineIndexCount == len(indexNames) {
			return nil
		}
		retryCount++
		shouldContinue, sleepMs := retrySleeper(retryCount)
		if !shouldContinue {
			return fmt.Errorf("error waiting for indexes for bucket %s....", MD(c.BucketName()))
		}
		InfofCtx(ctx, KeyAll, "Indexes not ready - retrying...")
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
	return BuildDeferredIndexes(c, indexSet)
}

func (b *GocbV2Bucket) runQuery(statement string, n1qlOptions *gocb.QueryOptions) (*gocb.QueryResult, error) {
	b.waitForAvailQueryOp()

	if n1qlOptions == nil {
		n1qlOptions = &gocb.QueryOptions{}
	}
	queryResults, err := b.cluster.Query(statement, n1qlOptions)
	// In the event that we get an error during query we should release a view op as Close() will not be called.
	if err != nil {
		b.releaseQueryOp()
	}

	return queryResults, err
}

func (c *Collection) executeQuery(statement string) (sgbucket.QueryResultIterator, error) {
	queryResults, queryErr := c.Bucket.runQuery(statement, nil)
	if queryErr != nil {
		return nil, queryErr
	}

	resultsIterator := &gocbRawIterator{
		rawResult:                  queryResults.Raw(),
		concurrentQueryOpLimitChan: c.Bucket.queryOps,
	}
	return resultsIterator, nil
}

func (c *Collection) executeStatement(statement string) error {
	queryResults, queryErr := c.Bucket.runQuery(statement, nil)
	if queryErr != nil {
		return queryErr
	}

	// Drain results to return any non-query errors
	for queryResults.Next() {
	}
	closeErr := queryResults.Close()
	c.Bucket.releaseQueryOp()
	if closeErr != nil {
		return closeErr
	}
	return queryResults.Err()
}

func (c *Collection) IsErrNoResults(err error) bool {
	return err == gocb.ErrNoResult
}

func (c *Collection) GetIndexes() (indexes []string, err error) {

	indexes = []string{}
	var opts *gocb.GetAllQueryIndexesOptions
	if c.IsSupported(sgbucket.BucketStoreFeatureCollections) {
		opts = &gocb.GetAllQueryIndexesOptions{
			ScopeName:      c.ScopeName(),
			CollectionName: c.CollectionName(),
		}
	}
	indexInfo, err := c.Bucket.cluster.QueryIndexes().GetAllIndexes(c.BucketName(), opts)
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
	return c.Bucket.cluster.WaitUntilReady(timeout,
		&gocb.WaitUntilReadyOptions{ServiceTypes: []gocb.ServiceType{gocb.ServiceTypeQuery}},
	)
}
