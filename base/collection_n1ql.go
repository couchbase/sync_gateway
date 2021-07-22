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
	"errors"
	"strings"
	"time"

	"github.com/couchbase/gocb"
	sgbucket "github.com/couchbase/sg-bucket"
	pkgerrors "github.com/pkg/errors"
)

var _ N1QLStore = &Collection{}

// Keyspace for a collection is bucket name until wider collection support is added
func (c *Collection) Keyspace() string {
	return c.Bucket().Name()
}

func (c *Collection) Query(statement string, params map[string]interface{}, consistency ConsistencyMode, adhoc bool) (resultsIterator sgbucket.QueryResultIterator, err error) {

	bucketStatement := strings.Replace(statement, KeyspaceQueryToken, c.Keyspace(), -1)

	n1qlOptions := &gocb.QueryOptions{
		ScanConsistency: gocb.QueryScanConsistency(consistency),
		Adhoc:           adhoc,
		NamedParameters: params,
	}

	waitTime := 10 * time.Millisecond
	for i := 1; i <= MaxQueryRetries; i++ {
		Tracef(KeyQuery, "Executing N1QL query: %v - %+v", UD(bucketStatement), UD(params))
		queryResults, queryErr := c.cluster.Query(bucketStatement, n1qlOptions)
		if queryErr == nil {
			resultsIterator := &gocbRawIterator{
				rawResult: queryResults.Raw(),
			}
			return resultsIterator, queryErr
		}

		// Timeout error - return named error
		if errors.Is(queryErr, gocb.ErrTimeout) {
			return resultsIterator, ErrViewTimeoutError
		}

		// Non-retry error - return
		if !isTransientIndexerError(queryErr) {
			Warnf("Error when querying index using statement: [%s] parameters: [%+v] error:%v", UD(bucketStatement), UD(params), queryErr)
			return resultsIterator, pkgerrors.WithStack(queryErr)
		}

		// Indexer error - wait then retry
		err = queryErr
		Warnf("Indexer error during query - retry %d/%d after %v.  Error: %v", i, MaxQueryRetries, waitTime, queryErr)
		time.Sleep(waitTime)

		waitTime = waitTime * 2
	}

	Warnf("Exceeded max retries for query when querying index using statement: [%s] parameters: [%+v], err:%v", UD(bucketStatement), UD(params), err)
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

func (c *Collection) WaitForIndexOnline(indexName string) error {
	return WaitForIndexOnline(c, indexName)
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

func (c *Collection) executeQuery(statement string) (sgbucket.QueryResultIterator, error) {
	n1qlOptions := &gocb.QueryOptions{}
	queryResults, queryErr := c.cluster.Query(statement, n1qlOptions)
	if queryErr != nil {
		return nil, queryErr
	}
	resultsIterator := &gocbRawIterator{
		rawResult: queryResults.Raw(),
	}
	return resultsIterator, nil
}

func (c *Collection) executeStatement(statement string) error {
	n1qlOptions := &gocb.QueryOptions{}
	queryResults, queryErr := c.cluster.Query(statement, n1qlOptions)
	if queryErr != nil {
		return queryErr
	}

	// Drain results to return any non-query errors
	for queryResults.Next() {
	}
	closeErr := queryResults.Close()
	if closeErr != nil {
		return closeErr
	}
	return queryResults.Err()
}

func (c *Collection) IsErrNoResults(err error) bool {
	return err == gocb.ErrNoResult
}

func (c *Collection) getIndexes() (indexes []string, err error) {
	return nil, errors.New("not implemented")
}
