/*
Copyright 2023-Present Couchbase, Inc.

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

var _ N1QLStore = &ClusterOnlyN1QLStore{}

// ClusterOnlyN1qlStore implements the N1QLStore using only a cluster connection.
// Currently still intended for use for operations against a single collection, but maintains that
// information via metadata, and so supports sharing of the underlying gocb.Cluster with other
// ClusterOnlyN1QLStore instances.  Anticipates future refactoring of N1QLStore to differentiate between
// collection-scoped and non-collection-scoped operations.
type ClusterOnlyN1QLStore struct {
	cluster             *gocb.Cluster
	bucketName          string // User to build keyspace for query when not otherwise set
	scopeName           string // Used to build keyspace for query when not otherwise set
	collectionName      string // Used to build keyspace for query when not otherwise set
	supportsCollections bool
}

func NewClusterOnlyN1QLStore(cluster *gocb.Cluster, bucketName, scopeName, collectionName string) (*ClusterOnlyN1QLStore, error) {

	clusterOnlyn1qlStore := &ClusterOnlyN1QLStore{
		cluster:        cluster,
		bucketName:     bucketName,
		scopeName:      scopeName,
		collectionName: collectionName,
	}

	major, minor, err := getClusterVersion(cluster)
	if err != nil {
		return nil, err
	}
	clusterOnlyn1qlStore.supportsCollections = isMinimumVersion(uint64(major), uint64(minor), 7, 0)

	return clusterOnlyn1qlStore, nil

}

func (cl *ClusterOnlyN1QLStore) GetName() string {
	return cl.bucketName
}

func (cl *ClusterOnlyN1QLStore) BucketName() string {
	return cl.bucketName
}

func (cl *ClusterOnlyN1QLStore) BuildDeferredIndexes(ctx context.Context, indexSet []string) error {
	return BuildDeferredIndexes(ctx, cl, indexSet)
}

func (cl *ClusterOnlyN1QLStore) CreateIndex(ctx context.Context, indexName string, expression string, filterExpression string, options *N1qlIndexOptions) error {
	return CreateIndex(ctx, cl, indexName, expression, filterExpression, options)
}

func (cl *ClusterOnlyN1QLStore) CreatePrimaryIndex(ctx context.Context, indexName string, options *N1qlIndexOptions) error {
	return CreatePrimaryIndex(ctx, cl, indexName, options)
}

func (cl *ClusterOnlyN1QLStore) ExplainQuery(ctx context.Context, statement string, params map[string]interface{}) (plan map[string]interface{}, err error) {
	return ExplainQuery(ctx, cl, statement, params)
}

func (cl *ClusterOnlyN1QLStore) DropIndex(ctx context.Context, indexName string) error {
	return DropIndex(ctx, cl, indexName)
}

// IndexMetaKeyspaceID returns the value of keyspace_id for the system:indexes table for the collection.
func (cl *ClusterOnlyN1QLStore) IndexMetaKeyspaceID() string {
	return IndexMetaKeyspaceID(cl.bucketName, cl.scopeName, cl.collectionName)
}

// IndexMetaBucketID returns the value of bucket_id for the system:indexes table for the collection.
func (cl *ClusterOnlyN1QLStore) IndexMetaBucketID() string {
	if IsDefaultCollection(cl.scopeName, cl.collectionName) {
		return ""
	}
	return cl.bucketName
}

// IndexMetaScopeID returns the value of scope_id for the system:indexes table for the collection.
func (cl *ClusterOnlyN1QLStore) IndexMetaScopeID() string {
	if IsDefaultCollection(cl.scopeName, cl.collectionName) {
		return ""
	}
	return cl.scopeName
}

func (cl *ClusterOnlyN1QLStore) Query(ctx context.Context, statement string, params map[string]interface{}, consistency ConsistencyMode, adhoc bool) (resultsIterator sgbucket.QueryResultIterator, err error) {
	keyspaceStatement := strings.Replace(statement, KeyspaceQueryToken, cl.EscapedKeyspace(), -1)

	n1qlOptions := &gocb.QueryOptions{
		ScanConsistency: gocb.QueryScanConsistency(consistency),
		Adhoc:           adhoc,
		NamedParameters: params,
	}

	waitTime := 10 * time.Millisecond
	for i := 1; i <= MaxQueryRetries; i++ {
		TracefCtx(ctx, KeyQuery, "Executing N1QL query: %v - %+v", UD(keyspaceStatement), UD(params))
		queryResults, queryErr := cl.runQuery(keyspaceStatement, n1qlOptions)
		if queryErr == nil {
			resultsIterator := &gocbRawIterator{
				rawResult:                  queryResults.Raw(),
				concurrentQueryOpLimitChan: nil,
			}
			return resultsIterator, queryErr
		}

		// Timeout error - return named error
		if errors.Is(queryErr, gocb.ErrTimeout) {
			return resultsIterator, ErrViewTimeoutError
		}

		// Non-retry error - return
		if !isTransientIndexerError(queryErr) {
			WarnfCtx(ctx, "Error when querying index using statement: [%s] parameters: [%+v] error:%v", UD(keyspaceStatement), UD(params), queryErr)
			return resultsIterator, pkgerrors.WithStack(queryErr)
		}

		// Indexer error - wait then retry
		err = queryErr
		WarnfCtx(ctx, "Indexer error during query - retry %d/%d after %v.  Error: %v", i, MaxQueryRetries, waitTime, queryErr)
		time.Sleep(waitTime)

		waitTime = waitTime * 2
	}

	WarnfCtx(ctx, "Exceeded max retries for query when querying index using statement: [%s] parameters: [%+v], err:%v", UD(keyspaceStatement), UD(params), err)
	return nil, err
}

// executeQuery runs a N1QL query against the cluster.  Does not throttle query ops.
func (cl *ClusterOnlyN1QLStore) executeQuery(statement string) (sgbucket.QueryResultIterator, error) {
	queryResults, queryErr := cl.runQuery(statement, nil)
	if queryErr != nil {
		return nil, queryErr
	}

	resultsIterator := &gocbRawIterator{
		rawResult:                  queryResults.Raw(),
		concurrentQueryOpLimitChan: nil,
	}
	return resultsIterator, nil
}

func (cl *ClusterOnlyN1QLStore) executeStatement(statement string) error {
	queryResults, queryErr := cl.runQuery(statement, nil)
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

func (cl *ClusterOnlyN1QLStore) runQuery(statement string, n1qlOptions *gocb.QueryOptions) (*gocb.QueryResult, error) {
	if n1qlOptions == nil {
		n1qlOptions = &gocb.QueryOptions{}
	}
	queryResults, err := cl.cluster.Query(statement, n1qlOptions)

	return queryResults, err
}

func (cl *ClusterOnlyN1QLStore) WaitForIndexesOnline(ctx context.Context, indexNames []string, failfast bool) error {
	return WaitForIndexesOnline(ctx, cl.cluster, cl.bucketName, cl.scopeName, cl.collectionName, indexNames, failfast)
}

func (cl *ClusterOnlyN1QLStore) GetIndexMeta(ctx context.Context, indexName string) (exists bool, meta *IndexMeta, err error) {
	return GetIndexMeta(ctx, cl, indexName)
}

func (cl *ClusterOnlyN1QLStore) IsErrNoResults(err error) bool {
	return err == gocb.ErrNoResult
}

// EscapedKeyspace returns the escaped fully-qualified identifier for the keyspace (e.g. `bucket`.`scope`.`collection`)
func (cl *ClusterOnlyN1QLStore) EscapedKeyspace() string {
	if !cl.supportsCollections {
		return fmt.Sprintf("`%s`", cl.bucketName)
	}
	return fmt.Sprintf("`%s`.`%s`.`%s`", cl.bucketName, cl.scopeName, cl.collectionName)
}

func (cl *ClusterOnlyN1QLStore) GetIndexes() (indexes []string, err error) {
	if cl.supportsCollections {
		return GetAllIndexes(cl.cluster, cl.bucketName, cl.scopeName, cl.collectionName)
	} else {
		return GetAllIndexes(cl.cluster, cl.bucketName, "", "")
	}
}

// waitUntilQueryServiceReady will wait for the specified duration until the query service is available.
func (cl *ClusterOnlyN1QLStore) waitUntilQueryServiceReady(timeout time.Duration) error {
	return cl.cluster.WaitUntilReady(timeout,
		&gocb.WaitUntilReadyOptions{ServiceTypes: []gocb.ServiceType{gocb.ServiceTypeQuery}},
	)
}

// ClusterOnlyN1QLStore allows callers to set the scope and collection per operation
func (cl *ClusterOnlyN1QLStore) SetScopeAndCollection(scName ScopeAndCollectionName) {
	cl.scopeName = scName.Scope
	cl.collectionName = scName.Collection
}
