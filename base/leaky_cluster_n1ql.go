// Copyright 2024-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package base

import (
	"context"
	"time"

	sgbucket "github.com/couchbase/sg-bucket"
)

// LeakyClusterOnlyN1QLStore is a wrapper around ClusterOnlyN1QLStore that allows for a callback to be set in queries.
type LeakyClusterOnlyN1QLStore struct {
	cl            *ClusterOnlyN1QLStore
	queryCallback N1QLQueryCallback
}

var _ ClusterN1QLStore = &LeakyClusterOnlyN1QLStore{}

// N1QLQueryCallback is a callback that can be replace N1QLStore.executeQuery. If callParent is true, the result of the callback will not be returned.
type N1QLQueryCallback func(statement string) (callParent bool, results sgbucket.QueryResultIterator, err error)

func NewLeakyClusterOnlyN1QLStore(store *ClusterOnlyN1QLStore, queryCallback N1QLQueryCallback) *LeakyClusterOnlyN1QLStore {
	return &LeakyClusterOnlyN1QLStore{
		cl:            store,
		queryCallback: queryCallback,
	}
}

// GetName returns a human-readable name.
func (lcl *LeakyClusterOnlyN1QLStore) GetName() string {
	return lcl.cl.GetName()
}

func (lcl *LeakyClusterOnlyN1QLStore) BucketName() string {
	return lcl.cl.BucketName()
}

// BuildDeferredIndexes issues a BUILD INDEX command for any of the indexes that have state deferred.
func (lcl *LeakyClusterOnlyN1QLStore) BuildDeferredIndexes(ctx context.Context, indexSet []string) error {
	return lcl.cl.BuildDeferredIndexes(ctx, indexSet)
}

// CreateIndex issues a CREATE INDEX query for a specified index.
func (lcl *LeakyClusterOnlyN1QLStore) CreateIndex(ctx context.Context, indexName string, expression string, filterExpression string, options *N1qlIndexOptions) error {
	return lcl.cl.CreateIndex(ctx, indexName, expression, filterExpression, options)
}

// CreatePrimaryIndex issues a CREATE PRIMARY INDEX query for a specified index.
func (lcl *LeakyClusterOnlyN1QLStore) CreatePrimaryIndex(ctx context.Context, indexName string, options *N1qlIndexOptions) error {
	return lcl.cl.CreatePrimaryIndex(ctx, indexName, options)
}

// ExplainQuery returns the query plan for a specified statement.
func (lcl *LeakyClusterOnlyN1QLStore) ExplainQuery(ctx context.Context, statement string, params map[string]interface{}) (plan map[string]interface{}, err error) {
	return lcl.cl.ExplainQuery(ctx, statement, params)
}

// DropIndex issues a DROP INDEX query for a specified index.
func (lcl *LeakyClusterOnlyN1QLStore) DropIndex(ctx context.Context, indexName string) error {
	return lcl.cl.DropIndex(ctx, indexName)
}

// IndexMetaKeyspaceID returns the value of keyspace_id for the system:indexes table for the collection.
func (lcl *LeakyClusterOnlyN1QLStore) IndexMetaKeyspaceID() string {
	return lcl.cl.IndexMetaKeyspaceID()
}

// IndexMetaBucketID returns the value of bucket_id for the system:indexes table for the collection.
func (lcl *LeakyClusterOnlyN1QLStore) IndexMetaBucketID() string {
	return lcl.cl.IndexMetaBucketID()
}

// IndexMetaScopeID returns the value of scope_id for the system:indexes table for the collection.
func (lcl *LeakyClusterOnlyN1QLStore) IndexMetaScopeID() string {
	return lcl.cl.IndexMetaScopeID()
}

// Query runs a N1QL query and returns the results.
func (lcl *LeakyClusterOnlyN1QLStore) Query(ctx context.Context, statement string, params map[string]interface{}, consistency ConsistencyMode, adhoc bool) (resultsIterator sgbucket.QueryResultIterator, err error) {
	return lcl.cl.Query(ctx, statement, params, consistency, adhoc)
}

// executeQuery runs a N1QL query against the cluster.  Does not throttle query ops.
func (lcl *LeakyClusterOnlyN1QLStore) executeQuery(statement string) (sgbucket.QueryResultIterator, error) {
	if lcl.queryCallback != nil {
		callParent, results, err := lcl.queryCallback(statement)
		if !callParent {
			return results, err
		}
	}
	return lcl.cl.executeQuery(statement)
}

func (lcl *LeakyClusterOnlyN1QLStore) executeStatement(statement string) error {
	return lcl.cl.executeStatement(statement)
}

// WaitForIndexesOnline takes set of indexes and watches them till they're online.
func (lcl *LeakyClusterOnlyN1QLStore) WaitForIndexesOnline(ctx context.Context, indexNames []string, waitOption WaitForIndexesOnlineOption) error {
	return lcl.cl.WaitForIndexesOnline(ctx, indexNames, waitOption)
}

// GetIndexMeta retrieves the metadata for a specified index.
func (lcl *LeakyClusterOnlyN1QLStore) GetIndexMeta(ctx context.Context, indexName string) (exists bool, meta *IndexMeta, err error) {
	return lcl.cl.GetIndexMeta(ctx, indexName)
}

func (lcl *LeakyClusterOnlyN1QLStore) IsErrNoResults(err error) bool {
	return lcl.cl.IsErrNoResults(err)
}

// EscapedKeyspace returns the escaped fully-qualified identifier for the keyspace (e.g. `bucket`.`scope`.`collection`)
func (lcl *LeakyClusterOnlyN1QLStore) EscapedKeyspace() string {
	return lcl.cl.EscapedKeyspace()
}

func (lcl *LeakyClusterOnlyN1QLStore) GetIndexes(ctx context.Context) (indexes []string, err error) {
	return lcl.cl.GetIndexes(ctx)
}

// waitUntilQueryServiceReady will wait for the specified duration until the query service is available.
func (lcl *LeakyClusterOnlyN1QLStore) waitUntilQueryServiceReady(timeout time.Duration) error {
	return lcl.cl.waitUntilQueryServiceReady(timeout)
}

// LeakyClusterOnlyN1QLStore allows callers to set the scope and collection per operation
func (lcl *LeakyClusterOnlyN1QLStore) SetScopeAndCollection(scName ScopeAndCollectionName) {
	lcl.cl.SetScopeAndCollection(scName)
}
