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
	"fmt"
	"strings"
	"time"

	"golang.org/x/exp/slices"

	"github.com/couchbase/gocb/v2"
	sgbucket "github.com/couchbase/sg-bucket"
	pkgerrors "github.com/pkg/errors"
)

// gocb v1 and v2 have distinct declarations of consistency mode, but values are consistent
// for options used by SG (NotBounded, RequestPlus).  Defining our own version here to avoid
// versioned gocb imports outside of bucket/collection implementations.
type ConsistencyMode int

const (
	// NotBounded indicates no data consistency is required.
	NotBounded = ConsistencyMode(1)
	// RequestPlus indicates that request-level data consistency is required.
	RequestPlus = ConsistencyMode(2)
)

// N1QLStore defines the set of operations Sync Gateway uses to manage and interact with N1QL
type N1QLStore interface {
	GetName() string
	BuildDeferredIndexes(ctx context.Context, indexSet []string) error
	CreateIndex(ctx context.Context, indexName string, expression string, filterExpression string, options *N1qlIndexOptions) error
	CreatePrimaryIndex(ctx context.Context, indexName string, options *N1qlIndexOptions) error
	DropIndex(ctx context.Context, indexName string) error
	ExplainQuery(statement string, params map[string]interface{}) (plan map[string]interface{}, err error)
	GetIndexMeta(ctx context.Context, indexName string) (exists bool, meta *IndexMeta, err error)
	Query(statement string, params map[string]interface{}, consistency ConsistencyMode, adhoc bool) (results sgbucket.QueryResultIterator, err error)
	IsErrNoResults(error) bool
	EscapedKeyspace() string
	IndexMetaBucketID() string
	IndexMetaScopeID() string
	IndexMetaKeyspaceID() string
	BucketName() string
	WaitForIndexesOnline(ctx context.Context, indexNames []string, failfast bool) error

	// executeQuery performs the specified query without any built-in retry handling and returns the resultset
	executeQuery(statement string) (sgbucket.QueryResultIterator, error)

	// executeStatement executes the specified statement and closes the response, returning any errors received.
	executeStatement(statement string) error

	// getIndexes retrieves all index names, used by test harness
	GetIndexes() (indexes []string, err error)

	// waitUntilQueryServiceReady waits until the query service is ready to accept requests
	waitUntilQueryServiceReady(timeout time.Duration) error
}

func ExplainQuery(store N1QLStore, statement string, params map[string]interface{}) (plan map[string]interface{}, err error) {
	explainStatement := fmt.Sprintf("EXPLAIN %s", statement)
	explainResults, explainErr := store.Query(explainStatement, params, RequestPlus, true)

	if explainErr != nil {
		return nil, explainErr
	}

	firstRow := explainResults.NextBytes()
	err = explainResults.Close()
	if err != nil {
		return nil, err
	}

	unmarshalErr := JSONUnmarshal(firstRow, &plan)
	return plan, unmarshalErr
}

type indexManager struct {
	cluster        *gocb.QueryIndexManager
	collection     *gocb.CollectionQueryIndexManager
	bucketName     string
	scopeName      string
	collectionName string
}

func (im *indexManager) GetAllIndexes() ([]gocb.QueryIndex, error) {
	opts := &gocb.GetAllQueryIndexesOptions{
		RetryStrategy: &goCBv2FailFastRetryStrategy{},
	}

	if im.collection != nil {
		return im.collection.GetAllIndexes(opts)
	}
	opts.ScopeName = im.scopeName
	opts.CollectionName = im.collectionName
	return im.cluster.GetAllIndexes(im.bucketName, opts)
}

// CreateIndex issues a CREATE INDEX query in the current bucket, using the form:
//
//	CREATE INDEX indexName ON bucket.Name(expression) WHERE filterExpression WITH options
//
// Sample usage with resulting statement:
//
//	  CreateIndex("myIndex", "field1, field2, nested.field", "field1 > 0", N1qlIndexOptions{numReplica:1})
//	CREATE INDEX myIndex on myBucket(field1, field2, nested.field) WHERE field1 > 0 WITH {"numReplica":1}
func CreateIndex(ctx context.Context, store N1QLStore, indexName string, expression string, filterExpression string, options *N1qlIndexOptions) error {
	createStatement := fmt.Sprintf("CREATE INDEX `%s` ON %s(%s)", indexName, store.EscapedKeyspace(), expression)

	// Add filter expression, when present
	if filterExpression != "" {
		createStatement = fmt.Sprintf("%s WHERE %s", createStatement, filterExpression)
	}

	// Replace any KeyspaceQueryToken references in the index expression
	createStatement = strings.Replace(createStatement, KeyspaceQueryToken, store.EscapedKeyspace(), -1)

	createErr := createIndex(ctx, store, indexName, createStatement, options)
	if createErr != nil {
		if strings.Contains(createErr.Error(), "already exists") || strings.Contains(createErr.Error(), "duplicate index name") {
			return ErrAlreadyExists
		}
	}
	return createErr
}

func CreatePrimaryIndex(ctx context.Context, store N1QLStore, indexName string, options *N1qlIndexOptions) error {
	createStatement := fmt.Sprintf("CREATE PRIMARY INDEX `%s` ON %s", indexName, store.EscapedKeyspace())
	return createIndex(ctx, store, indexName, createStatement, options)
}

func createIndex(ctx context.Context, store N1QLStore, indexName string, createStatement string, options *N1qlIndexOptions) error {

	if options != nil {
		withClause, marshalErr := JSONMarshal(options)
		if marshalErr != nil {
			return marshalErr
		}
		createStatement = fmt.Sprintf(`%s with %s`, createStatement, withClause)
	}

	DebugfCtx(ctx, KeyQuery, "Attempting to create index using statement: [%s]", UD(createStatement))

	err := store.executeStatement(createStatement)
	if err == nil {
		return nil
	}

	if IsIndexerRetryIndexError(err) {
		InfofCtx(ctx, KeyQuery, "Indexer error creating index - waiting for server background retry.  Error:%v", err)
		// Wait for bucket to be created in background before returning
		return waitForIndexExistence(ctx, store, indexName, true)
	}

	if IsCreateDuplicateIndexError(err) {
		InfofCtx(ctx, KeyQuery, "Duplicate index creation in progress - waiting for index readiness.  Error:%v", err)
		// Wait for bucket to be created in background before returning
		return waitForIndexExistence(ctx, store, indexName, true)
	}

	return pkgerrors.WithStack(RedactErrorf("Error creating index with statement: %s.  Error: %v", UD(createStatement), err))
}

// Waits for index to exist/not exist.  Used in response to background create/drop processing by server.
func waitForIndexExistence(ctx context.Context, store N1QLStore, indexName string, shouldExist bool) error {

	worker := func() (shouldRetry bool, err error, value interface{}) {
		// GetIndexMeta has its own error retry handling,
		// but keep the retry logic up here for checking if the index exists.
		exists, _, err := store.GetIndexMeta(ctx, indexName)
		if err != nil {
			return false, err, nil
		}
		// If it's in the desired state, we're done
		if exists == shouldExist {
			return false, nil, nil
		}
		// Retry
		return true, nil, nil
	}

	// Kick off retry loop
	err, _ := RetryLoop("waitForIndexExistence", worker, CreateMaxDoublingSleeperFunc(25, 100, 15000))
	if err != nil {
		return pkgerrors.Wrapf(err, "Error during waitForIndexExistence for index %s", indexName)
	}

	return nil
}

// BuildDeferredIndexes issues a build command for any deferred sync gateway indexes associated with the bucket.
func BuildDeferredIndexes(ctx context.Context, s N1QLStore, indexSet []string) error {

	if len(indexSet) == 0 {
		return nil
	}

	// Only build indexes that are in deferred state.  Query system:indexes to validate the provided set of indexes
	statement := fmt.Sprintf("SELECT indexes.name, indexes.state FROM system:indexes WHERE indexes.keyspace_id = '%s'", s.IndexMetaKeyspaceID())

	if s.IndexMetaBucketID() != "" {
		statement += fmt.Sprintf("AND indexes.bucket_id = '%s' ", s.IndexMetaBucketID())
	}
	if s.IndexMetaScopeID() != "" {
		statement += fmt.Sprintf("AND indexes.scope_id = '%s' ", s.IndexMetaScopeID())
	}

	statement += fmt.Sprintf("AND indexes.name IN [%s]", StringSliceToN1QLArray(indexSet, "'"))
	// mod: bucket name

	results, err := s.executeQuery(statement)
	if err != nil {
		return err
	}
	deferredIndexes := make([]string, 0)
	var indexInfo struct {
		Name  string `json:"name"`
		State string `json:"state"`
	}
	for results.Next(&indexInfo) {
		// If index is deferred (not built), add to set of deferred indexes
		if indexInfo.State == IndexStateDeferred {
			deferredIndexes = append(deferredIndexes, indexInfo.Name)
		}
	}
	closeErr := results.Close()
	if closeErr != nil {
		return closeErr
	}

	if len(deferredIndexes) == 0 {
		return nil
	}

	InfofCtx(ctx, KeyQuery, "Building deferred indexes: %v", deferredIndexes)
	buildErr := buildIndexes(ctx, s, deferredIndexes)
	return buildErr
}

// BuildIndexes executes a BUILD INDEX statement in the current bucket, using the form:
//
//	BUILD INDEX ON `bucket.Name`(`index1`, `index2`, ...)
func buildIndexes(ctx context.Context, s N1QLStore, indexNames []string) error {
	if len(indexNames) == 0 {
		return nil
	}

	// Not using strings.Join because we want to escape each index name
	indexNameList := StringSliceToN1QLArray(indexNames, "`")

	buildStatement := fmt.Sprintf("BUILD INDEX ON %s(%s)", s.EscapedKeyspace(), indexNameList)
	err := s.executeStatement(buildStatement)

	// If indexer reports build will be completed in the background, wait to validate build actually happens.
	if IsIndexerRetryBuildError(err) {
		InfofCtx(context.TODO(), KeyQuery, "Indexer error creating index - waiting for background build.  Error:%v", err)
		// Wait for bucket to be created in background before returning
		return s.WaitForIndexesOnline(ctx, indexNames, false)
	}

	return err
}

// IndexMeta represents a Couchbase GSI index.
type IndexMeta struct {
	Name      string   `json:"name"`
	IsPrimary bool     `json:"is_primary"`
	Type      string   `json:"using"`
	State     string   `json:"state"`
	Keyspace  string   `json:"keyspace_id"`
	Namespace string   `json:"namespace_id"`
	IndexKey  []string `json:"index_key"`
}

type getIndexMetaRetryValues struct {
	exists bool
	meta   *IndexMeta
}

func GetIndexMeta(ctx context.Context, store N1QLStore, indexName string) (exists bool, meta *IndexMeta, err error) {

	worker := func() (shouldRetry bool, err error, value interface{}) {
		exists, meta, err := getIndexMetaWithoutRetry(store, indexName)
		if err != nil {
			// retry
			WarnfCtx(ctx, "Error from GetIndexMeta for index %s: %v will retry", indexName, err)
			return true, err, nil
		}
		return false, nil, getIndexMetaRetryValues{
			exists: exists,
			meta:   meta,
		}
	}

	// Kick off retry loop
	err, val := RetryLoop("GetIndexMeta", worker, CreateMaxDoublingSleeperFunc(25, 100, 15000))
	if err != nil {
		return false, nil, pkgerrors.Wrapf(err, "Error during GetIndexMeta for index %s", indexName)
	}

	valTyped, ok := val.(getIndexMetaRetryValues)
	if !ok {
		return false, nil, fmt.Errorf("Expected GetIndexMeta retry value to be getIndexMetaRetryValues but got %T", val)
	}

	return valTyped.exists, valTyped.meta, nil
}

func getIndexMetaWithoutRetry(store N1QLStore, indexName string) (exists bool, meta *IndexMeta, err error) {
	statement := fmt.Sprintf("SELECT state FROM system:indexes WHERE indexes.name = '%s' AND indexes.keyspace_id = '%s'", indexName, store.IndexMetaKeyspaceID())
	if store.IndexMetaBucketID() != "" {
		statement += fmt.Sprintf(" AND indexes.bucket_id = '%s'", store.IndexMetaBucketID())
	}
	if store.IndexMetaScopeID() != "" {
		statement += fmt.Sprintf(" AND indexes.scope_id = '%s'", store.IndexMetaScopeID())
	}
	results, queryErr := store.executeQuery(statement)
	if queryErr != nil {
		return false, nil, queryErr
	}

	indexInfo := &IndexMeta{}
	err = results.One(indexInfo)
	if err != nil {
		if store.IsErrNoResults(err) {
			return false, nil, nil
		} else {
			return true, nil, err
		}
	}
	return true, indexInfo, nil
}

// DropIndex drops the specified index from the current bucket.
func DropIndex(ctx context.Context, store N1QLStore, indexName string) error {
	statement := fmt.Sprintf("DROP INDEX default:%s.`%s`", store.EscapedKeyspace(), indexName)

	err := store.executeStatement(statement)
	if err != nil && !IsIndexerRetryIndexError(err) {
		return err
	}

	if IsIndexerRetryIndexError(err) {
		InfofCtx(ctx, KeyQuery, "Indexer error dropping index - waiting for server background retry.  Error:%v", err)
		// Wait for bucket to be dropped in background before returning
		return waitForIndexExistence(ctx, store, indexName, false)
	}

	return err
}

// AsN1QLStore tries to return the given DataStore as a N1QLStore, based on underlying buckets.
func AsN1QLStore(bucket DataStore) (N1QLStore, bool) {

	var underlyingDataStore DataStore
	switch typedBucket := bucket.(type) {
	case *Collection:
		return typedBucket, true
	case *LeakyDataStore:
		underlyingDataStore = typedBucket.dataStore
	default:
		// bail out for unrecognised/unsupported buckets
		return nil, false
	}

	return AsN1QLStore(underlyingDataStore)
}

// Index not found errors (returned by DropIndex) don't have a specific N1QL error code - they are of the form:
//
//	[5000] GSI index testIndex_not_found not found.
//
// Stuck with doing a string compare to differentiate between 'not found' and other errors
func IsIndexNotFoundError(err error) bool {
	return strings.Contains(err.Error(), "not found")
}

// 'IsIndexerRetry' type errors are of the form:
// error:[5000] GSI CreateIndex() - cause: Encountered transient error.  Index creation will be retried in background.  Error: Index testIndex_value will retry building in the background for reason: Bucket test_data_bucket In Recovery.
// error:[5000] GSI Drop() - cause: Fail to drop index on some indexer nodes.  Error=Encountered error when dropping index: Indexer In Recovery. Drop index will be retried in background.
// error:[5000] BuildIndexes - cause: Build index fails.  %vIndex testIndexDeferred will retry building in the background for reason: Build Already In Progress. Bucket test_data_bucket.
//
// https://issues.couchbase.com/browse/MB-19358 is filed to request improved indexer error codes for these scenarios (and others)
func IsIndexerRetryIndexError(err error) bool {
	if err == nil {
		return false
	}
	if strings.Contains(err.Error(), "will retry") || strings.Contains(err.Error(), "will be retried") {
		return true
	}
	return false
}

func IsCreateDuplicateIndexError(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), "duplicate index name")
}

func IsIndexerRetryBuildError(err error) bool {
	if err == nil {
		return false
	}
	if strings.Contains(err.Error(), "will retry") || strings.Contains(err.Error(), "will be retried") {
		return true
	}
	return false
}

// Check for transient indexer errors (can be retried)
func isTransientIndexerError(err error) bool {
	if err == nil {
		return false
	} else if strings.Contains(err.Error(), "Indexer rollback") {
		return true
	} else if IsIndexerRetryBuildError(err) {
		return true
	}

	return false
}

func SlowQueryLog(ctx context.Context, startTime time.Time, threshold time.Duration, messageFormat string, args ...interface{}) {
	if elapsed := time.Now().Sub(startTime); elapsed > threshold {
		InfofCtx(ctx, KeyQuery, messageFormat+" took "+elapsed.String(), args...)
	}
}

// Converts to a format like `value1`,`value2` when quote=`
func StringSliceToN1QLArray(values []string, quote string) string {
	if len(values) == 0 {
		return ""
	}
	asString := fmt.Sprintf("%s%s%s", quote, values[0], quote)
	for i := 1; i < len(values); i++ {
		asString = fmt.Sprintf("%s,%s%s%s", asString, quote, values[i], quote)
	}
	return asString
}

// gocbResultRaw wraps a raw gocb result (both view and n1ql) to implement
// the sgbucket.QueryResultIterator interface
type gocbResultRaw interface {

	// NextBytes returns the next row as bytes.
	NextBytes() []byte

	// Err returns any errors that have occurred on the stream
	Err() error

	// Close marks the results as closed, returning any errors that occurred during reading the results.
	Close() error

	// MetaData returns any meta-data that was available from this query as bytes.
	MetaData() ([]byte, error)
}

// GoCBQueryIterator wraps a gocb v2 ViewResultRaw to implement sgbucket.QueryResultIterator
type gocbRawIterator struct {
	rawResult                  gocbResultRaw
	concurrentQueryOpLimitChan chan struct{}
}

// Unmarshal a single result row into valuePtr, and then close the iterator
func (i *gocbRawIterator) One(valuePtr interface{}) error {
	if !i.Next(valuePtr) {
		err := i.Close()
		if err != nil {
			return nil
		}
		return gocb.ErrNoResult
	}

	// Ignore any errors occurring after we already have our result
	//  - follows approach used by gocb v1 One() implementation
	_ = i.Close()
	return nil
}

// Unmarshal the next result row into valuePtr.  Returns false when reaching end of result set
func (i *gocbRawIterator) Next(valuePtr interface{}) bool {

	nextBytes := i.rawResult.NextBytes()
	if nextBytes == nil {
		return false
	}

	err := JSONUnmarshal(nextBytes, &valuePtr)
	if err != nil {
		WarnfCtx(context.TODO(), "Unable to marshal view result row into value: %v", err)
		return false
	}
	return true
}

// Retrieve raw bytes for the next result row
func (i *gocbRawIterator) NextBytes() []byte {
	return i.rawResult.NextBytes()
}

// Closes the iterator.  Returns any row-level errors seen during iteration.
func (i *gocbRawIterator) Close() error {
	// Have to iterate over any remaining results to clear the reader
	// Otherwise we get "the result must be closed before accessing the meta-data" on close details on CBG-1666
	for i.rawResult.NextBytes() != nil {
		// noop to drain results
	}

	defer func() {
		if i.concurrentQueryOpLimitChan != nil {
			<-i.concurrentQueryOpLimitChan
		}
	}()

	// check for errors before closing?
	closeErr := i.rawResult.Close()
	if closeErr != nil {
		return closeErr
	}
	resultErr := i.rawResult.Err()
	return resultErr
}

func IndexMetaKeyspaceID(bucketName, scopeName, collectionName string) string {
	if IsDefaultCollection(scopeName, collectionName) {
		return bucketName
	}
	return collectionName
}

// WaitForIndexesOnline takes set of indexes and watches them till they're online.
func WaitForIndexesOnline(ctx context.Context, mgr *indexManager, indexNames []string, failfast bool) error {
	maxNumAttempts := 180
	if failfast {
		maxNumAttempts = 1
	}
	retrySleeper := CreateMaxDoublingSleeperFunc(maxNumAttempts, 100, 5000)
	retryCount := 0

	onlineIndexes := make(map[string]bool)

	for {
		watchedOnlineIndexCount := 0
		currIndexes, err := mgr.GetAllIndexes()
		if err != nil {
			return err
		}
		// check each of the current indexes state, add to map once finished to make sure each index online is only being logged once
		for i := 0; i < len(currIndexes); i++ {
			name := currIndexes[i].Name
			// use slices.Contains since the number of indexes is expected to be small
			if currIndexes[i].State == IndexStateOnline && slices.Contains(indexNames, name) {
				if !onlineIndexes[name] {
					InfofCtx(ctx, KeyAll, "Index %s is online", MD(name))
					onlineIndexes[name] = true
				}
			}
		}
		// check online index against indexes we watch to have online, increase counter as each comes online
		var offlineIndexes []string
		for _, listVal := range indexNames {
			if onlineIndexes[listVal] {
				watchedOnlineIndexCount++
			} else {
				offlineIndexes = append(offlineIndexes, listVal)
			}
		}

		if watchedOnlineIndexCount == len(indexNames) {
			return nil
		}
		retryCount++
		shouldContinue, sleepMs := retrySleeper(retryCount)
		if !shouldContinue {
			return fmt.Errorf("error waiting for indexes %s ...", strings.Join(offlineIndexes, ", "))
		}
		InfofCtx(ctx, KeyAll, "Indexes %s not ready - retrying...", strings.Join(offlineIndexes, ", "))
		time.Sleep(time.Millisecond * time.Duration(sleepMs))
	}
}

func GetAllIndexes(mgr *indexManager) (indexes []string, err error) {
	indexes = []string{}
	indexInfo, err := mgr.GetAllIndexes()
	if err != nil {
		return indexes, err
	}

	for _, indexInfo := range indexInfo {
		indexes = append(indexes, indexInfo.Name)
	}
	return indexes, nil
}
