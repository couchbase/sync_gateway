package base

import (
	"fmt"
	"strings"
	"time"

	"github.com/couchbase/gocb"
	pkgerrors "github.com/pkg/errors"
)

const BucketQueryToken = "$_bucket"   // Token used for bucket name replacement in query statements
const MaxQueryRetries = 30            // Maximum query retries on indexer error
const IndexStateOnline = "online"     // bucket state value, as returned by SELECT FROM system:indexes.  Index has been created and built.
const IndexStateDeferred = "deferred" // bucket state value, as returned by SELECT FROM system:indexes.  Index has been created but not built.
const IndexStatePending = "pending"   // bucket state value, as returned by SELECT FROM system:indexes.  Index has been created, build is in progress
const PrimaryIndexName = "#primary"

// IndexOptions used to build the 'with' clause
type N1qlIndexOptions struct {
	NumReplica      uint `json:"num_replica,omitempty"`          // Number of replicas
	IndexTombstones bool `json:"retain_deleted_xattr,omitempty"` // Whether system xattrs on tombstones should be indexed
	DeferBuild      bool `json:"defer_build,omitempty"`          // Whether to defer initial build of index (requires a subsequent BUILD INDEX invocation)
}

type N1QLBucket interface {
	Bucket
	Query(statement string, params interface{}, consistency gocb.ConsistencyMode, adhoc bool) (results gocb.QueryResults, err error)
	ExplainQuery(statement string, params interface{}) (plain map[string]interface{}, err error)
	CreateIndex(indexName string, expression string, filterExpression string, options *N1qlIndexOptions) error
	BuildDeferredIndexes(indexSet []string) error
	CreatePrimaryIndex(indexName string, options *N1qlIndexOptions) error
	WaitForIndexOnline(indexName string) error
	GetIndexMeta(indexName string) (exists bool, meta *gocb.IndexInfo, err error)
	DropIndex(indexName string) error
}

var _ N1QLBucket = &CouchbaseBucketGoCB{}

// Query accepts a parameterized statement,  optional list of params, and an optional flag to force adhoc query execution.
// Params specified using the $param notation in the statement are intended to be used w/ N1QL prepared statements, and will be
// passed through as params to n1ql.  e.g.:
//
//	SELECT _sync.sequence FROM $_bucket WHERE _sync.sequence > $minSeq
//
// https://developer.couchbase.com/documentation/server/current/sdk/go/n1ql-queries-with-sdk.html for additional details.
// Will additionally replace all instances of BucketQueryToken($_bucket) in the statement
// with the bucket name.  'bucket' should not be included in params.
//
// If adhoc=true, prepared statement handling will be disabled.  Should only be set to true for queries that can't be prepared, e.g.:
//
//	SELECT _sync.channels.ABC.seq from $bucket
//
// Query retries on Indexer Errors, as these are normally transient
func (bucket *CouchbaseBucketGoCB) Query(statement string, params interface{}, consistency gocb.ConsistencyMode, adhoc bool) (results gocb.QueryResults, err error) {
	bucketStatement := strings.Replace(statement, BucketQueryToken, bucket.GetName(), -1)
	n1qlQuery := gocb.NewN1qlQuery(bucketStatement)
	n1qlQuery = n1qlQuery.AdHoc(adhoc)
	n1qlQuery = n1qlQuery.Consistency(consistency)

	waitTime := 10 * time.Millisecond
	for i := 1; i <= MaxQueryRetries; i++ {

		Tracef(KeyQuery, "Executing N1QL query: %v", UD(n1qlQuery))
		queryResults, queryErr := bucket.executeN1qlQuery(n1qlQuery, params)

		if queryErr == nil {
			return queryResults, queryErr
		}

		// Timeout error - return named error
		if isGoCBQueryTimeoutError(queryErr) {
			return queryResults, ErrViewTimeoutError
		}

		// Non-retry error - return
		if !isTransientIndexerError(queryErr) {
			Warnf("Error when querying index using statement: [%s] parameters: [%+v] error:%v", UD(bucketStatement), UD(params), queryErr)
			return queryResults, pkgerrors.WithStack(queryErr)
		}

		// Indexer error - wait then retry
		err = queryErr
		Warnf("Indexer error during query - retry %d/%d after %v.  Error: %v", i, MaxQueryRetries, waitTime, queryErr)
		time.Sleep(waitTime)

		waitTime = time.Duration(waitTime * 2)
	}

	Warnf("Exceeded max retries for query when querying index using statement: [%s], err:%v", UD(bucketStatement), err)
	return nil, err
}

func (bucket *CouchbaseBucketGoCB) ExplainQuery(statement string, params interface{}) (plan map[string]interface{}, err error) {
	explainStatement := fmt.Sprintf("EXPLAIN %s", statement)
	explainResults, explainErr := bucket.Query(explainStatement, params, gocb.RequestPlus, true)

	if explainErr != nil {
		return nil, explainErr
	}

	firstRow := explainResults.NextBytes()
	unmarshalErr := JSONUnmarshal(firstRow, &plan)
	return plan, unmarshalErr
}

// CreateIndex issues a CREATE INDEX query in the current bucket, using the form:
//
//	CREATE INDEX indexName ON bucket.Name(expression) WHERE filterExpression WITH options
//
// Sample usage with resulting statement:
//
//	  CreateIndex("myIndex", "field1, field2, nested.field", "field1 > 0", N1qlIndexOptions{numReplica:1})
//	CREATE INDEX myIndex on myBucket(field1, field2, nested.field) WHERE field1 > 0 WITH {"numReplica":1}
func (bucket *CouchbaseBucketGoCB) CreateIndex(indexName string, expression string, filterExpression string, options *N1qlIndexOptions) error {

	createStatement := fmt.Sprintf("CREATE INDEX `%s` ON `%s`(%s)", indexName, bucket.GetName(), expression)

	// Add filter expression, when present
	if filterExpression != "" {
		createStatement = fmt.Sprintf("%s WHERE %s", createStatement, filterExpression)
	}

	// Replace any BucketQueryToken references in the index expression
	createStatement = strings.Replace(createStatement, BucketQueryToken, bucket.GetName(), -1)

	createErr := bucket.createIndex(indexName, createStatement, options)
	if createErr != nil && strings.Contains(createErr.Error(), "already exists") {
		return ErrAlreadyExists
	}
	return createErr
}

// BuildIndexes executes a BUILD INDEX statement in the current bucket, using the form:
//
//	BUILD INDEX ON `bucket.Name`(`index1`, `index2`, ...)
func (bucket *CouchbaseBucketGoCB) buildIndexes(indexNames []string) error {

	if len(indexNames) == 0 {
		return nil
	}

	// Not using strings.Join because we want to escape each index name
	indexNameList := StringSliceToN1QLArray(indexNames, "`")

	buildStatement := fmt.Sprintf("BUILD INDEX ON `%s`(%s)", bucket.GetName(), indexNameList)
	n1qlQuery := gocb.NewN1qlQuery(buildStatement)
	_, err := bucket.executeN1qlQuery(n1qlQuery, nil)

	// If indexer reports build will be completed in the background, wait to validate build actually happens.
	if IsIndexerRetryBuildError(err) {
		Infof(KeyQuery, "Indexer error creating index - waiting for background build.  Error:%v", err)
		// Wait for bucket to be created in background before returning
		for _, indexName := range indexNames {
			waitErr := bucket.WaitForIndexOnline(indexName)
			if waitErr != nil {
				return waitErr
			}
		}
		return nil
	}
	return err
}

// Issues a build command for any deferred sync gateway indexes associated with the bucket.
func (bucket *CouchbaseBucketGoCB) BuildDeferredIndexes(indexSet []string) error {

	if len(indexSet) == 0 {
		return nil
	}

	// Only build indexes that are in deferred state.  Query system:indexes to validate the provided set of indexes
	statement := fmt.Sprintf("SELECT indexes.name, indexes.state "+
		"FROM system:indexes "+
		"WHERE indexes.keyspace_id = '%s' "+
		"AND indexes.name IN [%s]",
		bucket.GetName(), StringSliceToN1QLArray(indexSet, "'"))
	n1qlQuery := gocb.NewN1qlQuery(statement)
	results, err := bucket.executeN1qlQuery(n1qlQuery, nil)
	if err != nil {
		return err
	}
	deferredIndexes := make([]string, 0)
	var indexInfo gocb.IndexInfo
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

	Infof(KeyQuery, "Building deferred indexes: %v", deferredIndexes)
	buildErr := bucket.buildIndexes(deferredIndexes)
	return buildErr
}

// executeN1qlQuery acts as a simple wrapper around the gocb ExecuteN1qlQuery whilst providing a sync gateway imposed
// concurrent query limit
func (bucket *CouchbaseBucketGoCB) executeN1qlQuery(query *gocb.N1qlQuery, params interface{}) (gocb.QueryResults, error) {
	bucket.waitForAvailViewOp()
	defer bucket.releaseViewOp()

	return bucket.ExecuteN1qlQuery(query, params)
}

// CreateIndex creates the specified index in the current bucket using on the specified index expression.
func (bucket *CouchbaseBucketGoCB) CreatePrimaryIndex(indexName string, options *N1qlIndexOptions) error {

	createStatement := fmt.Sprintf("CREATE PRIMARY INDEX `%s` ON `%s`", indexName, bucket.GetName())
	return bucket.createIndex(indexName, createStatement, options)
}

func (bucket *CouchbaseBucketGoCB) createIndex(indexName string, createStatement string, options *N1qlIndexOptions) error {

	if options != nil {
		withClause, marshalErr := JSONMarshal(options)
		if marshalErr != nil {
			return marshalErr
		}
		createStatement = fmt.Sprintf(`%s with %s`, createStatement, withClause)
	}

	Debugf(KeyQuery, "Attempting to create index using statement: [%s]", UD(createStatement))
	n1qlQuery := gocb.NewN1qlQuery(createStatement)
	results, err := bucket.executeN1qlQuery(n1qlQuery, nil)
	if err != nil && !IsIndexerRetryIndexError(err) {
		return pkgerrors.WithStack(RedactErrorf("Error creating index with statement: %s.  Error: %v", UD(createStatement), err))
	}

	if IsIndexerRetryIndexError(err) {
		Infof(KeyQuery, "Indexer error creating index - waiting for server background retry.  Error:%v", err)
		// Wait for bucket to be created in background before returning
		return bucket.waitForBucketExistence(indexName, true)
	}

	closeErr := results.Close()
	if closeErr != nil {
		return closeErr
	}

	return nil
}

// Waits for index state to be online.  Waits no longer than provided timeout
func (bucket *CouchbaseBucketGoCB) WaitForIndexOnline(indexName string) error {

	worker := func() (shouldRetry bool, err error, value interface{}) {
		exists, indexMeta, getMetaErr := bucket.GetIndexMeta(indexName)
		if exists && indexMeta.State == IndexStateOnline {
			return false, nil, nil
		}
		return true, getMetaErr, nil
	}

	// Kick off retry loop
	err, _ := RetryLoop("WaitForIndexOnline", worker, CreateMaxDoublingSleeperFunc(25, 100, 15000))
	if err != nil {
		return pkgerrors.Wrapf(err, "WaitForIndexOnline for index %s", MD(indexName).Redact())
	}

	return nil
}

// Waits for bucket to exist/not exist.  Used in response to background create/drop processing by server.
func (bucket *CouchbaseBucketGoCB) waitForBucketExistence(indexName string, shouldExist bool) error {

	worker := func() (shouldRetry bool, err error, value interface{}) {
		// GetIndexMeta has its own error retry handling,
		// but keep the retry logic up here for checking if the index exists.
		exists, _, err := bucket.GetIndexMeta(indexName)
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
	err, _ := RetryLoop("waitForBucketExistence", worker, CreateMaxDoublingSleeperFunc(25, 100, 15000))
	if err != nil {
		return pkgerrors.Wrapf(err, "Error during waitForBucketExistence for index %s", indexName)
	}

	return nil
}

type getIndexMetaRetryValues struct {
	exists bool
	meta   *gocb.IndexInfo
}

func (bucket *CouchbaseBucketGoCB) GetIndexMeta(indexName string) (exists bool, meta *gocb.IndexInfo, err error) {

	worker := func() (shouldRetry bool, err error, value interface{}) {
		exists, meta, err := bucket.getIndexMetaWithoutRetry(indexName)
		if err != nil {
			// retry
			Warnf("Error from GetIndexMeta: %v will retry", err)
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

func (bucket *CouchbaseBucketGoCB) getIndexMetaWithoutRetry(indexName string) (exists bool, meta *gocb.IndexInfo, err error) {
	statement := fmt.Sprintf("SELECT state from system:indexes WHERE indexes.name = '%s' AND indexes.keyspace_id = '%s'", indexName, bucket.GetName())
	n1qlQuery := gocb.NewN1qlQuery(statement)
	results, err := bucket.executeN1qlQuery(n1qlQuery, nil)
	if err != nil {
		return false, nil, err
	}

	indexInfo := &gocb.IndexInfo{}
	err = results.One(indexInfo)
	if err != nil {
		if err == gocb.ErrNoResults {
			return false, nil, nil
		} else {
			return true, nil, err
		}
	}
	return true, indexInfo, nil
}

// CreateIndex drops the specified index from the current bucket.
func (bucket *CouchbaseBucketGoCB) DropIndex(indexName string) error {
	statement := fmt.Sprintf("DROP INDEX `%s`.`%s`", bucket.GetName(), indexName)
	n1qlQuery := gocb.NewN1qlQuery(statement)

	results, err := bucket.executeN1qlQuery(n1qlQuery, nil)
	if err != nil && !IsIndexerRetryIndexError(err) {
		return err
	}

	if IsIndexerRetryIndexError(err) {
		Infof(KeyQuery, "Indexer error dropping index - waiting for server background retry.  Error:%v", err)
		// Wait for bucket to be dropped in background before returning
		return bucket.waitForBucketExistence(indexName, false)
	}

	closeErr := results.Close()
	if closeErr != nil {
		return closeErr
	}
	return err
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
//	https://issues.couchbase.com/browse/MB-19358 is filed to request improved indexer error codes for these scenarios (and others)
func IsIndexerRetryIndexError(err error) bool {
	if err == nil {
		return false
	}
	if strings.Contains(err.Error(), "will retry") || strings.Contains(err.Error(), "will be retried") {
		return true
	}
	return false
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

func SlowQueryLog(startTime time.Time, threshold time.Duration, messageFormat string, args ...interface{}) {
	if elapsed := time.Now().Sub(startTime); elapsed > threshold {
		Infof(KeyQuery, messageFormat+" took "+elapsed.String(), args...)
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
