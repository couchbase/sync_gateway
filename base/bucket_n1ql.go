package base

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/couchbase/gocb"
	pkgerrors "github.com/pkg/errors"
)

const BucketQueryToken = "$_bucket"   // Token used for bucket name replacement in query statements
const MaxQueryRetries = 30            // Maximum query retries on indexer error
const IndexStateOnline = "online"     // bucket state value, as returned by SELECT FROM system:indexes
const IndexStateDeferred = "deferred" // bucket state value, as returned by SELECT FROM system:indexes

var SlowQueryWarningThreshold time.Duration

// IndexOptions used to build the 'with' clause
type N1qlIndexOptions struct {
	NumReplica      uint `json:"num_replica,omitempty"`          // Number of replicas
	IndexTombstones bool `json:"retain_deleted_xattr,omitempty"` // Whether system xattrs on tombstones should be indexed
	DeferBuild      bool `json:"defer_build,omitempty"`          // Whether to defer initial build of index (requires a subsequent BUILD INDEX invocation)
}

type IndexMetadata struct {
	State string // Index state (e.g. 'online')
}

// Query accepts a parameterized statement,  optional list of params, and an optional flag to force adhoc query execution.
// Params specified using the $param notation in the statement are intended to be used w/ N1QL prepared statements, and will be
// passed through as params to n1ql.  e.g.:
//   SELECT _sync.sequence FROM $_bucket WHERE _sync.sequence > $minSeq
// https://developer.couchbase.com/documentation/server/current/sdk/go/n1ql-queries-with-sdk.html for additional details.
// Will additionally replace all instances of BucketQueryToken($_bucket) in the statement
// with the bucket name.  'bucket' should not be included in params.
//
// If adhoc=true, prepared statement handling will be disabled.  Should only be set to true for queries that can't be prepared, e.g.:
//  SELECT _sync.channels.ABC.seq from $bucket
//
// Query retries on Indexer Errors, as these are normally transient
func (bucket *CouchbaseBucketGoCB) Query(statement string, params interface{}, consistency gocb.ConsistencyMode, adhoc bool) (results gocb.QueryResults, err error) {
	bucketStatement := strings.Replace(statement, BucketQueryToken, bucket.GetName(), -1)
	n1qlQuery := gocb.NewN1qlQuery(bucketStatement)
	n1qlQuery = n1qlQuery.AdHoc(adhoc)
	n1qlQuery = n1qlQuery.Consistency(consistency)

	waitTime := 10 * time.Millisecond
	for i := 1; i <= MaxQueryRetries; i++ {
		queryResults, queryErr := bucket.ExecuteN1qlQuery(n1qlQuery, params)
		if queryErr == nil {
			return queryResults, queryErr
		}

		// Timeout error - return named error
		if isGoCBTimeoutError(queryErr) {
			return queryResults, ErrViewTimeoutError
		}

		// Non-retry error - return
		if !isIndexerError(queryErr) {
			Warnf(KeyAll, "Error when querying index using statement: [%s]", UD(bucketStatement))
			return queryResults, queryErr
		}

		// Indexer error - wait then retry
		err = queryErr
		Warnf(KeyAll, "Indexer error during query - retry %d/%d", i, MaxQueryRetries)
		time.Sleep(waitTime)
		waitTime = time.Duration(waitTime * 2)
	}

	Warnf(KeyAll, "Exceeded max retries for query when querying index using statement: [%s], err:%v", UD(bucketStatement), err)
	return nil, err
}

// CreateIndex issues a CREATE INDEX query in the current bucket, using the form:
//   CREATE INDEX indexName ON bucket.Name(expression) WHERE filterExpression WITH options
// Sample usage with resulting statement:
//     CreateIndex("myIndex", "field1, field2, nested.field", "field1 > 0", N1qlIndexOptions{numReplica:1})
//   CREATE INDEX myIndex on myBucket(field1, field2, nested.field) WHERE field1 > 0 WITH {"numReplica":1}
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
		return ErrIndexAlreadyExists
	}
	return createErr
}

// BuildIndexes executes a BUILD INDEX statement in the current bucket, using the form:
//   BUILD INDEX ON `bucket.Name`(`index1`, `index2`, ...)
func (bucket *CouchbaseBucketGoCB) BuildIndexes(indexNames []string) error {

	if len(indexNames) == 0 {
		return nil
	}

	// Not using strings.Join because we want to escape each index name
	indexNameList := fmt.Sprintf("`%s`", indexNames[0])
	for i := 1; i < len(indexNames); i++ {
		indexNameList = fmt.Sprintf("%s,`%s`", indexNameList, indexNames[i])
	}

	buildStatement := fmt.Sprintf("BUILD INDEX ON `%s`(%s)", bucket.GetName(), indexNameList)
	n1qlQuery := gocb.NewN1qlQuery(buildStatement)
	_, err := bucket.ExecuteN1qlQuery(n1qlQuery, nil)

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

// CreateIndex creates the specified index in the current bucket using on the specified index expression.
func (bucket *CouchbaseBucketGoCB) CreatePrimaryIndex(indexName string, options *N1qlIndexOptions) error {

	createStatement := fmt.Sprintf("CREATE PRIMARY INDEX `%s` ON `%s`", indexName, bucket.GetName())
	return bucket.createIndex(indexName, createStatement, options)
}

func (bucket *CouchbaseBucketGoCB) createIndex(indexName string, createStatement string, options *N1qlIndexOptions) error {

	if options != nil {
		withClause, marshalErr := json.Marshal(options)
		if marshalErr != nil {
			return marshalErr
		}
		createStatement = fmt.Sprintf(`%s with %s`, createStatement, withClause)
	}

	Debugf(KeyIndex, "Attempting to create index using statement: [%s]", UD(createStatement))
	n1qlQuery := gocb.NewN1qlQuery(createStatement)
	results, err := bucket.ExecuteN1qlQuery(n1qlQuery, nil)
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
	description := fmt.Sprintf("GetIndexMeta for index %s", indexName)
	err, _ := RetryLoop(description, worker, CreateMaxDoublingSleeperFunc(25, 100, 15000))

	return err
}

// Waits for bucket to exist/not exist.  Used in response to background create/drop processing by server.
func (bucket *CouchbaseBucketGoCB) waitForBucketExistence(indexName string, shouldExist bool) error {

	worker := func() (shouldRetry bool, err error, value interface{}) {
		exists, _, getMetaErr := bucket.GetIndexMeta(indexName)
		if getMetaErr != nil {
			return false, getMetaErr, nil
		}
		// If it's in the desired state, we're done
		if exists == shouldExist {
			return false, nil, nil
		}
		// Retry
		return true, nil, nil
	}

	// Kick off retry loop
	description := fmt.Sprintf("GetIndexMeta for index %s", indexName)
	err, _ := RetryLoop(description, worker, CreateMaxDoublingSleeperFunc(25, 100, 15000))

	return err
}

func (bucket *CouchbaseBucketGoCB) GetIndexMeta(indexName string) (exists bool, meta *IndexMetadata, err error) {
	statement := fmt.Sprintf("SELECT state from system:indexes WHERE indexes.name = '%s' AND indexes.keyspace_id = '%s'", indexName, bucket.GetName())
	n1qlQuery := gocb.NewN1qlQuery(statement)
	results, err := bucket.ExecuteN1qlQuery(n1qlQuery, nil)
	if err != nil {
		return false, nil, err
	}

	indexMeta := &IndexMetadata{}
	err = results.One(indexMeta)
	if err != nil {
		if err == gocb.ErrNoResults {
			return false, nil, nil
		} else {
			return true, nil, err
		}
	}
	return true, indexMeta, nil
}

// CreateIndex drops the specified index from the current bucket.
func (bucket *CouchbaseBucketGoCB) DropIndex(indexName string) error {
	statement := fmt.Sprintf("DROP INDEX `%s`.`%s`", bucket.GetName(), indexName)
	n1qlQuery := gocb.NewN1qlQuery(statement)

	results, err := bucket.ExecuteN1qlQuery(n1qlQuery, nil)
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
//   [5000] GSI index testIndex_not_found not found.
// Stuck with doing a string compare to differentiate between 'not found' and other errors
func IsIndexNotFoundError(err error) bool {
	return strings.Contains(err.Error(), "not found")
}

// 'IsIndexerRetry' type errors are of the form:
// error:[5000] GSI CreateIndex() - cause: Encountered transient error.  Index creation will be retried in background.  Error: Index testIndex_value will retry building in the background for reason: Bucket test_data_bucket In Recovery.
// error:[5000] GSI Drop() - cause: Fail to drop index on some indexer nodes.  Error=Encountered error when dropping index: Indexer In Recovery. Drop index will be retried in background.
// error:[5000] BuildIndexes - cause: Build index fails.  %vIndex testIndexDeferred will retry building in the background for reason: Build Already In Progress. Bucket test_data_bucket.
//  https://issues.couchbase.com/browse/MB-19358 is filed to request improved indexer error codes for these scenarios (and others)
func IsIndexerRetryIndexError(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), "will be retried in background")
}

func IsIndexerRetryBuildError(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), "will retry building in the background")
}

// Check for transient indexer errors (can be retried)
func isIndexerError(err error) bool {

	if err == nil {
		return false
	}

	if strings.Contains(err.Error(), "Indexer rollback") {
		return true
	}

	return false
}

func QueryCloseErrors(closeError error) []error {

	if closeError == nil {
		return nil
	}

	closeErrors := make([]error, 0)
	switch v := closeError.(type) {
	case *gocb.MultiError:
		for _, multiErr := range v.Errors {
			closeErrors = append(closeErrors, multiErr)
		}
	default:
		closeErrors = append(closeErrors, v)
	}

	return closeErrors

}

func SlowQueryLog(startTime time.Time, messageFormat string, args ...interface{}) {
	if elapsed := time.Now().Sub(startTime); elapsed > SlowQueryWarningThreshold {
		Infof(KeyQuery, messageFormat+" took "+elapsed.String(), args...)
	}
}
