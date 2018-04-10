package base

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/couchbase/gocb"
	pkgerrors "github.com/pkg/errors"
)

const BucketQueryToken = "$_bucket" // Token used for bucket name replacement in query statements
const MaxQueryRetries = 30          // Maximum query retries on indexer error
const IndexStateOnline = "online"   // bucket state value, as returned by SELECT FROM system:indexes

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
			Warn("Error when querying index using statement: [%s]", bucketStatement)
			return queryResults, queryErr
		}

		// Indexer error - wait then retry
		err = queryErr
		Warn("Indexer error during query - retry %d/%d", i, MaxQueryRetries)
		time.Sleep(waitTime)
		waitTime = time.Duration(waitTime * 2)
	}

	Warn("Exceeded max retries for query when querying index using statement: [%s], err:%v", bucketStatement, err)
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

	return bucket.createIndex(createStatement, options)
}

// CreateIndex creates the specified index in the current bucket using on the specified index expression.
func (bucket *CouchbaseBucketGoCB) CreatePrimaryIndex(indexName string, options *N1qlIndexOptions) error {

	createStatement := fmt.Sprintf("CREATE PRIMARY INDEX %s ON %s", indexName, bucket.GetName())
	return bucket.createIndex(createStatement, options)
}

func (bucket *CouchbaseBucketGoCB) createIndex(createStatement string, options *N1qlIndexOptions) error {

	if options != nil {
		withClause, marshalErr := json.Marshal(options)
		if marshalErr != nil {
			return marshalErr
		}
		createStatement = fmt.Sprintf(`%s with %s`, createStatement, withClause)
	}

	LogTo("Index+", "Attempting to create index using statement: [%s]", createStatement)
	n1qlQuery := gocb.NewN1qlQuery(createStatement)
	results, err := bucket.ExecuteN1qlQuery(n1qlQuery, nil)
	if err != nil && !IsRecoverableCreateIndexError(err) {
		return pkgerrors.Wrapf(err, "Error creating index with statement: %s", createStatement)
	}

	if IsRecoverableCreateIndexError(err) {
		LogTo("Query", "Recoverable error creating index with statement: %s, error:%v", createStatement, err)
		return nil
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
	if err != nil {
		return err
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

// 'Bucket in Recovery' type errors are of the form:
// error:[5000] GSI CreateIndex() - cause: Encountered transient error.  Index creation will be retried in background.  Error: Index testIndex_value will retry building in the background for reason: Bucket test_data_bucket In Recovery.
func IsRecoverableCreateIndexError(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), "Index creation will be retried in background")
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
