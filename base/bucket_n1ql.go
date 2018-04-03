package base

import (
	"fmt"
	"strings"

	"github.com/couchbase/gocb"
)

const BucketQueryToken = "$_bucket" // Token used for bucket name replacement in query statements

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
func (bucket *CouchbaseBucketGoCB) Query(statement string, params interface{}, consistency gocb.ConsistencyMode, adhoc bool) (gocb.QueryResults, error) {
	bucketStatement := strings.Replace(statement, BucketQueryToken, bucket.GetName(), -1)
	n1qlQuery := gocb.NewN1qlQuery(bucketStatement)
	n1qlQuery = n1qlQuery.AdHoc(adhoc)
	n1qlQuery = n1qlQuery.Consistency(consistency)

	LogTo("Index+", "Attempting to query index using statement: [%s]", bucketStatement)
	results, err := bucket.ExecuteN1qlQuery(n1qlQuery, params)

	if isGoCBTimeoutError(err) {
		return results, ErrViewTimeoutError
	}

	return results, err
}

// CreateIndex creates the specified index in the current bucket using on the specified index expression.
func (bucket *CouchbaseBucketGoCB) CreateIndex(indexName string, expression string, numReplica uint) error {

	createStatement := fmt.Sprintf("CREATE INDEX %s ON %s(%s)", indexName, bucket.GetName(), expression)

	if numReplica > 0 {
		createStatement = fmt.Sprintf("%s with {num_replica:%d}", createStatement, numReplica)
	}

	LogTo("Index+", "Attempting to create index using statement: [%s]", createStatement)
	n1qlQuery := gocb.NewN1qlQuery(createStatement)
	results, err := bucket.ExecuteN1qlQuery(n1qlQuery, nil)
	if err != nil {
		return err
	}

	closeErr := results.Close()
	if closeErr != nil {
		return closeErr
	}

	return nil
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
	statement := fmt.Sprintf("DROP INDEX %s.%s", bucket.GetName(), indexName)
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
