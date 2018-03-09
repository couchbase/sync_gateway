package base

import (
	"fmt"
	"strings"

	"github.com/couchbase/gocb"
)

const BucketQueryToken = "$_bucket" // Token used for bucket name replacement in query statements

func (bucket *CouchbaseBucketGoCB) N1QLQuery(statement string, prepared bool, consistency gocb.ConsistencyMode, params map[string]interface{}) (gocb.QueryResults, error) {

	n1qlQuery := gocb.NewN1qlQuery(statement)
	n1qlQuery.AdHoc(!prepared)
	n1qlQuery.Consistency(consistency)

	results, err := bucket.ExecuteN1qlQuery(n1qlQuery, params)

	if isGoCBTimeoutError(err) {
		return results, ErrViewTimeoutError
	}

	return results, err
}

func (bucket *CouchbaseBucketGoCB) CreateIndex(indexName string, expression string, numReplica uint) error {

	createStatement := fmt.Sprintf("CREATE INDEX %s ON %s(%s)", indexName, bucket.GetName(), expression)

	if numReplica > 0 {
		createStatement = fmt.Sprintf("%s with {num_replica:%d}", createStatement, numReplica)
	}

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
func (bucket *CouchbaseBucketGoCB) Query(statement string, params interface{}, adhoc bool) (gocb.QueryResults, error) {
	bucketStatement := strings.Replace(statement, BucketQueryToken, bucket.GetName(), -1)
	n1qlQuery := gocb.NewN1qlQuery(bucketStatement)
	n1qlQuery = n1qlQuery.AdHoc(adhoc)
	return bucket.ExecuteN1qlQuery(n1qlQuery, params)
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
