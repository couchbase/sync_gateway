package base

import (
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/couchbase/gocb"
	pkgerrors "github.com/pkg/errors"
)

func (c *Collection) Query(statement string, params interface{}, consistency uint, adhoc bool) (results ClusterQueryIterator, err error) {

	bucketStatement := strings.Replace(statement, BucketQueryToken, c.Bucket().Name(), -1)

	n1qlOptions := &gocb.QueryOptions{
		ScanConsistency: gocb.QueryScanConsistency(consistency),
		Adhoc:           adhoc,
	}

	waitTime := 10 * time.Millisecond
	for i := 1; i <= MaxQueryRetries; i++ {
		Tracef(KeyQuery, "Executing N1QL query: %v", UD(bucketStatement))
		queryResults, queryErr := c.cluster.Query(bucketStatement, n1qlOptions)

		resultsIterator := ClusterQueryIterator{result: queryResults}
		if queryErr == nil {
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

		waitTime = time.Duration(waitTime * 2)
	}

	Warnf("Exceeded max retries for query when querying index using statement: [%s], err:%v", UD(bucketStatement), err)
	return ClusterQueryIterator{}, err
}

type ClusterQueryIterator struct {
	result *gocb.QueryResult
}

func (i *ClusterQueryIterator) One(valuePtr interface{}) error {
	return i.result.One(valuePtr)
}

func (i *ClusterQueryIterator) Next(valuePtr interface{}) bool {
	ok := i.result.Next()
	if !ok {
		return false
	}
	err := i.result.Row(valuePtr)
	if err != nil {
		Infof(KeyQuery, "Error getting Next from query result: %v", err)
	}
	return err == nil
}

func (i *ClusterQueryIterator) NextBytes() []byte {
	ok := i.result.Next()
	if !ok {
		return nil
	}
	var rawResult json.RawMessage
	err := i.result.Row(&rawResult)
	if err != nil {
		Infof(KeyQuery, "Error getting NextBytes from query result: %v", err)
	}
	return rawResult
}

func (i *ClusterQueryIterator) Close() error {
	return i.result.Close()
}
