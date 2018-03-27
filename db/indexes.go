package db

import (
	"fmt"
	"strings"
	"sync"

	"github.com/couchbase/gocb"
	"github.com/couchbase/sync_gateway/base"
	pkgerrors "github.com/pkg/errors"
)

const (
	indexNameFormat = "sg_%s_%s%d" // Name, xattrs, version.  e.g. "sg_channels_x1"
	syncToken       = "$sync"      // Sync token, used for xattr/non-xattr handling in n1ql statements
	syncNoXattr     = "_sync"      // Replacement for $sync token for non-xattr indexes
)

var syncXattr = fmt.Sprintf("meta(%s).xattrs._sync", base.BucketQueryToken) // Replacement for $sync token for xattr indexes

type SGIndexType int

const (
	IndexAccess SGIndexType = iota
	IndexChannels
	indexTypeCount // Used for iteration
)

var (
	// Simple index names - input to indexNameFormat
	indexNames = map[SGIndexType]string{
		IndexAccess:   "access",
		IndexChannels: "channels",
	}

	// Index versions - must be incremented when index definition changes
	indexVersions = map[SGIndexType]int{
		IndexAccess:   1,
		IndexChannels: 1,
	}

	// Statements used to create index
	createStatements = map[SGIndexType]string{
		IndexAccess:   "ALL (ARRAY (op.name) FOR op IN OBJECT_PAIRS($sync.access) END)",
		IndexChannels: "ALL (ARRAY [op.name, LEAST($sync.sequence,op.val.seq), op.val.seq] FOR op IN OBJECT_PAIRS($sync.channels) END), $sync.rev, $sync.sequence",
	}

	// Queries used to check readiness on startup.  Only required for critical indexes.
	readinessQueries = map[SGIndexType]string{
		IndexAccess: `SELECT $sync.access.foo as val 
		 			   FROM %s 
		 			   WHERE ANY op in OBJECT_PAIRS($sync.access) SATISFIES op.name = 'foo' end 
		 			   LIMIT 1`,
		IndexChannels: `SELECT  [op.name, LEAST($sync.sequence, op.val.seq),op.val.rev][1] AS sequence
		                 FROM %s
		                 UNNEST OBJECT_PAIRS($sync.channels) AS op
		                 WHERE [op.name, LEAST($sync.sequence, op.val.seq),op.val.rev]  BETWEEN  ["foo", 0] AND ["foo", 1]
		                 ORDER BY sequence
		                 LIMIT 1`,
	}
)

var sgIndexes map[SGIndexType]SGIndex

// Initialize index definitions
func init() {
	sgIndexes = make(map[SGIndexType]SGIndex, indexTypeCount)
	for i := SGIndexType(0); i < indexTypeCount; i++ {
		sgIndex := SGIndex{
			simpleName:      indexNames[i],
			version:         indexVersions[i],
			createStatement: createStatements[i],
		}
		// If a readiness query is specified for this index, mark the index as required and add to SGIndex
		readinessQuery, ok := readinessQueries[i]
		if ok {
			sgIndex.required = true
			sgIndex.readinessQuery = readinessQuery
		}

		sgIndexes[i] = sgIndex
	}
}

// SGIndex is used to manage the set of constants associated with each index definition
type SGIndex struct {
	simpleName       string // Simplified index name (used to build fullIndexName)
	createStatement  string // Statement used to create index
	version          int    // Index version.  Must be incremented any time the index definition changes
	previousVersions []int  // Previous versions of the index that will be removed during post_upgrade cleanup
	required         bool   // Whether SG blocks on startup until this index is ready
	readinessQuery   string // Query used to determine view readiness
}

func (i *SGIndex) fullIndexName(useXattrs bool) string {
	return i.indexNameForVersion(i.version, useXattrs)
}

func (i *SGIndex) indexNameForVersion(version int, useXattrs bool) string {
	xattrsToken := ""
	if useXattrs {
		xattrsToken = "x"
	}
	return fmt.Sprintf(indexNameFormat, i.simpleName, xattrsToken, version)
}

// Creates index associated with specified SGIndex if not already present
func (i *SGIndex) createIfNeeded(bucket *base.CouchbaseBucketGoCB, useXattrs bool, numReplica uint) error {

	indexName := i.fullIndexName(useXattrs)

	exists, _, metaErr := bucket.GetIndexMeta(indexName)
	if metaErr != nil {
		return metaErr
	}
	if exists {
		return nil
	}

	// Create index
	createStatement := replaceSyncTokens(i.createStatement, useXattrs)

	sleeper := base.CreateDoublingSleeperFunc(
		11, //MaxNumRetries approx 10 seconds total retry duration
		5,  //InitialRetrySleepTimeMS
	)

	//start a retry loop to create index,
	worker := func() (shouldRetry bool, err error, value interface{}) {
		err = bucket.CreateIndex(indexName, createStatement, numReplica)
		if err != nil {
			base.Warn("Error creating index %s: %v - will retry.", indexName, err)
		}
		return err != nil, err, nil
	}

	description := fmt.Sprintf("Attempt to create index %s", indexName)
	err, _ := base.RetryLoop(description, worker, sleeper)

	if err != nil {
		return pkgerrors.Wrapf(err, "Error installing Couchbase index: %v", indexName)
	}

	return nil

}

// Initializes Sync Gateway indexes for bucket.  Creates required indexes if not found, then waits for index readiness.
func InitializeIndexes(bucket base.Bucket, useXattrs bool, numReplicas uint, numHousekeepingReplicas uint) error {

	gocbBucket, ok := bucket.(*base.CouchbaseBucketGoCB)
	if !ok {
		base.Log("Using a non-Couchbase bucket - indexes will not be created.")
		return nil
	}

	for _, sgIndex := range sgIndexes {
		err := sgIndex.createIfNeeded(gocbBucket, useXattrs, numReplicas)
		if err != nil {
			return fmt.Errorf("Unable to install index %s: %v", sgIndex.simpleName, err)
		}
	}

	return waitForIndexes(gocbBucket, useXattrs)
}

// Issue a consistency=request_plus query against critical indexes to guarantee indexing is complete and indexes are ready.
func waitForIndexes(bucket *base.CouchbaseBucketGoCB, useXattrs bool) error {
	var indexesWg sync.WaitGroup
	base.Logf("Verifying index availability for bucket %s...", bucket.GetName())
	indexErrors := make(chan error, len(sgIndexes))

	for _, sgIndex := range sgIndexes {
		if sgIndex.required {
			indexesWg.Add(1)
			go func(index SGIndex) {
				defer indexesWg.Done()
				queryStatement := replaceSyncTokens(index.readinessQuery, useXattrs)
				queryErr := waitForIndex(bucket, index.fullIndexName(useXattrs), queryStatement)
				if queryErr != nil {
					indexErrors <- queryErr
				}
			}(sgIndex)
		}
	}

	indexesWg.Wait()
	if len(indexErrors) > 0 {
		err := <-indexErrors
		close(indexErrors)
		return err
	}

	base.Logf("Indexes ready for bucket %s.", bucket.GetName())
	return nil
}

// Issues adhoc consistency=request_plus query to determine if specified is ready.  Retries on timeout.
func waitForIndex(bucket *base.CouchbaseBucketGoCB, indexName string, queryStatement string) error {

	for {
		_, err := bucket.Query(queryStatement, nil, gocb.RequestPlus, true)
		// Retry on timeout error, otherwise return
		if err == nil || err != base.ErrViewTimeoutError {
			return err
		} else {
			base.Logf("Timeout waiting for index %q to be ready for bucket %q - retrying...", indexName, bucket.GetName())
		}
	}

}

// Iterates over the index set, removing obsolete indexes:
//  - indexes based on the inverse value of xattrs being used by the database
//  - indexes associated with previous versions of the index, for either xattrs=true or xattrs=false
func removeObsoleteIndexes(bucket base.Bucket, previewOnly bool, useXattrs bool) (removedIndexes []string, err error) {

	gocbBucket, ok := bucket.(*base.CouchbaseBucketGoCB)
	if !ok {
		base.Warn("Cannot remove obsolete indexes for non-gocb bucket.")
		return
	}

	removedIndexes = make([]string, 0)
	for _, sgIndex := range sgIndexes {
		// Remove current version, opposite xattr setting, if present
		indexName := sgIndex.fullIndexName(!useXattrs)
		removedIndexes, err = removeObsoleteIndex(gocbBucket, removedIndexes, indexName, previewOnly)
		if err != nil {
			return removedIndexes, err
		}

		// Remove older versions, both xattr and non-xattr, if present
		for _, prevVersion := range sgIndex.previousVersions {
			xattrIndexName := sgIndex.indexNameForVersion(prevVersion, true)
			removedIndexes, err = removeObsoleteIndex(gocbBucket, removedIndexes, xattrIndexName, previewOnly)
			if err != nil {
				return removedIndexes, err
			}
			nonXattrIndexName := sgIndex.indexNameForVersion(prevVersion, false)
			removedIndexes, err = removeObsoleteIndex(gocbBucket, removedIndexes, nonXattrIndexName, previewOnly)
			if err != nil {
				return removedIndexes, err
			}
		}
	}
	return removedIndexes, nil
}

// Removes an obsolete index from the database.  In preview mode, checks for existence of the index only.
func removeObsoleteIndex(bucket *base.CouchbaseBucketGoCB, removedIndexes []string, indexName string, previewOnly bool) (removedIndexesOut []string, err error) {

	if previewOnly {
		// Check for index existence
		exists, _, getMetaErr := bucket.GetIndexMeta(indexName)
		if getMetaErr != nil {
			return removedIndexes, getMetaErr
		}
		if exists {
			removedIndexes = append(removedIndexes, indexName)
		}
		return removedIndexes, nil
	} else {
		err = bucket.DropIndex(indexName)
		// If no error, add to set of removed indexes and return
		if err == nil {
			removedIndexes = append(removedIndexes, indexName)
			return removedIndexes, nil
		}
		// If not found, no action required
		if base.IsIndexNotFoundError(err) {
			return removedIndexes, nil
		}
		// Unrecoverable error
		return removedIndexes, err
	}

}

// Replace sync tokens ($sync) in the provided statement with the appropriate token, depending on whether xattrs should be used.
func replaceSyncTokens(statement string, useXattrs bool) string {
	if useXattrs {
		return strings.Replace(statement, syncToken, syncXattr, -1)
	} else {
		return strings.Replace(statement, syncToken, syncNoXattr, -1)
	}
}
