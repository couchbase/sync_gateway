package db

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/couchbase/gocb"
	"github.com/couchbase/sync_gateway/base"
	pkgerrors "github.com/pkg/errors"
)

const (
	indexNameFormat = "sg_%s_%s%d" // Name, xattrs, version.  e.g. "sg_channels_x1"
	syncToken       = "$sync"      // Sync token, used to swap between xattr/non-xattr handling in n1ql statements

	// N1ql-encoded wildcard expression matching the '_sync:' prefix used for all sync gateway's system documents.
	// Need to escape the underscore in '_sync' to prevent it being treated as a N1QL wildcard
	SyncDocWildcard = `\\_sync:%`
)

// Index and query definitions use syncToken ($sync) to represent the location of sync gateway's metadata.
// When running with xattrs, that gets replaced with META().xattrs._sync (or META(bucketname).xattrs._sync for query).
// When running w/out xattrs, it's just replaced by the doc path `bucketname`._sync
// This gets replaced before the statement is sent to N1QL by the replaceSyncTokens methods.
var syncNoXattr = fmt.Sprintf("`%s`._sync", base.BucketQueryToken)
var syncXattr = "meta().xattrs._sync"
var syncXattrQuery = fmt.Sprintf("meta(`%s`).xattrs._sync", base.BucketQueryToken) // Replacement for $sync token for xattr queries

type SGIndexType int

const (
	IndexAccess SGIndexType = iota
	IndexRoleAccess
	IndexChannels
	IndexAllDocs
	IndexTombstones
	IndexSyncDocs
	indexTypeCount // Used for iteration
)

type SGIndexFlags uint8

const (
	IdxFlagXattrOnly       = SGIndexFlags(1 << iota) // Index should only be created when running w/ xattrs=true
	IdxFlagIndexTombstones                           // When xattrs=true, index should be created with {“retain_deleted_xattr”:true} in order to index tombstones
)

var (
	// Simple index names - input to indexNameFormat
	indexNames = map[SGIndexType]string{
		IndexAccess:     "access",
		IndexRoleAccess: "roleAccess",
		IndexChannels:   "channels",
		IndexAllDocs:    "allDocs",
		IndexTombstones: "tombstones",
		IndexSyncDocs:   "syncDocs",
	}

	// Index versions - must be incremented when index definition changes
	indexVersions = map[SGIndexType]int{
		IndexAccess:     1,
		IndexRoleAccess: 1,
		IndexChannels:   1,
		IndexAllDocs:    1,
		IndexTombstones: 1,
		IndexSyncDocs:   1,
	}

	// Expressions used to create index.
	// See https://issues.couchbase.com/browse/MB-28728 for details on IFMISSING handling in IndexChannels
	indexExpressions = map[SGIndexType]string{
		IndexAccess:     "ALL (ARRAY (op.name) FOR op IN OBJECT_PAIRS($sync.access) END)",
		IndexRoleAccess: "ALL (ARRAY (op.name) FOR op IN OBJECT_PAIRS($sync.role_access) END)",
		IndexChannels: "ALL (ARRAY [op.name, LEAST($sync.sequence,op.val.seq), IFMISSING(op.val.rev,null), IFMISSING(op.val.del,null)] FOR op IN OBJECT_PAIRS($sync.channels) END), " +
			"$sync.rev, $sync.sequence, $sync.flags",
		IndexAllDocs:    "$sync.sequence, $sync.rev, $sync.flags, $sync.deleted",
		IndexTombstones: "$sync.tombstoned_at",
		IndexSyncDocs:   "META().id",
	}

	indexFilterExpressions = map[SGIndexType]string{
		IndexAllDocs:  fmt.Sprintf("META().id NOT LIKE '%s'", SyncDocWildcard),
		IndexSyncDocs: fmt.Sprintf("META().id LIKE '%s'", SyncDocWildcard),
	}

	// Index flags - used to identify any custom handling
	indexFlags = map[SGIndexType]SGIndexFlags{
		IndexAccess:     IdxFlagIndexTombstones,
		IndexRoleAccess: IdxFlagIndexTombstones,
		IndexChannels:   IdxFlagIndexTombstones,
		IndexAllDocs:    IdxFlagIndexTombstones,
		IndexTombstones: IdxFlagXattrOnly | IdxFlagIndexTombstones,
	}

	// Queries used to check readiness on startup.  Only required for critical indexes.
	readinessQueries = map[SGIndexType]string{
		IndexAccess: "SELECT $sync.access.foo as val " +
			"FROM `%s` " +
			"WHERE ANY op in OBJECT_PAIRS($sync.access) SATISFIES op.name = 'foo' end " +
			"LIMIT 1",
		IndexRoleAccess: "SELECT $sync.role_access.foo as val " +
			"FROM `%s` " +
			"WHERE ANY op in OBJECT_PAIRS($sync.role_access) SATISFIES op.name = 'foo' end " +
			"LIMIT 1",
		IndexChannels: "SELECT  [op.name, LEAST($sync.sequence, op.val.seq),IFMISSING(op.val.rev,null), IFMISSING(op.val.del,null)][1] AS sequence " +
			"FROM `%s` " +
			"UNNEST OBJECT_PAIRS($sync.channels) AS op " +
			"WHERE [op.name, LEAST($sync.sequence, op.val.seq),IFMISSING(op.val.rev,null), IFMISSING(op.val.del,null)]  BETWEEN  ['foo', 0] AND ['foo', 1] " +
			"ORDER BY sequence " +
			"LIMIT 1",
	}
)

var sgIndexes map[SGIndexType]SGIndex

// Initialize index definitions
func init() {
	sgIndexes = make(map[SGIndexType]SGIndex, indexTypeCount)
	for i := SGIndexType(0); i < indexTypeCount; i++ {
		sgIndex := SGIndex{
			simpleName:       indexNames[i],
			version:          indexVersions[i],
			expression:       indexExpressions[i],
			filterExpression: indexFilterExpressions[i],
			flags:            indexFlags[i],
		}
		// If a readiness query is specified for this index, mark the index as required and add to SGIndex
		readinessQuery, ok := readinessQueries[i]
		if ok {
			sgIndex.required = true
			sgIndex.readinessQuery = fmt.Sprintf(readinessQuery, base.BucketQueryToken)
		}

		sgIndexes[i] = sgIndex
	}
}

// SGIndex is used to manage the set of constants associated with each index definition
type SGIndex struct {
	simpleName       string       // Simplified index name (used to build fullIndexName)
	expression       string       // Expression used to create index
	filterExpression string       // (Optional) Filter expression used to create index
	version          int          // Index version.  Must be incremented any time the index definition changes
	previousVersions []int        // Previous versions of the index that will be removed during post_upgrade cleanup
	required         bool         // Whether SG blocks on startup until this index is ready
	readinessQuery   string       // Query used to determine view readiness
	flags            SGIndexFlags // Additional index options
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

// Tombstone indexing is required for indexes that need to index the _sync xattrs even when the document
// body has been deleted (i.e. SG tombstones)
func (i *SGIndex) shouldIndexTombstones(useXattrs bool) bool {
	return (i.flags&IdxFlagIndexTombstones != 0 && useXattrs)
}

func (i *SGIndex) isXattrOnly() bool {
	return i.flags&IdxFlagXattrOnly != 0
}

// Creates index associated with specified SGIndex if not already present.  Always defers build - a subsequent BUILD INDEX
// will need to be invoked for any created indexes.
func (i *SGIndex) createIfNeeded(bucket *base.CouchbaseBucketGoCB, useXattrs bool, numReplica uint) (isDeferred bool, err error) {

	if i.isXattrOnly() && !useXattrs {
		return false, nil
	}

	indexName := i.fullIndexName(useXattrs)

	exists, indexMeta, metaErr := bucket.GetIndexMeta(indexName)
	if metaErr != nil {
		return false, metaErr
	}

	// For already existing indexes, check whether they need to be built.
	if exists {
		if indexMeta == nil {
			return false, fmt.Errorf("No metadata retrieved for existing index %s", indexName)
		}
		if indexMeta.State == base.IndexStateDeferred {
			// Two possible scenarios when index already exists in deferred state:
			//  1. Another SG is in the process of index creation
			//  2. SG previously crashed between index creation and index build.
			// GSI doesn't like concurrent build requests, so wait and recheck index state before treating as option 2.
			// (see known issue documented https://developer.couchbase.com/documentation/server/current/n1ql/n1ql-language-reference/build-index.html)
			base.Infof(base.KeyQuery, "Index %s already in deferred state - waiting 10s to re-evaluate before issuing build to avoid concurrent build requests.", indexName)
			time.Sleep(10 * time.Second)
			exists, indexMeta, metaErr = bucket.GetIndexMeta(indexName)
			if metaErr != nil || indexMeta == nil {
				return false, fmt.Errorf("Error retrieving index metadata after defer wait. IndexMeta: %v Error:%v", indexMeta, metaErr)
			}
			if indexMeta.State == base.IndexStateDeferred {
				return true, nil
			}
		}
		return false, nil
	}

	// Create index
	base.Infof(base.KeyQuery, "Index %s doesn't exist, creating...", indexName)
	isDeferred = true
	indexExpression := replaceSyncTokensIndex(i.expression, useXattrs)
	filterExpression := replaceSyncTokensIndex(i.filterExpression, useXattrs)

	options := &base.N1qlIndexOptions{
		DeferBuild:      true,
		NumReplica:      numReplica,
		IndexTombstones: i.shouldIndexTombstones(useXattrs),
	}

	sleeper := base.CreateDoublingSleeperFunc(
		11, //MaxNumRetries approx 10 seconds total retry duration
		5,  //InitialRetrySleepTimeMS
	)

	//start a retry loop to create index,
	worker := func() (shouldRetry bool, err error, value interface{}) {
		err = bucket.CreateIndex(indexName, indexExpression, filterExpression, options)
		if err != nil {
			// If index has already been created (race w/ other SG node), return without error
			if err == base.ErrIndexAlreadyExists {
				isDeferred = false // Index already exists, don't need to update.
				return false, nil, nil
			}
			if strings.Contains(err.Error(), "not enough indexer nodes") {
				return false, fmt.Errorf("Unable to create indexes with the specified number of replicas (%d).  Increase the number of index nodes, or modify 'num_index_replicas' in your Sync Gateway database config.", numReplica), nil
			}
			base.Warnf(base.KeyAll, "Error creating index %s: %v - will retry.", indexName, err)
		}
		return err != nil, err, nil
	}

	description := fmt.Sprintf("Attempt to create index %s", indexName)
	err, _ = base.RetryLoop(description, worker, sleeper)

	if err != nil {
		return false, pkgerrors.Wrapf(err, "Error installing Couchbase index: %v", indexName)
	}

	base.Infof(base.KeyQuery, "Index %s created successfully", indexName)
	return isDeferred, nil
}

// Initializes Sync Gateway indexes for bucket.  Creates required indexes if not found, then waits for index readiness.
func InitializeIndexes(bucket base.Bucket, useXattrs bool, numReplicas uint) error {

	gocbBucket, ok := base.AsGoCBBucket(bucket)
	if !ok {
		base.Warnf(base.KeyAll, "Using a non-Couchbase bucket: %T - indexes will not be created.", bucket)
		return nil
	}

	if !gocbBucket.HasN1qlNodes() {
		return errors.New("No available nodes running the Query Service. Either add the Query Service to your Couchbase Server cluster or set `use_views` to true in your Sync Gateway config")
	}

	base.Infof(base.KeyAll, "Initializing indexes with numReplicas: %d...", numReplicas)

	// Create any indexes that aren't present
	deferredIndexes := make([]string, 0)
	allSGIndexes := make([]string, 0)
	for _, sgIndex := range sgIndexes {
		fullIndexName := sgIndex.fullIndexName(useXattrs)
		isDeferred, err := sgIndex.createIfNeeded(gocbBucket, useXattrs, numReplicas)
		if err != nil {
			return base.RedactErrorf("Unable to install index %s: %v", base.MD(sgIndex.simpleName), err)
		}

		if isDeferred {
			deferredIndexes = append(deferredIndexes, fullIndexName)
		}
		allSGIndexes = append(allSGIndexes, fullIndexName)
	}

	// Issue BUILD INDEX for any deferred indexes.
	if len(deferredIndexes) > 0 {
		buildErr := gocbBucket.BuildDeferredIndexes(deferredIndexes)
		if buildErr != nil {
			base.Infof(base.KeyQuery, "Error building deferred indexes.  Error: %v", buildErr)
			return buildErr
		}
	}

	// Wait for newly built indexes to be online
	for _, indexName := range deferredIndexes {
		gocbBucket.WaitForIndexOnline(indexName)
	}

	// Wait for initial readiness queries to complete
	return waitForIndexes(gocbBucket, useXattrs)
}

// Issue a consistency=request_plus query against critical indexes to guarantee indexing is complete and indexes are ready.
func waitForIndexes(bucket *base.CouchbaseBucketGoCB, useXattrs bool) error {
	var indexesWg sync.WaitGroup
	base.Infof(base.KeyAll, "Verifying index availability for bucket %s...", base.MD(bucket.GetName()))
	indexErrors := make(chan error, len(sgIndexes))

	for _, sgIndex := range sgIndexes {
		if sgIndex.required {
			indexesWg.Add(1)
			go func(index SGIndex) {
				defer indexesWg.Done()
				base.Debugf(base.KeyQuery, "Verifying index availability for index %s...", base.MD(index.fullIndexName(useXattrs)))
				queryStatement := replaceSyncTokensQuery(index.readinessQuery, useXattrs)
				queryErr := waitForIndex(bucket, index.fullIndexName(useXattrs), queryStatement)
				if queryErr != nil {
					base.Warnf(base.KeyAll, "Query error for statement [%s], err:%v", queryStatement, queryErr)
					indexErrors <- queryErr
				}
				base.Debugf(base.KeyQuery, "Index %s verified as ready", base.MD(index.fullIndexName(useXattrs)))
			}(sgIndex)
		}
	}

	indexesWg.Wait()
	if len(indexErrors) > 0 {
		err := <-indexErrors
		close(indexErrors)
		return err
	}

	base.Infof(base.KeyAll, "Indexes ready for bucket %s.", base.MD(bucket.GetName()))
	return nil
}

// Issues adhoc consistency=request_plus query to determine if specified is ready.  Retries indefinitely on timeout, backoff retry on indexer error.
func waitForIndex(bucket *base.CouchbaseBucketGoCB, indexName string, queryStatement string) error {

	for {
		_, err := bucket.Query(queryStatement, nil, gocb.RequestPlus, true)
		// Retry on timeout error, otherwise return
		if err == nil {
			return nil
		}
		if err == base.ErrViewTimeoutError {
			base.Infof(base.KeyAll, "Timeout waiting for index %q to be ready for bucket %q - retrying...", base.MD(indexName), base.MD(bucket.GetName()))
		} else {
			return err
		}
	}

}

// Iterates over the index set, removing obsolete indexes:
//  - indexes based on the inverse value of xattrs being used by the database
//  - indexes associated with previous versions of the index, for either xattrs=true or xattrs=false
func removeObsoleteIndexes(bucket base.Bucket, previewOnly bool, useXattrs bool) (removedIndexes []string, err error) {

	gocbBucket, ok := base.AsGoCBBucket(bucket)
	if !ok {
		base.Warnf(base.KeyAll, "Cannot remove obsolete indexes for non-gocb bucket - skipping.")
		return
	}

	// Build set of candidates for cleanup
	removalCandidates := make([]string, 0)
	for _, sgIndex := range sgIndexes {
		// Current version, opposite xattr setting
		removalCandidates = append(removalCandidates, sgIndex.fullIndexName(!useXattrs))
		// Older versions, both xattr and non-xattr
		for _, prevVersion := range sgIndex.previousVersions {
			removalCandidates = append(removalCandidates, sgIndex.indexNameForVersion(prevVersion, true))
			removalCandidates = append(removalCandidates, sgIndex.indexNameForVersion(prevVersion, false))
		}
	}

	// Attempt removal of candidates, adding to set of removedIndexes when found
	removedIndexes = make([]string, 0)
	for _, indexName := range removalCandidates {
		removed, removeError := removeObsoleteIndex(gocbBucket, indexName, previewOnly)
		if removeError != nil {
			return removedIndexes, removeError
		}
		if removed {
			removedIndexes = append(removedIndexes, indexName)
		}
	}

	return removedIndexes, nil
}

// Removes an obsolete index from the database.  In preview mode, checks for existence of the index only.
func removeObsoleteIndex(bucket *base.CouchbaseBucketGoCB, indexName string, previewOnly bool) (removed bool, err error) {

	if previewOnly {
		// Check for index existence
		exists, _, getMetaErr := bucket.GetIndexMeta(indexName)
		if getMetaErr != nil {
			return false, getMetaErr
		}
		return exists, nil
	} else {
		err = bucket.DropIndex(indexName)
		// If no error, add to set of removed indexes and return
		if err == nil {
			return true, nil
		}
		// If not found, no action required
		if base.IsIndexNotFoundError(err) {
			return false, nil
		}
		// Unrecoverable error
		return false, err
	}

}

// Replace sync tokens ($sync) in the provided createIndex statement with the appropriate token, depending on whether xattrs should be used.
func replaceSyncTokensIndex(statement string, useXattrs bool) string {
	if useXattrs {
		return strings.Replace(statement, syncToken, syncXattr, -1)
	} else {
		return strings.Replace(statement, syncToken, syncNoXattr, -1)
	}
}

// Replace sync tokens ($sync) in the provided createIndex statement with the appropriate token, depending on whether xattrs should be used.
func replaceSyncTokensQuery(statement string, useXattrs bool) string {
	if useXattrs {
		return strings.Replace(statement, syncToken, syncXattrQuery, -1)
	} else {
		return strings.Replace(statement, syncToken, syncNoXattr, -1)
	}
}
