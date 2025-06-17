/*
Copyright 2018-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package db

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/couchbase/sync_gateway/base"
	pkgerrors "github.com/pkg/errors"
)

const (
	indexNameFormat              = "sg_%s_%s%d"    // Name, xattrs, version.  e.g. "sg_channels_x1"
	partitionableIndexNameFormat = "%s_p%d"        // indexName, numPartitions
	syncRelativeToken            = "$relativesync" // Relative sync token (no keyspace), used to swap between xattr/non-xattr handling in n1ql statements
	syncToken                    = "$sync"         // Sync token, used to swap between xattr/non-xattr handling in n1ql statements
	indexToken                   = "$idx"          // Index token, used to hint which index should be used for the query

	// N1ql-encoded wildcard expression matching the '_sync:' prefix used for all sync gateway's system documents.
	// Need to escape the underscore in '_sync' to prevent it being treated as a N1QL wildcard
	SyncDocWildcard  = `\\_sync:%`
	SyncUserWildcard = `\\_sync:user:%`
	SyncRoleWildcard = `\\_sync:role:%`

	DefaultNumIndexPartitions uint32 = 1
)

// Index and query definitions use syncToken ($sync) to represent the location of sync gateway's metadata.
// When running with xattrs, that gets replaced with META().xattrs._sync (or META(bucketname).xattrs._sync for query).
// When running w/out xattrs, it's just replaced by the doc path `bucketname`._sync
// This gets replaced before the statement is sent to N1QL by the replaceSyncTokens methods.
var syncNoXattr = base.SyncPropertyName
var syncNoXattrQuery = fmt.Sprintf("%s.%s", base.KeyspaceQueryAlias, base.SyncPropertyName)
var syncXattr = "meta().xattrs." + base.SyncXattrName
var syncXattrQuery = fmt.Sprintf("meta(%s).xattrs.%s", base.KeyspaceQueryAlias, base.SyncXattrName) // Replacement for $sync token for xattr queries

type SGIndexType int

const (
	IndexAccess SGIndexType = iota
	IndexRoleAccess
	IndexChannels
	IndexAllDocs
	IndexTombstones
	IndexSyncDocs
	IndexUser
	IndexRole
	indexTypeCount // Used for iteration
)

type SGIndexFlags uint8

const (
	IdxFlagXattrOnly         = SGIndexFlags(1 << iota) // Index should only be created when running w/ xattrs=true
	IdxFlagIndexTombstones                             // When xattrs=true, index should be created with {“retain_deleted_xattr”:true} in order to index tombstones
	IdxFlagMetadataOnly                                // Index should only be created when running against metadata store
	IdxFlagPrincipalDocsOnly                           // Index necessary for principal docs
)

type indexCreationMode int

const (
	Always indexCreationMode = iota
	SeparatePrincipalIndexes
	LegacySyncDocsIndex
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
		IndexUser:       "users",
		IndexRole:       "roles",
	}

	// Index versions - must be incremented when index definition changes
	indexVersions = map[SGIndexType]int{
		IndexAccess:     1,
		IndexRoleAccess: 1,
		IndexChannels:   1,
		IndexAllDocs:    1,
		IndexTombstones: 1,
		IndexSyncDocs:   1,
		IndexUser:       1,
		IndexRole:       1,
	}

	// Previous index versions - must be appended to when index version changes
	indexPreviousVersions = map[SGIndexType][]int{
		IndexAccess:     {},
		IndexRoleAccess: {},
		IndexChannels:   {},
		IndexAllDocs:    {},
		IndexTombstones: {},
		IndexSyncDocs:   {},
		IndexUser:       {},
		IndexRole:       {},
	}

	// Expressions used to create index.
	// See https://issues.couchbase.com/browse/MB-28728 for details on IFMISSING handling in IndexChannels
	indexExpressions = map[SGIndexType]string{
		IndexAccess:     "ALL (ARRAY (op.name) FOR op IN OBJECT_PAIRS($sync.access) END)",
		IndexRoleAccess: "ALL (ARRAY (op.name) FOR op IN OBJECT_PAIRS($sync.role_access) END)",
		IndexChannels: "ALL (ARRAY [op.name, LEAST($sync.`sequence`,op.val.seq), IFMISSING(op.val.rev,null), IFMISSING(op.val.del,null)] FOR op IN OBJECT_PAIRS($sync.channels) END), " +
			"$sync.rev, $sync.`sequence`, $sync.flags",
		IndexAllDocs:    "$sync.`sequence`, $sync.rev, $sync.flags, $sync.deleted",
		IndexTombstones: "$sync.tombstoned_at",
		IndexSyncDocs:   "META().id",
		IndexUser:       "META().id, name, email, disabled",
		IndexRole:       "META().id, name, deleted",
	}

	indexFilterExpressions = map[SGIndexType]string{
		IndexAllDocs:  fmt.Sprintf("META().id NOT LIKE '%s'", SyncDocWildcard),
		IndexSyncDocs: fmt.Sprintf("META().id LIKE '%s'", SyncDocWildcard),
		IndexUser:     fmt.Sprintf("META().id LIKE '%s'", SyncUserWildcard),
		IndexRole:     fmt.Sprintf("META().id LIKE '%s'", SyncRoleWildcard),
	}

	// Index flags - used to identify any custom handling
	indexFlags = map[SGIndexType]SGIndexFlags{
		IndexAccess:     IdxFlagIndexTombstones,
		IndexRoleAccess: IdxFlagIndexTombstones,
		IndexChannels:   IdxFlagIndexTombstones,
		IndexAllDocs:    IdxFlagIndexTombstones,
		IndexSyncDocs:   IdxFlagMetadataOnly | IdxFlagPrincipalDocsOnly,
		IndexUser:       IdxFlagMetadataOnly | IdxFlagPrincipalDocsOnly,
		IndexRole:       IdxFlagMetadataOnly | IdxFlagPrincipalDocsOnly,
		IndexTombstones: IdxFlagXattrOnly | IdxFlagIndexTombstones,
	}

	// mode for when to create index
	indexCreationModes = map[SGIndexType]indexCreationMode{
		IndexAccess:     Always,
		IndexRoleAccess: Always,
		IndexChannels:   Always,
		IndexAllDocs:    Always,
		IndexTombstones: Always,
		IndexSyncDocs:   LegacySyncDocsIndex,
		IndexUser:       SeparatePrincipalIndexes,
		IndexRole:       SeparatePrincipalIndexes,
	}

	// Queries used to check readiness on startup.  Only required for critical indexes.
	readinessQueries = map[SGIndexType]string{
		IndexAccess: "SELECT $sync.access.foo as val " +
			"FROM %s AS %s " +
			"USE INDEX ($idx) " +
			"WHERE ANY op in OBJECT_PAIRS($relativesync.access) SATISFIES op.name = 'foo' end " +
			"LIMIT 1",
		IndexRoleAccess: "SELECT $sync.role_access.foo as val " +
			"FROM %s AS %s " +
			"USE INDEX ($idx) " +
			"WHERE ANY op in OBJECT_PAIRS($relativesync.role_access) SATISFIES op.name = 'foo' end " +
			"LIMIT 1",
		IndexChannels: "SELECT  [op.name, LEAST($sync.`sequence`, op.val.seq),IFMISSING(op.val.rev,null), IFMISSING(op.val.del,null)][1] AS `sequence` " +
			"FROM %s AS %s " +
			"USE INDEX ($idx) " +
			"UNNEST OBJECT_PAIRS($relativesync.channels) AS op " +
			"WHERE [op.name, LEAST($sync.`sequence`, op.val.seq),IFMISSING(op.val.rev,null), IFMISSING(op.val.del,null)]  BETWEEN  ['foo', 0] AND ['foo', 1] " +
			"ORDER BY [op.name, LEAST($sync.`sequence`, op.val.seq),IFMISSING(op.val.rev,null),IFMISSING(op.val.del,null)] " +
			"LIMIT 1",
	}
	partitionableIndexes = map[SGIndexType]bool{
		IndexAllDocs:  true,
		IndexChannels: true,
	}
)

// sgIndexes is the global definition of indexes defined at initialization time
var sgIndexes map[SGIndexType]SGIndex

// Initialize index definitions
func init() {
	sgIndexes = GetSGIndexes()
}

// GetSGIndexes returns the set of indexes defined for Sync Gateway.
func GetSGIndexes() map[SGIndexType]SGIndex {
	sgIndexes := make(map[SGIndexType]SGIndex, indexTypeCount)
	for i := SGIndexType(0); i < indexTypeCount; i++ {
		sgIndex := SGIndex{
			simpleName:       indexNames[i],
			Version:          indexVersions[i],
			PreviousVersions: indexPreviousVersions[i],
			expression:       indexExpressions[i],
			filterExpression: indexFilterExpressions[i],
			flags:            indexFlags[i],
			creationMode:     indexCreationModes[i],
			partitionable:    partitionableIndexes[i],
		}
		// If a readiness query is specified for this index, mark the index as required and add to SGIndex
		readinessQuery, ok := readinessQueries[i]
		if ok {
			sgIndex.required = true
			sgIndex.readinessQuery = fmt.Sprintf(readinessQuery, base.KeyspaceQueryToken, base.KeyspaceQueryAlias)
		}

		sgIndexes[i] = sgIndex
	}
	return sgIndexes
}

// SGIndex is used to manage the set of constants associated with each index definition
type SGIndex struct {
	simpleName       string            // Simplified index name (used to build fullIndexName)
	expression       string            // Expression used to create index
	filterExpression string            // (Optional) Filter expression used to create index
	Version          int               // Index version.  Must be incremented any time the index definition changes
	PreviousVersions []int             // Previous versions of the index that will be removed during post_upgrade cleanup
	required         bool              // Whether SG blocks on startup until this index is ready
	readinessQuery   string            // Query used to determine view readiness
	flags            SGIndexFlags      // Additional index options
	creationMode     indexCreationMode // Signal when to create indexes
	partitionable    bool              // Whether the index is partitionable
}

func (i *SGIndex) fullIndexName(useXattrs bool, numPartitions uint32) string {
	return i.indexNameForVersion(i.Version, useXattrs, numPartitions)
}

func (i *SGIndex) indexNameForVersion(version int, useXattrs bool, numPartitions uint32) string {
	xattrsToken := ""
	if useXattrs {
		xattrsToken = "x"
	}
	indexName := fmt.Sprintf(indexNameFormat, i.simpleName, xattrsToken, version)
	if i.partitionable && numPartitions > 1 {
		indexName = fmt.Sprintf(partitionableIndexNameFormat, indexName, numPartitions)
	}
	return indexName
}

// Tombstone indexing is required for indexes that need to index the _sync xattrs even when the document
// body has been deleted (i.e. SG tombstones)
func (i *SGIndex) shouldIndexTombstones(useXattrs bool) bool {
	return (i.flags&IdxFlagIndexTombstones != 0 && useXattrs)
}

// isMetadataOnly refers to an index that is only applicable to create for a location that does metadata storage
func (i *SGIndex) isMetadataOnly() bool {
	return i.flags&IdxFlagMetadataOnly != 0
}

// isPrincipalOnly refers to an index that is only applicable for querying principal docs.
func (i *SGIndex) isPrincipalOnly() bool {
	return i.flags&IdxFlagPrincipalDocsOnly != 0
}

func (i *SGIndex) isXattrOnly() bool {
	return i.flags&IdxFlagXattrOnly != 0
}

// shouldCreate returns if given index should be created.
func (i *SGIndex) shouldCreate(options InitializeIndexOptions) bool {
	if options.LegacySyncDocsIndex {
		if i.creationMode == SeparatePrincipalIndexes {
			return false
		}
	} else {
		if i.creationMode == LegacySyncDocsIndex {
			return false
		}

	}
	if i.isXattrOnly() && !options.UseXattrs {
		return false
	}

	if i.isMetadataOnly() && options.MetadataIndexes == IndexesWithoutMetadata {
		return false
	}

	if !i.isMetadataOnly() && options.MetadataIndexes == IndexesMetadataOnly {
		return false
	}

	if !i.isPrincipalOnly() && options.MetadataIndexes == IndexesPrincipalOnly {
		return false
	}
	return true
}

// Creates index associated with specified SGIndex if not already present.  Always defers build - a subsequent BUILD INDEX
// will need to be invoked for any created indexes.
func (i *SGIndex) createIfNeeded(ctx context.Context, bucket base.N1QLStore, options InitializeIndexOptions) error {
	if options.NumPartitions < 1 {
		return fmt.Errorf("Invalid number of partitions specified for index %s: %d, needs to be greater than 0", i.simpleName, options.NumPartitions)
	}
	indexName := i.fullIndexName(options.UseXattrs, options.NumPartitions)

	// Create index
	base.InfofCtx(ctx, base.KeyQuery, "Creating index %s if it doesn't already exist...", indexName)
	indexExpression := replaceSyncTokensIndex(i.expression, options.UseXattrs)
	filterExpression := replaceSyncTokensIndex(i.filterExpression, options.UseXattrs)

	n1qlOptions := &base.N1qlIndexOptions{
		DeferBuild:      true,
		NumReplica:      options.NumReplicas,
		IndexTombstones: i.shouldIndexTombstones(options.UseXattrs),
	}
	if i.partitionable && options.NumPartitions > 1 {
		n1qlOptions.NumPartitions = &options.NumPartitions
	}

	// Initial retry 1 seconds, max wait 30s, waits up to 10m
	sleeper := base.CreateMaxDoublingSleeperFunc(20, 1000, 30000)

	// start a retry loop to create index,
	worker := func() (shouldRetry bool, err error, value interface{}) {
		err = bucket.CreateIndexIfNotExists(ctx, indexName, indexExpression, filterExpression, n1qlOptions)
		if err != nil {
			switch {
			case strings.Contains(err.Error(), "syntax error"):
				return false, err, nil
			case strings.Contains(err.Error(), "not enough indexer nodes"):
				return false, fmt.Errorf("Unable to create indexes with the specified number of replicas (%d).  Increase the number of index nodes, or modify 'num_index_replicas' in your Sync Gateway database config.", options.NumReplicas), nil
			case errors.Is(err, base.ErrIndexBackgroundRetry):
				base.DebugfCtx(ctx, base.KeyQuery, "Index %q creation failed but will be retried on server - will wait for index readiness: %v", indexName, err)
				return false, err, nil
			}
			base.WarnfCtx(ctx, "Error creating index %s: %v - will retry.", indexName, err)
		}
		return err != nil, err, nil
	}

	description := fmt.Sprintf("Attempt to create index %s", indexName)
	err, _ := base.RetryLoop(ctx, description, worker, sleeper)

	if err != nil {
		return pkgerrors.Wrapf(err, "Error installing Couchbase index: %v", indexName)
	}

	base.InfofCtx(ctx, base.KeyQuery, "Index %s created successfully", indexName)
	return nil
}

// CollectionIndexesType defines whether a collection represents the metadata collection, a standard collection, or both
type CollectionIndexesType int

type CollectionIndexes map[base.ScopeAndCollectionName]map[string]struct{}

const (
	IndexesWithoutMetadata CollectionIndexesType = iota // indexes for non-default collection that holds data
	IndexesMetadataOnly                                 // indexes for metadata collection where default collection is not on the database
	IndexesAll                                          // indexes for default.default when it is a configured collection on a database
	IndexesPrincipalOnly                                // indexes for principal docs
)

// InitializeIndexOptions are options used for building Sync Gateway indexes, or waiting for it to come online.
type InitializeIndexOptions struct {
	WaitForIndexesOnlineOption base.WaitForIndexesOnlineOption // how long to wait for indexes to become online
	NumReplicas                uint                            // number of indexer nodes for this index
	MetadataIndexes            CollectionIndexesType           // indicate which indexes to create
	UseXattrs                  bool                            // if true, create indexes on xattrs, otherwise, use inline sync data
	NumPartitions              uint32                          // number of partitions to use for the index
	LegacySyncDocsIndex        bool                            // if true, create legacy sync docs index (for backwards compatibility)
}

type sgIndexToBuild struct {
	fullIndexName string
	sgIndex       SGIndex
}

type sgIndexesToBuild []sgIndexToBuild

func (s sgIndexesToBuild) FullIndexNames() []string {
	names := make([]string, 0, len(s))
	for _, sgIndex := range s {
		names = append(names, sgIndex.fullIndexName)
	}
	return names
}

// Initializes Sync Gateway indexes for datastore.  Creates required indexes if not found, then waits for index readiness.
func InitializeIndexes(ctx context.Context, n1QLStore base.N1QLStore, options InitializeIndexOptions) error {
	if options.NumPartitions < 1 {
		return fmt.Errorf("Invalid number of partitions specified: %d, needs to be greater than 0", options.NumPartitions)
	}

	requiredIndexes := make(sgIndexesToBuild, 0, len(sgIndexes))
	for _, sgIndex := range sgIndexes {
		if !sgIndex.shouldCreate(options) {
			base.DebugfCtx(ctx, base.KeyAll, "Skipping index: %s ...", sgIndex.simpleName)
			continue
		}
		fullIndexName := sgIndex.fullIndexName(options.UseXattrs, options.NumPartitions)
		requiredIndexes = append(requiredIndexes, sgIndexToBuild{fullIndexName: fullIndexName, sgIndex: sgIndex})
	}

	if len(requiredIndexes) == 0 {
		// not expected to get here - all our collections require at least one index ...
		base.AssertfCtx(ctx, "No indexes to create - expected at least one index to be created, but none were found")
		return nil
	}

	indexesMeta, err := base.GetIndexesMeta(ctx, n1QLStore, requiredIndexes.FullIndexNames())
	if err != nil {
		base.WarnfCtx(ctx, "Error getting indexes meta: %v - continuing to create", err)
	}

	// Happy-path optimization (for each collection):
	// Drop indexes from the list if they're already online to avoid any per-index init cost.
	requiredIndexes = slices.DeleteFunc(requiredIndexes, func(idx sgIndexToBuild) bool {
		meta, exists := indexesMeta[idx.fullIndexName]
		if exists && meta.State == base.IndexStateOnline {
			base.DebugfCtx(ctx, base.KeyAll, "Index %s is already online", idx.fullIndexName)
			// remove from list of indexes to create/wait for
			return true
		}
		base.DebugfCtx(ctx, base.KeyAll, "Index %s is not online - will issue CREATE INDEX statement and wait for readiness", idx.fullIndexName)
		return false
	})

	if len(requiredIndexes) == 0 {
		// all indexes are already online so we can return early without any per-index queries
		base.InfofCtx(ctx, base.KeyAll, "Indexes ready - already online when checked")
		return nil
	}

	base.InfofCtx(ctx, base.KeyAll, "Initializing %d indexes with numReplicas: %d...", len(requiredIndexes), options.NumReplicas)

	for _, idx := range requiredIndexes {
		err := idx.sgIndex.createIfNeeded(ctx, n1QLStore, options)
		if err != nil {
			if !errors.Is(err, base.ErrIndexBackgroundRetry) {
				return base.RedactErrorf("Unable to install index %s: %v", base.MD(idx.sgIndex.simpleName), err)
			}
		}
	}

	fullIndexNames := requiredIndexes.FullIndexNames()
	// Issue BUILD INDEX for any deferred indexes.
	buildErr := base.BuildDeferredIndexes(ctx, n1QLStore, fullIndexNames)
	if buildErr != nil {
		base.InfofCtx(ctx, base.KeyQuery, "Error building deferred indexes.  Error: %v", buildErr)
		return buildErr
	}

	// Issue a consistency=request_plus query against critical indexes to guarantee indexing is complete and indexes are ready.
	base.InfofCtx(ctx, base.KeyAll, "Verifying index availability...")
	if err := n1QLStore.WaitForIndexesOnline(ctx, fullIndexNames, options.WaitForIndexesOnlineOption); err != nil {
		return err
	}

	base.InfofCtx(ctx, base.KeyAll, "Indexes ready")
	return nil
}

// Return true if the string representation of the error contains
// the substring "[5000]" and false otherwise.
func isIndexerError(err error) bool {
	return err != nil && strings.Contains(err.Error(), "[5000]")
}

// Replace sync tokens ($sync and $relativesync) in the provided createIndex statement with the appropriate token, depending on whether xattrs should be used.
func replaceSyncTokensIndex(statement string, useXattrs bool) string {
	if useXattrs {
		str := strings.ReplaceAll(statement, syncRelativeToken, syncXattr)
		return strings.ReplaceAll(str, syncToken, syncXattr)
	} else {
		str := strings.ReplaceAll(statement, syncRelativeToken, syncNoXattr)
		return strings.ReplaceAll(str, syncToken, syncNoXattr)
	}
}

// Replace sync tokens ($sync) in the provided createIndex statement with the appropriate token, depending on whether xattrs should be used.
func replaceSyncTokensQuery(statement string, useXattrs bool) string {
	if useXattrs {
		str := strings.ReplaceAll(statement, syncRelativeToken, syncXattrQuery)
		return strings.ReplaceAll(str, syncToken, syncXattrQuery)
	} else {
		str := strings.ReplaceAll(statement, syncRelativeToken, syncNoXattrQuery)
		return strings.ReplaceAll(str, syncToken, syncNoXattrQuery)
	}
}

// Replace index tokens ($idx) in the provided createIndex statement with the appropriate token, depending on whether xattrs should be used.
func replaceIndexTokensQuery(statement string, idx SGIndex, useXattrs bool, numPartitions uint32) string {
	return strings.Replace(statement, indexToken, idx.fullIndexName(useXattrs, numPartitions), -1)
}

// GetIndexName returns names of the indexes that would be created for specific options.
func GetIndexNames(options InitializeIndexOptions, indexDefs map[SGIndexType]SGIndex) []string {
	indexNames := make([]string, 0)

	for _, sgIndex := range indexDefs {
		if sgIndex.isXattrOnly() && !options.UseXattrs {
			continue
		}
		if sgIndex.shouldCreate(options) {
			indexNames = append(indexNames, sgIndex.fullIndexName(options.UseXattrs, options.NumPartitions))
		}
	}
	return indexNames
}

// ShouldUseLegacySyncDocsIndex returns true if the syncDocs index should be used for queries of principal docs. Returns false if targeted users and roles indexes should be used.
func ShouldUseLegacySyncDocsIndex(ctx context.Context, collection base.N1QLStore, useXattrs bool) bool {
	onlinePrincipalIndexes, err := GetOnlinePrincipalIndexes(context.Background(), collection, useXattrs)
	if err != nil {
		base.WarnfCtx(ctx, "Error getting online status of principal indexes: %v, falling back to using syncDocs index", err)
		return false
	}
	return shouldUseLegacySyncDocsIndex(onlinePrincipalIndexes)
}

// shouldUseLegacySyncDocsIndex returns true if the syncDocs index should be used for queries of principal docs. Returns false if targeted users and roles indexes should be used.
func shouldUseLegacySyncDocsIndex(onlineIndexes []SGIndexType) bool {
	// if user and role indexes are available, use them
	if slices.Contains(onlineIndexes, IndexUser) && slices.Contains(onlineIndexes, IndexRole) {
		return false
	}
	// use syncDocs index if is is available, otherwise build user and role indexes
	return slices.Contains(onlineIndexes, IndexSyncDocs)
}

// GetOnlinePrincipalIndexes returns the principal indexes that exist and are online for a given collection. This code runs without N1QL retries and will return an error quickly in the case of a retryable N1QL failure. Does not return an error if no indexes are found.
func GetOnlinePrincipalIndexes(ctx context.Context, collection base.N1QLStore, useXattrs bool) ([]SGIndexType, error) {
	possibleIndexes := make(map[string]SGIndexType)
	for sgIndexType, sgIndex := range sgIndexes {
		if !sgIndex.isPrincipalOnly() {
			continue
		}
		possibleIndexes[sgIndex.fullIndexName(useXattrs, DefaultNumIndexPartitions)] = sgIndexType
	}
	meta, err := base.GetIndexesMeta(ctx, collection, slices.Collect(maps.Keys(possibleIndexes)))
	if err != nil {
		return nil, err
	}
	var onlineIndexes []SGIndexType
	for _, index := range meta {
		if index.State == base.IndexStateOnline {
			onlineIndexes = append(onlineIndexes, possibleIndexes[index.Name])
		}
	}
	return onlineIndexes, nil
}

// RemoveUnusedIndexes removes Sync Gateway indexes that are not in use from the bucket given a collection of indexes that are in use. Only datastores which are keys of inUseIndexes are considered, other datastores will be ignored. It returns a list of removed indexes. Running with preview=true will not remove any indexes, but will return the list of indexes that would be removed.
func RemoveUnusedIndexes(ctx context.Context, bucket base.Bucket, inUseIndexes CollectionIndexes, preview bool) (removedIndexes []string, err error) {
	var errs *base.MultiError
	for dsName, inUseIndexes := range inUseIndexes {
		dsName, err := bucket.NamedDataStore(dsName)
		if err != nil {
			errs = errs.Append(fmt.Errorf("failed to get datastore %s: %w", base.MD(dsName), err))
			continue
		}
		n1qlStore, ok := dsName.(base.N1QLStore)
		if !ok {
			errs = errs.Append(fmt.Errorf("datastore %s(%T) is not a N1QLStore", base.MD(dsName), dsName))
			continue
		}
		allIndexes, err := n1qlStore.GetIndexes()
		if err != nil {
			errs = errs.Append(fmt.Errorf("failed to get indexes for datastore %s: %w", base.MD(dsName), err))
			continue
		}
		// Iterate over all indexes and remove those that are not in use
		for _, indexName := range allIndexes {
			if !isSGIndex(indexName) {
				continue
			}
			if _, ok := inUseIndexes[indexName]; ok {
				continue
			}
			removedIndexes = append(removedIndexes,
				fmt.Sprintf("`%s`.`%s`.%s", dsName.ScopeName(), dsName.CollectionName(), indexName))
			if preview {
				continue
			}
			err = n1qlStore.DropIndex(ctx, indexName)
			if err != nil {
				errs = errs.Append(fmt.Errorf("failed to drop index %s %s: %w", base.MD(dsName), indexName, err))
				continue
			}
		}
	}
	slices.Sort(removedIndexes) // sort for ease of testing
	return removedIndexes, errs.ErrorOrNil()
}

// isSGIndex returns true if the index name is a Sync Gateway index lexicographically.
func isSGIndex(indexName string) bool {
	if !strings.HasPrefix(indexName, "sg_") {
		return false
	}
	for _, sgIndex := range sgIndexes {
		if strings.HasPrefix(indexName, "sg_"+sgIndex.simpleName+"_") {
			return true
		}
	}
	return false
}
