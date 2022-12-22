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
	"strings"
	"time"

	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"
)

// Used for queries that only return doc id
type QueryIdRow struct {
	Id string
}

const (
	N1QLMaxInt64 = 9223372036854775000 // Use this for compatibly with all server versions, see MB-54930 for discussion
)

const (
	QueryTypeAccess              = "access"
	QueryTypeRoleAccess          = "roleAccess"
	QueryTypeChannels            = "channels"
	QueryTypeChannelsStar        = "channelsStar"
	QueryTypeSequences           = "sequences"
	QueryTypePrincipals          = "principals"
	QueryTypeRoles               = "roles"
	QueryTypeRolesExcludeDeleted = "rolesExcludeDeleted"
	QueryTypeSessions            = "sessions"
	QueryTypeTombstones          = "tombstones"
	QueryTypeResync              = "resync"
	QueryTypeAllDocs             = "allDocs"
	QueryTypeUsers               = "users"
	QueryTypeUserFunctionPrefix  = "function:" // Prefix applied to named functions from config file
)

type SGQuery struct {
	name      string // Query name - used for logging/stats
	statement string // Query statement
	adhoc     bool   // Whether the query should be run as an ad-hoc (non-prepared)
}

var QueryAccess = SGQuery{
	name: QueryTypeAccess,
	statement: fmt.Sprintf(
		"SELECT $sync.access.`$$selectUserName` as `value` "+
			"FROM %s AS %s "+
			"USE INDEX ($idx) "+
			"WHERE any op in object_pairs($relativesync.access) satisfies op.name = $userName end;",
		base.KeyspaceQueryToken, base.KeyspaceQueryAlias),
	adhoc: true,
}

var QueryRoleAccess = SGQuery{
	name: QueryTypeRoleAccess,
	statement: fmt.Sprintf(
		"SELECT $sync.role_access.`$$selectUserName` as `value` "+
			"FROM %s AS %s "+
			"USE INDEX ($idx) "+
			"WHERE any op in object_pairs($relativesync.role_access) satisfies op.name = $userName end;",
		base.KeyspaceQueryToken, base.KeyspaceQueryAlias),
	adhoc: true,
}

// QueryAccessRow used for response from both QueryAccess and QueryRoleAccess
type QueryAccessRow struct {
	Value channels.TimedSet
}

const (
	// Placeholder to substitute active only filter in channel query.
	activeOnlyFilter = "$$activeOnlyFilter"

	// Filter expression to be used in channel query to select only active documents.
	activeOnlyFilterExpression = "AND ($sync.flags IS MISSING OR BITTEST($sync.flags,1) = false) "
)

var QueryChannels = SGQuery{
	name: QueryTypeChannels,
	statement: fmt.Sprintf(
		"SELECT [op.name, LEAST($sync.sequence, op.val.seq),IFMISSING(op.val.rev,null),IFMISSING(op.val.del,null)][1] AS seq, "+
			"[op.name, LEAST($sync.sequence, op.val.seq),IFMISSING(op.val.rev,null),IFMISSING(op.val.del,null)][2] AS rRev, "+
			"[op.name, LEAST($sync.sequence, op.val.seq),IFMISSING(op.val.rev,null),IFMISSING(op.val.del,null)][3] AS rDel, "+
			"$sync.rev AS rev, "+
			"$sync.flags AS flags, "+
			"META(%s).id AS id "+
			"FROM %s AS %s "+
			"USE INDEX ($idx) "+
			"UNNEST OBJECT_PAIRS($relativesync.channels) AS op "+
			"WHERE ([op.name, LEAST($sync.sequence, op.val.seq),IFMISSING(op.val.rev,null),IFMISSING(op.val.del,null)]  "+
			"BETWEEN  [$channelName, $startSeq] AND [$channelName, $endSeq]) "+
			"%s"+
			"ORDER BY [op.name, LEAST($sync.sequence, op.val.seq),IFMISSING(op.val.rev,null),IFMISSING(op.val.del,null)]",
		base.KeyspaceQueryAlias,
		base.KeyspaceQueryToken, base.KeyspaceQueryAlias,
		activeOnlyFilter),
	adhoc: false,
}

var QueryStarChannel = SGQuery{
	name: QueryTypeChannelsStar,
	statement: fmt.Sprintf(
		"SELECT $sync.sequence AS seq, "+
			"$sync.rev AS rev, "+
			"$sync.flags AS flags, "+
			"META(%s).id AS id "+
			"FROM %s AS %s "+
			"USE INDEX ($idx) "+
			"WHERE $sync.sequence >= $startSeq AND $sync.sequence < $endSeq "+
			"AND META().id NOT LIKE '%s' %s"+
			"ORDER BY $sync.sequence",
		base.KeyspaceQueryAlias,
		base.KeyspaceQueryToken, base.KeyspaceQueryAlias,
		SyncDocWildcard, activeOnlyFilter),
	adhoc: false,
}

var QuerySequences = SGQuery{
	name: QueryTypeSequences,
	statement: fmt.Sprintf(
		"SELECT $sync.sequence AS seq, "+
			"$sync.rev AS rev, "+
			"$sync.flags AS flags, "+
			"META(%s).id AS id "+
			"FROM %s AS %s "+
			"USE INDEX($idx) "+
			"WHERE $sync.sequence IN $inSequences "+
			"AND META().id NOT LIKE '%s'",
		base.KeyspaceQueryAlias,
		base.KeyspaceQueryToken, base.KeyspaceQueryAlias,
		SyncDocWildcard),
	adhoc: false,
}

type QueryChannelsRow struct {
	Id         string `json:"id,omitempty"`
	Rev        string `json:"rev,omitempty"`
	Sequence   uint64 `json:"seq,omitempty"`
	Flags      uint8  `json:"flags,omitempty"`
	RemovalRev string `json:"rRev,omitempty"`
	RemovalDel bool   `json:"rDel,omitempty"`
}

var QueryPrincipals = SGQuery{
	name: QueryTypePrincipals,
	statement: fmt.Sprintf(
		"SELECT META(%s).id "+
			"FROM %s AS %s "+
			"USE INDEX($idx) "+
			"WHERE META(%s).id LIKE '%s' "+
			"AND (META(%s).id LIKE '%s' "+
			"OR META(%s).id LIKE '%s') "+
			"AND META(%s).id >= $%s "+ // Uses >= for inclusive startKey
			"ORDER BY META(%s).id",
		base.KeyspaceQueryAlias,
		base.KeyspaceQueryToken, base.KeyspaceQueryAlias,
		base.KeyspaceQueryAlias, SyncDocWildcard,
		base.KeyspaceQueryAlias, SyncUserWildcard,
		base.KeyspaceQueryAlias, SyncRoleWildcard,
		base.KeyspaceQueryAlias, QueryParamStartKey,
		base.KeyspaceQueryAlias),
	adhoc: false,
}

var QueryAllRolesUsingRoleIdx = SGQuery{
	name: QueryTypeRoles,
	statement: fmt.Sprintf(
		"SELECT META(%s).id "+
			"FROM %s AS %s "+
			"USE INDEX($idx) "+
			"WHERE META(%s).id LIKE '%s' "+
			"AND META(%s).id >= $%s "+ // Uses >= for inclusive startKey
			"ORDER BY META(%s).id",
		base.KeyspaceQueryAlias,
		base.KeyspaceQueryToken, base.KeyspaceQueryAlias,
		base.KeyspaceQueryAlias, SyncRoleWildcard,
		base.KeyspaceQueryAlias, QueryParamStartKey,
		base.KeyspaceQueryAlias),
	adhoc: false,
}

var QueryAllRolesUsingSyncDocsIdx = SGQuery{
	name: QueryTypeRoles,
	statement: fmt.Sprintf(
		"SELECT META(%s).id "+
			"FROM %s AS %s "+
			"USE INDEX($idx) "+
			"WHERE META(%s).id LIKE '%s' "+
			"AND META(%s).id LIKE '%s' "+
			"AND META(%s).id >= $%s "+ // Uses >= for inclusive startKey
			"ORDER BY META(%s).id",
		base.KeyspaceQueryAlias,
		base.KeyspaceQueryToken, base.KeyspaceQueryAlias,
		base.KeyspaceQueryAlias, SyncDocWildcard,
		base.KeyspaceQueryAlias, SyncRoleWildcard,
		base.KeyspaceQueryAlias, QueryParamStartKey,
		base.KeyspaceQueryAlias),
	adhoc: false,
}

var QueryRolesExcludeDeleted = SGQuery{
	name: QueryTypeRolesExcludeDeleted,
	statement: fmt.Sprintf(
		"SELECT META(%s).id "+
			"FROM %s AS %s "+
			"USE INDEX($idx) "+
			"WHERE META(%s).id LIKE '%s' "+
			"AND META(%s).id LIKE '%s' "+
			"AND (%s.deleted IS MISSING OR %s.deleted = false) "+
			"AND META(%s).id >= $%s "+ // Uses >= for inclusive startKey
			"ORDER BY META(%s).id",
		base.KeyspaceQueryAlias,
		base.KeyspaceQueryToken, base.KeyspaceQueryAlias,
		base.KeyspaceQueryAlias, SyncDocWildcard,
		base.KeyspaceQueryAlias, SyncRoleWildcard,
		base.KeyspaceQueryAlias, base.KeyspaceQueryAlias,
		base.KeyspaceQueryAlias, QueryParamStartKey,
		base.KeyspaceQueryAlias),
	adhoc: false,
}

var QueryRolesExcludeDeletedUsingRoleIdx = SGQuery{
	name: QueryTypeRolesExcludeDeleted,
	statement: fmt.Sprintf(
		"SELECT META(%s).id "+
			"FROM %s AS %s "+
			"USE INDEX($idx) "+
			"WHERE META(%s).id LIKE '%s' "+
			"AND (%s.deleted IS MISSING OR %s.deleted = false) "+
			"AND META(%s).id >= $%s "+ // Uses >= for inclusive startKey
			"ORDER BY META(%s).id",
		base.KeyspaceQueryAlias,
		base.KeyspaceQueryToken, base.KeyspaceQueryAlias,
		base.KeyspaceQueryAlias, SyncRoleWildcard,
		base.KeyspaceQueryAlias, base.KeyspaceQueryAlias,
		base.KeyspaceQueryAlias, QueryParamStartKey,
		base.KeyspaceQueryAlias),
	adhoc: false,
}

type QueryUsersRow struct {
	Name     string `json:"name"`
	Email    string `json:"email,omitempty"`
	Disabled bool   `json:"disabled,omitempty"`
}

var QueryUsers = SGQuery{
	name: QueryTypeUsers,
	statement: fmt.Sprintf(
		"SELECT %s.name, "+
			"%s.email, "+
			"%s.disabled "+
			"FROM %s as %s "+
			"USE INDEX($idx) "+
			"WHERE META(%s).id LIKE '%s' "+
			"AND META(%s).id >= $%s "+ // Using >= to match QueryPrincipals startKey handling
			"ORDER BY META(%s).id",
		base.KeyspaceQueryAlias,
		base.KeyspaceQueryAlias,
		base.KeyspaceQueryAlias,
		base.KeyspaceQueryToken, base.KeyspaceQueryAlias,
		base.KeyspaceQueryAlias, SyncUserWildcard,
		base.KeyspaceQueryAlias, QueryParamStartKey,
		base.KeyspaceQueryAlias),
	adhoc: false,
}

var QueryUsersUsingSyncDocsIdx = SGQuery{
	name: QueryTypeUsers,
	statement: fmt.Sprintf(
		"SELECT %s.name, "+
			"%s.email, "+
			"%s.disabled "+
			"FROM %s as %s "+
			"USE INDEX($idx) "+
			"WHERE META(%s).id LIKE '%s' "+
			"AND META(%s).id LIKE '%s' "+
			"AND META(%s).id >= $%s "+ // Using >= to match QueryPrincipals startKey handling
			"ORDER BY META(%s).id",
		base.KeyspaceQueryAlias,
		base.KeyspaceQueryAlias,
		base.KeyspaceQueryAlias,
		base.KeyspaceQueryToken, base.KeyspaceQueryAlias,
		base.KeyspaceQueryAlias, SyncDocWildcard,
		base.KeyspaceQueryAlias, SyncUserWildcard,
		base.KeyspaceQueryAlias, QueryParamStartKey,
		base.KeyspaceQueryAlias),
	adhoc: false,
}

var QuerySessions = SGQuery{
	name: QueryTypeSessions,
	statement: fmt.Sprintf(
		"SELECT META(%s).id "+
			"FROM %s AS %s "+
			"USE INDEX($idx) "+
			"WHERE META(%s).id LIKE '%s' "+
			"AND META(%s).id LIKE '%s' "+
			"AND username = $userName",
		base.KeyspaceQueryAlias,
		base.KeyspaceQueryToken, base.KeyspaceQueryAlias,
		base.KeyspaceQueryAlias, SyncDocWildcard,
		base.KeyspaceQueryAlias, SyncSessionWildcard),
	adhoc: false,
}

var QuerySessionsUsingSessionIdx = SGQuery{
	name: QueryTypeSessions,
	statement: fmt.Sprintf(
		"SELECT META(%s).id "+
			"FROM %s AS %s "+
			"USE INDEX($idx) "+
			"WHERE META(%s).id LIKE '%s' "+
			"AND username = $userName",
		base.KeyspaceQueryAlias,
		base.KeyspaceQueryToken, base.KeyspaceQueryAlias,
		base.KeyspaceQueryAlias, SyncSessionWildcard),
	adhoc: false,
}

var QueryTombstones = SGQuery{
	name: QueryTypeTombstones,
	statement: fmt.Sprintf(
		"SELECT META(%s).id "+
			"FROM %s AS %s "+
			"USE INDEX ($idx) "+
			"WHERE $sync.tombstoned_at BETWEEN 0 AND $olderThan",
		base.KeyspaceQueryAlias,
		base.KeyspaceQueryToken, base.KeyspaceQueryAlias),
	adhoc: false,
}

// QueryAllDocs is using the star channel's index, which is indexed by sequence, then ordering the results by doc id.
// We currently don't have a performance-tuned use of AllDocs today - if needed, should create a custom index indexed by doc id.
// Note: QueryAllDocs function may appends additional filter and ordering of the form:
//
//	AND META(base.KeyspaceQueryAlias).id >= '%s'
//	AND META(base.KeyspaceQueryAlias).id <= '%s'
//	ORDER BY META(base.KeyspaceQueryAlias).id
var QueryAllDocs = SGQuery{
	name: QueryTypeAllDocs,
	statement: fmt.Sprintf(
		"SELECT META(%s).id as id, "+
			"$sync.rev as r, "+
			"$sync.sequence as s, "+
			"$sync.channels as c "+
			"FROM %s AS %s "+
			"USE INDEX ($idx) "+
			"WHERE $sync.sequence > 0 AND "+ // Required to use IndexAllDocs
			"META(%s).id NOT LIKE '%s' "+
			"AND $sync IS NOT MISSING "+
			"AND ($sync.flags IS MISSING OR BITTEST($sync.flags,1) = false)",
		base.KeyspaceQueryAlias,
		base.KeyspaceQueryToken, base.KeyspaceQueryAlias,
		base.KeyspaceQueryAlias, SyncDocWildcard),
	adhoc: false,
}

// Query Parameters used as parameters in prepared statements.  Note that these are hardcoded into the query definitions above,
// for improved query readability.
const (
	QueryParamChannelName = "channelName"
	QueryParamStartSeq    = "startSeq"
	QueryParamEndSeq      = "endSeq"
	QueryParamUserName    = "userName"
	QueryParamOlderThan   = "olderThan"
	QueryParamInSequences = "inSequences"
	QueryParamStartKey    = "startkey"
	QueryParamEndKey      = "endkey"
	QueryParamLimit       = "limit"

	// Variables in the select clause can't be parameterized, require additional handling
	QuerySelectUserName = "$$selectUserName"
)

// N1QlQueryWithStats is a wrapper for gocbBucket.Query that performs additional diagnostic processing (expvars, slow query logging)
func N1QLQueryWithStats(ctx context.Context, dataStore base.DataStore, queryName string, statement string, params map[string]interface{}, consistency base.ConsistencyMode, adhoc bool, dbStats *base.DbStats, slowQueryWarningThreshold time.Duration) (results sgbucket.QueryResultIterator, err error) {

	startTime := time.Now()
	if threshold := slowQueryWarningThreshold; threshold > 0 {
		defer base.SlowQueryLog(ctx, startTime, threshold, "N1QL Query(%q)", queryName)
	}

	n1QLStore, ok := base.AsN1QLStore(dataStore)
	if !ok {
		return nil, errors.New("Cannot perform N1QL query on non-Couchbase bucket.")
	}

	results, err = n1QLStore.Query(statement, params, consistency, adhoc)

	if len(queryName) > 0 {
		queryStat := dbStats.Query(queryName)
		if queryStat == nil {
			// This panic is more recognizable than the one that will otherwise occur below
			panic(fmt.Sprintf("DbStats has no entry for query %q", queryName))
		}
		if err != nil {
			queryStat.QueryErrorCount.Add(1)
		}
		queryStat.QueryCount.Add(1)
		queryStat.QueryTime.Add(time.Since(startTime).Nanoseconds())
	}
	return results, err
}

// ViewQueryWithStats is a wrapper for gocbBucket.Query that performs additional diagnostic processing (expvars, slow query logging)
func (context *DatabaseContext) ViewQueryWithStats(ctx context.Context, dataStore base.DataStore, ddoc string, viewName string, params map[string]interface{}) (results sgbucket.QueryResultIterator, err error) {

	startTime := time.Now()
	if threshold := context.Options.SlowQueryWarningThreshold; threshold > 0 {
		defer base.SlowQueryLog(ctx, startTime, threshold, "View Query (%s.%s)", ddoc, viewName)
	}

	queryStat := context.DbStats.Query(fmt.Sprintf(base.StatViewFormat, ddoc, viewName))

	viewStore, ok := base.AsViewStore(dataStore)
	if !ok {
		return results, fmt.Errorf("Datastore does not support views")
	}

	results, err = viewStore.ViewQuery(ddoc, viewName, params)
	if err != nil {
		queryStat.QueryErrorCount.Add(1)
	}
	queryStat.QueryCount.Add(1)
	queryStat.QueryTime.Add(time.Since(startTime).Nanoseconds())

	return results, err
}

// Query to compute the set of channels granted to the specified user via the Sync Function
func (dbCollection *DatabaseCollection) QueryAccess(ctx context.Context, username string) (sgbucket.QueryResultIterator, error) {

	// View Query
	if dbCollection.useViews() {
		opts := map[string]interface{}{"stale": false, "key": username}
		return dbCollection.dbCtx.ViewQueryWithStats(ctx, dbCollection.dataStore, DesignDocSyncGateway(), ViewAccess, opts)
	}

	// N1QL Query
	if username == "" {
		base.WarnfCtx(ctx, "QueryAccess called with empty username - returning empty result iterator")
		return &EmptyResultIterator{}, nil
	}
	accessQueryStatement := dbCollection.buildAccessQuery(username)
	params := make(map[string]interface{}, 0)
	params[QueryParamUserName] = username

	return N1QLQueryWithStats(ctx, dbCollection.dataStore, QueryAccess.name, accessQueryStatement, params, base.RequestPlus, QueryAccess.adhoc, dbCollection.dbStats(), dbCollection.slowQueryWarningThreshold())
}

// Builds the query statement for an access N1QL query.
func (dbCollection *DatabaseCollection) buildAccessQuery(username string) string {
	statement := replaceSyncTokensQuery(QueryAccess.statement, dbCollection.UseXattrs())

	// SG usernames don't allow back tick, but guard username in select clause for additional safety
	username = strings.Replace(username, "`", "``", -1)
	statement = strings.Replace(statement, QuerySelectUserName, username, -1)
	statement = replaceIndexTokensQuery(statement, sgIndexes[IndexAccess], dbCollection.UseXattrs())
	return statement
}

// Query to compute the set of roles granted to the specified user via the Sync Function
func (dbCollection *DatabaseCollection) QueryRoleAccess(ctx context.Context, username string) (sgbucket.QueryResultIterator, error) {

	// View Query
	if dbCollection.useViews() {
		opts := map[string]interface{}{"stale": false, "key": username}
		return dbCollection.dbCtx.ViewQueryWithStats(ctx, dbCollection.dataStore, DesignDocSyncGateway(), ViewRoleAccess, opts)
	}

	// N1QL Query
	if username == "" {
		base.WarnfCtx(ctx, "QueryRoleAccess called with empty username")
		return &EmptyResultIterator{}, nil
	}

	accessQueryStatement := dbCollection.buildRoleAccessQuery(username)
	params := make(map[string]interface{}, 0)
	params[QueryParamUserName] = username
	return N1QLQueryWithStats(ctx, dbCollection.dataStore, QueryTypeRoleAccess, accessQueryStatement, params, base.RequestPlus, QueryRoleAccess.adhoc, dbCollection.dbStats(), dbCollection.slowQueryWarningThreshold())
}

// TODO: Remove
func (context *DatabaseContext) buildRoleAccessQuery(username string) string {
	return context.GetSingleDatabaseCollection().buildRoleAccessQuery(username)
}

// Builds the query statement for a roleAccess N1QL query.
func (context *DatabaseCollection) buildRoleAccessQuery(username string) string {
	statement := replaceSyncTokensQuery(QueryRoleAccess.statement, context.UseXattrs())

	// SG usernames don't allow back tick, but guard username in select clause for additional safety
	username = strings.Replace(username, "`", "``", -1)
	statement = strings.Replace(statement, QuerySelectUserName, username, -1)
	statement = replaceIndexTokensQuery(statement, sgIndexes[IndexRoleAccess], context.UseXattrs())
	return statement
}

// Query to compute the set of documents assigned to the specified channel within the sequence range
func (context *DatabaseCollection) QueryChannels(ctx context.Context, channelName string, startSeq uint64, endSeq uint64, limit int, activeOnly bool) (sgbucket.QueryResultIterator, error) {

	if context.useViews() {
		opts := changesViewOptions(channelName, startSeq, endSeq, limit)
		return context.dbCtx.ViewQueryWithStats(ctx, context.dataStore, DesignDocSyncGateway(), ViewChannels, opts)
	}

	// N1QL Query
	// Standard channel index/query doesn't support the star channel.  For star channel queries, QueryStarChannel
	// (which is backed by IndexAllDocs) is used.  The QueryStarChannel result schema is a subset of the
	// QueryChannels result schema (removal handling isn't needed for the star channel).
	channelQueryStatement, params := context.buildChannelsQuery(channelName, startSeq, endSeq, limit, activeOnly)
	return N1QLQueryWithStats(ctx, context.dataStore, QueryChannels.name, channelQueryStatement, params, base.RequestPlus, QueryChannels.adhoc, context.dbStats(), context.slowQueryWarningThreshold())
}

// Query to retrieve keys for the specified sequences.  View query uses star channel, N1QL query uses IndexAllDocs
func (context *DatabaseCollection) QuerySequences(ctx context.Context, sequences []uint64) (sgbucket.QueryResultIterator, error) {

	if len(sequences) == 0 {
		return nil, errors.New("No sequences specified for QueryChannelsForSequences")
	}

	if context.useViews() {
		opts := changesViewForSequencesOptions(sequences)
		return context.dbCtx.ViewQueryWithStats(ctx, context.dataStore, DesignDocSyncGateway(), ViewChannels, opts)
	}

	// N1QL Query
	sequenceQueryStatement := replaceSyncTokensQuery(QuerySequences.statement, context.UseXattrs())
	sequenceQueryStatement = replaceIndexTokensQuery(sequenceQueryStatement, sgIndexes[IndexAllDocs], context.UseXattrs())

	params := make(map[string]interface{})
	params[QueryParamInSequences] = sequences

	return N1QLQueryWithStats(ctx, context.dataStore, QuerySequences.name, sequenceQueryStatement, params, base.RequestPlus, QueryChannels.adhoc, context.dbStats(), context.slowQueryWarningThreshold())
}

// buildsChannelsQuery constructs the query statement and query parameters for a channels N1QL query.  Also used by unit tests to validate
// query is covering.
func (context *DatabaseCollection) buildChannelsQuery(channelName string, startSeq uint64, endSeq uint64, limit int, activeOnly bool) (statement string, params map[string]interface{}) {

	channelQuery := QueryChannels
	index := sgIndexes[IndexChannels]
	if channelName == channels.UserStarChannel && context.dbCtx.AllDocsIndexExists {
		channelQuery = QueryStarChannel
		index = sgIndexes[IndexAllDocs]
	}

	channelQueryStatement := replaceActiveOnlyFilter(channelQuery.statement, activeOnly)
	channelQueryStatement = replaceSyncTokensQuery(channelQueryStatement, context.UseXattrs())
	channelQueryStatement = replaceIndexTokensQuery(channelQueryStatement, index, context.UseXattrs())
	if limit > 0 {
		channelQueryStatement = fmt.Sprintf("%s LIMIT %d", channelQueryStatement, limit)
	}

	// Channel queries use a prepared query
	params = make(map[string]interface{}, 3)
	params[QueryParamChannelName] = channelName
	params[QueryParamStartSeq] = N1QLSafeUint64(startSeq)
	// If endSeq isn't defined, set to max int64.
	if endSeq == 0 {
		endSeq = N1QLMaxInt64
	} else if endSeq < N1QLMaxInt64 {
		// channels query isn't based on inclusive end - add one to ensure complete result set
		endSeq++
	}
	params[QueryParamEndSeq] = N1QLSafeUint64(endSeq)

	return channelQueryStatement, params
}

// N1QL only supports int64 values (https://issues.couchbase.com/browse/MB-24464), so restrict parameter values to this range
func N1QLSafeUint64(value uint64) uint64 {
	if value > N1QLMaxInt64 {
		return N1QLMaxInt64
	}
	return value
}

func (context *DatabaseCollection) QueryResync(ctx context.Context, limit int, startSeq, endSeq uint64) (sgbucket.QueryResultIterator, error) {
	return context.QueryChannels(ctx, channels.UserStarChannel, startSeq, endSeq, limit, false)
}

// Query to retrieve the set of user and role doc ids, using the syncDocs index
func (context *DatabaseContext) QueryPrincipals(ctx context.Context, startKey string, limit int) (sgbucket.QueryResultIterator, error) {

	// View Query
	if context.Options.UseViews {
		opts := map[string]interface{}{"stale": false}

		if limit > 0 {
			opts[QueryParamLimit] = limit
		}

		if startKey != "" {
			opts[QueryParamStartKey] = startKey
		}

		return context.ViewQueryWithStats(ctx, context.MetadataStore, DesignDocSyncGateway(), ViewPrincipals, opts)
	}

	queryStatement := replaceIndexTokensQuery(QueryPrincipals.statement, sgIndexes[IndexSyncDocs], context.UseXattrs())

	params := make(map[string]interface{})
	params[QueryParamStartKey] = startKey

	if limit > 0 {
		queryStatement = fmt.Sprintf("%s LIMIT %d", queryStatement, limit)
	}

	// N1QL Query
	return N1QLQueryWithStats(ctx, context.MetadataStore, QueryPrincipals.name, queryStatement, params, base.RequestPlus, QueryPrincipals.adhoc, context.DbStats, context.Options.SlowQueryWarningThreshold)
}

// Query to retrieve user details, using the syncDocs or users index
func (context *DatabaseContext) QueryUsers(ctx context.Context, startKey string, limit int) (sgbucket.QueryResultIterator, error) {

	// View Query
	if context.Options.UseViews {
		return nil, errors.New("QueryUsers does not support views")
	}

	queryStatement, params := context.BuildUsersQuery(startKey, limit)

	// N1QL Query
	return N1QLQueryWithStats(ctx, context.MetadataStore, QueryTypeUsers, queryStatement, params, base.RequestPlus, QueryUsers.adhoc, context.DbStats, context.Options.SlowQueryWarningThreshold)
}

// BuildUsersQuery builds the query statement and query parameters for a Users N1QL query. Also used by unit tests to validate
// query is covering.
func (context *DatabaseContext) BuildUsersQuery(startKey string, limit int) (string, map[string]interface{}) {
	var queryStatement string
	if context.IsServerless() {
		queryStatement = replaceIndexTokensQuery(QueryUsers.statement, sgIndexes[IndexUser], context.UseXattrs())
	} else {
		queryStatement = replaceIndexTokensQuery(QueryUsersUsingSyncDocsIdx.statement, sgIndexes[IndexSyncDocs], context.UseXattrs())
	}

	params := make(map[string]interface{})
	params[QueryParamStartKey] = startKey

	if limit > 0 {
		queryStatement = fmt.Sprintf("%s LIMIT %d", queryStatement, limit)
	}
	return queryStatement, params
}

// Retrieves role ids using the syncDocs or roles index, excluding deleted roles
func (context *DatabaseContext) QueryRoles(ctx context.Context, startKey string, limit int) (sgbucket.QueryResultIterator, error) {

	// View Query
	if context.Options.UseViews {
		opts := map[string]interface{}{"stale": false}

		if limit > 0 {
			opts[QueryParamLimit] = limit
		}

		if startKey != "" {
			opts[QueryParamStartKey] = startKey
		}

		return context.ViewQueryWithStats(ctx, context.MetadataStore, DesignDocSyncGateway(), ViewRolesExcludeDeleted, opts)
	}

	queryStatement, params := context.BuildRolesQuery(startKey, limit)

	// N1QL Query
	return N1QLQueryWithStats(ctx, context.MetadataStore, QueryRolesExcludeDeleted.name, queryStatement, params, base.RequestPlus, QueryRolesExcludeDeleted.adhoc, context.DbStats, context.Options.SlowQueryWarningThreshold)
}

// BuildRolesQuery builds the query statement and query parameters for a Roles N1QL query. Also used by unit tests to validate
// query is covering.
func (context *DatabaseContext) BuildRolesQuery(startKey string, limit int) (string, map[string]interface{}) {

	var queryStatement string
	if context.IsServerless() {
		queryStatement = replaceIndexTokensQuery(QueryRolesExcludeDeletedUsingRoleIdx.statement, sgIndexes[IndexRole], context.UseXattrs())
	} else {
		queryStatement = replaceIndexTokensQuery(QueryRolesExcludeDeleted.statement, sgIndexes[IndexSyncDocs], context.UseXattrs())
	}

	params := make(map[string]interface{})
	params[QueryParamStartKey] = startKey

	if limit > 0 {
		queryStatement = fmt.Sprintf("%s LIMIT %d", queryStatement, limit)
	}

	return queryStatement, params
}

// Retrieves role ids using the roles index, includes deleted roles
func (context *DatabaseContext) QueryAllRoles(ctx context.Context, startKey string, limit int) (sgbucket.QueryResultIterator, error) {

	// View Query
	if context.Options.UseViews {
		return nil, fmt.Errorf("QueryAllRoles doesn't support views")
	}

	var queryStatement string
	if context.IsServerless() {
		queryStatement = replaceIndexTokensQuery(QueryAllRolesUsingRoleIdx.statement, sgIndexes[IndexRole], context.UseXattrs())
	} else {
		queryStatement = replaceIndexTokensQuery(QueryAllRolesUsingSyncDocsIdx.statement, sgIndexes[IndexSyncDocs], context.UseXattrs())
	}

	params := make(map[string]interface{})
	params[QueryParamStartKey] = startKey

	if limit > 0 {
		queryStatement = fmt.Sprintf("%s LIMIT %d", queryStatement, limit)
	}

	// N1QL Query
	return N1QLQueryWithStats(ctx, context.MetadataStore, QueryRolesExcludeDeleted.name, queryStatement, params, base.RequestPlus, QueryRolesExcludeDeleted.adhoc, context.DbStats, context.Options.SlowQueryWarningThreshold)
}

// Query to retrieve the set of sessions, using the syncDocs index
func (context *DatabaseContext) QuerySessions(ctx context.Context, userName string) (sgbucket.QueryResultIterator, error) {

	// View Query
	if context.Options.UseViews {
		opts := Body{"stale": false}
		opts[QueryParamStartKey] = userName
		opts[QueryParamEndKey] = userName
		return context.ViewQueryWithStats(ctx, context.MetadataStore, DesignDocSyncHousekeeping(), ViewSessions, opts)
	}

	queryStatement, params := context.BuildSessionsQuery(userName)
	return N1QLQueryWithStats(ctx, context.MetadataStore, QueryTypeSessions, queryStatement, params, base.RequestPlus, QuerySessions.adhoc, context.DbStats, context.Options.SlowQueryWarningThreshold)
}

// BuildSessionsQuery builds the query statement and query parameters for a Sessions N1QL query. Also used by unit tests to validate
// query is covering.
func (context *DatabaseContext) BuildSessionsQuery(userName string) (string, map[string]interface{}) {
	var queryStatement string
	if context.IsServerless() {
		queryStatement = replaceIndexTokensQuery(QuerySessionsUsingSessionIdx.statement, sgIndexes[IndexSession], context.UseXattrs())
	} else {
		queryStatement = replaceIndexTokensQuery(QuerySessions.statement, sgIndexes[IndexSyncDocs], context.UseXattrs())
	}

	params := make(map[string]interface{}, 1)
	params[QueryParamUserName] = userName
	return queryStatement, params
}

type AllDocsViewQueryRow struct {
	Key   string
	Value struct {
		RevID    string   `json:"r"`
		Sequence uint64   `json:"s"`
		Channels []string `json:"c"`
	}
}

type AllDocsIndexQueryRow struct {
	Id       string
	RevID    string              `json:"r"`
	Sequence uint64              `json:"s"`
	Channels channels.ChannelMap `json:"c"`
}

// AllDocs returns all non-deleted documents in the bucket between startKey and endKey
func (context *DatabaseCollection) QueryAllDocs(ctx context.Context, startKey string, endKey string) (sgbucket.QueryResultIterator, error) {

	// View Query
	if context.useViews() {
		opts := Body{"stale": false, "reduce": false}
		if startKey != "" {
			opts[QueryParamStartKey] = startKey
		}
		if endKey != "" {
			opts[QueryParamEndKey] = endKey
		}
		return context.dbCtx.ViewQueryWithStats(ctx, context.dataStore, DesignDocSyncHousekeeping(), ViewAllDocs, opts)
	}

	// N1QL Query
	allDocsQueryStatement := replaceSyncTokensQuery(QueryAllDocs.statement, context.UseXattrs())
	allDocsQueryStatement = replaceIndexTokensQuery(allDocsQueryStatement, sgIndexes[IndexAllDocs], context.UseXattrs())

	params := make(map[string]interface{}, 0)
	if startKey != "" {
		allDocsQueryStatement = fmt.Sprintf("%s AND META(%s).id >= $startkey",
			allDocsQueryStatement, base.KeyspaceQueryAlias)
		params[QueryParamStartKey] = startKey
	}
	if endKey != "" {
		allDocsQueryStatement = fmt.Sprintf("%s AND META(%s).id <= $endkey",
			allDocsQueryStatement, base.KeyspaceQueryAlias)
		params[QueryParamEndKey] = endKey
	}

	allDocsQueryStatement = fmt.Sprintf("%s ORDER BY META(%s).id",
		allDocsQueryStatement, base.KeyspaceQueryAlias)

	return N1QLQueryWithStats(ctx, context.dataStore, QueryTypeAllDocs, allDocsQueryStatement, params, base.RequestPlus, QueryAllDocs.adhoc, context.dbStats(), context.slowQueryWarningThreshold())
}

func (context *DatabaseCollection) QueryTombstones(ctx context.Context, olderThan time.Time, limit int) (sgbucket.QueryResultIterator, error) {

	// View Query
	if context.useViews() {
		opts := Body{
			"stale":            false,
			QueryParamStartKey: 1,
			QueryParamEndKey:   olderThan.Unix(),
		}
		if limit != 0 {
			opts[QueryParamLimit] = limit
		}
		return context.dbCtx.ViewQueryWithStats(ctx, context.dataStore, DesignDocSyncHousekeeping(), ViewTombstones, opts)
	}

	// N1QL Query
	tombstoneQueryStatement := replaceSyncTokensQuery(QueryTombstones.statement, context.UseXattrs())
	tombstoneQueryStatement = replaceIndexTokensQuery(tombstoneQueryStatement, sgIndexes[IndexTombstones], context.UseXattrs())
	if limit != 0 {
		tombstoneQueryStatement = fmt.Sprintf("%s LIMIT %d", tombstoneQueryStatement, limit)
	}

	params := map[string]interface{}{
		QueryParamOlderThan: olderThan.Unix(),
	}

	return N1QLQueryWithStats(ctx, context.dataStore, QueryTypeTombstones, tombstoneQueryStatement, params, base.RequestPlus, QueryTombstones.adhoc, context.dbStats(), context.slowQueryWarningThreshold())
}

func changesViewOptions(channelName string, startSeq, endSeq uint64, limit int) map[string]interface{} {
	endKey := []interface{}{channelName, endSeq}
	if endSeq == 0 {
		endKey[1] = map[string]interface{}{} // infinity
	}
	optMap := map[string]interface{}{
		"stale":            false,
		QueryParamStartKey: []interface{}{channelName, startSeq},
		QueryParamEndKey:   endKey,
	}
	if limit > 0 {
		optMap[QueryParamLimit] = limit
	}
	return optMap
}

func changesViewForSequencesOptions(sequences []uint64) map[string]interface{} {

	keys := make([]interface{}, len(sequences))
	for i, sequence := range sequences {
		key := []interface{}{channels.UserStarChannel, sequence}
		keys[i] = key
	}

	optMap := map[string]interface{}{
		"stale": false,
		"keys":  keys,
	}
	return optMap
}

type EmptyResultIterator struct{}

func (e *EmptyResultIterator) One(valuePtr interface{}) error {
	return nil
}

func (e *EmptyResultIterator) Next(valuePtr interface{}) bool {
	return false
}

func (e *EmptyResultIterator) NextBytes() []byte {
	return []byte{}
}

func (e *EmptyResultIterator) Close() error {
	return nil
}

// Replace $$activeOnlyFilter placeholder with activeOnlyFilterExpression if activeOnly is true
// and an empty string otherwise in the channel query statement.
func replaceActiveOnlyFilter(statement string, activeOnly bool) string {
	if activeOnly {
		return strings.Replace(statement, activeOnlyFilter, activeOnlyFilterExpression, -1)
	} else {
		return strings.Replace(statement, activeOnlyFilter, "", -1)
	}
}
