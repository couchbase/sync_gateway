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
	"math"
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
	QueryTypeAccess       = "access"
	QueryTypeRoleAccess   = "roleAccess"
	QueryTypeChannels     = "channels"
	QueryTypeChannelsStar = "channelsStar"
	QueryTypeSequences    = "sequences"
	QueryTypePrincipals   = "principals"
	QueryTypeSessions     = "sessions"
	QueryTypeTombstones   = "tombstones"
	QueryTypeResync       = "resync"
	QueryTypeAllDocs      = "allDocs"
	QueryTypeUsers        = "users"
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
			"FROM `%s` "+
			"USE INDEX ($idx) "+
			"WHERE any op in object_pairs($sync.access) satisfies op.name = $userName end;",
		base.KeyspaceQueryToken),
	adhoc: true,
}

var QueryRoleAccess = SGQuery{
	name: QueryTypeRoleAccess,
	statement: fmt.Sprintf(
		"SELECT $sync.role_access.`$$selectUserName` as `value` "+
			"FROM `%s` "+
			"USE INDEX ($idx) "+
			"WHERE any op in object_pairs($sync.role_access) satisfies op.name = $userName end;",
		base.KeyspaceQueryToken),
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
			"META(`%s`).id AS id "+
			"FROM `%s` "+
			"USE INDEX ($idx) "+
			"UNNEST OBJECT_PAIRS($sync.channels) AS op "+
			"WHERE ([op.name, LEAST($sync.sequence, op.val.seq),IFMISSING(op.val.rev,null),IFMISSING(op.val.del,null)]  "+
			"BETWEEN  [$channelName, $startSeq] AND [$channelName, $endSeq]) "+
			"%s"+
			"ORDER BY [op.name, LEAST($sync.sequence, op.val.seq),IFMISSING(op.val.rev,null),IFMISSING(op.val.del,null)]",
		base.KeyspaceQueryToken, base.KeyspaceQueryToken, activeOnlyFilter),
	adhoc: false,
}

var QueryStarChannel = SGQuery{
	name: QueryTypeChannelsStar,
	statement: fmt.Sprintf(
		"SELECT $sync.sequence AS seq, "+
			"$sync.rev AS rev, "+
			"$sync.flags AS flags, "+
			"META(`%s`).id AS id "+
			"FROM `%s` "+
			"USE INDEX ($idx) "+
			"WHERE $sync.sequence >= $startSeq AND $sync.sequence < $endSeq "+
			"AND META().id NOT LIKE '%s' %s"+
			"ORDER BY $sync.sequence",
		base.KeyspaceQueryToken, base.KeyspaceQueryToken, SyncDocWildcard, activeOnlyFilter),
	adhoc: false,
}

var QuerySequences = SGQuery{
	name: QueryTypeSequences,
	statement: fmt.Sprintf(
		"SELECT $sync.sequence AS seq, "+
			"$sync.rev AS rev, "+
			"$sync.flags AS flags, "+
			"META(`%s`).id AS id "+
			"FROM `%s` "+
			"USE INDEX($idx) "+
			"WHERE $sync.sequence IN $inSequences "+
			"AND META().id NOT LIKE '%s'",
		base.KeyspaceQueryToken, base.KeyspaceQueryToken, SyncDocWildcard),
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
		"SELECT META(`%s`).id "+
			"FROM `%s` "+
			"USE INDEX($idx) "+
			"WHERE META(`%s`).id LIKE '%s' "+
			"AND (META(`%s`).id LIKE '%s' "+
			"OR META(`%s`).id LIKE '%s') "+
			"AND META(`%s`).id >= $%s "+ // Uses >= for inclusive startKey
			"ORDER BY META(`%s`).id",
		base.KeyspaceQueryToken,
		base.KeyspaceQueryToken,
		base.KeyspaceQueryToken, SyncDocWildcard,
		base.KeyspaceQueryToken, `\\_sync:user:%`,
		base.KeyspaceQueryToken, `\\_sync:role:%`,
		base.KeyspaceQueryToken, QueryParamStartKey,
		base.KeyspaceQueryToken),
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
		"SELECT `%s`.name, "+
			"`%s`.email, "+
			"`%s`.disabled "+
			"FROM `%s` "+
			"USE INDEX($idx) "+
			"WHERE META(`%s`).id LIKE '%s' "+
			"AND META(`%s`).id >= $%s "+ // Using >= to match QueryPrincipals startKey handling
			"ORDER BY META(`%s`).id",
		base.KeyspaceQueryToken,
		base.KeyspaceQueryToken,
		base.KeyspaceQueryToken,
		base.KeyspaceQueryToken,
		base.KeyspaceQueryToken, `\\_sync:user:%`,
		base.KeyspaceQueryToken, QueryParamStartKey,
		base.KeyspaceQueryToken),
	adhoc: false,
}

var QuerySessions = SGQuery{
	name: QueryTypeSessions,
	statement: fmt.Sprintf(
		"SELECT META(`%s`).id "+
			"FROM `%s` "+
			"USE INDEX($idx) "+
			"WHERE META(`%s`).id LIKE '%s' "+
			"AND META(`%s`).id LIKE '%s' "+
			"AND username = $userName",
		base.KeyspaceQueryToken, base.KeyspaceQueryToken, base.KeyspaceQueryToken, SyncDocWildcard, base.KeyspaceQueryToken, `\\_sync:session:%`),
	adhoc: false,
}
var QueryTombstones = SGQuery{
	name: QueryTypeTombstones,
	statement: fmt.Sprintf(
		"SELECT META(`%s`).id "+
			"FROM `%s` "+
			"USE INDEX ($idx) "+
			"WHERE $sync.tombstoned_at BETWEEN 0 AND $olderThan",
		base.KeyspaceQueryToken, base.KeyspaceQueryToken),
	adhoc: false,
}

// QueryResync and QueryImport both use IndexAllDocs.  If these need to be revisited for performance reasons,
// they could be retooled to use covering indexes, where the id filtering is done at indexing time.  Given that this code
// doesn't even do pagination currently, it's likely that this functionality should just be replaced by an ad-hoc
// DCP stream.
var QueryResync = SGQuery{
	name: QueryTypeResync,
	statement: fmt.Sprintf(
		"SELECT META(`%s`).id "+
			"FROM `%s` "+
			"USE INDEX ($idx) "+
			"WHERE META(`%s`).id NOT LIKE '%s' "+
			"AND $sync.sequence > 0", // Required to use IndexAllDocs
		base.KeyspaceQueryToken, base.KeyspaceQueryToken, base.KeyspaceQueryToken, SyncDocWildcard),
	adhoc: false,
}

// QueryAllDocs is using the star channel's index, which is indexed by sequence, then ordering the results by doc id.
// We currently don't have a performance-tuned use of AllDocs today - if needed, should create a custom index indexed by doc id.
// Note: QueryAllDocs function may appends additional filter and ordering of the form:
//    AND META(`bucket`).id >= '%s'
//    AND META(`bucket`).id <= '%s'
//    ORDER BY META(`bucket`).id
var QueryAllDocs = SGQuery{
	name: QueryTypeAllDocs,
	statement: fmt.Sprintf(
		"SELECT META(`%s`).id as id, "+
			"$sync.rev as r, "+
			"$sync.sequence as s, "+
			"$sync.channels as c "+
			"FROM `%s` "+
			"USE INDEX ($idx) "+
			"WHERE $sync.sequence > 0 AND "+ // Required to use IndexAllDocs
			"META(`%s`).id NOT LIKE '%s' "+
			"AND $sync IS NOT MISSING "+
			"AND ($sync.flags IS MISSING OR BITTEST($sync.flags,1) = false)",
		base.KeyspaceQueryToken, base.KeyspaceQueryToken, base.KeyspaceQueryToken, SyncDocWildcard),
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
func (context *DatabaseContext) N1QLQueryWithStats(ctx context.Context, queryName string, statement string, params map[string]interface{}, consistency base.ConsistencyMode, adhoc bool) (results sgbucket.QueryResultIterator, err error) {

	startTime := time.Now()
	if threshold := context.Options.SlowQueryWarningThreshold; threshold > 0 {
		defer base.SlowQueryLog(ctx, startTime, threshold, "N1QL Query(%q)", queryName)
	}

	n1QLStore, ok := base.AsN1QLStore(context.Bucket)
	if !ok {
		return nil, errors.New("Cannot perform N1QL query on non-Couchbase bucket.")
	}

	queryStat := context.DbStats.Query(queryName)

	results, err = n1QLStore.Query(statement, params, consistency, adhoc)
	if err != nil {
		queryStat.QueryErrorCount.Add(1)
	}

	queryStat.QueryCount.Add(1)
	queryStat.QueryTime.Add(time.Since(startTime).Nanoseconds())

	return results, err
}

// N1QlQueryWithStats is a wrapper for gocbBucket.Query that performs additional diagnostic processing (expvars, slow query logging)
func (context *DatabaseContext) ViewQueryWithStats(ctx context.Context, ddoc string, viewName string, params map[string]interface{}) (results sgbucket.QueryResultIterator, err error) {

	startTime := time.Now()
	if threshold := context.Options.SlowQueryWarningThreshold; threshold > 0 {
		defer base.SlowQueryLog(ctx, startTime, threshold, "View Query (%s.%s)", ddoc, viewName)
	}

	queryStat := context.DbStats.Query(fmt.Sprintf(base.StatViewFormat, ddoc, viewName))

	results, err = context.Bucket.ViewQuery(ddoc, viewName, params)
	if err != nil {
		queryStat.QueryErrorCount.Add(1)
	}
	queryStat.QueryCount.Add(1)
	queryStat.QueryTime.Add(time.Since(startTime).Nanoseconds())

	return results, err
}

// Query to compute the set of channels granted to the specified user via the Sync Function
func (context *DatabaseContext) QueryAccess(ctx context.Context, username string) (sgbucket.QueryResultIterator, error) {

	// View Query
	if context.Options.UseViews {
		opts := map[string]interface{}{"stale": false, "key": username}
		return context.ViewQueryWithStats(ctx, DesignDocSyncGateway(), ViewAccess, opts)
	}

	// N1QL Query
	if username == "" {
		base.WarnfCtx(ctx, "QueryAccess called with empty username - returning empty result iterator")
		return &EmptyResultIterator{}, nil
	}
	accessQueryStatement := context.buildAccessQuery(username)
	params := make(map[string]interface{}, 0)
	params[QueryParamUserName] = username

	return context.N1QLQueryWithStats(ctx, QueryAccess.name, accessQueryStatement, params, base.RequestPlus, QueryAccess.adhoc)
}

// Builds the query statement for an access N1QL query.
func (context *DatabaseContext) buildAccessQuery(username string) string {
	statement := replaceSyncTokensQuery(QueryAccess.statement, context.UseXattrs())

	// SG usernames don't allow back tick, but guard username in select clause for additional safety
	username = strings.Replace(username, "`", "``", -1)
	statement = strings.Replace(statement, QuerySelectUserName, username, -1)
	statement = replaceIndexTokensQuery(statement, sgIndexes[IndexAccess], context.UseXattrs())
	return statement
}

// Query to compute the set of roles granted to the specified user via the Sync Function
func (context *DatabaseContext) QueryRoleAccess(ctx context.Context, username string) (sgbucket.QueryResultIterator, error) {

	// View Query
	if context.Options.UseViews {
		opts := map[string]interface{}{"stale": false, "key": username}
		return context.ViewQueryWithStats(ctx, DesignDocSyncGateway(), ViewRoleAccess, opts)
	}

	// N1QL Query
	if username == "" {
		base.WarnfCtx(ctx, "QueryRoleAccess called with empty username")
		return &EmptyResultIterator{}, nil
	}

	accessQueryStatement := context.buildRoleAccessQuery(username)
	params := make(map[string]interface{}, 0)
	params[QueryParamUserName] = username
	return context.N1QLQueryWithStats(ctx, QueryTypeRoleAccess, accessQueryStatement, params, base.RequestPlus, QueryRoleAccess.adhoc)
}

// Builds the query statement for a roleAccess N1QL query.
func (context *DatabaseContext) buildRoleAccessQuery(username string) string {
	statement := replaceSyncTokensQuery(QueryRoleAccess.statement, context.UseXattrs())

	// SG usernames don't allow back tick, but guard username in select clause for additional safety
	username = strings.Replace(username, "`", "``", -1)
	statement = strings.Replace(statement, QuerySelectUserName, username, -1)
	statement = replaceIndexTokensQuery(statement, sgIndexes[IndexRoleAccess], context.UseXattrs())
	return statement
}

// Query to compute the set of documents assigned to the specified channel within the sequence range
func (context *DatabaseContext) QueryChannels(ctx context.Context, channelName string, startSeq uint64, endSeq uint64, limit int, activeOnly bool) (sgbucket.QueryResultIterator, error) {

	if context.Options.UseViews {
		opts := changesViewOptions(channelName, startSeq, endSeq, limit)
		return context.ViewQueryWithStats(ctx, DesignDocSyncGateway(), ViewChannels, opts)
	}

	// N1QL Query
	// Standard channel index/query doesn't support the star channel.  For star channel queries, QueryStarChannel
	// (which is backed by IndexAllDocs) is used.  The QueryStarChannel result schema is a subset of the
	// QueryChannels result schema (removal handling isn't needed for the star channel).
	channelQueryStatement, params := context.buildChannelsQuery(channelName, startSeq, endSeq, limit, activeOnly)

	return context.N1QLQueryWithStats(ctx, QueryChannels.name, channelQueryStatement, params, base.RequestPlus, QueryChannels.adhoc)
}

// Query to retrieve keys for the specified sequences.  View query uses star channel, N1QL query uses IndexAllDocs
func (context *DatabaseContext) QuerySequences(ctx context.Context, sequences []uint64) (sgbucket.QueryResultIterator, error) {

	if len(sequences) == 0 {
		return nil, errors.New("No sequences specified for QueryChannelsForSequences")
	}

	if context.Options.UseViews {
		opts := changesViewForSequencesOptions(sequences)
		return context.ViewQueryWithStats(ctx, DesignDocSyncGateway(), ViewChannels, opts)
	}

	// N1QL Query
	sequenceQueryStatement := replaceSyncTokensQuery(QuerySequences.statement, context.UseXattrs())
	sequenceQueryStatement = replaceIndexTokensQuery(sequenceQueryStatement, sgIndexes[IndexAllDocs], context.UseXattrs())

	params := make(map[string]interface{})
	params[QueryParamInSequences] = sequences

	return context.N1QLQueryWithStats(ctx, QuerySequences.name, sequenceQueryStatement, params, base.RequestPlus, QueryChannels.adhoc)
}

// Builds the query statement and query parameters for a channels N1QL query.  Also used by unit tests to validate
// query is covering.
func (context *DatabaseContext) buildChannelsQuery(channelName string, startSeq uint64, endSeq uint64, limit int, activeOnly bool) (statement string, params map[string]interface{}) {

	channelQuery := QueryChannels
	index := sgIndexes[IndexChannels]
	if channelName == channels.UserStarChannel {
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
	params[QueryParamStartSeq] = startSeq
	if endSeq == 0 {
		// If endSeq isn't defined, set to max uint64
		endSeq = math.MaxUint64
	} else if endSeq < math.MaxUint64 {
		// channels query isn't based on inclusive end - add one to ensure complete result set
		endSeq++
	}
	params[QueryParamEndSeq] = endSeq

	return channelQueryStatement, params
}

func (context *DatabaseContext) QueryResync(ctx context.Context, limit int, startSeq, endSeq uint64) (sgbucket.QueryResultIterator, error) {
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

		return context.ViewQueryWithStats(ctx, DesignDocSyncGateway(), ViewPrincipals, opts)
	}

	queryStatement := replaceIndexTokensQuery(QueryPrincipals.statement, sgIndexes[IndexSyncDocs], context.UseXattrs())

	params := make(map[string]interface{})
	params[QueryParamStartKey] = startKey

	if limit > 0 {
		queryStatement = fmt.Sprintf("%s LIMIT %d", queryStatement, limit)
	}

	// N1QL Query
	return context.N1QLQueryWithStats(ctx, QueryTypePrincipals, queryStatement, params, base.RequestPlus, QueryPrincipals.adhoc)
}

// Query to retrieve user details, using the syncDocs index
func (context *DatabaseContext) QueryUsers(ctx context.Context, startKey string, limit int) (sgbucket.QueryResultIterator, error) {

	// View Query
	if context.Options.UseViews {
		return nil, errors.New("QueryUsers does not support views")
	}

	queryStatement := replaceIndexTokensQuery(QueryUsers.statement, sgIndexes[IndexSyncDocs], context.UseXattrs())

	params := make(map[string]interface{})
	params[QueryParamStartKey] = startKey

	if limit > 0 {
		queryStatement = fmt.Sprintf("%s LIMIT %d", queryStatement, limit)
	}

	// N1QL Query
	return context.N1QLQueryWithStats(ctx, QueryTypeUsers, queryStatement, params, base.RequestPlus, QueryUsers.adhoc)
}

// Query to retrieve the set of sessions, using the syncDocs index
func (context *DatabaseContext) QuerySessions(ctx context.Context, userName string) (sgbucket.QueryResultIterator, error) {

	// View Query
	if context.Options.UseViews {
		opts := Body{"stale": false}
		opts[QueryParamStartKey] = userName
		opts[QueryParamEndKey] = userName
		return context.ViewQueryWithStats(ctx, DesignDocSyncHousekeeping(), ViewSessions, opts)
	}

	queryStatement := replaceIndexTokensQuery(QuerySessions.statement, sgIndexes[IndexSyncDocs], context.UseXattrs())

	// N1QL Query
	params := make(map[string]interface{}, 1)
	params[QueryParamUserName] = userName
	return context.N1QLQueryWithStats(ctx, QueryTypeSessions, queryStatement, params, base.RequestPlus, QuerySessions.adhoc)
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
func (context *DatabaseContext) QueryAllDocs(ctx context.Context, startKey string, endKey string) (sgbucket.QueryResultIterator, error) {

	// View Query
	if context.Options.UseViews {
		opts := Body{"stale": false, "reduce": false}
		if startKey != "" {
			opts[QueryParamStartKey] = startKey
		}
		if endKey != "" {
			opts[QueryParamEndKey] = endKey
		}
		return context.ViewQueryWithStats(ctx, DesignDocSyncHousekeeping(), ViewAllDocs, opts)
	}

	bucketName := context.Bucket.GetName()

	// N1QL Query
	allDocsQueryStatement := replaceSyncTokensQuery(QueryAllDocs.statement, context.UseXattrs())
	allDocsQueryStatement = replaceIndexTokensQuery(allDocsQueryStatement, sgIndexes[IndexAllDocs], context.UseXattrs())

	params := make(map[string]interface{}, 0)
	if startKey != "" {
		allDocsQueryStatement = fmt.Sprintf("%s AND META(`%s`).id >= $startkey",
			allDocsQueryStatement, bucketName)
		params[QueryParamStartKey] = startKey
	}
	if endKey != "" {
		allDocsQueryStatement = fmt.Sprintf("%s AND META(`%s`).id <= $endkey",
			allDocsQueryStatement, bucketName)
		params[QueryParamEndKey] = endKey
	}

	allDocsQueryStatement = fmt.Sprintf("%s ORDER BY META(`%s`).id",
		allDocsQueryStatement, bucketName)

	return context.N1QLQueryWithStats(ctx, QueryTypeAllDocs, allDocsQueryStatement, params, base.RequestPlus, QueryAllDocs.adhoc)
}

func (context *DatabaseContext) QueryTombstones(ctx context.Context, olderThan time.Time, limit int) (sgbucket.QueryResultIterator, error) {

	// View Query
	if context.Options.UseViews {
		opts := Body{
			"stale":            false,
			QueryParamStartKey: 1,
			QueryParamEndKey:   olderThan.Unix(),
		}
		if limit != 0 {
			opts[QueryParamLimit] = limit
		}
		return context.ViewQueryWithStats(ctx, DesignDocSyncHousekeeping(), ViewTombstones, opts)
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

	return context.N1QLQueryWithStats(ctx, QueryTypeTombstones, tombstoneQueryStatement, params, base.RequestPlus, QueryTombstones.adhoc)
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
