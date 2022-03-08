package base

import (
	"context"
	"sync"

	"github.com/couchbase/cbgt"
)

// cbgtCredentials are global map of dbname to basic auth creds.  Updated on cbgt manager creation, required
// for couchbase-dcp-sg feed type to retrieve per-db credentials without requiring server context handle
// Cannot use the serverContext to retrieve this information, as the cbgt manager for a database is initialized
// before the database is added to the server context's set of databases
var cbgtCredentials map[string]cbgtCreds
var cbgtCredentialsLock sync.Mutex

type cbgtCreds struct {
	username string
	password string
}

const SOURCE_GOCOUCHBASE_DCP_SG = "couchbase-dcp-sg"

// When SG isn't using x.509 authentication, it's necessary to pass bucket credentials
// to cbgt for use when setting up the DCP feed.  These need to be passed as AuthUser and
// AuthPassword in the DCP source parameters.
// The dbname is stored in the cfg, and the credentials for that db retrieved from databaseCredentials.
// The SOURCE_GOCOUCHBASE_DCP_SG feed type is a wrapper for SOURCE_GOCOUCHBASE_DCP that adds
// the credential information to the DCP parameters before calling the underlying method.
func init() {
	cbgtCredentials = make(map[string]cbgtCreds)
	cbgt.RegisterFeedType(SOURCE_GOCOUCHBASE_DCP_SG, &cbgt.FeedType{
		Start:           SGFeedStartDCPFeed,
		Partitions:      SGFeedPartitions,
		PartitionSeqs:   SGFeedPartitionSeqs,
		Stats:           SGFeedStats,
		PartitionLookUp: cbgt.CouchbaseSourceVBucketLookUp,
		//SourceUUIDLookUp: SGFeedSourceUUIDLookUp,     // TODO: cbgt 1.2.0
		Public: false, // Won't be listed in /api/managerMeta output.
		Description: "general/" + SOURCE_GOCOUCHBASE_DCP_SG +
			" - a Couchbase Server bucket will be the data source," +
			" via DCP protocol",
		StartSample: cbgt.NewDCPFeedParams(),
	})
}

type SGFeedSourceParams struct {
	// Used to specify whether the applications are interested
	// in receiving the xattrs information in a dcp stream.
	IncludeXAttrs bool `json:"includeXAttrs,omitempty"`

	// Used to pass the SG database name to SGFeed* shims
	DbName string `json:"sg_dbname,omitempty"`
}

type SGFeedIndexParams struct {
	// Used to retrieve the dest implementation (importListener))
	DestKey string `json:"destKey,omitempty"`
}

// cbgtFeedParams returns marshalled cbgt.DCPFeedParams as string, to be passed as feedparams during cbgt.Manager init.
// Used to pass basic auth credentials and xattr flag to cbgt.
func cbgtFeedParams(spec BucketSpec, dbName string) (string, error) {
	feedParams := &SGFeedSourceParams{}
	feedParams.DbName = dbName

	if spec.UseXattrs {
		feedParams.IncludeXAttrs = true
	}

	paramBytes, err := JSONMarshal(feedParams)
	if err != nil {
		return "", err
	}
	return string(paramBytes), nil
}

// cbgtIndexParams returns marshalled indexParams as string, to be passed as indexParams during cbgt index creation.
// Used to retrieve the dest implementation for a given feed
func cbgtIndexParams(destKey string) (string, error) {
	indexParams := &SGFeedIndexParams{}
	indexParams.DestKey = destKey

	paramBytes, err := JSONMarshal(indexParams)
	if err != nil {
		return "", err
	}
	return string(paramBytes), nil
}

// SGFeed* functions add credentials to sourceParams before calling the underlying
// cbgt function
func SGFeedStartDCPFeed(mgr *cbgt.Manager, feedName, indexName, indexUUID,
	sourceType, sourceName, bucketUUID, params string,
	dests map[string]cbgt.Dest) error {

	paramsWithAuth := addCbgtAuthToDCPParams(params)
	return cbgt.StartDCPFeed(mgr, feedName, indexName, indexUUID, sourceType, sourceName, bucketUUID,
		paramsWithAuth, dests)
}

func SGFeedPartitions(sourceType, sourceName, sourceUUID, sourceParams,
	serverIn string, options map[string]string) (partitions []string, err error) {

	sourceParamsWithAuth := addCbgtAuthToDCPParams(sourceParams)
	return cbgt.CouchbasePartitions(sourceType, sourceName, sourceUUID, sourceParamsWithAuth,
		serverIn, options)
}

func SGFeedPartitionSeqs(sourceType, sourceName, sourceUUID,
	sourceParams, serverIn string, options map[string]string) (map[string]cbgt.UUIDSeq, error) {

	sourceParamsWithAuth := addCbgtAuthToDCPParams(sourceParams)
	return cbgt.CouchbasePartitionSeqs(sourceType, sourceName, sourceUUID, sourceParamsWithAuth,
		serverIn, options)
}

func SGFeedStats(sourceType, sourceName, sourceUUID, sourceParams, serverIn string, options map[string]string,
	statsKind string) (map[string]interface{}, error) {

	sourceParamsWithAuth := addCbgtAuthToDCPParams(sourceParams)
	return cbgt.CouchbaseStats(sourceType, sourceName, sourceUUID,
		sourceParamsWithAuth, serverIn, options, statsKind)
}

/*  TODO: enable when moving to cbgt 1.2.0
func SGFeedSourceUUIDLookUp(sourceName, sourceParams, serverIn string, options map[string]string) (string, error) {
	sourceParamsWithAuth := addCbgtAuthToDCPParams(sourceParams)
	return cbgt.CouchbaseSourceUUIDLookUp(sourceName, sourceParamsWithAuth, serverIn, options)
}

*/

// addCbgtAuthToDCPParams gets the dbName from the incoming dcpParams, and checks for credentials
// stored in databaseCredentials.  If found, adds those to the params as authUser/authPassword.
// If dbname is present,
func addCbgtAuthToDCPParams(dcpParams string) string {

	var sgSourceParams SGFeedSourceParams

	unmarshalErr := JSONUnmarshal([]byte(dcpParams), &sgSourceParams)
	if unmarshalErr != nil {
		WarnfCtx(context.Background(), "Unable to unmarshal params provided by cbgt as sgSourceParams: %v", unmarshalErr)
		return dcpParams
	}

	if sgSourceParams.DbName == "" {
		InfofCtx(context.Background(), KeyImport, "Database name not specified in dcp params, feed credentials not added")
		return dcpParams
	}

	username, password, ok := getCbgtCredentials(sgSourceParams.DbName)
	if !ok {
		// no stored credentials includes the valid x.509 auth case
		InfofCtx(context.Background(), KeyImport, "No feed credentials stored for db from sourceParams: %s", MD(sgSourceParams.DbName))
		return dcpParams
	}

	var feedParamsWithAuth cbgt.DCPFeedParams
	unmarshalDCPErr := JSONUnmarshal([]byte(dcpParams), &feedParamsWithAuth)
	if unmarshalDCPErr != nil {
		WarnfCtx(context.Background(), "Unable to unmarshal params provided by cbgt as dcpFeedParams: %v", unmarshalDCPErr)
	}

	// Add creds to params
	feedParamsWithAuth.AuthUser = username
	feedParamsWithAuth.AuthPassword = password

	marshalledParamsWithAuth, marshalErr := JSONMarshal(feedParamsWithAuth)
	if marshalErr != nil {
		WarnfCtx(context.Background(), "Unable to marshal updated cbgt dcp params: %v", marshalErr)
		return dcpParams
	}

	return string(marshalledParamsWithAuth)
}

func addCbgtCredentials(dbName, username, password string) {
	cbgtCredentialsLock.Lock()
	cbgtCredentials[dbName] = cbgtCreds{
		username: username,
		password: password,
	}
	cbgtCredentialsLock.Unlock()
}

func removeCbgtCredentials(dbName string) {
	cbgtCredentialsLock.Lock()
	delete(cbgtCredentials, dbName)
	cbgtCredentialsLock.Unlock()
}

func getCbgtCredentials(dbName string) (username, password string, ok bool) {
	cbgtCredentialsLock.Lock()
	creds, found := cbgtCredentials[dbName]
	if found {
		username = creds.username
		password = creds.password
	}
	cbgtCredentialsLock.Unlock()
	return username, password, found
}
