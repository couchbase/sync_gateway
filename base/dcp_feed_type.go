// Copyright 2022-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package base

import (
	"context"
	"crypto/x509"
	"fmt"
	"io"
	"net/http"
	"sync"

	"github.com/couchbase/cbgt"
)

// cbgtCredentials are global map of dbname to basic auth creds.  Updated on cbgt manager creation, required
// for couchbase-dcp-sg feed type to retrieve per-db credentials without requiring server context handle
// Cannot use the serverContext to retrieve this information, as the cbgt manager for a database is initialized
// before the database is added to the server context's set of databases
var cbgtCredentials map[string]cbgtCreds

// cbgtBucketToDBName maps bucket names to DB names for DBs currently registered with CBGT. Necessary because in some
// contexts we only have access to the bucket name.
var cbgtBucketToDBName map[string]string

var cbgtGlobalsLock sync.Mutex

// cbgtCredentials are bucket specific credentials for connecting to Couchbase Server
type cbgtCreds struct {
	username       string         // Couchbase Server username, if using basic authentication
	password       string         // Couchbase Server username, if using basic authentication
	clientCertPath string         // Couchbase Server client certificate(public key), if using x509 authentication. If specified, username and password will be ignored.
	clientKeyPath  string         // Couchbase Server client certificate(private key), if using x509 authentication. If specified, username and password will be ignored.
	certPool       *x509.CertPool // If using TLS, the root certificates for verifying Couchbase Server. If TLSSkipVerify is set, this value should be nil.
	useTLS         bool           // If using couchbases:// this should be true.
}

// This used to be called SOURCE_GOCOUCHBASE_DCP_SG (with the same string value).
const SOURCE_DCP_SG = "couchbase-dcp-sg"

// cbgtRootCAsProvider implements cbgt.RootCAsProvider. It returns a x509.CertPool factory with the root certificates
// for the given bucket. Edge cases:
// * If it returns a function that returns nil, TLS is used but certificate validation is disabled.
// * If it returns a nil function, TLS is disabled altogether.
func cbgtRootCAsProvider(bucketName, bucketUUID, sourceParams string) func() *x509.CertPool {
	ctx := BucketNameCtx(context.Background(), bucketName) // this function is global, so reconstruct context
	feedParams, err := getSGFeedSourceParams(sourceParams)
	if err != nil {
		AssertfCtx(ctx, "Unable to unmarshal params provided by cbgt inside cbgtRootCAsProvider: %v: %s. Continuing without TLS authentication.", err, UD(sourceParams))
		return nil
	}

	if feedParams.DbName == "" {
		// consider switching to AssertfCtx one CBG-4730 is fixed
		InfofCtx(ctx, KeyDCP, "Database name not specified in dcp params %s during cbgtRootCAsProvider. Continuing without TLS authentication.")
		return nil
	}

	creds, ok := getCbgtCredentials(feedParams.DbName)
	if !ok {
		// consider switching to AssertfCtx one CBG-4730 is fixed
		InfofCtx(ctx, KeyDCP, "No feed credentials stored for db %s from sourceParams during cbgtRootCAsProvider. Continuing without TLS authentication.", MD(feedParams.DbName))
		return nil
	}
	if !creds.useTLS {
		return nil
	}
	return func() *x509.CertPool {
		return creds.certPool
	}
}

// cbgt's default GetPoolsDefaultForBucket only works with cbauth
func cbgtGetPoolsDefaultForBucket(server, bucket string, scopes bool) ([]byte, error) {
	ctx := BucketNameCtx(context.Background(), bucket) // this function is global, so reconstruct context
	cbgtGlobalsLock.Lock()
	dbName, ok := cbgtBucketToDBName[bucket]
	if !ok {
		cbgtGlobalsLock.Unlock()
		return nil, fmt.Errorf("SG GetPoolsDefaultForBucket: no DB for bucket %v", MD(bucket).Redact())
	}
	creds, ok := cbgtCredentials[dbName]
	if !ok {
		cbgtGlobalsLock.Unlock()
		return nil, fmt.Errorf("SG GetPoolsDefaultForBucket: no credentials for DB %v (bucket %v)", MD(dbName).Redact(), MD(bucket).Redact())
	}
	// creds is not a pointer, safe to unlock
	cbgtGlobalsLock.Unlock()

	url := server + "/pools/default/buckets/" + bucket
	if scopes {
		url += "/scopes"
	}

	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("SG GetPoolsDefaultForBucket: failed to init request: %v", err)
	}
	req.SetBasicAuth(creds.username, creds.password)

	res, err := cbgt.HttpClient().Do(req)
	if err != nil {
		return nil, fmt.Errorf("SG GetPoolsDefaultForBucket: failed request: %v", err)
	}
	defer func() {
		if err := res.Body.Close(); err != nil {
			WarnfCtx(ctx, "Failed to close %v request body: %v", MD(url).Redact(), err)
		}
	}()

	body, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, fmt.Errorf("SG GetPoolsDefaultForBucket: failed to read body: %v", err)
	}
	if len(body) == 0 {
		return nil, fmt.Errorf("SG GetPoolsDefaultForBucket: empty body")
	}
	return body, nil
}

// When SG isn't using x.509 authentication, it's necessary to pass bucket credentials
// to cbgt for use when setting up the DCP feed.  These need to be passed as AuthUser and
// AuthPassword in the DCP source parameters.
// The dbname is stored in the cfg, and the credentials for that db retrieved from databaseCredentials.
// The SOURCE_DCP_SG feed type is a wrapper for SOURCE_GOCB_DCP that adds
// the credential information to the DCP parameters before calling the underlying method.
func init() {
	cbgtCredentials = make(map[string]cbgtCreds)
	cbgtBucketToDBName = make(map[string]string)
	// NB: we use the same feed type *name* as Lithium nodes, but run it using gocbcore rather than cbdatasource. If only
	// streaming the default collection, there is no functional difference.
	cbgt.RegisterFeedType(SOURCE_DCP_SG, &cbgt.FeedType{
		Start:            SGGoCBFeedStartDCPFeed,
		Partitions:       SGGoCBFeedPartitions,
		SourceUUIDLookUp: SGGocbSourceUUIDLookup,
		// PartitionSeqs is only necessary if we use the CBGT REST API or StopAfter in our FeedParams, which we don't
		// Stats is only used by the CBGT REST API.
		Public: false, // Won't be listed in /api/managerMeta output.
		Description: "general/" + SOURCE_DCP_SG +
			" - a Couchbase Server bucket will be the data source," +
			" via DCP protocol.",
		StartSample: cbgt.NewDCPFeedParams(),
	})
	cbgt.RootCAsProvider = cbgtRootCAsProvider
	cbgt.UserAgentStr = VersionString
	cbgt.GetPoolsDefaultForBucket = cbgtGetPoolsDefaultForBucket
}

// SGFeedSourceParams is a wrapper for cbgt's parameters.
type SGFeedSourceParams struct {
	cbgt.DCPFeedParams

	// Used to pass the SG database name to SGFeed* shims
	DbName string `json:"sg_dbname,omitempty"`
}

type SGFeedIndexParams struct {
	// Used to retrieve the dest implementation (importListener))
	DestKey string `json:"destKey,omitempty"`
}

// cbgtFeedParams returns marshalled cbgt.DCPFeedParams as string, to be passed as feedparams during cbgt.Manager init.
// Used to pass basic auth credentials and xattr flag to cbgt.
func cbgtFeedParams(ctx context.Context, scope string, collections []string, dbName string) (string, error) {
	feedParams := &SGFeedSourceParams{
		DbName: dbName,
		DCPFeedParams: cbgt.DCPFeedParams{
			AutoReconnectAfterRollback: true, // This should be here I think
			IncludeXAttrs:              true,
		},
	}

	if len(collections) > 0 {
		feedParams.Scope = scope
		feedParams.Collections = collections
	}

	paramBytes, err := JSONMarshal(feedParams)
	if err != nil {
		return "", err
	}
	TracefCtx(ctx, KeyDCP, "CBGT feed params: %v", UD(string(paramBytes)))
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

func SGGoCBFeedStartDCPFeed(mgr *cbgt.Manager, feedName, indexName, indexUUID,
	sourceType, sourceName, bucketUUID, params string,
	dests map[string]cbgt.Dest) error {
	ctx := context.TODO() // this global and we don't have bucket name here to add to context
	paramsWithAuth := addCbgtAuthToDCPParams(ctx, params)
	return cbgt.StartGocbcoreDCPFeed(mgr, feedName, indexName, indexUUID, sourceType, sourceName, bucketUUID,
		paramsWithAuth, dests)
}

func SGGoCBFeedPartitions(sourceType, sourceName, sourceUUID, sourceParams,
	serverIn string, options map[string]string) (partitions []string, err error) {

	ctx := context.TODO() // this global and we don't have bucket name here to add to context
	sourceParamsWithAuth := addCbgtAuthToDCPParams(ctx, sourceParams)
	return cbgt.CBPartitions(sourceType, sourceName, sourceUUID, sourceParamsWithAuth,
		serverIn, options)
}

func SGGocbSourceUUIDLookup(sourceName, sourceParams, serverIn string,
	options map[string]string) (string, error) {
	return cbgt.CBSourceUUIDLookUp(sourceName, sourceParams, serverIn, options)
}

// getSGFeedSourceParams unmarshals the feed parameters from cbgt DCP feed sourceParams. Returns an error if the parameters can not be unmarshalled.
func getSGFeedSourceParams(params string) (SGFeedSourceParams, error) {
	var sgSourceParams SGFeedSourceParams
	err := JSONUnmarshal([]byte(params), &sgSourceParams)
	return sgSourceParams, err
}

// addCbgtAuthToDCPParams gets the dbName from the incoming dcpParams, and checks for credentials
// stored in databaseCredentials.  If found, adds those to the params as authUser/authPassword.
// If dbname is present,
func addCbgtAuthToDCPParams(ctx context.Context, dcpParams string) string {
	feedParams, err := getSGFeedSourceParams(dcpParams)
	if err != nil {
		AssertfCtx(ctx, "Unable to unmarshal params provided by cbgt as sgSourceParams: %v: %s", err, UD(dcpParams))
		return dcpParams
	}

	if feedParams.DbName == "" {
		// consider switching to AssertfCtx one CBG-4730 is fixed
		InfofCtx(ctx, KeyDCP, "Database name not specified in dcp params, feed credentials not added, import feed will not be able to authenticate.")
		return dcpParams
	}

	creds, ok := getCbgtCredentials(feedParams.DbName)
	if !ok {
		// consider switching to AssertfCtx one CBG-4730 is fixed
		InfofCtx(ctx, KeyDCP, "No feed credentials stored for db from sourceParams: %s, import feed will not be able to authenticate.", MD(feedParams.DbName))
		return dcpParams
	}

	// Add creds to params
	if creds.clientCertPath != "" && creds.clientKeyPath != "" {
		feedParams.ClientCertPath = creds.clientCertPath
		feedParams.ClientKeyPath = creds.clientKeyPath
	} else {
		feedParams.AuthUser = creds.username
		feedParams.AuthPassword = creds.password
	}

	marshalledParamsWithAuth, marshalErr := JSONMarshal(feedParams)
	if marshalErr != nil {
		WarnfCtx(ctx, "Unable to marshal updated cbgt dcp params: %v. Import feed will not be able to authenticate.", marshalErr)
		return dcpParams
	}

	return string(marshalledParamsWithAuth)
}

// addCbgtCredentials registers a particular bucket and database name for cbgt's global lookup callbacks.
func addCbgtCredentials(dbName, bucketName string, creds cbgtCreds) {
	cbgtGlobalsLock.Lock()
	cbgtCredentials[dbName] = creds
	cbgtBucketToDBName[bucketName] = dbName
	cbgtGlobalsLock.Unlock()
}

func removeCbgtCredentials(dbName string) {
	cbgtGlobalsLock.Lock()
	delete(cbgtCredentials, dbName)
	for bucket, db := range cbgtBucketToDBName {
		if db == dbName {
			delete(cbgtBucketToDBName, bucket)
		}
	}
	cbgtGlobalsLock.Unlock()
}

func getCbgtCredentials(dbName string) (cbgtCreds, bool) {
	cbgtGlobalsLock.Lock()
	creds, found := cbgtCredentials[dbName]
	cbgtGlobalsLock.Unlock() // cbgtCreds is not a pointer type, safe to unlock
	return creds, found
}
