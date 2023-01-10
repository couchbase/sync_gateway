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

// cbgtRootCertPools is a map of bucket UUIDs to cert pools for its CA certs. The documentation comment of
// cbgtRootCAsProvider describes the behaviour of different values.
var cbgtRootCertPools map[string]*x509.CertPool
var cbgtGlobalsLock sync.Mutex

type cbgtCreds struct {
	username       string
	password       string
	clientCertPath string
	clientKeyPath  string
}

// This used to be called SOURCE_GOCOUCHBASE_DCP_SG (with the same string value).
const SOURCE_DCP_SG = "couchbase-dcp-sg"

// cbgtRootCAsProvider implements cbgt.RootCAsProvider. It returns a x509.CertPool factory with the root certificates
// for the given bucket. Edge cases:
// * If it returns a function that returns nil, TLS is used but certificate validation is disabled.
// * If it returns a nil function, TLS is disabled altogether.
func cbgtRootCAsProvider(bucketName, bucketUUID string) func() *x509.CertPool {
	cbgtGlobalsLock.Lock()
	pool, ok := cbgtRootCertPools[bucketUUID]
	cbgtGlobalsLock.Unlock()
	if ok {
		return func() *x509.CertPool {
			return pool
		}
	}
	TracefCtx(context.TODO(), KeyDCP, "Bucket %v not found in root cert pools, not using TLS.", MD(bucketName))
	return nil
}

// cbgt's default GetPoolsDefaultForBucket only works with cbauth
func cbgtGetPoolsDefaultForBucket(server, bucket string, scopes bool) ([]byte, error) {
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
			WarnfCtx(context.TODO(), "Failed to close %v request body: %v", MD(url).Redact(), err)
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
	cbgtRootCertPools = make(map[string]*x509.CertPool)
	cbgtBucketToDBName = make(map[string]string)
	// NB: we use the same feed type *name* as Lithium nodes, but run it using gocbcore rather than cbdatasource. If only
	// streaming the default collection, there is no functional difference.
	cbgt.RegisterFeedType(SOURCE_DCP_SG, &cbgt.FeedType{
		Start:      SGGoCBFeedStartDCPFeed,
		Partitions: SGGoCBFeedPartitions,
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

type SGFeedSourceParams struct {
	// Used to specify whether the applications are interested
	// in receiving the xattrs information in a dcp stream.
	IncludeXAttrs bool `json:"includeXAttrs,omitempty"`

	// Used to pass the SG database name to SGFeed* shims
	DbName string `json:"sg_dbname,omitempty"`

	// Scope within the bucket to stream data from.
	Scope string `json:"scope,omitempty"`

	// Collections within the scope that the feed would cover.
	Collections []string `json:"collections,omitempty"`
}

type SGFeedIndexParams struct {
	// Used to retrieve the dest implementation (importListener))
	DestKey string `json:"destKey,omitempty"`
}

// cbgtFeedParams returns marshalled cbgt.DCPFeedParams as string, to be passed as feedparams during cbgt.Manager init.
// Used to pass basic auth credentials and xattr flag to cbgt.
func cbgtFeedParams(spec BucketSpec, scope string, collections []string, dbName string) (string, error) {
	feedParams := &SGFeedSourceParams{}
	feedParams.DbName = dbName

	if spec.UseXattrs {
		feedParams.IncludeXAttrs = true
	}

	if len(collections) > 0 {
		feedParams.Scope = scope
		feedParams.Collections = collections
	}

	paramBytes, err := JSONMarshal(feedParams)
	if err != nil {
		return "", err
	}
	TracefCtx(context.TODO(), KeyDCP, "CBGT feed params: %v", UD(string(paramBytes)))
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

	paramsWithAuth := addCbgtAuthToDCPParams(params)
	return cbgt.StartGocbcoreDCPFeed(mgr, feedName, indexName, indexUUID, sourceType, sourceName, bucketUUID,
		paramsWithAuth, dests)
}

func SGGoCBFeedPartitions(sourceType, sourceName, sourceUUID, sourceParams,
	serverIn string, options map[string]string) (partitions []string, err error) {

	sourceParamsWithAuth := addCbgtAuthToDCPParams(sourceParams)
	return cbgt.CBPartitions(sourceType, sourceName, sourceUUID, sourceParamsWithAuth,
		serverIn, options)
}

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

	creds, ok := getCbgtCredentials(sgSourceParams.DbName)
	if !ok {
		InfofCtx(context.Background(), KeyImport, "No feed credentials stored for db from sourceParams: %s", MD(sgSourceParams.DbName))
		return dcpParams
	}

	var feedParamsWithAuth cbgt.DCPFeedParams
	unmarshalDCPErr := JSONUnmarshal([]byte(dcpParams), &feedParamsWithAuth)
	if unmarshalDCPErr != nil {
		WarnfCtx(context.Background(), "Unable to unmarshal params provided by cbgt as dcpFeedParams: %v", unmarshalDCPErr)
	}

	// Add creds to params
	if creds.clientCertPath != "" && creds.clientKeyPath != "" {
		feedParamsWithAuth.ClientCertPath = creds.clientCertPath
		feedParamsWithAuth.ClientKeyPath = creds.clientKeyPath
	} else {
		feedParamsWithAuth.AuthUser = creds.username
		feedParamsWithAuth.AuthPassword = creds.password
	}

	marshalledParamsWithAuth, marshalErr := JSONMarshal(feedParamsWithAuth)
	if marshalErr != nil {
		WarnfCtx(context.Background(), "Unable to marshal updated cbgt dcp params: %v", marshalErr)
		return dcpParams
	}

	return string(marshalledParamsWithAuth)
}

func addCbgtCredentials(dbName, bucketName, username, password, clientCertPath, clientKeyPath string) {
	cbgtGlobalsLock.Lock()
	cbgtCredentials[dbName] = cbgtCreds{
		username:       username,
		password:       password,
		clientCertPath: clientCertPath,
		clientKeyPath:  clientKeyPath,
	}
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

// See the comment of cbgtRootCAsProvider for usage details.
func setCbgtRootCertsForBucket(bucketUUID string, pool *x509.CertPool) {
	cbgtGlobalsLock.Lock()
	defer cbgtGlobalsLock.Unlock()
	cbgtRootCertPools[bucketUUID] = pool
}
