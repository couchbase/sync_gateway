// Copyright 2022-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package base

import (
	"bytes"
	"context"
	"crypto/x509"
	"fmt"
	"net/http"
	"slices"

	"github.com/couchbase/cbgt"
)

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
		InfofCtx(ctx, KeyDCP, "Database name not specified in dcp params %#v during cbgtRootCAsProvider. Continuing without TLS authentication.", UD(feedParams))
		return nil
	}

	creds, ok := cbgtGlobals.getDBCredentials(feedParams.DbName)
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
func cbgtGetPoolsDefaultForBucket(server, bucketName string, scopes bool) ([]byte, error) {
	ctx := BucketNameCtx(context.Background(), bucketName) // this function is global, so reconstruct context
	bucket, ok := cbgtGlobals.getBucket(bucketName)
	if !ok {
		return nil, fmt.Errorf("SG GetPoolsDefaultForBucket: no cbgt manager registered for bucket %v", MD(bucketName).Redact())
	}

	uri := "/pools/default/buckets/" + bucketName
	if scopes {
		uri += "/scopes"
	}
	ctx, cancel := context.WithDeadline(ctx, bucket.getBucketOpDeadline())
	defer cancel()
	body, statusCode, err := bucket.MgmtRequest(ctx, http.MethodGet, uri, "", nil)
	if err != nil {
		return nil, fmt.Errorf("SG GetPoolsDefaultForBucket: failed request: %w", err)
	}
	// cbgt detects a deleted bucket from the body of a 404 response, so return that body without an error.
	if statusCode != http.StatusOK && statusCode != http.StatusNotFound {
		return nil, fmt.Errorf("SG GetPoolsDefaultForBucket: unexpected status code %d: %s", statusCode, body)
	}
	if len(body) == 0 {
		return nil, fmt.Errorf("SG GetPoolsDefaultForBucket: empty body")
	}
	return body, nil
}

// When SG isn't using x.509 authentication, it's necessary to pass bucket credentials
// to cbgt for use when setting up the DCP feed.  These need to be passed as AuthUser and
// AuthPassword in the DCP source parameters.
// The credentials are looked up from the cbgt manager that starts the feed.
// The SOURCE_DCP_SG feed type is a wrapper for SOURCE_GOCB_DCP that adds
// the credential information to the DCP parameters before calling the underlying method.
func init() {
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
	cbgt.StopAfterSourceParams

	// Used to pass the SG database name to SGFeed* shims
	DbName string `json:"sg_dbname,omitempty"`
}

// Equal reports whether p and other are the same, ignoring Collections order (unstable, see
// cbgtFeedParams) and JSON key order (jsoniter doesn't guarantee one). Compares via
// JSONMarshalCanonical rather than field-by-field since the embedded cbgt types could grow
// new fields in a version bump.
func (p SGFeedSourceParams) Equal(other SGFeedSourceParams) bool {
	p.Collections = slices.Clone(p.Collections)
	slices.Sort(p.Collections)
	other.Collections = slices.Clone(other.Collections)
	slices.Sort(other.Collections)

	pBytes, err := JSONMarshalCanonical(p)
	if err != nil {
		return false
	}
	otherBytes, err := JSONMarshalCanonical(other)
	if err != nil {
		return false
	}
	return bytes.Equal(pBytes, otherBytes)
}

// SGFeedSourceParamsEqual unmarshals a and b as SGFeedSourceParams and reports whether they're equal.
func SGFeedSourceParamsEqual(a, b string) bool {
	var pa, pb SGFeedSourceParams
	if err := JSONUnmarshal([]byte(a), &pa); err != nil {
		return false
	}
	if err := JSONUnmarshal([]byte(b), &pb); err != nil {
		return false
	}
	return pa.Equal(pb)
}

type SGFeedIndexParams struct {
	// Used to retrieve the dest implementation (importListener))
	DestKey string `json:"destKey,omitempty"`
}

// Equal returns true if p and other represent the same feed index parameters.
func (p SGFeedIndexParams) Equal(other SGFeedIndexParams) bool {
	return p.DestKey == other.DestKey
}

// SGFeedIndexParamsEqual unmarshals a and b as SGFeedIndexParams and reports whether they're equal.
func SGFeedIndexParamsEqual(a, b string) bool {
	var pa, pb SGFeedIndexParams
	if err := JSONUnmarshal([]byte(a), &pa); err != nil {
		return false
	}
	if err := JSONUnmarshal([]byte(b), &pb); err != nil {
		return false
	}
	return pa.Equal(pb)
}

// cbgtFeedParams returns marshalled cbgt.DCPFeedParams as string. This contains information to for a given information , to be passed as feedparams during cbgt.Manager init.
func cbgtFeedParams(ctx context.Context, opts ShardedDCPOptions) (string, error) {
	feedParams := &SGFeedSourceParams{
		DbName: opts.DBName,
		DCPFeedParams: cbgt.DCPFeedParams{
			AutoReconnectAfterRollback: true,
			IncludeXAttrs:              true,
		},
	}
	if opts.EndSeqNos != nil {
		feedParams.StopAfterSourceParams.StopAfter = "markReached"
		seqMap := make(map[string]cbgt.UUIDSeq, len(opts.EndSeqNos))
		for vbNo, seqNo := range opts.EndSeqNos {
			seqMap[cbgtVbNoToPartition(vbNo)] = cbgt.UUIDSeq{Seq: seqNo}
		}
		feedParams.StopAfterSourceParams.MarkPartitionSeqs = seqMap
	}
	if len(opts.Collections) > 1 {
		return "", RedactErrorf("cbgtFeedParams: multiple scopes not supported, got %v", MD(opts.Collections))
	} else if len(opts.Collections) > 0 {
		for s, c := range opts.Collections {
			feedParams.Scope = s
			feedParams.Collections = c
		}
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
	ctx := BucketNameCtx(context.Background(), sourceName) // this function is global, so reconstruct context
	feedParams, err := getSGFeedSourceParams(params)
	if err != nil {
		return fmt.Errorf("unable to unmarshal params provided by cbgt as sgSourceParams: %w", err)
	}
	creds, ok := cbgtGlobals.getManagerCredentials(mgr)
	if !ok {
		return fmt.Errorf("no feed credentials stored for cbgt manager %s", MD(mgr.UUID()).Redact())
	}
	paramsWithAuth := addCredsToDCPParams(ctx, feedParams, creds, params)
	return cbgt.StartGocbcoreDCPFeed(mgr, feedName, indexName, indexUUID, sourceType, sourceName, bucketUUID,
		paramsWithAuth, dests)
}

// SGGoCBFeedPartitions returns one partition per vbucket of the registered bucket, instead of cbgt.CBPartitions
// opening a new connection to Couchbase Server to count vbuckets.
func SGGoCBFeedPartitions(sourceType, sourceName, sourceUUID, sourceParams,
	serverIn string, options map[string]string) (partitions []string, err error) {
	ctx := BucketNameCtx(context.Background(), sourceName) // this function is global, so reconstruct context
	bucket, ok := cbgtGlobals.getBucket(sourceName)
	if !ok {
		return nil, fmt.Errorf("SG FeedPartitions: no cbgt manager registered for bucket %v", MD(sourceName).Redact())
	}
	numVBuckets, err := bucket.GetMaxVbno(ctx)
	if err != nil {
		return nil, fmt.Errorf("SG FeedPartitions: %w", err)
	}
	partitions = make([]string, numVBuckets)
	for vbNo := range numVBuckets {
		partitions[vbNo] = cbgtVbNoToPartition(vbNo)
	}
	return partitions, nil
}

// SGGocbSourceUUIDLookup returns the UUID of the registered bucket, instead of cbgt.CBSourceUUIDLookUp creating
// cbgt's cached stats agent for the bucket without credentials.
func SGGocbSourceUUIDLookup(sourceName, sourceParams, serverIn string,
	options map[string]string) (string, error) {
	ctx := BucketNameCtx(context.Background(), sourceName) // this function is global, so reconstruct context
	bucket, ok := cbgtGlobals.getBucket(sourceName)
	if !ok {
		return "", fmt.Errorf("SG SourceUUIDLookup: no cbgt manager registered for bucket %v", MD(sourceName).Redact())
	}
	return bucket.UUID(ctx)
}

// getSGFeedSourceParams unmarshals the feed parameters from cbgt DCP feed sourceParams. Returns an error if the parameters can not be unmarshalled.
func getSGFeedSourceParams(params string) (SGFeedSourceParams, error) {
	var sgSourceParams SGFeedSourceParams
	err := JSONUnmarshal([]byte(params), &sgSourceParams)
	return sgSourceParams, err
}

// addCredsToDCPParams returns feedParams marshalled with creds added, or originalParams if marshalling fails.
func addCredsToDCPParams(ctx context.Context, feedParams SGFeedSourceParams, creds cbgtCreds, originalParams string) string {
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
		return originalParams
	}

	return string(marshalledParamsWithAuth)
}

// GetHighSeqNos retrieves the maximum sequence numbers for each vbucket.
func GetHighSeqNos(ctx context.Context, bucket Bucket) (map[uint16]uint64, error) {
	b, err := AsGocbV2Bucket(bucket)
	if err != nil {
		return nil, fmt.Errorf("GetHighSeqNos: bucket is not a GocbV2Bucket: %w", err)
	}
	numVbuckets, err := bucket.GetMaxVbno(ctx)
	if err != nil {
		return nil, err
	}

	// Note: extending to be per collection requires an enhancement in gocbcore to support 0x48 memcached command
	// for a non dcp agent, see gocbcore.DCPAgent.GetVBucketSeqNos
	_, highSeqNos, err := b.GetStatsVbSeqno(numVbuckets, true)
	if err != nil {
		return nil, fmt.Errorf("unable to obtain high seqnos: %w", err)
	}
	return highSeqNos, nil
}
