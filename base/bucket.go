//  Copyright 2013-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package base

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/couchbase/gocb/v2"
	"github.com/couchbase/gocbcore/v10"
	"github.com/couchbase/gocbcore/v10/memd"
	"github.com/couchbase/gomemcached"
	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbaselabs/walrus"
	pkgerrors "github.com/pkg/errors"
	"gopkg.in/couchbaselabs/gocbconnstr.v1"
)

const (
	TapFeedType = "tap"
	DcpFeedType = "dcp"
)

const (
	DefaultPool = "default"
)

const DefaultViewTimeoutSecs = 75 // 75s

// WrappingBucket interface used to identify buckets that wrap an underlying
// bucket (leaky bucket, logging bucket)
type WrappingBucket interface {
	GetUnderlyingBucket() Bucket
}

// WrappingDataStore interface used to identify datastores that wrap an underlying
// datastore (leaky datastore)
type WrappingDatastore interface {
	GetUnderlyingDataStore() DataStore
}

// CouchbaseBucketStore defines operations specific to Couchbase Bucket
type CouchbaseBucketStore interface {
	GetName() string
	MgmtEps() ([]string, error)
	MetadataPurgeInterval(ctx context.Context) (time.Duration, error)
	MaxTTL(context.Context) (int, error)
	HttpClient(context.Context) *http.Client
	GetSpec() BucketSpec
	GetMaxVbno() (uint16, error)

	// GetStatsVbSeqno retrieves the high sequence number for all vbuckets and returns
	// a map of UUIDS and a map of high sequence numbers (map from vbno -> seq)
	GetStatsVbSeqno(maxVbno uint16, useAbsHighSeqNo bool) (uuids map[uint16]uint64, highSeqnos map[uint16]uint64, seqErr error)

	// mgmtRequest uses the CouchbaseBucketStore's http client to make an http request against a management endpoint.
	mgmtRequest(ctx context.Context, method, uri, contentType string, body io.Reader) (*http.Response, error)
}

func AsCouchbaseBucketStore(b Bucket) (CouchbaseBucketStore, bool) {
	couchbaseBucket, ok := GetBaseBucket(b).(CouchbaseBucketStore)
	return couchbaseBucket, ok
}

// GetBaseBucket returns the lowest level non-wrapping bucket wrapped by one or more WrappingBuckets
func GetBaseBucket(b Bucket) Bucket {
	if wb, ok := b.(WrappingBucket); ok {
		return GetBaseBucket(wb.GetUnderlyingBucket())
	}
	return b
}

// GetBaseDataStore returns the lowest level non-wrapping datastore wrapped by one or more WrappingBuckets
func GetBaseDataStore(ds DataStore) DataStore {
	if wds, ok := ds.(WrappingDatastore); ok {
		return GetBaseDataStore(wds.GetUnderlyingDataStore())
	}
	return ds
}

// AsDataStoreName is a temporary thing until DataStoreName is implemented on wrappers (pending further design work on FQName...)
func AsDataStoreName(ds DataStore) (sgbucket.DataStoreName, bool) {
	dsn, ok := GetBaseDataStore(ds).(sgbucket.DataStoreName)
	return dsn, ok
}

func init() {
	// Increase max memcached request size to 20M bytes, to support large docs (attachments!)
	// arriving in a tap feed. (see issues #210, #333, #342)
	gomemcached.MaxBodyLen = int(20 * 1024 * 1024)
}

type DataStore sgbucket.DataStore
type Bucket sgbucket.BucketStore

type FeedArguments sgbucket.FeedArguments
type TapFeed sgbucket.MutationFeed

type AuthHandler interface {
	GetCredentials() (string, string, string)
}

type CouchbaseBucketType int

// Full specification of how to connect to a bucket
type BucketSpec struct {
	Server, BucketName, FeedType  string
	Auth                          AuthHandler
	Certpath, Keypath, CACertPath string         // X.509 auth parameters
	TLSSkipVerify                 bool           // Use insecureSkipVerify when secure scheme (couchbases) is used and cacertpath is undefined
	KvTLSPort                     int            // Port to use for memcached over TLS.  Required for cbdatasource auth when using TLS
	MaxNumRetries                 int            // max number of retries before giving up
	InitialRetrySleepTimeMS       int            // the initial time to sleep in between retry attempts (in millisecond), which will double each retry
	UseXattrs                     bool           // Whether to use xattrs to store _sync metadata.  Used during view initialization
	ViewQueryTimeoutSecs          *uint32        // the view query timeout in seconds (default: 75 seconds)
	MaxConcurrentQueryOps         *int           // maximum number of concurrent query operations (default: DefaultMaxConcurrentQueryOps)
	BucketOpTimeout               *time.Duration // How long bucket ops should block returning "operation timed out". If nil, uses GoCB default.  GoCB buckets only.
	KvPoolSize                    int            // gocb kv_pool_size - number of pipelines per node. Initialized on GetGoCBConnString
	KvBufferSize                  int            // gocb kv buffer size for number of pipelines made. Inititialised on the gocb connection string
	DcpBuffer                     int            // gocb dcp buffer size inititialised on the gocb connection string
}

// Create a RetrySleeper based on the bucket spec properties.  Used to retry bucket operations after transient errors.
func (spec BucketSpec) RetrySleeper() RetrySleeper {
	return CreateDoublingSleeperFunc(spec.MaxNumRetries, spec.InitialRetrySleepTimeMS)
}

func (spec BucketSpec) MaxRetrySleeper(maxSleepMs int) RetrySleeper {
	return CreateMaxDoublingSleeperFunc(spec.MaxNumRetries, spec.InitialRetrySleepTimeMS, maxSleepMs)
}

func (spec BucketSpec) IsWalrusBucket() bool {
	return ServerIsWalrus(spec.Server)
}

func (spec BucketSpec) IsTLS() bool {
	return ServerIsTLS(spec.Server)
}

func (spec BucketSpec) UseClientCert() bool {
	if spec.Certpath == "" || spec.Keypath == "" {
		return false
	}
	return true
}

type GoCBConnStringParams struct {
	// The KV pool size, KV buffer size and DCP buffer size are passed down to gocbcore.
	// Defaults to different values based on being in serverless or not.
	KVPoolSize    int
	KVBufferSize  int
	DCPBufferSize int
}

// FillDefaults replaces any unset fields in this GoCBConnStringParams with their default values.
func (p *GoCBConnStringParams) FillDefaults() {
	if p.KVPoolSize == 0 {
		p.KVPoolSize = DefaultGocbKvPoolSize
	}
	if p.KVBufferSize == 0 {
		p.KVBufferSize = 0
	}
	if p.DCPBufferSize == 0 {
		p.DCPBufferSize = 0
	}
}

// GetGoCBConnString builds a gocb connection string based on BucketSpec.Server.
func (spec *BucketSpec) GetGoCBConnString(params *GoCBConnStringParams) (string, error) {
	if params == nil {
		params = &GoCBConnStringParams{}
	}
	params.FillDefaults()
	connSpec, err := gocbconnstr.Parse(spec.Server)
	if err != nil {
		return "", err
	}

	if connSpec.Options == nil {
		connSpec.Options = map[string][]string{}
	}

	asValues := url.Values(connSpec.Options)

	// Add kv_pool_size as used in both GoCB versions
	poolSizeFromConnStr := asValues.Get("kv_pool_size")
	if poolSizeFromConnStr == "" {
		asValues.Set("kv_pool_size", strconv.Itoa(params.KVPoolSize))
		spec.KvPoolSize = params.KVPoolSize
	} else {
		spec.KvPoolSize, _ = strconv.Atoi(poolSizeFromConnStr)
	}

	kvBufferfromConnStr := asValues.Get("kv_buffer_size")
	if kvBufferfromConnStr == "" && params.KVBufferSize != 0 {
		asValues.Set("kv_buffer_size", strconv.Itoa(params.KVBufferSize))
		spec.KvBufferSize = params.KVBufferSize
	} else {
		spec.KvBufferSize, _ = strconv.Atoi(kvBufferfromConnStr)
	}

	dcpBufferfromConnStr := asValues.Get("dcp_buffer_size")
	if dcpBufferfromConnStr == "" && params.DCPBufferSize != 0 {
		asValues.Set("dcp_buffer_size", strconv.Itoa(params.DCPBufferSize))
		spec.DcpBuffer = params.DCPBufferSize
	} else {
		spec.DcpBuffer, _ = strconv.Atoi(dcpBufferfromConnStr)
	}

	addGoCBv2ConnValues(spec, &asValues)

	connSpec.Options = asValues
	return connSpec.String(), nil
}

// addGoCBv2ConnValues adds URL values for GoCBv2 based on the bucket spec and default SG values.
func addGoCBv2ConnValues(spec *BucketSpec, connValues *url.Values) {
	connValues.Set("max_perhost_idle_http_connections", strconv.Itoa(DefaultHttpMaxIdleConnsPerHost))
	connValues.Set("max_idle_http_connections", DefaultHttpMaxIdleConns)
	connValues.Set("idle_http_connection_timeout", DefaultHttpIdleConnTimeoutMilliseconds)

	if spec.CACertPath != "" {
		connValues.Set("ca_cert_path", spec.CACertPath)
	}
}

func (b BucketSpec) GetViewQueryTimeout() time.Duration {
	return time.Duration(b.GetViewQueryTimeoutMs()) * time.Millisecond
}

func (b BucketSpec) GetViewQueryTimeoutMs() uint64 {
	// If the user doesn't specify any timeout, default to 75s
	if b.ViewQueryTimeoutSecs == nil {
		return DefaultViewTimeoutSecs * 1000
	}

	// If the user specifies 0, then translate that to "No timeout"
	if *b.ViewQueryTimeoutSecs == 0 {
		return 1000 * 60 * 60 * 24 * 365 * 10 // 10 years in milliseconds
	}

	return uint64(*b.ViewQueryTimeoutSecs * 1000)
}

// TLSConfig creates a TLS configuration and populates the certificates
// Errors will get logged then nil is returned.
func (b BucketSpec) TLSConfig(ctx context.Context) *tls.Config {
	var certPool *x509.CertPool = nil
	if !b.TLSSkipVerify { // Add certs if ServerTLSSkipVerify is not set
		var err error
		certPool, err = getRootCAs(ctx, b.CACertPath)
		if err != nil {
			ErrorfCtx(ctx, "Error creating tlsConfig for DCP processing: %v", err)
			return nil
		}
	}

	tlsConfig := &tls.Config{
		RootCAs:            certPool,
		InsecureSkipVerify: b.TLSSkipVerify,
	}

	// If client cert and key are provided, add to config as x509 key pair
	if b.Certpath != "" && b.Keypath != "" {
		cert, err := tls.LoadX509KeyPair(b.Certpath, b.Keypath)
		if err != nil {
			ErrorfCtx(ctx, "Error creating tlsConfig for DCP processing: %v", err)
			return nil
		}
		tlsConfig.Certificates = []tls.Certificate{cert}
	}

	return tlsConfig
}

func (b BucketSpec) GocbAuthenticator() (gocb.Authenticator, error) {
	var username, password string
	if b.Auth != nil {
		username, password, _ = b.Auth.GetCredentials()
	}
	return GoCBv2Authenticator(username, password, b.Certpath, b.Keypath)
}

func (b BucketSpec) GocbcoreAuthProvider() (gocbcore.AuthProvider, error) {
	var username, password string
	if b.Auth != nil {
		username, password, _ = b.Auth.GetCredentials()
	}
	return GoCBCoreAuthConfig(username, password, b.Certpath, b.Keypath)
}

func GetStatsVbSeqno(stats map[string]map[string]string, maxVbno uint16, useAbsHighSeqNo bool) (uuids map[uint16]uint64, highSeqnos map[uint16]uint64, seqErr error) {

	// GetStats response is in the form map[serverURI]map[]
	uuids = make(map[uint16]uint64, maxVbno)
	highSeqnos = make(map[uint16]uint64, maxVbno)
	for _, serverMap := range stats {
		for i := uint16(0); i < maxVbno; i++ {
			// stats come map with keys in format:
			//   vb_nn:uuid
			//   vb_nn:high_seqno
			//   vb_nn:abs_high_seqno
			//   vb_nn:purge_seqno
			uuidKey := fmt.Sprintf("vb_%d:uuid", i)

			// workaround for https://github.com/couchbase/sync_gateway/issues/1371
			highSeqnoKey := ""
			if useAbsHighSeqNo {
				highSeqnoKey = fmt.Sprintf("vb_%d:abs_high_seqno", i)
			} else {
				highSeqnoKey = fmt.Sprintf("vb_%d:high_seqno", i)
			}

			highSeqno, err := strconv.ParseUint(serverMap[highSeqnoKey], 10, 64)
			// Each node will return seqnos for its active and replica vBuckets. Iterating over all nodes will give us
			// numReplicas*maxVbno results. Rather than filter by active/replica (which would require a separate STATS call)
			// simply pick the highest.
			if err == nil && highSeqno > highSeqnos[i] {
				highSeqnos[i] = highSeqno
				uuid, err := strconv.ParseUint(serverMap[uuidKey], 10, 64)
				if err == nil {
					uuids[i] = uuid
				}
			}
		}
	}
	return

}

func GetBucket(ctx context.Context, spec BucketSpec) (bucket Bucket, err error) {
	if spec.IsWalrusBucket() {
		InfofCtx(ctx, KeyAll, "Opening Walrus database %s on <%s>", MD(spec.BucketName), SD(spec.Server))
		sgbucket.SetLogging(ConsoleLogKey().Enabled(KeyBucket))
		bucket, err = walrus.GetCollectionBucket(spec.Server, spec.BucketName)
		// If feed type is not specified (defaults to DCP) or isn't TAP, wrap with pseudo-vbucket handling for walrus
		if spec.FeedType != TapFeedType {
			bucket = &LeakyBucket{bucket: bucket, config: &LeakyBucketConfig{TapFeedVbuckets: true}}
		}
	} else {

		username := ""
		if spec.Auth != nil {
			username, _, _ = spec.Auth.GetCredentials()
		}
		InfofCtx(ctx, KeyAll, "Opening Couchbase database %s on <%s> as user %q", MD(spec.BucketName), SD(spec.Server), UD(username))

		bucket, err = GetGoCBv2Bucket(ctx, spec)
		if err != nil {
			return nil, err
		}

		// If XATTRS are enabled via enable_shared_bucket_access config flag, assert that Couchbase Server is 5.0
		// or later, otherwise refuse to connect to the bucket since pre 5.0 versions don't support XATTRs
		if spec.UseXattrs {
			if !bucket.IsSupported(sgbucket.BucketStoreFeatureXattrs) {
				WarnfCtx(context.Background(), "If using XATTRS, Couchbase Server version must be >= 5.0.")
				return nil, ErrFatalBucketConnection
			}
		}

	}

	// TODO: CBG-2529 - LoggingBucket has been removed - pending a new approach to logging all bucket operations
	// if LogDebugEnabled(KeyBucket) {
	// bucket = &LoggingBucket{bucket: bucket}
	// }

	return bucket, nil
}

// GetCounter returns a uint64 result for the given counter key.
// If the given key is not found in the bucket, this function returns a result of zero.
func GetCounter(datastore DataStore, k string) (result uint64, err error) {
	_, err = datastore.Get(k, &result)
	if datastore.IsError(err, sgbucket.KeyNotFoundError) {
		return 0, nil
	}
	return result, err
}

func IsKeyNotFoundError(datastore DataStore, err error) bool {

	if err == nil {
		return false
	}

	unwrappedErr := pkgerrors.Cause(err)
	return datastore.IsError(unwrappedErr, sgbucket.KeyNotFoundError)
}

func IsCasMismatch(err error) bool {
	if err == nil {
		return false
	}

	unwrappedErr := pkgerrors.Cause(err)

	// GoCB V2 handling
	if isKVError(unwrappedErr, memd.StatusKeyExists) || isKVError(unwrappedErr, memd.StatusNotStored) {
		return true
	}

	// GoCouchbase/Walrus handling
	if strings.Contains(unwrappedErr.Error(), "CAS mismatch") {
		return true
	}

	return false
}

// Returns mutation feed type for bucket.  Will first return the feed type from the spec, when present.  If not found, returns default feed type for bucket
// (DCP for any couchbase bucket, TAP otherwise)
func GetFeedType(bucket Bucket) (feedType string) {
	switch typedBucket := bucket.(type) {
	case *GocbV2Bucket:
		return DcpFeedType
	case *walrus.CollectionBucket:
		return DcpFeedType
	case *LeakyBucket:
		return GetFeedType(typedBucket.bucket)
	case *TestBucket:
		return GetFeedType(typedBucket.Bucket)
	case *walrus.WalrusBucket:
		return TapFeedType
	default:
		// unknown bucket type?
		return TapFeedType
	}
}

// Gets the bucket max TTL, or 0 if no TTL was set.  Sync gateway should fail to bring the DB online if this is non-zero,
// since it's not meant to operate against buckets that auto-delete data.
func getMaxTTL(ctx context.Context, store CouchbaseBucketStore) (int, error) {
	var bucketResponseWithMaxTTL struct {
		MaxTTLSeconds int `json:"maxTTL,omitempty"`
	}

	uri := fmt.Sprintf("/pools/default/buckets/%s", store.GetSpec().BucketName)
	resp, err := store.mgmtRequest(ctx, http.MethodGet, uri, "application/json", nil)
	if err != nil {
		return -1, err
	}

	defer func() { _ = resp.Body.Close() }()

	respBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return -1, err
	}

	if err := JSONUnmarshal(respBytes, &bucketResponseWithMaxTTL); err != nil {
		return -1, err
	}

	return bucketResponseWithMaxTTL.MaxTTLSeconds, nil
}

// Get the Server UUID of the bucket, this is also known as the Cluster UUID
func GetServerUUID(ctx context.Context, store CouchbaseBucketStore) (uuid string, err error) {
	resp, err := store.mgmtRequest(ctx, http.MethodGet, "/pools", "application/json", nil)
	if err != nil {
		return "", err
	}

	defer func() { _ = resp.Body.Close() }()

	respBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}

	var responseJson struct {
		ServerUUID string `json:"uuid"`
	}

	if err := JSONUnmarshal(respBytes, &responseJson); err != nil {
		return "", err
	}

	return responseJson.ServerUUID, nil
}

// Gets the metadata purge interval for the bucket.  First checks for a bucket-specific value.  If not
// found, retrieves the cluster-wide value.
func getMetadataPurgeInterval(ctx context.Context, store CouchbaseBucketStore) (time.Duration, error) {

	// Bucket-specific settings
	uri := fmt.Sprintf("/pools/default/buckets/%s", store.GetName())
	bucketPurgeInterval, err := retrievePurgeInterval(ctx, store, uri)
	if bucketPurgeInterval > 0 || err != nil {
		return bucketPurgeInterval, err
	}

	// Cluster-wide settings
	uri = fmt.Sprintf("/settings/autoCompaction")
	clusterPurgeInterval, err := retrievePurgeInterval(ctx, store, uri)
	if clusterPurgeInterval > 0 || err != nil {
		return clusterPurgeInterval, err
	}

	return 0, nil

}

// Helper function to retrieve a Metadata Purge Interval from server and convert to hours.  Works for any uri
// that returns 'purgeInterval' as a root-level property (which includes the two server endpoints for
// bucket and server purge intervals).
func retrievePurgeInterval(ctx context.Context, bucket CouchbaseBucketStore, uri string) (time.Duration, error) {

	// Both of the purge interval endpoints (cluster and bucket) return purgeInterval in the same way
	var purgeResponse struct {
		PurgeInterval float64 `json:"purgeInterval,omitempty"`
	}

	resp, err := bucket.mgmtRequest(ctx, http.MethodGet, uri, "application/json", nil)
	if err != nil {
		return 0, err
	}

	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode == http.StatusForbidden {
		WarnfCtx(ctx, "403 Forbidden attempting to access %s.  Bucket user must have Bucket Full Access and Bucket Admin roles to retrieve metadata purge interval.", UD(uri))
	} else if resp.StatusCode != http.StatusOK {
		return 0, errors.New(resp.Status)
	}

	respBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, err
	}

	if err := JSONUnmarshal(respBytes, &purgeResponse); err != nil {
		return 0, err
	}

	// Server purge interval is a float value, in days.  Round up to hours
	purgeIntervalHours := int(purgeResponse.PurgeInterval*24 + 0.5)
	return time.Duration(purgeIntervalHours) * time.Hour, nil
}

func ensureBodyClosed(ctx context.Context, body io.ReadCloser) {
	err := body.Close()
	if err != nil {
		DebugfCtx(ctx, KeyBucket, "Failed to close socket: %v", err)
	}
}

// AsViewStore returns a ViewStore if the underlying dataStore implements ViewStore.
func AsViewStore(ds DataStore) (sgbucket.ViewStore, bool) {
	viewStore, ok := ds.(sgbucket.ViewStore)
	return viewStore, ok
}

// AsSubdocStore returns a SubdocStore if the underlying dataStore implements and supports subdoc operations.
func AsSubdocStore(ds DataStore) (sgbucket.SubdocStore, bool) {
	subdocStore, ok := ds.(sgbucket.SubdocStore)
	return subdocStore, ok && ds.IsSupported(sgbucket.BucketStoreFeatureSubdocOperations)
}

// WaitUntilDataStoreExists will try to perform an operation in the given DataStore until it can succeed.
//
// There's no WaitForReady operation in GoCB for collections, only Buckets, so attempting to use Exists in this way this seems like our best option to check for availability.
func WaitUntilDataStoreExists(ctx context.Context, ds DataStore) error {
	return WaitForNoError(ctx, func() error {
		_, err := ds.Exists("WaitUntilDataStoreExists")
		return err
	})
}

// RequireNoBucketTTL ensures there is no MaxTTL set on the bucket (SG #3314)
func RequireNoBucketTTL(ctx context.Context, b Bucket) error {
	cbs, ok := AsCouchbaseBucketStore(b)
	if !ok {
		// Not a Couchbase bucket - no TTL check to do
		return nil
	}

	maxTTL, err := cbs.MaxTTL(ctx)
	if err != nil {
		return err
	}

	if maxTTL != 0 {
		return fmt.Errorf("Backing Couchbase Server bucket has a non-zero MaxTTL value: %d.  Please set MaxTTL to 0 in Couchbase Server Admin UI and try again.", maxTTL)
	}

	return nil
}
