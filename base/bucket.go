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
	"io/ioutil"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/couchbase/go-couchbase"
	"github.com/couchbase/gocb/v2"
	"github.com/couchbase/gocbcore/v10"
	"github.com/couchbase/gocbcore/v10/memd"
	"github.com/couchbase/gomemcached"
	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbaselabs/walrus"
	pkgerrors "github.com/pkg/errors"
	gocbV1 "gopkg.in/couchbase/gocb.v1"
	"gopkg.in/couchbaselabs/gocbconnstr.v1"
)

const (
	TapFeedType = "tap"
	DcpFeedType = "dcp"
)

const (
	DefaultPool = "default"
)

const (
	GoCB                   CouchbaseDriver = iota // Use GoCB driver with default Transcoder
	GoCBCustomSGTranscoder                        // Use GoCB driver with a custom Transcoder
	GoCBv2                                        // Use gocb v2 driver
)

const (
	DataBucket CouchbaseBucketType = iota
	IndexBucket
)

const DefaultViewTimeoutSecs = 75 // 75s

// WrappingBucket interface used to identify buckets that wrap an underlying
// bucket (leaky bucket, logging bucket)
type WrappingBucket interface {
	GetUnderlyingBucket() Bucket
}

// CouchbaseStore defines operations specific to Couchbase data stores
type CouchbaseStore interface {
	BucketName() string
	MgmtEps() ([]string, error)
	MetadataPurgeInterval() (time.Duration, error)
	ServerUUID() (uuid string, err error)
	MaxTTL() (int, error)
	HttpClient() *http.Client
	GetExpiry(k string) (expiry uint32, getMetaError error)
	GetSpec() BucketSpec
	GetMaxVbno() (uint16, error)

	// GetStatsVbSeqno retrieves the high sequence number for all vbuckets and returns
	// a map of UUIDS and a map of high sequence numbers (map from vbno -> seq)
	GetStatsVbSeqno(maxVbno uint16, useAbsHighSeqNo bool) (uuids map[uint16]uint64, highSeqnos map[uint16]uint64, seqErr error)

	// mgmtRequest uses the CouchbaseStore's http client to make an http request against a management endpoint.
	mgmtRequest(method, uri, contentType string, body io.Reader) (*http.Response, error)
}

func AsCouchbaseStore(b Bucket) (CouchbaseStore, bool) {
	couchbaseBucket, ok := GetBaseBucket(b).(CouchbaseStore)
	return couchbaseBucket, ok
}

// GetBaseBucket returns the lowest level non-wrapping bucket wrapped by one or more WrappingBuckets
func GetBaseBucket(b Bucket) Bucket {
	wb, ok := b.(WrappingBucket)
	if ok {
		return GetBaseBucket(wb.GetUnderlyingBucket())
	}
	return b
}

func ChooseCouchbaseDriver(bucketType CouchbaseBucketType) CouchbaseDriver {

	// Otherwise use the default driver for the bucket type
	// return DefaultDriverForBucketType[bucketType]
	switch bucketType {
	case DataBucket:
		return GoCBv2
	case IndexBucket:
		return GoCBv2
	default:
		// If a new bucket type is added and this method isn't updated, flag a warning (or, could panic)
		WarnfCtx(context.Background(), "Unexpected bucket type: %v", bucketType)
		return GoCBv2
	}

}

func (couchbaseDriver CouchbaseDriver) String() string {
	switch couchbaseDriver {
	case GoCB:
		return "GoCB"
	case GoCBCustomSGTranscoder:
		return "GoCBCustomSGTranscoder"
	case GoCBv2:
		return "GoCBv2"
	default:
		return "UnknownCouchbaseDriver"
	}
}

func AsCouchbaseDriver(d string) CouchbaseDriver {
	switch d {
	case "GoCB":
		return GoCB
	case "GoCBCustomSGTranscoder":
		return GoCBCustomSGTranscoder
	case "GoCBv2":
		return GoCBv2
	default:
		return GoCBv2
	}

}

func init() {
	// Increase max memcached request size to 20M bytes, to support large docs (attachments!)
	// arriving in a tap feed. (see issues #210, #333, #342)
	gomemcached.MaxBodyLen = int(20 * 1024 * 1024)
}

type Bucket sgbucket.DataStore
type FeedArguments sgbucket.FeedArguments
type TapFeed sgbucket.MutationFeed

type AuthHandler couchbase.AuthHandler
type CouchbaseDriver int
type CouchbaseBucketType int

// Full specification of how to connect to a bucket
type BucketSpec struct {
	Server, BucketName, FeedType  string
	Auth                          AuthHandler
	CouchbaseDriver               CouchbaseDriver
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

// GetGoCBConnString builds a gocb (v1 or v2 depending on the BucketSpec.CouchbaseDriver) connection string based on BucketSpec.Server.
func (spec *BucketSpec) GetGoCBConnString() (string, error) {
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
		asValues.Set("kv_pool_size", DefaultGocbKvPoolSize)
		spec.KvPoolSize, _ = strconv.Atoi(DefaultGocbKvPoolSize)
	} else {
		spec.KvPoolSize, _ = strconv.Atoi(poolSizeFromConnStr)
	}

	if spec.CouchbaseDriver == GoCBv2 {
		addGoCBv2ConnValues(spec, &asValues)
	} else {
		addGoCBv1ConnValues(spec, &asValues)
	}

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

// addGoCBv1ConnValues adds URL values for GoCBv1 based on the bucket spec and default SG values.
func addGoCBv1ConnValues(spec *BucketSpec, connValues *url.Values) {
	connValues.Set("http_max_idle_conns_per_host", strconv.Itoa(DefaultHttpMaxIdleConnsPerHost))
	connValues.Set("http_max_idle_conns", DefaultHttpMaxIdleConns)
	connValues.Set("http_idle_conn_timeout", DefaultHttpIdleConnTimeoutMilliseconds)

	connValues.Set("n1ql_timeout", fmt.Sprintf("%d", spec.GetViewQueryTimeoutMs()))

	connValues.Set("operation_tracing", "false")

	if spec.Certpath != "" && spec.Keypath != "" {
		connValues.Set("certpath", spec.Certpath)
		connValues.Set("keypath", spec.Keypath)
	}
	if spec.CACertPath != "" {
		connValues.Set("cacertpath", spec.CACertPath)
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
func (b BucketSpec) TLSConfig() *tls.Config {
	var certPool *x509.CertPool = nil
	if !b.TLSSkipVerify { // Add certs if ServerTLSSkipVerify is not set
		var err error
		certPool, err = getRootCAs(b.CACertPath)
		if err != nil {
			ErrorfCtx(context.Background(), "Error creating tlsConfig for DCP processing: %v", err)
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
			ErrorfCtx(context.Background(), "Error creating tlsConfig for DCP processing: %v", err)
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

type couchbaseFeedImpl struct {
	*couchbase.TapFeed
	events chan sgbucket.FeedEvent
}

var (
	versionString string
)

func (feed *couchbaseFeedImpl) Events() <-chan sgbucket.FeedEvent {
	return feed.events
}

func (feed *couchbaseFeedImpl) WriteEvents() chan<- sgbucket.FeedEvent {
	return feed.events
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

func GetBucket(spec BucketSpec) (bucket Bucket, err error) {
	if spec.IsWalrusBucket() {
		InfofCtx(context.TODO(), KeyAll, "Opening Walrus database %s on <%s>", MD(spec.BucketName), SD(spec.Server))
		sgbucket.SetLogging(ConsoleLogKey().Enabled(KeyBucket))
		bucket, err = walrus.GetBucket(spec.Server, DefaultPool, spec.BucketName)
		// If feed type is not specified (defaults to DCP) or isn't TAP, wrap with pseudo-vbucket handling for walrus
		if spec.FeedType != TapFeedType {
			bucket = &LeakyBucket{bucket: bucket, config: LeakyBucketConfig{TapFeedVbuckets: true}}
		}
	} else {

		username := ""
		if spec.Auth != nil {
			username, _, _ = spec.Auth.GetCredentials()
		}
		InfofCtx(context.TODO(), KeyAll, "%v Opening Couchbase database %s on <%s> as user %q", spec.CouchbaseDriver, MD(spec.BucketName), SD(spec.Server), UD(username))

		switch spec.CouchbaseDriver {
		case GoCB, GoCBCustomSGTranscoder:
			if strings.ToLower(spec.FeedType) == TapFeedType {
				return nil, fmt.Errorf("unsupported feed type: %v", spec.FeedType)
			} else {
				bucket, err = GetCouchbaseBucketGoCB(spec)
			}
		case GoCBv2:
			bucket, err = GetCouchbaseCollection(spec)
		default:
			return nil, fmt.Errorf("%w: unexpected CouchbaseDriver: %v", ErrFatalBucketConnection, spec.CouchbaseDriver)
		}

		if err != nil {
			return nil, err
		}

		// If XATTRS are enabled via enable_shared_bucket_access config flag, assert that Couchbase Server is 5.0
		// or later, otherwise refuse to connect to the bucket since pre 5.0 versions don't support XATTRs
		if spec.UseXattrs {
			if !bucket.IsSupported(sgbucket.DataStoreFeatureXattrs) {
				WarnfCtx(context.Background(), "If using XATTRS, Couchbase Server version must be >= 5.0.")
				return nil, ErrFatalBucketConnection
			}
		}

	}

	if LogDebugEnabled(KeyBucket) {
		bucket = &LoggingBucket{bucket: bucket}
	}
	return
}

// GetCounter returns a uint64 result for the given counter key.
// If the given key is not found in the bucket, this function returns a result of zero.
func GetCounter(bucket Bucket, k string) (result uint64, err error) {
	_, err = bucket.Get(k, &result)
	if bucket.IsError(err, sgbucket.KeyNotFoundError) {
		return 0, nil
	}
	return result, err
}

func IsKeyNotFoundError(bucket Bucket, err error) bool {

	if err == nil {
		return false
	}

	unwrappedErr := pkgerrors.Cause(err)
	return bucket.IsError(unwrappedErr, sgbucket.KeyNotFoundError)
}

func IsCasMismatch(err error) bool {
	if err == nil {
		return false
	}

	unwrappedErr := pkgerrors.Cause(err)

	// GoCB handling
	if unwrappedErr == gocbV1.ErrKeyExists || unwrappedErr == gocbV1.ErrNotStored {
		return true
	}

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
	case *CouchbaseBucketGoCB:
		if typedBucket.Spec.FeedType != "" {
			return strings.ToLower(typedBucket.Spec.FeedType)
		} else {
			return DcpFeedType
		}
	case *Collection:
		return DcpFeedType
	case *LeakyBucket:
		return GetFeedType(typedBucket.bucket)
	case *LoggingBucket:
		return GetFeedType(typedBucket.bucket)
	case *TestBucket:
		return GetFeedType(typedBucket.Bucket)
	default:
		return TapFeedType
	}
}

// Gets the bucket max TTL, or 0 if no TTL was set.  Sync gateway should fail to bring the DB online if this is non-zero,
// since it's not meant to operate against buckets that auto-delete data.
func getMaxTTL(store CouchbaseStore) (int, error) {
	var bucketResponseWithMaxTTL struct {
		MaxTTLSeconds int `json:"maxTTL,omitempty"`
	}

	uri := fmt.Sprintf("/pools/default/buckets/%s", store.GetSpec().BucketName)
	resp, err := store.mgmtRequest(http.MethodGet, uri, "application/json", nil)
	if err != nil {
		return -1, err
	}

	defer func() { _ = resp.Body.Close() }()

	respBytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return -1, err
	}

	if err := JSONUnmarshal(respBytes, &bucketResponseWithMaxTTL); err != nil {
		return -1, err
	}

	return bucketResponseWithMaxTTL.MaxTTLSeconds, nil
}

// Get the Server UUID of the bucket, this is also known as the Cluster UUID
func getServerUUID(store CouchbaseStore) (uuid string, err error) {
	resp, err := store.mgmtRequest(http.MethodGet, "/pools", "application/json", nil)
	if err != nil {
		return "", err
	}

	defer func() { _ = resp.Body.Close() }()

	respBytes, err := ioutil.ReadAll(resp.Body)
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
func getMetadataPurgeInterval(store CouchbaseStore) (time.Duration, error) {

	// Bucket-specific settings
	uri := fmt.Sprintf("/pools/default/buckets/%s", store.BucketName())
	bucketPurgeInterval, err := retrievePurgeInterval(store, uri)
	if bucketPurgeInterval > 0 || err != nil {
		return bucketPurgeInterval, err
	}

	// Cluster-wide settings
	uri = fmt.Sprintf("/settings/autoCompaction")
	clusterPurgeInterval, err := retrievePurgeInterval(store, uri)
	if clusterPurgeInterval > 0 || err != nil {
		return clusterPurgeInterval, err
	}

	return 0, nil

}

// Helper function to retrieve a Metadata Purge Interval from server and convert to hours.  Works for any uri
// that returns 'purgeInterval' as a root-level property (which includes the two server endpoints for
// bucket and server purge intervals).
func retrievePurgeInterval(bucket CouchbaseStore, uri string) (time.Duration, error) {

	// Both of the purge interval endpoints (cluster and bucket) return purgeInterval in the same way
	var purgeResponse struct {
		PurgeInterval float64 `json:"purgeInterval,omitempty"`
	}

	resp, err := bucket.mgmtRequest(http.MethodGet, uri, "application/json", nil)
	if err != nil {
		return 0, err
	}

	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode == http.StatusForbidden {
		WarnfCtx(resp.Request.Context(), "403 Forbidden attempting to access %s.  Bucket user must have Bucket Full Access and Bucket Admin roles to retrieve metadata purge interval.", UD(uri))
	} else if resp.StatusCode != http.StatusOK {
		return 0, errors.New(resp.Status)
	}

	respBytes, err := ioutil.ReadAll(resp.Body)
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

func ensureBodyClosed(body io.ReadCloser) {
	err := body.Close()
	if err != nil {
		DebugfCtx(context.TODO(), KeyBucket, "Failed to close socket: %v", err)
	}
}
