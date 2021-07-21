//  Copyright 2013-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package base

import (
	"crypto/tls"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/couchbase/go-couchbase"
	"github.com/couchbase/gocbcore/memd"
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

// WrappingBucket interface used to identify buckets that wrap an underlying
// bucket (leaky bucket, logging bucket)
type WrappingBucket interface {
	GetUnderlyingBucket() Bucket
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
		return GoCBCustomSGTranscoder
	case IndexBucket:
		return GoCB
	default:
		// If a new bucket type is added and this method isn't updated, flag a warning (or, could panic)
		Warnf("Unexpected bucket type: %v", bucketType)
		return GoCB
	}

}

func (couchbaseDriver CouchbaseDriver) String() string {
	switch couchbaseDriver {
	case GoCB:
		return "GoCB"
	case GoCBCustomSGTranscoder:
		return "GoCBCustomSGTranscoder"
	default:
		return "UnknownCouchbaseDriver"
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
	CACertUnsetTlsSkipVerify      bool           // Use insecureSkipVerify when secure scheme (couchbases) is used and cacertpath is undefined
	KvTLSPort                     int            // Port to use for memcached over TLS.  Required for cbdatasource auth when using TLS
	MaxNumRetries                 int            // max number of retries before giving up
	InitialRetrySleepTimeMS       int            // the initial time to sleep in between retry attempts (in millisecond), which will double each retry
	UseXattrs                     bool           // Whether to use xattrs to store _sync metadata.  Used during view initialization
	ViewQueryTimeoutSecs          *uint32        // the view query timeout in seconds (default: 75 seconds)
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
	return strings.HasPrefix(spec.Server, "couchbases") || strings.HasPrefix(spec.Server, "https")
}

func (spec BucketSpec) UseClientCert() bool {
	if spec.Certpath == "" || spec.Keypath == "" {
		return false
	}
	return true
}

// Builds a gocb connection string based on BucketSpec.Server.
// Adds idle connection configuration, and X.509 auth settings when
// certpath/keypath/cacertpath specified.
func (spec *BucketSpec) GetGoCBConnString() (string, error) {

	connSpec, err := gocbconnstr.Parse(spec.Server)
	if err != nil {
		return "", err
	}

	// Increase the number of idle connections per-host to fix SG #3534
	if connSpec.Options == nil {
		connSpec.Options = map[string][]string{}
	}
	asValues := url.Values(connSpec.Options)
	asValues.Set("http_max_idle_conns_per_host", DefaultHttpMaxIdleConnsPerHost)
	asValues.Set("http_max_idle_conns", DefaultHttpMaxIdleConns)
	asValues.Set("http_idle_conn_timeout", DefaultHttpIdleConnTimeoutMilliseconds)
	asValues.Set("n1ql_timeout", fmt.Sprintf("%d", spec.GetViewQueryTimeoutMs()))

	poolSizeFromConnStr := asValues.Get("kv_pool_size")
	if poolSizeFromConnStr == "" {
		asValues.Set("kv_pool_size", DefaultGocbKvPoolSize)
		spec.KvPoolSize, _ = strconv.Atoi(DefaultGocbKvPoolSize)
	} else {
		spec.KvPoolSize, _ = strconv.Atoi(poolSizeFromConnStr)
	}
	asValues.Set("operation_tracing", "false")

	if spec.Certpath != "" && spec.Keypath != "" {
		asValues.Set("certpath", spec.Certpath)
		asValues.Set("keypath", spec.Keypath)
	}
	if spec.CACertPath != "" {
		asValues.Set("cacertpath", spec.CACertPath)
	}

	connSpec.Options = asValues
	return connSpec.String(), nil

}

func (b BucketSpec) GetViewQueryTimeout() time.Duration {
	return time.Duration(b.GetViewQueryTimeoutMs()) * time.Millisecond
}

func (b BucketSpec) GetViewQueryTimeoutMs() uint64 {
	// If the user doesn't specify any timeout, default to 75s
	if b.ViewQueryTimeoutSecs == nil {
		return 75 * 1000
	}

	// If the user specifies 0, then translate that to "No timeout"
	if *b.ViewQueryTimeoutSecs == 0 {
		return 1000 * 60 * 60 * 24 * 365 * 10 // 10 years in milliseconds
	}

	return uint64(*b.ViewQueryTimeoutSecs * 1000)
}

func (b BucketSpec) TLSConfig() *tls.Config {
	tlsConfig, err := TLSConfigForX509(b.CACertUnsetTlsSkipVerify, b.CACertPath, b.Certpath, b.Keypath)
	if err != nil {
		Errorf("Error creating tlsConfig for DCP processing: %v", err)
		return nil
	}
	return tlsConfig
}

// TLSConfigForX509 returns a TLSConfig based on the specified certificate paths.  If none are provided then the system
// root pool is used (except on Windows which will error), unless CACertUnsetTlsSkipVerify is true which will return
// tlsConfig with InsecureSkipVerify: true.
func TLSConfigForX509(caCertUnsetTlsSkipVerify bool, caCertPath, certPath, keyPath string) (tlsConfig *tls.Config, err error) {
	certPool, err := getRootCAs(caCertUnsetTlsSkipVerify, caCertPath)
	if err != nil {
		return nil, err
	}
	tlsConfig = &tls.Config{
		RootCAs:            certPool,
		InsecureSkipVerify: caCertUnsetTlsSkipVerify,
	}

	// If client cert and key are provided, add to config as x509 key pair
	if certPath != "" && keyPath != "" {
		cert, err := tls.LoadX509KeyPair(certPath, keyPath)
		if err != nil {
			return nil, err
		}

		tlsConfig.Certificates = []tls.Certificate{cert}
	}

	return tlsConfig, nil
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
		Infof(KeyAll, "Opening Walrus database %s on <%s>", MD(spec.BucketName), SD(spec.Server))
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
		Infof(KeyAll, "%v Opening Couchbase database %s on <%s> as user %q", spec.CouchbaseDriver, MD(spec.BucketName), SD(spec.Server), UD(username))

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
			panic(fmt.Sprintf("Unexpected CouchbaseDriver: %v", spec.CouchbaseDriver))
		}

		if err != nil {
			if pkgerrors.Cause(err) == gocbV1.ErrAuthError {
				Warnf("Unable to authenticate as user %q: %v", UD(username), err)
				return nil, ErrFatalBucketConnection
			}
			return nil, err
		}

		// If XATTRS are enabled via enable_shared_bucket_access config flag, assert that Couchbase Server is 5.0
		// or later, otherwise refuse to connect to the bucket since pre 5.0 versions don't support XATTRs
		if spec.UseXattrs {
			if !bucket.IsSupported(sgbucket.DataStoreFeatureXattrs) {
				Warnf("If using XATTRS, Couchbase Server version must be >= 5.0.")
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
	if unwrappedErr == gocbV1.ErrKeyExists {
		return true
	}

	// GoCB V2 handling
	if isKVError(unwrappedErr, memd.StatusKeyExists) {
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
	case *LeakyBucket:
		return GetFeedType(typedBucket.bucket)
	case *LoggingBucket:
		return GetFeedType(typedBucket.bucket)
	default:
		return TapFeedType
	}
}
