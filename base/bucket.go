//  Copyright (c) 2013 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package base

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"
	"net"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/couchbase/go-couchbase"
	"github.com/couchbase/gocb"
	"github.com/couchbase/gomemcached"
	memcached "github.com/couchbase/gomemcached/client"
	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbaselabs/gocbconnstr"
	"github.com/couchbaselabs/walrus"
	pkgerrors "github.com/pkg/errors"
)

const (
	TapFeedType      = "tap"
	DcpFeedType      = "dcp"
	DcpShardFeedType = "dcpshard"
)

const (
	GoCB                   CouchbaseDriver = iota // Use GoCB driver with default Transcoder
	GoCBCustomSGTranscoder                        // Use GoCB driver with a custom Transcoder
)

const (
	DataBucket CouchbaseBucketType = iota
	IndexBucket
	ShadowBucket
)

func ChooseCouchbaseDriver(bucketType CouchbaseBucketType) CouchbaseDriver {

	// Otherwise use the default driver for the bucket type
	// return DefaultDriverForBucketType[bucketType]
	switch bucketType {
	case DataBucket:
		return GoCBCustomSGTranscoder
	case IndexBucket:
		return GoCB
	case ShadowBucket:
		return GoCB // NOTE: should work against against both GoCouchbase and GoCB
	default:
		// If a new bucket type is added and this method isn't updated, flag a warning (or, could panic)
		Warnf(KeyAll, "Unexpected bucket type: %v", bucketType)
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

// TODO: unalias these and just pass around sgbucket.X everywhere
type Bucket sgbucket.Bucket
type FeedArguments sgbucket.FeedArguments
type TapFeed sgbucket.MutationFeed

type AuthHandler couchbase.AuthHandler
type CouchbaseDriver int
type CouchbaseBucketType int

// Full specification of how to connect to a bucket
type BucketSpec struct {
	Server, PoolName, BucketName, FeedType string
	Auth                                   AuthHandler
	CouchbaseDriver                        CouchbaseDriver
	Certpath, Keypath, CACertPath          string         // X.509 auth parameters
	KvTLSPort                              int            // Port to use for memcached over TLS.  Required for cbdatasource auth when using TLS
	MaxNumRetries                          int            // max number of retries before giving up
	InitialRetrySleepTimeMS                int            // the initial time to sleep in between retry attempts (in millisecond), which will double each retry
	UseXattrs                              bool           // Whether to use xattrs to store _sync metadata.  Used during view initialization
	ViewQueryTimeoutSecs                   *uint32        // the view query timeout in seconds (default: 75 seconds)
	BucketOpTimeout                        *time.Duration // How long bucket ops should block returning "operation timed out". If nil, uses GoCB default.  GoCB buckets only.
}

// Create a RetrySleeper based on the bucket spec properties.  Used to retry bucket operations after transient errors.
func (spec BucketSpec) RetrySleeper() RetrySleeper {
	return CreateDoublingSleeperFunc(spec.MaxNumRetries, spec.InitialRetrySleepTimeMS)
}

func (spec BucketSpec) IsWalrusBucket() bool {
	return strings.Contains(spec.Server, "walrus:")
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
func (spec BucketSpec) GetGoCBConnString() (string, error) {

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

// TLSConnect method is passed to cbdatasource, to be used when creating a TLS-enabled memcached connection.
// Establishes a connection, then wraps w/ gomemcached Client for use by cbdatasource.
// Will include client cert (x.509) authentication when specified in the BucketSpec.
// Adheres to approach used by gocb - can be removed once SG switches to gocb's DCP client.
func (b BucketSpec) TLSConnect(prot, dest string) (rv *memcached.Client, err error) {

	d := net.Dialer{
		Deadline: time.Now().Add(30 * time.Second),
	}

	host, port, err := SplitHostPort(dest)
	if err != nil {
		return nil, err
	}

	port = fmt.Sprintf("%d", b.KvTLSPort)
	dest = host + ":" + port

	Infof(KeyAll, "Establishing TLS connection for DCP to destination %s", dest)

	conn, err := d.Dial("tcp", dest)
	if err != nil {
		return nil, err
	}

	tcpConn, isTcpConn := conn.(*net.TCPConn)
	if !isTcpConn {
		return nil, fmt.Errorf("Unable to convert connection to TCPConn during DCP TLS connection (Connection type:%T)", conn)
	} else {
		err = tcpConn.SetNoDelay(false)
		if err != nil {
			return nil, pkgerrors.Wrapf(err, "Error setting NoDelay on tcpConn during TLS Connect")
		}

		tlsConfig, configErr := TLSConfigForX509(b.Certpath, b.Keypath, b.CACertPath)
		if configErr != nil {
			return nil, pkgerrors.Wrapf(configErr, "Error creating TLSConfig for DCP TLS connection")
		}

		tlsConfig.ServerName = host

		tlsConn := tls.Client(tcpConn, tlsConfig)
		tlsErr := tlsConn.Handshake()
		if tlsErr != nil {
			return nil, pkgerrors.Wrapf(tlsErr, "TLS handshake failed while establishing DCP TLS connection")
		}
		return memcached.Wrap(tlsConn)
	}

}

// Returns a TLSConfig based on the specified certificate paths.  If none are provided, returns tlsConfig with
// InsecureSkipVerify:true.
func TLSConfigForX509(certpath, keypath, cacertpath string) (*tls.Config, error) {

	tlsConfig := &tls.Config{}
	if cacertpath != "" {
		cacertpaths := []string{cacertpath}
		rootCerts := x509.NewCertPool()
		for _, path := range cacertpaths {
			cacert, err := ioutil.ReadFile(path)
			if err != nil {
				return nil, err
			}
			ok := rootCerts.AppendCertsFromPEM(cacert)
			if !ok {
				return nil, fmt.Errorf("can't append certs from PEM")
			}
		}
		tlsConfig.RootCAs = rootCerts
	} else {
		// A root CA cert is required in order to secure TLS communication from a client that doesn't maintain it's own store
		// of trusted CA certs (i.e. any SDK-based client).  If a root CA cert isn't provided, set InsecureSkipVerify=true to
		// accept any cert provided by the server. This follows the pattern being used by the SDK for TLS connections:
		// https://github.com/couchbase/gocbcore/blob/7b68c492c29f3f952a00a4ba97dac14cc4b2b57e/agent.go#L236
		tlsConfig.InsecureSkipVerify = true
	}

	// If client cert and key are provided, add to config as x509 key pair
	if certpath != "" && keypath != "" {
		cert, err := tls.LoadX509KeyPair(certpath, keypath)
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

func IsMinimumServerVersion(bucket Bucket, minMajor uint64, minMinor uint64) (bool, error) {

	major, minor, _, err := bucket.CouchbaseServerVersion()
	if err != nil {
		return false, err
	}

	return isMinimumVersion(major, minor, minMajor, minMinor), nil

}

// Crc32c macro expansion is used to avoid conflicting with the Couchbase Eventing module, which also uses XATTRS.
// Since Couchbase Eventing was introduced in Couchbase Server 5.5, the Crc32c macro expansion only needs to be done on 5.5 or later.
func IsCrc32cMacroExpansionSupported(bucket Bucket) (bool, error) {
	return IsMinimumServerVersion(bucket, 5, 5)
}

func GetBucket(spec BucketSpec, callback sgbucket.BucketNotifyFn) (bucket Bucket, err error) {
	if isWalrus, _ := regexp.MatchString(`^(walrus:|file:|/|\.)`, spec.Server); isWalrus {
		Infof(KeyAll, "Opening Walrus database %s on <%s>", MD(spec.BucketName), SD(spec.Server))
		sgbucket.SetLogging(ConsoleLogKey().Enabled(KeyBucket))
		bucket, err = walrus.GetBucket(spec.Server, spec.PoolName, spec.BucketName)
		// If feed type is not specified (defaults to DCP) or isn't TAP, wrap with pseudo-vbucket handling for walrus
		if spec.FeedType == "" || spec.FeedType != TapFeedType {
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

		default:
			panic(fmt.Sprintf("Unexpected CouchbaseDriver: %v", spec.CouchbaseDriver))
		}

		if err != nil {
			if pkgerrors.Cause(err) == gocb.ErrAuthError {
				Warnf(KeyAll, "Unable to authenticate as user %q: %v", UD(username), err)
				return nil, ErrFatalBucketConnection
			}
			return nil, err
		}

		// If XATTRS are enabled via enable_shared_bucket_access config flag, assert that Couchbase Server is 5.0
		// or later, otherwise refuse to connect to the bucket since pre 5.0 versions don't support XATTRs
		if spec.UseXattrs {
			xattrsSupported, errServerVersion := IsMinimumServerVersion(bucket, 5, 0)
			if errServerVersion != nil {
				return nil, errServerVersion
			}
			if !xattrsSupported {
				Warnf(KeyAll, "If using XATTRS, Couchbase Server version must be >= 5.0.")
				return nil, ErrFatalBucketConnection
			}
		}

	}

	if LogDebugEnabled(KeyBucket) {
		bucket = &LoggingBucket{bucket: bucket}
	}
	return
}

func WriteCasJSON(bucket Bucket, key string, value interface{}, cas uint64, exp uint32, callback func(v interface{}) (interface{}, error)) (casOut uint64, err error) {

	// If there's an incoming value, attempt to write with that first
	if value != nil {
		casOut, err := bucket.WriteCas(key, 0, exp, cas, value, 0)
		if err == nil {
			return casOut, nil
		}
	}

	for {
		var currentValue interface{}
		cas, err := bucket.Get(key, &currentValue)
		if err != nil {
			Warnf(KeyAll, "WriteCasJSON got error when calling Get: %v", err)
			return 0, err
		}
		updatedValue, err := callback(currentValue)
		if err != nil {
			Warnf(KeyAll, "WriteCasJSON got error when calling callback: %v", err)
			return 0, err
		}
		if updatedValue == nil {
			// callback returned empty value - cancel write
			return cas, nil
		}
		casOut, err := bucket.WriteCas(key, 0, exp, cas, updatedValue, 0)
		if err != nil {
			// CAS failure - reload block for another try
		} else {
			return casOut, nil
		}
	}
}

func WriteCasRaw(bucket Bucket, key string, value []byte, cas uint64, exp uint32, callback func([]byte) ([]byte, error)) (casOut uint64, err error) {

	// If there's an incoming value, attempt to write with that first
	if len(value) > 0 {
		casOut, err := bucket.WriteCas(key, 0, exp, cas, value, sgbucket.Raw)
		if err == nil {
			return casOut, nil
		}
	}

	for {
		currentValue, cas, err := bucket.GetRaw(key)
		if err != nil {
			Warnf(KeyAll, "WriteCasRaw got error when calling GetRaw: %v", err)
			return 0, err
		}
		currentValue, err = callback(currentValue)
		if err != nil {
			Warnf(KeyAll, "WriteCasRaw got error when calling callback: %v", err)
			return 0, err
		}
		if len(currentValue) == 0 {
			// callback returned empty value - cancel write
			return cas, nil
		}
		casOut, err := bucket.WriteCas(key, 0, exp, cas, currentValue, sgbucket.Raw)
		if err != nil {
			// CAS failure - reload block for another try
		} else {
			return casOut, nil
		}
	}
}

func IsKeyNotFoundError(bucket Bucket, err error) bool {

	if err == nil {
		return false
	}

	unwrappedErr := pkgerrors.Cause(err)

	if unwrappedErr == gocb.ErrKeyNotFound {
		return true
	}

	if _, ok := unwrappedErr.(sgbucket.MissingError); ok {
		return true
	}

	return false

}

func IsCasMismatch(err error) bool {
	if err == nil {
		return false
	}

	unwrappedErr := pkgerrors.Cause(err)

	// GoCB handling
	if unwrappedErr == gocb.ErrKeyExists {
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
		if typedBucket.spec.FeedType != "" {
			return strings.ToLower(typedBucket.spec.FeedType)
		} else {
			return DcpFeedType
		}
	case *LeakyBucket:
		return GetFeedType(typedBucket.bucket)
	case *LoggingBucket:
		return GetFeedType(typedBucket.bucket)
	case *StatsBucket:
		return GetFeedType(typedBucket.bucket)
	default:
		return TapFeedType
	}
}
