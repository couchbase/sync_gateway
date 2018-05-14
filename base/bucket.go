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
	"errors"
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
	"github.com/couchbase/gomemcached/client"
	"github.com/couchbase/sg-bucket"
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
	GoCouchbase            CouchbaseDriver = iota
	GoCB                                   // Use GoCB driver with default Transcoder
	GoCBCustomSGTranscoder                 // Use GoCB driver with a custom Transcoder
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
	case GoCouchbase:
		return "GoCouchbase"
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

	// If the user doesn't specify any timeout, default to 75s
	if b.ViewQueryTimeoutSecs == nil {
		return time.Second * 75
	}

	// If the user specifies 0, then translate that to "No timeout"
	if *b.ViewQueryTimeoutSecs == 0 {
		return time.Hour * 24 * 7 * 365 * 10 // 10 years
	}

	return time.Duration(*b.ViewQueryTimeoutSecs) * time.Second

}

// TLSConnect method is passed to cbdatasource, to be used when creating a TLS-enabled memcached connection.
// Establishes a connection, then wraps w/ gomemcached Client for use by cbdatasource.
// Will include client cert (x.509) authentication when specified in the BucketSpec.
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

		tlsConfig := &tls.Config{}
		if b.Certpath != "" && b.Keypath != "" {
			var configErr error
			tlsConfig, configErr = TLSConfigForX509(b.Certpath, b.Keypath, b.CACertPath)
			if configErr != nil {
				return nil, pkgerrors.Wrapf(configErr, "Error adding x509 to TLSConfig for DCP TLS connection")
			}
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

func TLSConfigForX509(certpath, keypath, cacertpath string) (*tls.Config, error) {

	cacertpaths := []string{cacertpath}

	tlsConfig := &tls.Config{}

	if len(cacertpaths) > 0 {
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

	if certpath != "" && keypath != "" {
		cert, err := tls.LoadX509KeyPair(certpath, keypath)
		if err != nil {
			return nil, err
		}

		tlsConfig.Certificates = []tls.Certificate{cert}
	}
	return tlsConfig, nil
}

// Implementation of sgbucket.Bucket that talks to a Couchbase server
type CouchbaseBucket struct {
	*couchbase.Bucket            // the underlying go-couchbase bucket
	spec              BucketSpec // keep a copy of the BucketSpec for DCP usage
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

func (bucket *CouchbaseBucket) GetName() string {
	return bucket.Name
}

func (bucket *CouchbaseBucket) Add(k string, exp uint32, v interface{}) (added bool, err error) {
	return bucket.Bucket.Add(k, int(exp), v)
}

func (bucket *CouchbaseBucket) AddRaw(k string, exp uint32, v []byte) (added bool, err error) {
	return bucket.Bucket.Add(k, int(exp), v)
}

func (bucket *CouchbaseBucket) Get(k string, v interface{}) (cas uint64, err error) {
	err = bucket.Bucket.Gets(k, v, &cas)
	return cas, err
}

func (bucket *CouchbaseBucket) GetRaw(k string) (v []byte, cas uint64, err error) {
	v, _, cas, err = bucket.Bucket.GetsRaw(k)
	return v, cas, err
}

func (bucket *CouchbaseBucket) GetAndTouchRaw(k string, exp uint32) (rv []byte, cas uint64, err error) {
	return bucket.Bucket.GetAndTouchRaw(k, int(exp))
}

func (bucket *CouchbaseBucket) Set(k string, exp uint32, v interface{}) error {
	return bucket.Bucket.Set(k, int(exp), v)
}

func (bucket *CouchbaseBucket) SetRaw(k string, exp uint32, v []byte) error {
	return bucket.Bucket.SetRaw(k, int(exp), v)
}

func (bucket *CouchbaseBucket) Incr(k string, amt, def uint64, exp uint32) (uint64, error) {
	return bucket.Bucket.Incr(k, amt, def, int(exp))
}

func (bucket *CouchbaseBucket) Write(k string, flags int, exp uint32, v interface{}, opt sgbucket.WriteOptions) (err error) {
	return bucket.Bucket.Write(k, flags, int(exp), v, couchbase.WriteOptions(opt))
}

func (bucket *CouchbaseBucket) WriteCas(k string, flags int, exp uint32, cas uint64, v interface{}, opt sgbucket.WriteOptions) (casOut uint64, err error) {
	return bucket.Bucket.WriteCas(k, flags, int(exp), cas, v, couchbase.WriteOptions(opt))
}

func (bucket *CouchbaseBucket) Update(k string, exp uint32, callback sgbucket.UpdateFunc) error {
	cbCallback := func(current []byte) (updated []byte, err error) {
		updated, _, err = callback(current)
		return updated, err
	}
	return bucket.Bucket.Update(k, int(exp), cbCallback)
}

func (bucket *CouchbaseBucket) Remove(k string, cas uint64) (casOut uint64, err error) {
	Warnf(KeyAll, "CouchbaseBucket doesn't support cas-safe removal - handling as simple delete")
	return 0, bucket.Bucket.Delete(k)

}
func (bucket *CouchbaseBucket) SetBulk(entries []*sgbucket.BulkSetEntry) (err error) {
	panic("SetBulk not implemented")
}

func (bucket *CouchbaseBucket) WriteCasWithXattr(k string, xattr string, exp uint32, cas uint64, v interface{}, xv interface{}) (casOut uint64, err error) {
	Warnf(KeyAll, "WriteCasWithXattr not implemented by CouchbaseBucket")
	return 0, errors.New("WriteCasWithXattr not implemented by CouchbaseBucket")
}

func (bucket *CouchbaseBucket) GetWithXattr(k string, xattr string, rv interface{}, xv interface{}) (cas uint64, err error) {
	Warnf(KeyAll, "GetWithXattr not implemented by CouchbaseBucket")
	return 0, errors.New("GetWithXattr not implemented by CouchbaseBucket")
}

func (bucket *CouchbaseBucket) DeleteWithXattr(k string, xattr string) error {
	Warnf(KeyAll, "DeleteWithXattr not implemented by CouchbaseBucket")
	return errors.New("DeleteWithXattr not implemented by CouchbaseBucket")
}

func (bucket *CouchbaseBucket) WriteUpdate(k string, exp uint32, callback sgbucket.WriteUpdateFunc) error {
	cbCallback := func(current []byte) (updated []byte, opt couchbase.WriteOptions, err error) {
		updated, walrusOpt, _, err := callback(current)
		opt = couchbase.WriteOptions(walrusOpt)
		return
	}
	return bucket.Bucket.WriteUpdate(k, int(exp), cbCallback)
}

func (bucket *CouchbaseBucket) WriteUpdateWithXattr(k string, xattr string, exp uint32, previous *sgbucket.BucketDocument, callback sgbucket.WriteUpdateWithXattrFunc) (casOut uint64, err error) {
	Warnf(KeyAll, "WriteUpdateWithXattr not implemented by CouchbaseBucket")
	return 0, errors.New("WriteUpdateWithXattr not implemented by CouchbaseBucket")
}

func (bucket *CouchbaseBucket) View(ddoc, name string, params map[string]interface{}) (sgbucket.ViewResult, error) {

	//Query view in retry loop backing off double the delay each time
	worker := func() (shouldRetry bool, err error, value interface{}) {

		vres := sgbucket.ViewResult{}

		err = bucket.Bucket.ViewCustom(ddoc, name, params, &vres)

		//Only retry if view Object not found as view may still be initialising
		shouldRetry = err != nil && strings.Contains(err.Error(), "404 Object Not Found")

		return shouldRetry, err, vres
	}

	// Kick off retry loop
	description := fmt.Sprintf("Query View: %v", name)
	err, result := RetryLoop(description, worker, bucket.spec.RetrySleeper())

	if err != nil {
		return sgbucket.ViewResult{}, err
	}

	vres, ok := result.(sgbucket.ViewResult)
	if !ok {
		return vres, RedactErrorf("Error converting view result %v to sgbucket.ViewResult", UD(result))
	}
	return vres, err
}

func (bucket *CouchbaseBucket) ViewQuery(ddoc, name string, params map[string]interface{}) (sgbucket.QueryResultIterator, error) {
	result, err := bucket.View(ddoc, name, params)
	return &result, err
}

func (bucket *CouchbaseBucket) StartTapFeed(args sgbucket.FeedArguments) (sgbucket.MutationFeed, error) {

	cbArgs := memcached.TapArguments{
		Backfill: args.Backfill,
		Dump:     args.Dump,
		KeysOnly: args.KeysOnly,
	}
	cbFeed, err := bucket.Bucket.StartTapFeed(&cbArgs)
	if err != nil {
		return nil, err
	}

	// Create a bridge from the Couchbase tap feed to a Sgbucket tap feed:
	events := make(chan sgbucket.FeedEvent)
	tapFeed := couchbaseFeedImpl{cbFeed, events}
	go func() {
		for cbEvent := range cbFeed.C {
			events <- sgbucket.FeedEvent{
				Opcode: sgbucket.FeedOpcode(cbEvent.Opcode),
				Expiry: cbEvent.Expiry,
				Flags:  cbEvent.Flags,
				Key:    cbEvent.Key,
				Value:  cbEvent.Value,
				Cas:    cbEvent.Cas,
				VbNo:   cbEvent.VBucket,
			}
		}
	}()
	return &tapFeed, nil
}

func (bucket *CouchbaseBucket) StartDCPFeed(args sgbucket.FeedArguments, callback sgbucket.FeedEventCallbackFunc) error {
	return StartDCPFeed(bucket, bucket.spec, args, callback)
}

// Goes out to the bucket and gets the high sequence number for all vbuckets and returns
// a map of UUIDS and a map of high sequence numbers (map from vbno -> seq)
func (bucket *CouchbaseBucket) GetStatsVbSeqno(maxVbno uint16, useAbsHighSeqNo bool) (uuids map[uint16]uint64, highSeqnos map[uint16]uint64, seqErr error) {

	stats := bucket.Bucket.GetStats("vbucket-seqno")
	if len(stats) == 0 {
		// If vbucket-seqno map is empty, bucket doesn't support DCP
		seqErr = errors.New("vbucket-seqno call returned empty map.")
		return
	}

	return GetStatsVbSeqno(stats, maxVbno, useAbsHighSeqNo)

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
			if err == nil && highSeqno > 0 {
				highSeqnos[i] = highSeqno
				uuid, err := strconv.ParseUint(serverMap[uuidKey], 10, 64)
				if err == nil {
					uuids[i] = uuid
				}
			}
		}
		// We're only using a single server, so can break after the first entry in the map.
		break
	}
	return

}

func (bucket *CouchbaseBucket) GetMaxVbno() (uint16, error) {

	var maxVbno uint16
	vbsMap := bucket.Bucket.VBServerMap()
	if vbsMap == nil || vbsMap.VBucketMap == nil {
		return 0, errors.New("Error retrieving VBServerMap")
	}
	maxVbno = uint16(len(vbsMap.VBucketMap))

	return maxVbno, nil
}

func (bucket *CouchbaseBucket) Dump() {
	Warnf(KeyAll, "Dump not implemented for couchbaseBucket")
}

func (bucket *CouchbaseBucket) CouchbaseServerVersion() (major uint64, minor uint64, micro string, err error) {

	if versionString == "" {
		stats := bucket.Bucket.GetStats("")

		for _, serverMap := range stats {
			versionString = serverMap["version"]
			// We only check the version of the first server, hopefully same for whole cluster
			break
		}
	}

	return ParseCouchbaseServerVersion(versionString)

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

func ParseCouchbaseServerVersion(versionString string) (major uint64, minor uint64, micro string, err error) {

	if versionString == "" {
		return 0, 0, "", errors.New("version not defined in GetStats map")
	}

	arr := strings.SplitN(versionString, ".", 4)

	major, err = strconv.ParseUint(arr[0], 10, 8)

	if err != nil {
		return 0, 0, "", errors.New("Unable to parse version major component ")
	}
	minor, err = strconv.ParseUint(arr[1], 10, 8)

	if err != nil {
		return 0, 0, "", errors.New("Unable to parse version minor component ")
	}

	micro = arr[2]

	return

}

func (bucket *CouchbaseBucket) ErrorIfPostCBServerMajorVersion(lastMajorVersionAllowed uint64) error {
	major, _, _, err := bucket.CouchbaseServerVersion()
	if err != nil {
		return err
	}

	if major > lastMajorVersionAllowed {
		Warnf(KeyAll, "Couchbase Server major version is %v, which is later than %v", major, lastMajorVersionAllowed)
		return ErrFatalBucketConnection
	}
	return nil
}

func (bucket *CouchbaseBucket) UUID() (string, error) {
	return bucket.Bucket.UUID, nil
}

// Creates a Bucket that talks to a real live Couchbase server.
func GetCouchbaseBucket(spec BucketSpec, callback sgbucket.BucketNotifyFn) (bucket *CouchbaseBucket, err error) {
	client, err := couchbase.ConnectWithAuth(spec.Server, spec.Auth)
	if err != nil {
		return
	}
	poolName := spec.PoolName
	if poolName == "" {
		poolName = "default"
	}
	pool, err := client.GetPool(poolName)
	if err != nil {
		return
	}
	cbbucket, err := pool.GetBucket(spec.BucketName)
	if err != nil {
		return nil, err
	}

	bucket = &CouchbaseBucket{cbbucket, spec}

	if spec.FeedType == TapFeedType {
		// TAP was removed in Couchbase Server 5.x, so ensure connecting to 4.x, else error.  See SG Issue #2523
		if errVersionCheck := bucket.ErrorIfPostCBServerMajorVersion(4); errVersionCheck != nil {
			return nil, errVersionCheck
		}
	}

	spec.MaxNumRetries = 10
	spec.InitialRetrySleepTimeMS = 5

	// Start bucket updater - see SG issue 1011
	cbbucket.RunBucketUpdater(func(bucket string, err error) {
		Warnf(KeyAll, "Bucket Updater for bucket %s returned error: %v", MD(bucket), err)

		if callback != nil {
			callback(bucket, err)
		}
	})

	return
}

func GetBucket(spec BucketSpec, callback sgbucket.BucketNotifyFn) (bucket Bucket, err error) {
	if isWalrus, _ := regexp.MatchString(`^(walrus:|file:|/|\.)`, spec.Server); isWalrus {
		Infof(KeyAll, "Opening Walrus database %s on <%s>", MD(spec.BucketName), SD(spec.Server))
		sgbucket.SetLogging(LogEnabled("Walrus"))
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
				Warnf(KeyAll, "Cannot use TAP feed in conjunction with GoCB driver, reverting to go-couchbase")
				bucket, err = GetCouchbaseBucket(spec, callback)
			} else {
				bucket, err = GetCouchbaseBucketGoCB(spec)
			}

		case GoCouchbase:
			bucket, err = GetCouchbaseBucket(spec, callback)
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

	switch bucket.(type) {
	case *CouchbaseBucket:
		if strings.Contains(unwrappedErr.Error(), "Not found") {
			return true
		}
	default:
		if _, ok := unwrappedErr.(sgbucket.MissingError); ok {
			return true
		}
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
	case *CouchbaseBucket:
		if typedBucket.spec.FeedType != "" {
			return strings.ToLower(typedBucket.spec.FeedType)
		} else {
			return DcpFeedType
		}
	case *CouchbaseBucketGoCB:
		if typedBucket.spec.FeedType != "" {
			return strings.ToLower(typedBucket.spec.FeedType)
		} else {
			return DcpFeedType
		}
	case *LeakyBucket:
		return GetFeedType(typedBucket.bucket)
	default:
		return TapFeedType
	}
}
