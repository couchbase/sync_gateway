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
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/couchbase/go-couchbase"
	"github.com/couchbase/go-couchbase/cbdatasource"
	"github.com/couchbase/gomemcached"
	memcached "github.com/couchbase/gomemcached/client"
	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbaselabs/walrus"
)

const (
	TapFeedType      = "tap"
	DcpFeedType      = "dcp"
	DcpShardFeedType = "dcpshard"
)

const (
	GoCouchbase CouchbaseDriver = iota
	GoCB
)

func init() {
	// Increase max memcached request size to 20M bytes, to support large docs (attachments!)
	// arriving in a tap feed. (see issues #210, #333, #342)
	gomemcached.MaxBodyLen = int(20 * 1024 * 1024)
}

type Bucket sgbucket.Bucket
type TapArguments sgbucket.TapArguments
type TapFeed sgbucket.TapFeed
type AuthHandler couchbase.AuthHandler
type CouchbaseDriver int

// Full specification of how to connect to a bucket
type BucketSpec struct {
	Server, PoolName, BucketName, FeedType string
	Auth                                   AuthHandler
	CouchbaseDriver                        CouchbaseDriver
	MaxNumRetries                          int // max number of retries before giving up
	InitialRetrySleepTimeMS                int // the initial time to sleep in between retry attempts (in millisecond), which will double each retry
}

// Implementation of sgbucket.Bucket that talks to a Couchbase server
type CouchbaseBucket struct {
	*couchbase.Bucket            // the underlying go-couchbase bucket
	spec              BucketSpec // keep a copy of the BucketSpec for DCP usage
}

type couchbaseFeedImpl struct {
	*couchbase.TapFeed
	events chan sgbucket.TapEvent
}

var (
	versionString string
)

func (feed *couchbaseFeedImpl) Events() <-chan sgbucket.TapEvent {
	return feed.events
}

func (feed *couchbaseFeedImpl) WriteEvents() chan<- sgbucket.TapEvent {
	return feed.events
}

func (bucket CouchbaseBucket) GetName() string {
	return bucket.Name
}

func (bucket CouchbaseBucket) Get(k string, v interface{}) (cas uint64, err error) {
	err = bucket.Bucket.Gets(k, v, &cas)
	return cas, err
}

func (bucket CouchbaseBucket) GetRaw(k string) (v []byte, cas uint64, err error) {
	v, _, cas, err = bucket.Bucket.GetsRaw(k)
	return v, cas, err
}

func (bucket CouchbaseBucket) Write(k string, flags int, exp int, v interface{}, opt sgbucket.WriteOptions) (err error) {
	return bucket.Bucket.Write(k, flags, exp, v, couchbase.WriteOptions(opt))
}

func (bucket CouchbaseBucket) WriteCas(k string, flags int, exp int, cas uint64, v interface{}, opt sgbucket.WriteOptions) (casOut uint64, err error) {
	return bucket.Bucket.WriteCas(k, flags, exp, cas, v, couchbase.WriteOptions(opt))
}

func (bucket CouchbaseBucket) Update(k string, exp int, callback sgbucket.UpdateFunc) error {
	return bucket.Bucket.Update(k, exp, couchbase.UpdateFunc(callback))
}

func (bucket CouchbaseBucket) SetBulk(entries []*sgbucket.BulkSetEntry) (err error) {
	panic("SetBulk not implemented")
}

func (bucket CouchbaseBucket) WriteUpdate(k string, exp int, callback sgbucket.WriteUpdateFunc) error {
	cbCallback := func(current []byte) (updated []byte, opt couchbase.WriteOptions, err error) {
		updated, walrusOpt, err := callback(current)
		opt = couchbase.WriteOptions(walrusOpt)
		return
	}
	return bucket.Bucket.WriteUpdate(k, exp, cbCallback)
}

func (bucket CouchbaseBucket) View(ddoc, name string, params map[string]interface{}) (sgbucket.ViewResult, error) {

	//Query view in retry loop backing off double the delay each time
	worker := func() (shouldRetry bool, err error, value interface{}) {

		vres := sgbucket.ViewResult{}

		err = bucket.Bucket.ViewCustom(ddoc, name, params, &vres)

		//Only retry if view Object not found as view may still be initialising
		shouldRetry = err != nil && strings.Contains(err.Error(), "404 Object Not Found")

		return shouldRetry, err, vres
	}

	sleeper := CreateDoublingSleeperFunc(
		bucket.spec.MaxNumRetries,
		bucket.spec.InitialRetrySleepTimeMS,
	)

	// Kick off retry loop
	description := fmt.Sprintf("Query View: %v", name)
	err, result := RetryLoop(description, worker, sleeper)

	if err != nil {
		return sgbucket.ViewResult{}, err
	}

	vres, ok := result.(sgbucket.ViewResult)
	if !ok {
		return vres, fmt.Errorf("Error converting view result %v to sgbucket.ViewResult", result)
	}
	return vres, err
}

func (bucket CouchbaseBucket) StartTapFeed(args sgbucket.TapArguments) (sgbucket.TapFeed, error) {

	// Uses tap by default, unless DCP is explicitly specified
	switch bucket.spec.FeedType {
	case DcpFeedType:
		return bucket.StartDCPFeed(args)

	case DcpShardFeedType:

		// CBGT initialization
		LogTo("Feed", "Starting CBGT feed?%v", bucket.GetName())

		// Create the TapEvent feed channel that will be passed back to the caller
		eventFeed := make(chan sgbucket.TapEvent, 10)

		//  - create a new SimpleFeed and pass in the eventFeed channel
		feed := &SimpleFeed{
			eventFeed: eventFeed,
		}
		return feed, nil

	default:
		LogTo("Feed", "Using TAP feed for bucket: %q (based on feed_type specified in config file", bucket.GetName())
		return bucket.StartCouchbaseTapFeed(args)

	}

}

func (bucket CouchbaseBucket) StartCouchbaseTapFeed(args sgbucket.TapArguments) (sgbucket.TapFeed, error) {
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
	events := make(chan sgbucket.TapEvent)
	tapFeed := couchbaseFeedImpl{cbFeed, events}
	go func() {
		for cbEvent := range cbFeed.C {
			events <- sgbucket.TapEvent{
				Opcode:   sgbucket.TapOpcode(cbEvent.Opcode),
				Expiry:   cbEvent.Expiry,
				Flags:    cbEvent.Flags,
				Key:      cbEvent.Key,
				Value:    cbEvent.Value,
				Sequence: cbEvent.Cas,
				VbNo:     cbEvent.VBucket,
			}
		}
	}()
	return &tapFeed, nil
}

// Start cbdatasource-based DCP feed, using DCPReceiver.
func (bucket CouchbaseBucket) StartDCPFeed(args sgbucket.TapArguments) (sgbucket.TapFeed, error) {

	// Recommended usage of cbdatasource is to let it manage it's own dedicated connection, so we're not
	// reusing the bucket connection we've already established.
	urls := []string{bucket.spec.Server}
	poolName := bucket.spec.PoolName
	if poolName == "" {
		poolName = "default"
	}
	bucketName := bucket.spec.BucketName

	vbucketIdsArr := []uint16(nil) // nil means get all the vbuckets.

	dcpReceiver := NewDCPReceiver()

	dcpReceiver.SetBucketNotifyFn(args.Notify)

	maxVbno, err := bucket.GetMaxVbno()
	if err != nil {
		return nil, err
	}

	startSeqnos := make(map[uint16]uint64, maxVbno)
	vbuuids := make(map[uint16]uint64, maxVbno)

	// GetStatsVbSeqno retrieves high sequence number for each vbucket, to enable starting
	// DCP stream from that position.  Also being used as a check on whether the server supports
	// DCP.
	statsUuids, highSeqnos, err := bucket.GetStatsVbSeqno(maxVbno, false)
	if err != nil {
		return nil, errors.New("Error retrieving stats-vbseqno - DCP not supported")
	}

	if args.Backfill == sgbucket.TapNoBackfill {
		// For non-backfill, use vbucket uuids, high sequence numbers
		LogTo("Feed+", "Seeding seqnos: %v", highSeqnos)
		vbuuids = statsUuids
		startSeqnos = highSeqnos
	}
	dcpReceiver.SeedSeqnos(vbuuids, startSeqnos)

	LogTo("Feed+", "Connecting to new bucket datasource.  URLs:%s, pool:%s, name:%s, auth:%s", urls, poolName, bucketName, bucket.spec.Auth)
	bds, err := cbdatasource.NewBucketDataSource(
		urls,
		poolName,
		bucketName,
		"",
		vbucketIdsArr,
		bucket.spec.Auth,
		dcpReceiver,
		nil,
	)
	if err != nil {
		return nil, err
	}

	events := make(chan sgbucket.TapEvent)
	dcpFeed := couchbaseDCPFeedImpl{bds, events}

	if err = bds.Start(); err != nil {
		return nil, err
	}

	go func() {
		for dcpEvent := range dcpReceiver.GetEventFeed() {
			events <- dcpEvent
		}
	}()

	return &dcpFeed, nil
}

// Goes out to the bucket and gets the high sequence number for all vbuckets and returns
// a map of UUIDS and a map of high sequence numbers (map from vbno -> seq)
func (bucket CouchbaseBucket) GetStatsVbSeqno(maxVbno uint16, useAbsHighSeqNo bool) (uuids map[uint16]uint64, highSeqnos map[uint16]uint64, seqErr error) {

	stats := bucket.Bucket.GetStats("vbucket-seqno")
	if len(stats) == 0 {
		// If vbucket-seqno map is empty, bucket doesn't support DCP
		seqErr = errors.New("vbucket-seqno call returned empty map.")
		return
	}

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

func (bucket CouchbaseBucket) GetMaxVbno() (uint16, error) {

	var maxVbno uint16
	vbsMap := bucket.Bucket.VBServerMap()
	if vbsMap == nil || vbsMap.VBucketMap == nil {
		return 0, errors.New("Error retrieving VBServerMap")
	}
	maxVbno = uint16(len(vbsMap.VBucketMap))

	return maxVbno, nil
}

func (bucket CouchbaseBucket) Dump() {
	Warn("Dump not implemented for couchbaseBucket")
}

func (bucket CouchbaseBucket) CBSVersion() (major uint64, minor uint64, micro string, err error) {

	if versionString == "" {
		stats := bucket.Bucket.GetStats("")

		for _, serverMap := range stats {
			versionString = serverMap["version"]
			// We only check the version of the first server, hopefully same for whole cluster
			break
		}
	}

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

// Creates a Bucket that talks to a real live Couchbase server.
func GetCouchbaseBucket(spec BucketSpec, callback sgbucket.BucketNotifyFn) (bucket Bucket, err error) {
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
	spec.MaxNumRetries = 10
	spec.InitialRetrySleepTimeMS = 5
	if err == nil {
		// Start bucket updater - see SG issue 1011
		cbbucket.RunBucketUpdater(func(bucket string, err error) {
			Warn("Bucket Updater for bucket %s returned error: %v", bucket, err)

			if callback != nil {
				callback(bucket, err)
			}
		})
		bucket = CouchbaseBucket{cbbucket, spec}
	}

	return
}

func GetBucket(spec BucketSpec, callback sgbucket.BucketNotifyFn) (bucket Bucket, err error) {
	if isWalrus, _ := regexp.MatchString(`^(walrus:|file:|/|\.)`, spec.Server); isWalrus {
		Logf("Opening Walrus database %s on <%s>", spec.BucketName, spec.Server)
		sgbucket.SetLogging(LogEnabled("Walrus"))
		bucket, err = walrus.GetBucket(spec.Server, spec.PoolName, spec.BucketName)
		// If feed type is specified and isn't TAP, wrap with pseudo-vbucket handling for walrus
		if spec.FeedType != "" && spec.FeedType != TapFeedType {
			bucket = &LeakyBucket{bucket: bucket, config: LeakyBucketConfig{TapFeedVbuckets: true}}
		}
	} else {

		suffix := ""
		if spec.Auth != nil {
			username, _, _ := spec.Auth.GetCredentials()
			suffix = fmt.Sprintf(" as user %q", username)
		}
		Logf("Opening Couchbase database %s on <%s>%s", spec.BucketName, spec.Server, suffix)

		if spec.CouchbaseDriver == GoCB {
			bucket, err = GetCouchbaseBucketGoCB(spec)
		} else {
			bucket, err = GetCouchbaseBucket(spec, callback)
		}

	}

	if LogEnabledExcludingLogStar("Bucket") {
		bucket = &LoggingBucket{bucket: bucket}
	}
	return
}

func WriteCasJSON(bucket Bucket, key string, value interface{}, cas uint64, exp int, callback func(v interface{}) (interface{}, error)) (casOut uint64, err error) {

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
			Warn("WriteCasJSON got error when calling Get:", err)
			return 0, err
		}
		updatedValue, err := callback(currentValue)
		if err != nil {
			Warn("WriteCasJSON got error when calling callback:", err)
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

func WriteCasRaw(bucket Bucket, key string, value []byte, cas uint64, exp int, callback func([]byte) ([]byte, error)) (casOut uint64, err error) {

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
			Warn("WriteCasRaw got error when calling GetRaw:", err)
			return 0, err
		}
		currentValue, err = callback(currentValue)
		if err != nil {
			Warn("WriteCasRaw got error when calling callback:", err)
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

	switch bucket.(type) {
	case CouchbaseBucket:
		if strings.Contains(err.Error(), "Not found") {
			return true
		}
	case CouchbaseBucketGoCB:
		if GoCBErrorType(err) == GoCBErr_MemdStatusKeyNotFound {
			return true
		}
	default:
		if _, ok := err.(sgbucket.MissingError); ok {
			return true
		}
	}

	return false

}

func IsCasMismatch(bucket Bucket, err error) bool {
	if err == nil {
		return false
	}

	switch bucket.(type) {
	case CouchbaseBucket:
		if strings.Contains(err.Error(), "CAS mismatch") {
			return true
		}
	case CouchbaseBucketGoCB:
		if GoCBErrorType(err) == GoCBErr_MemdStatusKeyExists {
			return true
		}
	default:
		if strings.Contains(err.Error(), "CAS mismatch") {
			return true
		}
	}

	return false
}
