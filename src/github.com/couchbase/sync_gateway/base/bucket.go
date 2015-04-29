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
	"github.com/couchbase/gomemcached/client"
	"github.com/couchbaselabs/walrus"
)

const TapFeedType = "tap"
const DcpFeedType = "dcp"

func init() {
	// Increase max memcached request size to 20M bytes, to support large docs (attachments!)
	// arriving in a tap feed. (see issues #210, #333, #342)
	gomemcached.MaxBodyLen = int(20 * 1024 * 1024)
}

type Bucket walrus.Bucket
type TapArguments walrus.TapArguments
type TapFeed walrus.TapFeed
type AuthHandler couchbase.AuthHandler

// Full specification of how to connect to a bucket
type BucketSpec struct {
	Server, PoolName, BucketName, FeedType string
	Auth                                   AuthHandler
}

// Implementation of walrus.Bucket that talks to a Couchbase server
type CouchbaseBucket struct {
	*couchbase.Bucket
	spec BucketSpec // keep a copy of the BucketSpec for DCP usage
}

type couchbaseFeedImpl struct {
	*couchbase.TapFeed
	events <-chan walrus.TapEvent
}

var versionString string

func (feed *couchbaseFeedImpl) Events() <-chan walrus.TapEvent {
	return feed.events
}

func (bucket CouchbaseBucket) GetName() string {
	return bucket.Name
}

func (bucket CouchbaseBucket) Write(k string, flags int, exp int, v interface{}, opt walrus.WriteOptions) (err error) {
	return bucket.Bucket.Write(k, flags, exp, v, couchbase.WriteOptions(opt))
}

func (bucket CouchbaseBucket) Update(k string, exp int, callback walrus.UpdateFunc) error {
	return bucket.Bucket.Update(k, exp, couchbase.UpdateFunc(callback))
}

func (bucket CouchbaseBucket) WriteUpdate(k string, exp int, callback walrus.WriteUpdateFunc) error {
	cbCallback := func(current []byte) (updated []byte, opt couchbase.WriteOptions, err error) {
		updated, walrusOpt, err := callback(current)
		opt = couchbase.WriteOptions(walrusOpt)
		return
	}
	return bucket.Bucket.WriteUpdate(k, exp, cbCallback)
}

func (bucket CouchbaseBucket) View(ddoc, name string, params map[string]interface{}) (walrus.ViewResult, error) {
	vres := walrus.ViewResult{}
	return vres, bucket.Bucket.ViewCustom(ddoc, name, params, &vres)
}

func (bucket CouchbaseBucket) StartTapFeed(args walrus.TapArguments) (walrus.TapFeed, error) {
	// Uses tap by default, unless DCP is explicitly specified
	if bucket.spec.FeedType == DcpFeedType {
		feed, err := bucket.StartDCPFeed(args)
		if err != nil {
			Warn("Unable to start DCP feed - reverting to using TAP feed: %s", err)
			return bucket.StartCouchbaseTapFeed(args)
		}
		LogTo("Feed", "Using DCP feed for bucket: %q", bucket.GetName())
		return feed, nil
	} else {
		LogTo("Feed", "Using TAP feed for bucket: %q (based on feed_type specified in config file", bucket.GetName())
		return bucket.StartCouchbaseTapFeed(args)
	}
}

func (bucket CouchbaseBucket) StartCouchbaseTapFeed(args walrus.TapArguments) (walrus.TapFeed, error) {
	cbArgs := memcached.TapArguments{
		Backfill: args.Backfill,
		Dump:     args.Dump,
		KeysOnly: args.KeysOnly,
	}
	cbFeed, err := bucket.Bucket.StartTapFeed(&cbArgs)
	if err != nil {
		return nil, err
	}

	// Create a bridge from the Couchbase tap feed to a Walrus tap feed:
	events := make(chan walrus.TapEvent)
	tapFeed := couchbaseFeedImpl{cbFeed, events}
	go func() {
		for cbEvent := range cbFeed.C {
			events <- walrus.TapEvent{
				Opcode:   walrus.TapOpcode(cbEvent.Opcode),
				Expiry:   cbEvent.Expiry,
				Flags:    cbEvent.Flags,
				Key:      cbEvent.Key,
				Value:    cbEvent.Value,
				Sequence: cbEvent.Cas,
			}
		}
	}()
	return &tapFeed, nil
}

// Start cbdatasource-based DCP feed, using DCPReceiver.
func (bucket CouchbaseBucket) StartDCPFeed(args walrus.TapArguments) (walrus.TapFeed, error) {

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

	maxVbno, err := bucket.getMaxVbno()
	if err != nil {
		return nil, err
	}

	startSeqnos := make(map[uint16]uint64, maxVbno)
	vbuuids := make(map[uint16]uint64, maxVbno)

	// GetStatsVbSeqno retrieves high sequence number for each vbucket, to enable starting
	// DCP stream from that position.  Also being used as a check on whether the server supports
	// DCP.
	statsUuids, highSeqnos, err := bucket.GetStatsVbSeqno(maxVbno)
	if err != nil {
		return nil, errors.New("Error retrieving stats-vbseqno - DCP not supported")
	}

	if args.Backfill == walrus.TapNoBackfill {
		// For non-backfill, use vbucket uuids, high sequence numbers
		LogTo("Feed+", "Seeding seqnos: %v", highSeqnos)
		vbuuids = statsUuids
		startSeqnos = highSeqnos
	}
	dcpReceiver.SeedSeqnos(vbuuids, startSeqnos)

	auth := bucket.getDcpAuthHandler()

	LogTo("Feed+", "Connecting to new bucket datasource.  URLs:%s, pool:%s, name:%s, auth:%s", urls, poolName, bucketName, auth)
	bds, err := cbdatasource.NewBucketDataSource(
		urls,
		poolName,
		bucketName,
		"",
		vbucketIdsArr,
		auth,
		dcpReceiver,
		nil,
	)
	if err != nil {
		return nil, err
	}

	events := make(chan walrus.TapEvent)
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

func (bucket CouchbaseBucket) getDcpAuthHandler() couchbase.AuthHandler {

	// Auth handler for DCP connection needs to support bucket name defaults
	username, password := "", ""
	if bucket.spec.Auth != nil {
		username, password, _ = bucket.spec.Auth.GetCredentials()
	}
	return &dcpAuth{username, password, bucket.spec.BucketName}
}

func (bucket CouchbaseBucket) GetStatsVbSeqno(maxVbno uint16) (uuids map[uint16]uint64, highSeqnos map[uint16]uint64, seqErr error) {

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
			highSeqnoKey := fmt.Sprintf("vb_%d:high_seqno", i)

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

func (bucket CouchbaseBucket) getMaxVbno() (uint16, error) {

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

	LogTo("CRUD+", "major = %i, minor = %i, micro = %s", major, minor, micro)

	return
}

// Creates a Bucket that talks to a real live Couchbase server.
func GetCouchbaseBucket(spec BucketSpec) (bucket Bucket, err error) {
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
	if err == nil {
		bucket = CouchbaseBucket{cbbucket, spec}
	}

	return
}

func GetBucket(spec BucketSpec) (bucket Bucket, err error) {
	if isWalrus, _ := regexp.MatchString(`^(walrus:|file:|/|\.)`, spec.Server); isWalrus {
		Logf("Opening Walrus database %s on <%s>", spec.BucketName, spec.Server)
		walrus.Logging = LogKeys["Walrus"]
		bucket, err = walrus.GetBucket(spec.Server, spec.PoolName, spec.BucketName)
	} else {
		suffix := ""
		if spec.Auth != nil {
			username, _, _ := spec.Auth.GetCredentials()
			suffix = fmt.Sprintf(" as user %q", username)
		}
		Logf("Opening Couchbase database %s on <%s>%s", spec.BucketName, spec.Server, suffix)
		bucket, err = GetCouchbaseBucket(spec)
	}

	if LogKeys["Bucket"] {
		bucket = &LoggingBucket{bucket: bucket}
	}
	return
}
