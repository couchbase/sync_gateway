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
	"fmt"
	"regexp"

	"github.com/couchbaselabs/go-couchbase"
	"github.com/couchbaselabs/walrus"
	"github.com/dustin/gomemcached"
	"github.com/dustin/gomemcached/client"
)

func init() {
	// Increase max memcached request size to 10M bytes, to support large docs (attachments!)
	// arriving in a tap feed. (see issue #210.)
	gomemcached.MaxBodyLen = int(10.0e6)
}

type Bucket walrus.Bucket
type TapArguments walrus.TapArguments
type TapFeed walrus.TapFeed
type AuthHandler couchbase.AuthHandler

// Full specification of how to connect to a bucket
type BucketSpec struct {
	Server, PoolName, BucketName string
	Auth                         AuthHandler
}

// Implementation of walrus.Bucket that talks to a Couchbase server
type couchbaseBucket struct {
	*couchbase.Bucket
}

type couchbaseFeedImpl struct {
	*couchbase.TapFeed
	events <-chan walrus.TapEvent
}

func (feed *couchbaseFeedImpl) Events() <-chan walrus.TapEvent {
	return feed.events
}

func (bucket couchbaseBucket) GetName() string {
	return bucket.Name
}

func (bucket couchbaseBucket) Write(k string, flags int, exp int, v interface{}, opt walrus.WriteOptions) (err error) {
	return bucket.Bucket.Write(k, flags, exp, v, couchbase.WriteOptions(opt))
}

func (bucket couchbaseBucket) Update(k string, exp int, callback walrus.UpdateFunc) error {
	return bucket.Bucket.Update(k, exp, couchbase.UpdateFunc(callback))
}

func (bucket couchbaseBucket) WriteUpdate(k string, exp int, callback walrus.WriteUpdateFunc) error {
	cbCallback := func(current []byte) (updated []byte, opt couchbase.WriteOptions, err error) {
		updated, walrusOpt, err := callback(current)
		opt = couchbase.WriteOptions(walrusOpt)
		return
	}
	return bucket.Bucket.WriteUpdate(k, exp, cbCallback)
}

func (bucket couchbaseBucket) View(ddoc, name string, params map[string]interface{}) (walrus.ViewResult, error) {
	vres := walrus.ViewResult{}
	return vres, bucket.Bucket.ViewCustom(ddoc, name, params, &vres)
}

func (bucket couchbaseBucket) StartTapFeed(args walrus.TapArguments) (walrus.TapFeed, error) {
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
	impl := couchbaseFeedImpl{cbFeed, events}
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
	return &impl, nil
}

func (bucket couchbaseBucket) Dump() {
	Warn("Dump not implemented for couchbaseBucket")
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
		bucket = couchbaseBucket{cbbucket}
	}
	return
}

func GetBucket(spec BucketSpec) (bucket Bucket, err error) {
	if isWalrus, _ := regexp.MatchString(`^(walrus:|file:|/|\.)`, spec.Server); isWalrus {
		Log("Opening Walrus database %s on <%s>", spec.BucketName, spec.Server)
		walrus.Logging = LogKeys["Walrus"]
		bucket, err = walrus.GetBucket(spec.Server, spec.PoolName, spec.BucketName)
	} else {
		suffix := ""
		if spec.Auth != nil {
			username, _ := spec.Auth.GetCredentials()
			suffix = fmt.Sprintf(" as user %q", username)
		}
		Log("Opening Couchbase database %s on <%s>%s", spec.BucketName, spec.Server, suffix)
		bucket, err = GetCouchbaseBucket(spec)
	}

	if LogKeys["Bucket"] {
		bucket = &LoggingBucket{bucket: bucket}
	}
	return
}
