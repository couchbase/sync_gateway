/*
Copyright 2016-Present Couchbase, Inc.
Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package base

import (
	"context"
	"expvar"
	"fmt"

	sgbucket "github.com/couchbase/sg-bucket"
)

var _ Bucket = &LeakyBucket{}

// A wrapper around a Bucket to support forced errors.  For testing use only.
type LeakyBucket struct {
	bucket      Bucket
	config      *LeakyBucketConfig
	collections map[string]*LeakyDataStore
}

var _ sgbucket.BucketStore = &LeakyBucket{}
var _ sgbucket.DynamicDataStoreBucket = &LeakyBucket{}

func NewLeakyBucket(bucket Bucket, config LeakyBucketConfig) *LeakyBucket {
	return &LeakyBucket{
		bucket:      bucket,
		config:      &config,
		collections: make(map[string]*LeakyDataStore),
	}
}

func (b *LeakyBucket) GetName() string {
	return b.bucket.GetName()
}

func (b *LeakyBucket) UUID() (string, error) {
	return b.bucket.UUID()
}

func (b *LeakyBucket) Close(ctx context.Context) {
	if !b.config.IgnoreClose {
		b.bucket.Close(ctx)
	}
}

// For walrus handling, ignore close needs to be set after the bucket is initialized
func (b *LeakyBucket) SetIgnoreClose(value bool) {
	b.config.IgnoreClose = value
}

func (b *LeakyBucket) CloseAndDelete(ctx context.Context) error {
	if bucket, ok := b.bucket.(sgbucket.DeleteableStore); ok {
		return bucket.CloseAndDelete(ctx)
	}
	return nil
}

func (b *LeakyBucket) IsSupported(feature sgbucket.BucketStoreFeature) bool {
	return b.bucket.IsSupported(feature)
}

func (b *LeakyBucket) GetMaxVbno() (uint16, error) {
	return b.bucket.GetMaxVbno()
}

func (b *LeakyBucket) DefaultDataStore() sgbucket.DataStore {
	return NewLeakyDataStore(b, b.bucket.DefaultDataStore(), b.config)
}

func (b *LeakyBucket) ListDataStores() ([]sgbucket.DataStoreName, error) {
	return b.bucket.ListDataStores()
}

func (b *LeakyBucket) NamedDataStore(name sgbucket.DataStoreName) (sgbucket.DataStore, error) {
	dataStore, err := b.bucket.NamedDataStore(name)
	if err != nil {
		return nil, err
	}
	return NewLeakyDataStore(b, dataStore, b.config), nil
}

func (b *LeakyBucket) GetUnderlyingBucket() Bucket {
	return b.bucket
}

func (b *LeakyBucket) CreateDataStore(ctx context.Context, name sgbucket.DataStoreName) error {
	dynamicDataStore, ok := b.GetUnderlyingBucket().(sgbucket.DynamicDataStoreBucket)
	if !ok {
		return fmt.Errorf("Bucket %T doesn't support dynamic collection creation", b.GetUnderlyingBucket())
	}
	return dynamicDataStore.CreateDataStore(ctx, name)
}

func (b *LeakyBucket) DropDataStore(name sgbucket.DataStoreName) error {
	dynamicDataStore, ok := b.GetUnderlyingBucket().(sgbucket.DynamicDataStoreBucket)
	if !ok {
		return fmt.Errorf("Bucket %T doesn't support dynamic collection creation", b.GetUnderlyingBucket())
	}
	return dynamicDataStore.DropDataStore(name)
}

// The config object that controls the LeakyBucket behavior
type LeakyBucketConfig struct {
	// Incr() fails N times before finally succeeding
	IncrTemporaryFailCount uint16

	// Allows us to force a number of failed executions of GetDDoc, DeleteDDoc and DropIndex. It will fail the
	// number of times specific in these values and then succeed.
	DDocDeleteErrorCount int
	DDocGetErrorCount    int

	DCPFeedMissingDocs []string // Emulate entry not appearing on DCP feed

	ForceErrorSetRawKeys []string // Issuing a SetRaw call with a specified key will return an error

	ForceTimeoutErrorOnUpdateKeys []string // Specified keys will return timeout error AFTER write is sent to server

	// Returns a partial error the first time ViewCustom is called
	FirstTimeViewCustomPartialError bool

	// QueryCallback allows tests to set a callback that will be issued prior to issuing a view query
	QueryCallback     func(ddoc, viewName string, params map[string]any) error
	PostQueryCallback func(ddoc, viewName string, params map[string]interface{}) // Issues callback after issuing query when bucket.ViewQuery is called

	N1QLQueryCallback func(ctx context.Context, statement string, params map[string]any, consistency ConsistencyMode, adhoc bool) error

	PostN1QLQueryCallback func()

	// UpdateCallback issues additional callback in WriteUpdate after standard callback completes, but prior to document write.  Allows
	// tests to trigger CAS retry handling by modifying the underlying document in a UpdateCallback implementation.
	UpdateCallback func(key string)

	// GetRawCallback issues a callback prior to running GetRaw. Allows tests to issue a doc mutation or deletion prior
	// to GetRaw being ran.
	GetRawCallback       func(key string) error
	GetWithXattrCallback func(key string) error

	PostUpdateCallback func(key string)

	SetXattrCallback func(key string) error

	// WriteWithXattrCallback is ran before WriteWithXattr is called. This can be used to trigger a CAS retry
	WriteWithXattrCallback func(key string)

	// IncrCallback issues a callback during incr.  Used for sequence allocation race tests
	IncrCallback func()

	// TouchCallback issues a callback during touch.
	TouchCallback func(key string) error

	// When IgnoreClose is set to true, bucket.Close() is a no-op.  Used when multiple references to a bucket are active.
	IgnoreClose bool
}

func (b *LeakyBucket) StartDCPFeed(ctx context.Context, args sgbucket.FeedArguments, callback sgbucket.FeedEventCallbackFunc, dbStats *expvar.Map) error {
	if len(b.config.DCPFeedMissingDocs) > 0 {
		wrappedCallback := func(event sgbucket.FeedEvent) bool {
			for _, key := range b.config.DCPFeedMissingDocs {
				if string(event.Key) == key {
					return false
				}
			}
			return callback(event)
		}
		return b.bucket.StartDCPFeed(ctx, args, wrappedCallback, dbStats)
	}
	return b.bucket.StartDCPFeed(ctx, args, callback, dbStats)
}
