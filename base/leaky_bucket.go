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
	"slices"
	"sync"

	sgbucket "github.com/couchbase/sg-bucket"
)

var _ Bucket = &LeakyBucket{}

// A wrapper around a Bucket to support forced errors.  For testing use only.
type LeakyBucket struct {
	bucket      Bucket
	_config     *LeakyBucketConfig
	configLock  sync.RWMutex
	collections map[string]*LeakyDataStore
}

var _ sgbucket.BucketStore = &LeakyBucket{}
var _ sgbucket.DynamicDataStoreBucket = &LeakyBucket{}

// NewLeakyBucket creates a wrapper around a Bucket to support forced errors. The configuration will be shared by all DataStores that belong to this bucket.
func NewLeakyBucket(bucket Bucket, config LeakyBucketConfig) *LeakyBucket {
	return &LeakyBucket{
		bucket:      bucket,
		_config:     &config,
		collections: make(map[string]*LeakyDataStore),
	}
}

func (b *LeakyBucket) GetName() string {
	return b.bucket.GetName()
}

func (b *LeakyBucket) UUID(ctx context.Context) (string, error) {
	return b.bucket.UUID(ctx)
}

func (b *LeakyBucket) Close(ctx context.Context) {
	if !b.getIgnoreClose() {
		b.bucket.Close(ctx)
	}
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

func (b *LeakyBucket) GetMaxVbno(ctx context.Context) (uint16, error) {
	return b.bucket.GetMaxVbno(ctx)
}

func (b *LeakyBucket) DefaultDataStore(ctx context.Context) sgbucket.DataStore {
	return NewLeakyDataStore(b, b.bucket.DefaultDataStore(ctx))
}

func (b *LeakyBucket) ListDataStores(ctx context.Context) ([]sgbucket.DataStoreName, error) {
	return b.bucket.ListDataStores(ctx)
}

func (b *LeakyBucket) NamedDataStore(ctx context.Context, name sgbucket.DataStoreName) (sgbucket.DataStore, error) {
	dataStore, err := b.bucket.NamedDataStore(ctx, name)
	if err != nil {
		return nil, err
	}
	return NewLeakyDataStore(b, dataStore), nil
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

func (b *LeakyBucket) DropDataStore(ctx context.Context, name sgbucket.DataStoreName) error {
	dynamicDataStore, ok := b.GetUnderlyingBucket().(sgbucket.DynamicDataStoreBucket)
	if !ok {
		return fmt.Errorf("Bucket %T doesn't support dynamic collection creation", b.GetUnderlyingBucket())
	}
	return dynamicDataStore.DropDataStore(ctx, name)
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
	PostQueryCallback func(ddoc, viewName string, params map[string]any) // Issues callback after issuing query when bucket.ViewQuery is called

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

	// UpdateXattrsCallback is called before UpdateXattrs. Useful for injecting delays or errors per-key.
	UpdateXattrsCallback func(key string)

	// WriteUpdateWithXattrsCallback is called before WriteUpdateWithXattrs. Useful for injecting delays per-key.
	WriteUpdateWithXattrsCallback func(key string)

	// IncrCallback issues a callback during incr.  Used for sequence allocation race tests
	IncrCallback func()

	// TouchCallback issues a callback during touch.
	TouchCallback func(key string) error

	// AddCallback issues a callback during Add.
	AddCallback func(key string) (bool, error)

	// WriteCasCallback issues a callback during WriteCas. If it returns a non-nil error,
	// the underlying WriteCas is skipped and the error is returned as-is. Useful for
	// injecting CasMismatchErr on the insert/replace path.
	WriteCasCallback func(key string) (uint64, error)

	CreateIndexIfNotExistsCallback func(indexName string)

	// When IgnoreClose is set to true, bucket.Close() is a no-op.  Used when multiple references to a bucket are active.
	IgnoreClose bool
}

func (b *LeakyBucket) StartDCPFeed(ctx context.Context, args sgbucket.FeedArguments, callback sgbucket.FeedEventCallbackFunc, dbStats *expvar.Map) error {
	missingDocs := b.getDCPFeedMissingDocs()
	if len(missingDocs) > 0 {
		wrappedCallback := func(event sgbucket.FeedEvent) bool {
			if slices.Contains(missingDocs, string(event.Key)) {
				return false
			}
			return callback(event)
		}
		return b.bucket.StartDCPFeed(ctx, args, wrappedCallback, dbStats)
	}
	return b.bucket.StartDCPFeed(ctx, args, callback, dbStats)
}

func (b *LeakyBucket) getIgnoreClose() bool {
	b.configLock.RLock()
	defer b.configLock.RUnlock()
	return b._config.IgnoreClose
}

func (b *LeakyBucket) getDCPFeedMissingDocs() []string {
	b.configLock.RLock()
	defer b.configLock.RUnlock()
	return slices.Clone(b._config.DCPFeedMissingDocs)
}

func (b *LeakyBucket) getRawCallback() func(string) error {
	b.configLock.RLock()
	defer b.configLock.RUnlock()
	return b._config.GetRawCallback
}

func (b *LeakyBucket) setRawCallback(fn func(string) error) {
	b.configLock.Lock()
	defer b.configLock.Unlock()
	b._config.GetRawCallback = fn
}

func (b *LeakyBucket) getWithXattrCallback() func(string) error {
	b.configLock.RLock()
	defer b.configLock.RUnlock()
	return b._config.GetWithXattrCallback
}

func (b *LeakyBucket) setWithXattrCallback(fn func(string) error) {
	b.configLock.Lock()
	defer b.configLock.Unlock()
	b._config.GetWithXattrCallback = fn
}

func (b *LeakyBucket) getTouchCallback() func(string) error {
	b.configLock.RLock()
	defer b.configLock.RUnlock()
	return b._config.TouchCallback
}

func (b *LeakyBucket) getAddCallback() func(string) (bool, error) {
	b.configLock.RLock()
	defer b.configLock.RUnlock()
	return b._config.AddCallback
}

func (b *LeakyBucket) getForceErrorSetRawKeys() []string {
	b.configLock.RLock()
	defer b.configLock.RUnlock()
	return slices.Clone(b._config.ForceErrorSetRawKeys)
}

func (b *LeakyBucket) getWriteCasCallback() func(string) (uint64, error) {
	b.configLock.RLock()
	defer b.configLock.RUnlock()
	return b._config.WriteCasCallback
}

func (b *LeakyBucket) setWriteCasCallback(fn func(string) (uint64, error)) {
	b.configLock.Lock()
	defer b.configLock.Unlock()
	b._config.WriteCasCallback = fn
}

func (b *LeakyBucket) getUpdateCallback() func(string) {
	b.configLock.RLock()
	defer b.configLock.RUnlock()
	return b._config.UpdateCallback
}

func (b *LeakyBucket) setUpdateCallback(fn func(string)) {
	b.configLock.Lock()
	defer b.configLock.Unlock()
	b._config.UpdateCallback = fn
}

func (b *LeakyBucket) getPostUpdateCallback() func(string) {
	b.configLock.RLock()
	defer b.configLock.RUnlock()
	return b._config.PostUpdateCallback
}

func (b *LeakyBucket) setPostUpdateCallback(fn func(string)) {
	b.configLock.Lock()
	defer b.configLock.Unlock()
	b._config.PostUpdateCallback = fn
}

func (b *LeakyBucket) getForceTimeoutErrorOnUpdateKeys() []string {
	b.configLock.RLock()
	defer b.configLock.RUnlock()
	return slices.Clone(b._config.ForceTimeoutErrorOnUpdateKeys)
}

func (b *LeakyBucket) getIncrTemporaryFailCount() uint16 {
	b.configLock.RLock()
	defer b.configLock.RUnlock()
	return b._config.IncrTemporaryFailCount
}

func (b *LeakyBucket) getIncrCallback() func() {
	b.configLock.RLock()
	defer b.configLock.RUnlock()
	return b._config.IncrCallback
}

// decrementDDocGetErrorCount atomically decrements DDocGetErrorCount if positive.
// Returns the post-decrement remaining count and whether the error should be returned.
func (b *LeakyBucket) decrementDDocGetErrorCount() (remaining int, shouldFail bool) {
	b.configLock.Lock()
	defer b.configLock.Unlock()
	if b._config.DDocGetErrorCount > 0 {
		b._config.DDocGetErrorCount--
		return b._config.DDocGetErrorCount, true
	}
	return 0, false
}

func (b *LeakyBucket) setDDocGetErrorCount(i int) {
	b.configLock.Lock()
	defer b.configLock.Unlock()
	b._config.DDocGetErrorCount = i
}

// decrementDDocDeleteErrorCount atomically decrements DDocDeleteErrorCount if positive.
// Returns the post-decrement remaining count and whether the error should be returned.
func (b *LeakyBucket) decrementDDocDeleteErrorCount() (remaining int, shouldFail bool) {
	b.configLock.Lock()
	defer b.configLock.Unlock()
	if b._config.DDocDeleteErrorCount > 0 {
		b._config.DDocDeleteErrorCount--
		return b._config.DDocDeleteErrorCount, true
	}
	return 0, false
}

func (b *LeakyBucket) setDDocDeleteErrorCount(i int) {
	b.configLock.Lock()
	defer b.configLock.Unlock()
	b._config.DDocDeleteErrorCount = i
}

func (b *LeakyBucket) getQueryCallback() func(string, string, map[string]any) error {
	b.configLock.RLock()
	defer b.configLock.RUnlock()
	return b._config.QueryCallback
}

func (b *LeakyBucket) setQueryCallback(fn func(string, string, map[string]any) error) {
	b.configLock.Lock()
	defer b.configLock.Unlock()
	b._config.QueryCallback = fn
}

func (b *LeakyBucket) getPostQueryCallback() func(string, string, map[string]any) {
	b.configLock.RLock()
	defer b.configLock.RUnlock()
	return b._config.PostQueryCallback
}

func (b *LeakyBucket) setPostQueryCallback(fn func(string, string, map[string]any)) {
	b.configLock.Lock()
	defer b.configLock.Unlock()
	b._config.PostQueryCallback = fn
}

// getAndClearFirstTimeViewCustomPartialError atomically reads and clears the flag.
func (b *LeakyBucket) getAndClearFirstTimeViewCustomPartialError() bool {
	b.configLock.Lock()
	defer b.configLock.Unlock()
	v := b._config.FirstTimeViewCustomPartialError
	if v {
		b._config.FirstTimeViewCustomPartialError = false
	}
	return v
}

func (b *LeakyBucket) setFirstTimeViewCustomPartialError(v bool) {
	b.configLock.Lock()
	defer b.configLock.Unlock()
	b._config.FirstTimeViewCustomPartialError = v
}

func (b *LeakyBucket) getWriteWithXattrCallback() func(string) {
	b.configLock.RLock()
	defer b.configLock.RUnlock()
	return b._config.WriteWithXattrCallback
}

func (b *LeakyBucket) getUpdateXattrsCallback() func(string) {
	b.configLock.RLock()
	defer b.configLock.RUnlock()
	return b._config.UpdateXattrsCallback
}

func (b *LeakyBucket) setUpdateXattrsCallback(fn func(string)) {
	b.configLock.Lock()
	defer b.configLock.Unlock()
	b._config.UpdateXattrsCallback = fn
}

func (b *LeakyBucket) getWriteUpdateWithXattrsCallback() func(string) {
	b.configLock.RLock()
	defer b.configLock.RUnlock()
	return b._config.WriteUpdateWithXattrsCallback
}

func (b *LeakyBucket) setWriteUpdateWithXattrsCallback(fn func(string)) {
	b.configLock.Lock()
	defer b.configLock.Unlock()
	b._config.WriteUpdateWithXattrsCallback = fn
}

func (b *LeakyBucket) getSetXattrCallback() func(string) error {
	b.configLock.RLock()
	defer b.configLock.RUnlock()
	return b._config.SetXattrCallback
}

func (b *LeakyBucket) getN1QLQueryCallback() func(context.Context, string, map[string]any, ConsistencyMode, bool) error {
	b.configLock.RLock()
	defer b.configLock.RUnlock()
	return b._config.N1QLQueryCallback
}

func (b *LeakyBucket) getPostN1QLQueryCallback() func() {
	b.configLock.RLock()
	defer b.configLock.RUnlock()
	return b._config.PostN1QLQueryCallback
}

func (b *LeakyBucket) setPostN1QLQueryCallback(fn func()) {
	b.configLock.Lock()
	defer b.configLock.Unlock()
	b._config.PostN1QLQueryCallback = fn
}

func (b *LeakyBucket) getCreateIndexIfNotExistsCallback() func(string) {
	b.configLock.RLock()
	defer b.configLock.RUnlock()
	return b._config.CreateIndexIfNotExistsCallback
}
