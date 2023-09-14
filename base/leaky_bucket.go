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
	"math"
	"time"

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

func (b *LeakyBucket) Close() {
	if !b.config.IgnoreClose {
		b.bucket.Close()
	}
}

// For walrus handling, ignore close needs to be set after the bucket is initialized
func (b *LeakyBucket) SetIgnoreClose(value bool) {
	b.config.IgnoreClose = value
}

func (b *LeakyBucket) CloseAndDelete() error {
	if bucket, ok := b.bucket.(sgbucket.DeleteableStore); ok {
		return bucket.CloseAndDelete()
	}
	return nil
}

func (b *LeakyBucket) IsSupported(feature sgbucket.BucketStoreFeature) bool {
	return b.bucket.IsSupported(feature)
}

func (b *LeakyBucket) GetMaxVbno() (uint16, error) {
	return b.bucket.GetMaxVbno()
}

func (b *LeakyBucket) IsError(err error, errorType sgbucket.DataStoreErrorType) bool {
	return b.bucket.IsError(err, errorType)
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

	// Emulate TAP/DCP feed de-dupliation behavior, such that within a
	// window of # of mutations or a timeout, mutations for a given document
	// will be filtered such that only the _latest_ mutation will make it through.
	TapFeedDeDuplication bool
	TapFeedVbuckets      bool     // Emulate vbucket numbers on feed
	TapFeedMissingDocs   []string // Emulate entry not appearing on tap feed

	ForceErrorSetRawKeys []string // Issuing a SetRaw call with a specified key will return an error

	// Returns a partial error the first time ViewCustom is called
	FirstTimeViewCustomPartialError bool
	PostQueryCallback               func(ddoc, viewName string, params map[string]interface{}) // Issues callback after issuing query when bucket.ViewQuery is called

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

	// When IgnoreClose is set to true, bucket.Close() is a no-op.  Used when multiple references to a bucket are active.
	IgnoreClose bool
}

func (b *LeakyBucket) StartTapFeed(args sgbucket.FeedArguments, dbStats *expvar.Map) (sgbucket.MutationFeed, error) {

	if b.config.TapFeedDeDuplication {
		return b.wrapFeedForDeduplication(args, dbStats)
	} else if len(b.config.TapFeedMissingDocs) > 0 {
		callback := func(event *sgbucket.FeedEvent) bool {
			for _, key := range b.config.TapFeedMissingDocs {
				if string(event.Key) == key {
					return false
				}
			}
			return true
		}
		return b.wrapFeed(args, callback, dbStats)
	} else if b.config.TapFeedVbuckets {
		// kick off the wrapped sgbucket tap feed
		walrusTapFeed, err := b.bucket.StartTapFeed(args, dbStats)
		if err != nil {
			return walrusTapFeed, err
		}
		// this is the sgbucket.MutationFeed impl we'll return to callers, which
		// will add vbucket information
		channel := make(chan sgbucket.FeedEvent, 10)
		vbTapFeed := &wrappedTapFeedImpl{
			channel:        channel,
			wrappedTapFeed: walrusTapFeed,
		}
		go func() {
			for event := range walrusTapFeed.Events() {
				key := string(event.Key)
				event.VbNo = uint16(sgbucket.VBHash(key, 1024))
				vbTapFeed.channel <- event
			}
			close(vbTapFeed.channel)
		}()
		return vbTapFeed, nil

	} else {
		return b.bucket.StartTapFeed(args, dbStats)
	}

}

func (b *LeakyBucket) StartDCPFeed(ctx context.Context, args sgbucket.FeedArguments, callback sgbucket.FeedEventCallbackFunc, dbStats *expvar.Map) error {
	return b.bucket.StartDCPFeed(ctx, args, callback, dbStats)
}

type EventUpdateFunc func(event *sgbucket.FeedEvent) bool

func (b *LeakyBucket) wrapFeed(args sgbucket.FeedArguments, callback EventUpdateFunc, dbStats *expvar.Map) (sgbucket.MutationFeed, error) {

	// kick off the wrapped sgbucket tap feed
	walrusTapFeed, err := b.bucket.StartTapFeed(args, dbStats)
	if err != nil {
		return walrusTapFeed, err
	}

	// create an output channel
	channel := make(chan sgbucket.FeedEvent, 10)

	// this is the sgbucket.MutationFeed impl we'll return to callers, which
	// will have missing entries
	wrapperFeed := &wrappedTapFeedImpl{
		channel:        channel,
		wrappedTapFeed: walrusTapFeed,
	}

	go func() {
		for event := range walrusTapFeed.Events() {
			// Callback returns false if the event should be skipped
			if callback(&event) {
				wrapperFeed.channel <- event
			}
		}
		close(wrapperFeed.channel)
	}()
	return wrapperFeed, nil
}

func (b *LeakyBucket) wrapFeedForDeduplication(args sgbucket.FeedArguments, dbStats *expvar.Map) (sgbucket.MutationFeed, error) {
	// create an output channel
	// start a goroutine which reads off the sgbucket tap feed
	//   - de-duplicate certain events
	//   - puts them to output channel

	// the number of changes that it will buffer up before de-duplicating
	deDuplicationWindowSize := 5

	// the timeout window in milliseconds after which it will flush to output, even if
	// the dedupe buffer has not filled up yet.
	deDuplicationTimeoutMs := time.Millisecond * 1000

	// kick off the wrapped sgbucket tap feed
	walrusTapFeed, err := b.bucket.StartTapFeed(args, dbStats)
	if err != nil {
		return walrusTapFeed, err
	}

	// create an output channel for de-duplicated events
	channel := make(chan sgbucket.FeedEvent, 10)

	// this is the sgbucket.MutationFeed impl we'll return to callers, which
	// will reead from the de-duplicated events channel
	dupeTapFeed := &wrappedTapFeedImpl{
		channel:        channel,
		wrappedTapFeed: walrusTapFeed,
	}

	go func() {
		defer close(dupeTapFeed.channel)
		// the buffer to hold tap events that are candidates for de-duplication
		deDupeBuffer := []sgbucket.FeedEvent{}

		timer := time.NewTimer(math.MaxInt64)
		for {
			select {
			case tapEvent, ok := <-walrusTapFeed.Events():
				if !ok {
					// channel closed, goroutine is done
					// dedupe and send what we currently have
					dedupeAndForward(deDupeBuffer, channel)
					return
				}
				deDupeBuffer = append(deDupeBuffer, tapEvent)

				// if we've collected enough, dedeupe and send what we have,
				// and reset buffer.
				if len(deDupeBuffer) >= deDuplicationWindowSize {
					dedupeAndForward(deDupeBuffer, channel)
					deDupeBuffer = []sgbucket.FeedEvent{}
				} else {
					_ = timer.Reset(deDuplicationTimeoutMs)
				}

			case <-timer.C:
				if len(deDupeBuffer) > 0 {
					// give up on waiting for the buffer to fill up,
					// de-dupe and send what we currently have
					dedupeAndForward(deDupeBuffer, channel)
					deDupeBuffer = []sgbucket.FeedEvent{}
				}
			}
		}

	}()
	return dupeTapFeed, nil
}

// An implementation of a sgbucket tap feed that wraps
// tap events on the upstream tap feed to better emulate real world
// TAP/DCP behavior.
type wrappedTapFeedImpl struct {
	channel        chan sgbucket.FeedEvent
	wrappedTapFeed sgbucket.MutationFeed
}

func (feed *wrappedTapFeedImpl) Close() error {
	return feed.wrappedTapFeed.Close()
}

func (feed *wrappedTapFeedImpl) Events() <-chan sgbucket.FeedEvent {
	return feed.channel
}

func (feed *wrappedTapFeedImpl) WriteEvents() chan<- sgbucket.FeedEvent {
	return feed.channel
}

func dedupeAndForward(tapEvents []sgbucket.FeedEvent, destChannel chan<- sgbucket.FeedEvent) {

	deduped := dedupeTapEvents(tapEvents)

	for _, tapEvent := range deduped {
		destChannel <- tapEvent
	}

}

func dedupeTapEvents(tapEvents []sgbucket.FeedEvent) []sgbucket.FeedEvent {

	// For each document key, keep track of the latest seen tapEvent
	// doc1 -> tapEvent with Seq=1
	// doc2 -> tapEvent with Seq=5
	// (if tapEvent with Seq=7 comes in for doc1, it will clobber existing)
	latestTapEventPerKey := map[string]sgbucket.FeedEvent{}

	for _, tapEvent := range tapEvents {
		key := string(tapEvent.Key)
		latestTapEventPerKey[key] = tapEvent
	}

	// Iterate over the original tapEvents, and only keep what
	// is in latestTapEventPerKey, and discard all previous mutations
	// of that doc.  This will preserve the original
	// sequence order as read off the feed.
	deduped := []sgbucket.FeedEvent{}
	for _, tapEvent := range tapEvents {
		key := string(tapEvent.Key)
		latestTapEventForKey := latestTapEventPerKey[key]
		if tapEvent.Cas == latestTapEventForKey.Cas {
			deduped = append(deduped, tapEvent)
		}
	}

	return deduped

}
