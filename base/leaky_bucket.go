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
	"errors"
	"expvar"
	"fmt"
	"math"
	"time"

	sgbucket "github.com/couchbase/sg-bucket"
)

// A wrapper around a Bucket to support forced errors.  For testing use only.
type LeakyBucket struct {
	bucket               Bucket
	incrCount            uint16
	deleteDDocErrorCount int
	getDDocErrorCount    int
	config               LeakyBucketConfig
}

// The config object that controls the LeakyBucket behavior
type LeakyBucketConfig struct {
	// Incr() fails N times before finally succeeding
	IncrTemporaryFailCount uint16

	// Allows us to force a number of failed executions of GetDDoc, DeleteDDoc and DropIndex. It will fail the
	// number of times specific in these values and then succeed.
	DDocDeleteErrorCount int
	DDocGetErrorCount    int

	// Allows us to force a specific index to be deleted. We will always error when attempting to delete an index if its
	// name is specified in this slice
	DropIndexErrorNames []string

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
	GetRawCallback func(key string)

	PostUpdateCallback func(key string)

	SetXattrCallback func(key string) error

	// WriteWithXattrCallback is ran before WriteWithXattr is called. This can be used to trigger a CAS retry
	WriteWithXattrCallback func(key string)

	// IncrCallback issues a callback during incr.  Used for sequence allocation race tests
	IncrCallback func()

	// When IgnoreClose is set to true, bucket.Close() is a no-op.  Used when multiple references to a bucket are active.
	IgnoreClose bool
}

func NewLeakyBucket(bucket Bucket, config LeakyBucketConfig) *LeakyBucket {
	return &LeakyBucket{
		bucket: bucket,
		config: config,
	}
}

var _ N1QLStore = &LeakyBucket{}

func (b *LeakyBucket) GetUnderlyingBucket() Bucket {
	return b.bucket
}

// For walrus handling, ignore close needs to be set after the bucket is initialized
func (b *LeakyBucket) SetIgnoreClose(value bool) {
	b.config.IgnoreClose = value
}

func (b *LeakyBucket) GetName() string {
	return b.bucket.GetName()
}
func (b *LeakyBucket) Get(k string, rv interface{}) (cas uint64, err error) {
	return b.bucket.Get(k, rv)
}

func (b *LeakyBucket) SetGetRawCallback(callback func(string)) {
	b.config.GetRawCallback = callback
}

func (b *LeakyBucket) GetRaw(k string) (v []byte, cas uint64, err error) {
	if b.config.GetRawCallback != nil {
		b.config.GetRawCallback(k)
	}
	return b.bucket.GetRaw(k)
}
func (b *LeakyBucket) GetAndTouchRaw(k string, exp uint32) (v []byte, cas uint64, err error) {
	return b.bucket.GetAndTouchRaw(k, exp)
}
func (b *LeakyBucket) Touch(k string, exp uint32) (cas uint64, err error) {
	return b.bucket.Touch(k, exp)
}
func (b *LeakyBucket) Add(k string, exp uint32, v interface{}) (added bool, err error) {
	return b.bucket.Add(k, exp, v)
}
func (b *LeakyBucket) AddRaw(k string, exp uint32, v []byte) (added bool, err error) {
	return b.bucket.AddRaw(k, exp, v)
}
func (b *LeakyBucket) Set(k string, exp uint32, opts *sgbucket.UpsertOptions, v interface{}) error {
	return b.bucket.Set(k, exp, opts, v)
}
func (b *LeakyBucket) SetRaw(k string, exp uint32, opts *sgbucket.UpsertOptions, v []byte) error {
	for _, errorKey := range b.config.ForceErrorSetRawKeys {
		if k == errorKey {
			return fmt.Errorf("Leaky bucket forced SetRaw error for key %s", k)
		}
	}
	return b.bucket.SetRaw(k, exp, opts, v)
}
func (b *LeakyBucket) Delete(k string) error {
	return b.bucket.Delete(k)
}
func (b *LeakyBucket) Remove(k string, cas uint64) (casOut uint64, err error) {
	return b.bucket.Remove(k, cas)
}
func (b *LeakyBucket) WriteCas(k string, flags int, exp uint32, cas uint64, v interface{}, opt sgbucket.WriteOptions) (uint64, error) {
	return b.bucket.WriteCas(k, flags, exp, cas, v, opt)
}
func (b *LeakyBucket) Update(k string, exp uint32, callback sgbucket.UpdateFunc) (casOut uint64, err error) {
	if b.config.UpdateCallback != nil {
		wrapperCallback := func(current []byte) (updated []byte, expiry *uint32, isDelete bool, err error) {
			updated, expiry, isDelete, err = callback(current)
			b.config.UpdateCallback(k)
			return updated, expiry, isDelete, err
		}
		return b.bucket.Update(k, exp, wrapperCallback)
	}

	casOut, err = b.bucket.Update(k, exp, callback)

	if b.config.PostUpdateCallback != nil {
		b.config.PostUpdateCallback(k)
	}

	return casOut, err
}

func (b *LeakyBucket) Incr(k string, amt, def uint64, exp uint32) (uint64, error) {

	if b.config.IncrTemporaryFailCount > 0 {
		if b.incrCount < b.config.IncrTemporaryFailCount {
			b.incrCount++
			return 0, errors.New(fmt.Sprintf("Incr forced abort (%d/%d), try again maybe?", b.incrCount, b.config.IncrTemporaryFailCount))
		}
		b.incrCount = 0

	}
	val, err := b.bucket.Incr(k, amt, def, exp)

	if b.config.IncrCallback != nil {
		b.config.IncrCallback()
	}
	return val, err
}

func (b *LeakyBucket) GetDDocs() (map[string]sgbucket.DesignDoc, error) {
	return b.bucket.GetDDocs()
}
func (b *LeakyBucket) GetDDoc(docname string) (ddoc sgbucket.DesignDoc, err error) {
	if b.config.DDocGetErrorCount > 0 {
		b.config.DDocGetErrorCount--
		return ddoc, errors.New(fmt.Sprintf("Artificial leaky bucket error %d fails remaining", b.config.DDocGetErrorCount))
	}
	return b.bucket.GetDDoc(docname)
}
func (b *LeakyBucket) PutDDoc(docname string, value *sgbucket.DesignDoc) error {
	return b.bucket.PutDDoc(docname, value)
}
func (b *LeakyBucket) DeleteDDoc(docname string) error {
	if b.config.DDocDeleteErrorCount > 0 {
		b.config.DDocDeleteErrorCount--
		return errors.New(fmt.Sprintf("Artificial leaky bucket error %d fails remaining", b.config.DDocDeleteErrorCount))
	}
	return b.bucket.DeleteDDoc(docname)
}
func (b *LeakyBucket) View(ddoc, name string, params map[string]interface{}) (sgbucket.ViewResult, error) {
	return b.bucket.View(ddoc, name, params)
}

func (b *LeakyBucket) ViewQuery(ddoc, name string, params map[string]interface{}) (sgbucket.QueryResultIterator, error) {
	iterator, err := b.bucket.ViewQuery(ddoc, name, params)

	if b.config.FirstTimeViewCustomPartialError {
		b.config.FirstTimeViewCustomPartialError = !b.config.FirstTimeViewCustomPartialError
		err = ErrPartialViewErrors
	}

	if b.config.PostQueryCallback != nil {
		b.config.PostQueryCallback(ddoc, name, params)
	}
	return iterator, err
}

func (b *LeakyBucket) GetMaxVbno() (uint16, error) {
	return b.bucket.GetMaxVbno()
}

func (b *LeakyBucket) WriteCasWithXattr(k string, xattr string, exp uint32, cas uint64, opts *sgbucket.MutateInOptions, v interface{}, xv interface{}) (casOut uint64, err error) {
	return b.bucket.WriteCasWithXattr(k, xattr, exp, cas, opts, v, xv)
}

func (b *LeakyBucket) WriteWithXattr(k string, xattrKey string, exp uint32, cas uint64, opts *sgbucket.MutateInOptions, value []byte, xattrValue []byte, isDelete bool, deleteBody bool) (casOut uint64, err error) {
	if b.config.WriteWithXattrCallback != nil {
		b.config.WriteWithXattrCallback(k)
	}
	return b.bucket.WriteWithXattr(k, xattrKey, exp, cas, opts, value, xattrValue, isDelete, deleteBody)
}

func (b *LeakyBucket) WriteUpdateWithXattr(k string, xattr string, userXattrKey string, exp uint32, opts *sgbucket.MutateInOptions, previous *sgbucket.BucketDocument, callback sgbucket.WriteUpdateWithXattrFunc) (casOut uint64, err error) {
	if b.config.UpdateCallback != nil {
		wrapperCallback := func(current []byte, xattr []byte, userXattr []byte, cas uint64) (updated []byte, updatedXattr []byte, deletedDoc bool, expiry *uint32, err error) {
			updated, updatedXattr, deletedDoc, expiry, err = callback(current, xattr, userXattr, cas)
			b.config.UpdateCallback(k)
			return updated, updatedXattr, deletedDoc, expiry, err
		}
		return b.bucket.WriteUpdateWithXattr(k, xattr, userXattrKey, exp, opts, previous, wrapperCallback)
	}
	return b.bucket.WriteUpdateWithXattr(k, xattr, userXattrKey, exp, opts, previous, callback)
}

func (b *LeakyBucket) SetXattr(k string, xattrKey string, xv []byte) (casOut uint64, err error) {
	if b.config.SetXattrCallback != nil {
		if err := b.config.SetXattrCallback(k); err != nil {
			return 0, err
		}
	}
	return b.bucket.SetXattr(k, xattrKey, xv)
}

func (b *LeakyBucket) RemoveXattr(k string, xattrKey string, cas uint64) (err error) {
	return b.bucket.RemoveXattr(k, xattrKey, cas)
}

func (b *LeakyBucket) DeleteXattrs(k string, xattrKeys ...string) (err error) {
	return b.bucket.DeleteXattrs(k, xattrKeys...)
}

func (b *LeakyBucket) SubdocInsert(docID string, fieldPath string, cas uint64, value interface{}) error {
	return b.bucket.SubdocInsert(docID, fieldPath, cas, value)
}

func (b *LeakyBucket) GetWithXattr(k string, xattr string, userXattrKey string, rv interface{}, xv interface{}, uxv interface{}) (cas uint64, err error) {
	return b.bucket.GetWithXattr(k, xattr, userXattrKey, rv, xv, uxv)
}

func (b *LeakyBucket) DeleteWithXattr(k string, xattr string) error {
	return b.bucket.DeleteWithXattr(k, xattr)
}

func (b *LeakyBucket) GetXattr(k string, xattr string, xv interface{}) (cas uint64, err error) {
	return b.bucket.GetXattr(k, xattr, xv)
}

func (b *LeakyBucket) GetSubDocRaw(k string, subdocKey string) ([]byte, uint64, error) {
	return b.bucket.GetSubDocRaw(k, subdocKey)
}

func (b *LeakyBucket) WriteSubDoc(k string, subdocKey string, cas uint64, value []byte) (uint64, error) {
	return b.bucket.WriteSubDoc(k, subdocKey, cas, value)
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
				event.VbNo = uint16(VBHash(key, 1024))
				vbTapFeed.channel <- event
			}
			close(vbTapFeed.channel)
		}()
		return vbTapFeed, nil

	} else {
		return b.bucket.StartTapFeed(args, dbStats)
	}

}

func (b *LeakyBucket) StartDCPFeed(args sgbucket.FeedArguments, callback sgbucket.FeedEventCallbackFunc, dbStats *expvar.Map) error {
	return b.bucket.StartDCPFeed(args, callback, dbStats)
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
					deDupeBuffer = []sgbucket.FeedEvent{}
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

func (b *LeakyBucket) Close() {
	if !b.config.IgnoreClose {
		b.bucket.Close()
	}
}
func (b *LeakyBucket) Dump() {
	b.bucket.Dump()
}

func (b *LeakyBucket) UUID() (string, error) {
	return b.bucket.UUID()
}

func (b *LeakyBucket) CloseAndDelete() error {
	if bucket, ok := b.bucket.(sgbucket.DeleteableStore); ok {
		return bucket.CloseAndDelete()
	}
	return nil
}

// Accessors to set leaky bucket config for a running bucket.  Used to tune properties on a walrus bucket created as part of rest tester - it will
// be a leaky bucket (due to DCP support), but there's no mechanism to pass in a leaky bucket config to a RestTester bucket at bucket creation time.
func (b *LeakyBucket) SetFirstTimeViewCustomPartialError(val bool) {
	b.config.FirstTimeViewCustomPartialError = val
}

func (b *LeakyBucket) SetPostQueryCallback(callback func(ddoc, viewName string, params map[string]interface{})) {
	b.config.PostQueryCallback = callback
}

func (b *LeakyBucket) SetPostN1QLQueryCallback(callback func()) {
	b.config.PostN1QLQueryCallback = callback
}

func (b *LeakyBucket) SetPostUpdateCallback(callback func(key string)) {
	b.config.PostUpdateCallback = callback
}

func (b *LeakyBucket) SetUpdateCallback(callback func(key string)) {
	b.config.UpdateCallback = callback
}

func (b *LeakyBucket) IsSupported(feature sgbucket.DataStoreFeature) bool {
	return b.bucket.IsSupported(feature)
}

func (b *LeakyBucket) Keyspace() string {
	n1qlStore, ok := AsN1QLStore(b.bucket)
	if !ok {
		return ""
	}
	return n1qlStore.Keyspace()
}

func (b *LeakyBucket) Query(statement string, params map[string]interface{}, consistency ConsistencyMode, adhoc bool) (results sgbucket.QueryResultIterator, err error) {
	n1qlStore, ok := AsN1QLStore(b.bucket)
	if !ok {
		return nil, errors.New("Not N1QL Store")
	}

	results, err = n1qlStore.Query(statement, params, consistency, adhoc)
	if b.config.PostN1QLQueryCallback != nil {
		b.config.PostN1QLQueryCallback()
	}

	return results, err
}

func (b *LeakyBucket) ExplainQuery(statement string, params map[string]interface{}) (plain map[string]interface{}, err error) {
	n1qlStore, ok := AsN1QLStore(b.bucket)
	if !ok {
		return nil, errors.New("Not N1QL Store")
	}
	return n1qlStore.ExplainQuery(statement, params)
}

func (b *LeakyBucket) CreateIndex(indexName string, expression string, filterExpression string, options *N1qlIndexOptions) error {
	n1qlStore, ok := AsN1QLStore(b.bucket)
	if !ok {
		return errors.New("Not N1QL Store")
	}
	return n1qlStore.CreateIndex(indexName, expression, filterExpression, options)
}

func (b *LeakyBucket) BuildDeferredIndexes(indexSet []string) error {
	n1qlStore, ok := AsN1QLStore(b.bucket)
	if !ok {
		return errors.New("Not N1QL Store")
	}
	return n1qlStore.BuildDeferredIndexes(indexSet)
}

func (b *LeakyBucket) CreatePrimaryIndex(indexName string, options *N1qlIndexOptions) error {
	n1qlStore, ok := AsN1QLStore(b.bucket)
	if !ok {
		return errors.New("Not N1QL Store")
	}
	return n1qlStore.CreatePrimaryIndex(indexName, options)
}

func (b *LeakyBucket) WaitForIndexOnline(indexName string) error {
	n1qlStore, ok := AsN1QLStore(b.bucket)
	if !ok {
		return errors.New("Not N1QL Store")
	}
	return n1qlStore.WaitForIndexOnline(indexName)
}

func (b *LeakyBucket) GetIndexMeta(indexName string) (exists bool, meta *IndexMeta, err error) {
	n1qlStore, ok := AsN1QLStore(b.bucket)
	if !ok {
		return false, nil, errors.New("Not N1QL Store")
	}
	return n1qlStore.GetIndexMeta(indexName)
}

func (b *LeakyBucket) DropIndex(indexName string) error {

	n1qlStore, ok := AsN1QLStore(b.bucket)
	if !ok {
		return errors.New("Not N1QL Store")
	}

	if len(b.config.DropIndexErrorNames) > 0 {
		for _, indexNameFail := range b.config.DropIndexErrorNames {
			if indexNameFail == indexName {
				return errors.New(fmt.Sprintf("Artificial leaky bucket error"))
			}
		}
	}

	return n1qlStore.DropIndex(indexName)
}

func (b *LeakyBucket) executeQuery(statement string) (results sgbucket.QueryResultIterator, err error) {
	n1qlStore, ok := AsN1QLStore(b.bucket)
	if !ok {
		return nil, errors.New("Not N1QL Store")
	}
	return n1qlStore.executeQuery(statement)
}

func (b *LeakyBucket) executeStatement(statement string) error {
	n1qlStore, ok := AsN1QLStore(b.bucket)
	if !ok {
		return errors.New("Not N1QL Store")
	}
	return n1qlStore.executeStatement(statement)
}

func (b *LeakyBucket) getIndexes() ([]string, error) {
	n1qlStore, ok := AsN1QLStore(b.bucket)
	if !ok {
		return nil, errors.New("Not N1QL Store")
	}
	return n1qlStore.getIndexes()
}

func (b *LeakyBucket) IsErrNoResults(err error) bool {
	n1qlStore, ok := AsN1QLStore(b.bucket)
	if !ok {
		return false
	}
	return n1qlStore.IsErrNoResults(err)
}

func (b *LeakyBucket) IsError(err error, errorType sgbucket.DataStoreErrorType) bool {
	return b.bucket.IsError(err, errorType)
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

// VBHash finds the vbucket for the given key.
func VBHash(key string, numVb int) uint32 {
	return sgbucket.VBHash(key, uint16(numVb))
}
