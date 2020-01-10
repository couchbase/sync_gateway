package base

import (
	"errors"
	"fmt"
	"time"

	"github.com/couchbase/gocb"
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

	// WriteUpdateCallback issues additional callback in WriteUpdate after standard callback completes, but prior to document write.  Allows
	// tests to trigger CAS retry handling by modifying the underlying document in a WriteUpdateCallback implementation.
	WriteUpdateCallback func(key string)

	// IncrCallback issues a callback during incr.  Used for sequence allocation race tests
	IncrCallback func()
}

func NewLeakyBucket(bucket Bucket, config LeakyBucketConfig) Bucket {
	return &LeakyBucket{
		bucket: bucket,
		config: config,
	}
}

var _ N1QLBucket = &LeakyBucket{}

func (b *LeakyBucket) GetUnderlyingBucket() Bucket {
	return b.bucket
}

func (b *LeakyBucket) GetName() string {
	return b.bucket.GetName()
}
func (b *LeakyBucket) Get(k string, rv interface{}) (cas uint64, err error) {
	return b.bucket.Get(k, rv)
}
func (b *LeakyBucket) GetRaw(k string) (v []byte, cas uint64, err error) {
	return b.bucket.GetRaw(k)
}
func (b *LeakyBucket) GetBulkRaw(keys []string) (map[string][]byte, error) {
	return b.bucket.GetBulkRaw(keys)
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
func (b *LeakyBucket) Append(k string, data []byte) error {
	return b.bucket.Append(k, data)
}
func (b *LeakyBucket) Set(k string, exp uint32, v interface{}) error {
	return b.bucket.Set(k, exp, v)
}
func (b *LeakyBucket) SetRaw(k string, exp uint32, v []byte) error {
	for _, errorKey := range b.config.ForceErrorSetRawKeys {
		if k == errorKey {
			return fmt.Errorf("Leaky bucket forced SetRaw error for key %s", k)
		}
	}
	return b.bucket.SetRaw(k, exp, v)
}
func (b *LeakyBucket) Delete(k string) error {
	return b.bucket.Delete(k)
}
func (b *LeakyBucket) Remove(k string, cas uint64) (casOut uint64, err error) {
	return b.bucket.Remove(k, cas)
}
func (b *LeakyBucket) Write(k string, flags int, exp uint32, v interface{}, opt sgbucket.WriteOptions) error {
	return b.bucket.Write(k, flags, exp, v, opt)
}
func (b *LeakyBucket) WriteCas(k string, flags int, exp uint32, cas uint64, v interface{}, opt sgbucket.WriteOptions) (uint64, error) {
	return b.bucket.WriteCas(k, flags, exp, cas, v, opt)
}
func (b *LeakyBucket) Update(k string, exp uint32, callback sgbucket.UpdateFunc) (casOut uint64, err error) {
	return b.bucket.Update(k, exp, callback)
}
func (b *LeakyBucket) WriteUpdate(k string, exp uint32, callback sgbucket.WriteUpdateFunc) (casOut uint64, err error) {
	if b.config.WriteUpdateCallback != nil {
		wrapperCallback := func(current []byte) (updated []byte, opt sgbucket.WriteOptions, expiry *uint32, err error) {
			updated, opt, expiry, err = callback(current)
			b.config.WriteUpdateCallback(k)
			return updated, opt, expiry, err
		}
		return b.bucket.WriteUpdate(k, exp, wrapperCallback)
	}
	return b.bucket.WriteUpdate(k, exp, callback)
}
func (b *LeakyBucket) SetBulk(entries []*sgbucket.BulkSetEntry) (err error) {
	return b.bucket.SetBulk(entries)
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

func (b *LeakyBucket) GetDDoc(docname string, value interface{}) error {
	if b.config.DDocGetErrorCount > 0 {
		b.config.DDocGetErrorCount--
		return errors.New(fmt.Sprintf("Artificial leaky bucket error %d fails remaining", b.config.DDocGetErrorCount))
	}
	return b.bucket.GetDDoc(docname, value)
}
func (b *LeakyBucket) PutDDoc(docname string, value interface{}) error {
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
func (b *LeakyBucket) ViewCustom(ddoc, name string, params map[string]interface{}, vres interface{}) error {
	err := b.bucket.ViewCustom(ddoc, name, params, vres)

	if b.config.FirstTimeViewCustomPartialError {
		b.config.FirstTimeViewCustomPartialError = !b.config.FirstTimeViewCustomPartialError
		err = ErrPartialViewErrors
	}

	return err
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

func (b *LeakyBucket) Refresh() error {
	return b.bucket.Refresh()
}

func (b *LeakyBucket) WriteCasWithXattr(k string, xattr string, exp uint32, cas uint64, v interface{}, xv interface{}) (casOut uint64, err error) {
	return b.bucket.WriteCasWithXattr(k, xattr, exp, cas, v, xv)
}

func (b *LeakyBucket) WriteUpdateWithXattr(k string, xattr string, exp uint32, previous *sgbucket.BucketDocument, callback sgbucket.WriteUpdateWithXattrFunc) (casOut uint64, err error) {
	if b.config.WriteUpdateCallback != nil {
		wrapperCallback := func(current []byte, xattr []byte, cas uint64) (updated []byte, updatedXattr []byte, deletedDoc bool, expiry *uint32, err error) {
			updated, updatedXattr, deletedDoc, expiry, err = callback(current, xattr, cas)
			b.config.WriteUpdateCallback(k)
			return updated, updatedXattr, deletedDoc, expiry, err
		}
		return b.bucket.WriteUpdateWithXattr(k, xattr, exp, previous, wrapperCallback)
	}
	return b.bucket.WriteUpdateWithXattr(k, xattr, exp, previous, callback)
}

func (b *LeakyBucket) GetWithXattr(k string, xattr string, rv interface{}, xv interface{}) (cas uint64, err error) {
	return b.bucket.GetWithXattr(k, xattr, rv, xv)
}

func (b *LeakyBucket) DeleteWithXattr(k string, xattr string) error {
	return b.bucket.DeleteWithXattr(k, xattr)
}

func (b *LeakyBucket) GetXattr(k string, xattr string, xv interface{}) (cas uint64, err error) {
	return b.bucket.GetXattr(k, xattr, xv)
}

func (b *LeakyBucket) StartTapFeed(args sgbucket.FeedArguments) (sgbucket.MutationFeed, error) {

	if b.config.TapFeedDeDuplication {
		return b.wrapFeedForDeduplication(args)
	} else if len(b.config.TapFeedMissingDocs) > 0 {
		callback := func(event *sgbucket.FeedEvent) bool {
			for _, key := range b.config.TapFeedMissingDocs {
				if string(event.Key) == key {
					return false
				}
			}
			return true
		}
		return b.wrapFeed(args, callback)
	} else if b.config.TapFeedVbuckets {
		// kick off the wrapped sgbucket tap feed
		walrusTapFeed, err := b.bucket.StartTapFeed(args)
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
		}()
		return vbTapFeed, nil

	} else {
		return b.bucket.StartTapFeed(args)
	}

}

func (b *LeakyBucket) StartDCPFeed(args sgbucket.FeedArguments, callback sgbucket.FeedEventCallbackFunc) error {
	return b.bucket.StartDCPFeed(args, callback)
}

type EventUpdateFunc func(event *sgbucket.FeedEvent) bool

func (b *LeakyBucket) wrapFeed(args sgbucket.FeedArguments, callback EventUpdateFunc) (sgbucket.MutationFeed, error) {

	// kick off the wrapped sgbucket tap feed
	walrusTapFeed, err := b.bucket.StartTapFeed(args)
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
	}()
	return wrapperFeed, nil
}

func (b *LeakyBucket) wrapFeedForDeduplication(args sgbucket.FeedArguments) (sgbucket.MutationFeed, error) {
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
	walrusTapFeed, err := b.bucket.StartTapFeed(args)
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

		// the buffer to hold tap events that are candidates for de-duplication
		deDupeBuffer := []sgbucket.FeedEvent{}

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
				}

			case <-time.After(deDuplicationTimeoutMs):

				// give up on waiting for the buffer to fill up,
				// de-dupe and send what we currently have
				dedupeAndForward(deDupeBuffer, channel)
				deDupeBuffer = []sgbucket.FeedEvent{}

			}
		}

	}()
	return dupeTapFeed, nil
}

func (b *LeakyBucket) Close() {
	b.bucket.Close()
}
func (b *LeakyBucket) Dump() {
	b.bucket.Dump()
}
func (b *LeakyBucket) VBHash(docID string) uint32 {
	if b.config.TapFeedVbuckets {
		return VBHash(docID, 1024)
	} else {
		return b.bucket.VBHash(docID)
	}
}

func (b *LeakyBucket) CouchbaseServerVersion() (major uint64, minor uint64, micro string) {
	return b.bucket.CouchbaseServerVersion()
}

func (b *LeakyBucket) UUID() (string, error) {
	return b.bucket.UUID()
}

func (b *LeakyBucket) CloseAndDelete() error {
	if bucket, ok := b.bucket.(sgbucket.DeleteableBucket); ok {
		return bucket.CloseAndDelete()
	}
	return nil
}

func (b *LeakyBucket) GetStatsVbSeqno(maxVbno uint16, useAbsHighSeqNo bool) (uuids map[uint16]uint64, highSeqnos map[uint16]uint64, seqErr error) {
	return b.bucket.GetStatsVbSeqno(maxVbno, useAbsHighSeqNo)
}

// Accessors to set leaky bucket config for a running bucket.  Used to tune properties on a walrus bucket created as part of rest tester - it will
// be a leaky bucket (due to DCP support), but there's no mechanism to pass in a leaky bucket config to a RestTester bucket at bucket creation time.
func (b *LeakyBucket) SetFirstTimeViewCustomPartialError(val bool) {
	b.config.FirstTimeViewCustomPartialError = val
}

func (b *LeakyBucket) SetPostQueryCallback(callback func(ddoc, viewName string, params map[string]interface{})) {
	b.config.PostQueryCallback = callback
}

func (b *LeakyBucket) IsSupported(feature sgbucket.BucketFeature) bool {
	return b.bucket.IsSupported(feature)
}

func (b *LeakyBucket) Query(statement string, params interface{}, consistency gocb.ConsistencyMode, adhoc bool) (results gocb.QueryResults, err error) {
	gocbBucket, ok := AsGoCBBucket(b.bucket)
	if !ok {
		return nil, errors.New("Not GOCB Bucket")
	}
	return gocbBucket.Query(statement, params, consistency, adhoc)
}

func (b *LeakyBucket) ExplainQuery(statement string, params interface{}) (plain map[string]interface{}, err error) {
	gocbBucket, ok := AsGoCBBucket(b.bucket)
	if !ok {
		return nil, errors.New("Not GOCB Bucket")
	}
	return gocbBucket.ExplainQuery(statement, params)
}

func (b *LeakyBucket) CreateIndex(indexName string, expression string, filterExpression string, options *N1qlIndexOptions) error {
	gocbBucket, ok := AsGoCBBucket(b.bucket)
	if !ok {
		return errors.New("Not GOCB Bucket")
	}
	return gocbBucket.CreateIndex(indexName, expression, filterExpression, options)
}

func (b *LeakyBucket) BuildDeferredIndexes(indexSet []string) error {
	gocbBucket, ok := AsGoCBBucket(b.bucket)
	if !ok {
		return errors.New("Not GOCB Bucket")
	}
	return gocbBucket.BuildDeferredIndexes(indexSet)
}

func (b *LeakyBucket) CreatePrimaryIndex(indexName string, options *N1qlIndexOptions) error {
	gocbBucket, ok := AsGoCBBucket(b.bucket)
	if !ok {
		return errors.New("Not GOCB Bucket")
	}
	return gocbBucket.CreatePrimaryIndex(indexName, options)
}

func (b *LeakyBucket) WaitForIndexOnline(indexName string) error {
	gocbBucket, ok := AsGoCBBucket(b.bucket)
	if !ok {
		return errors.New("Not GOCB Bucket")
	}
	return gocbBucket.WaitForIndexOnline(indexName)
}

func (b *LeakyBucket) GetIndexMeta(indexName string) (exists bool, meta *gocb.IndexInfo, err error) {
	gocbBucket, ok := AsGoCBBucket(b.bucket)
	if !ok {
		return false, nil, errors.New("Not GOCB Bucket")
	}
	return gocbBucket.GetIndexMeta(indexName)
}

func (b *LeakyBucket) DropIndex(indexName string) error {

	gocbBucket, ok := AsGoCBBucket(b.bucket)
	if !ok {
		return errors.New("Not GOCB Bucket")
	}

	if len(b.config.DropIndexErrorNames) > 0 {
		for _, indexNameFail := range b.config.DropIndexErrorNames {
			if indexNameFail == indexName {
				return errors.New(fmt.Sprintf("Artificial leaky bucket error"))
			}
		}
	}

	return gocbBucket.DropIndex(indexName)
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
