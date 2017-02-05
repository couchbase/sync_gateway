package base

import (
	"errors"
	"fmt"
	"time"

	sgbucket "github.com/couchbase/sg-bucket"
)

// A wrapper around a Bucket to support forced errors.  For testing use only.
type LeakyBucket struct {
	bucket    Bucket
	incrCount uint16
	config    LeakyBucketConfig
}

// The config object that controls the LeakyBucket behavior
type LeakyBucketConfig struct {

	// Incr() fails 3 times before finally succeeding
	IncrTemporaryFailCount uint16

	// Emulate TAP/DCP feed de-dupliation behavior, such that within a
	// window of # of mutations or a timeout, mutations for a given document
	// will be filtered such that only the _latest_ mutation will make it through.
	TapFeedDeDuplication bool
	TapFeedVbuckets      bool     // Emulate vbucket numbers on feed
	TapFeedMissingDocs   []string // Emulate entry not appearing on tap feed
}

func NewLeakyBucket(bucket Bucket, config LeakyBucketConfig) Bucket {
	return &LeakyBucket{
		bucket: bucket,
		config: config,
	}
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
func (b *LeakyBucket) GetAndTouchRaw(k string, exp int) (v []byte, cas uint64, err error) {
	return b.bucket.GetAndTouchRaw(k, exp)
}
func (b *LeakyBucket) Add(k string, exp int, v interface{}) (added bool, err error) {
	return b.bucket.Add(k, exp, v)
}
func (b *LeakyBucket) AddRaw(k string, exp int, v []byte) (added bool, err error) {
	return b.bucket.AddRaw(k, exp, v)
}
func (b *LeakyBucket) Append(k string, data []byte) error {
	return b.bucket.Append(k, data)
}
func (b *LeakyBucket) Set(k string, exp int, v interface{}) error {
	return b.bucket.Set(k, exp, v)
}
func (b *LeakyBucket) SetRaw(k string, exp int, v []byte) error {
	return b.bucket.SetRaw(k, exp, v)
}
func (b *LeakyBucket) Delete(k string) error {
	return b.bucket.Delete(k)
}
func (b *LeakyBucket) Write(k string, flags int, exp int, v interface{}, opt sgbucket.WriteOptions) error {
	return b.bucket.Write(k, flags, exp, v, opt)
}
func (b *LeakyBucket) WriteCas(k string, flags int, exp int, cas uint64, v interface{}, opt sgbucket.WriteOptions) (uint64, error) {
	return b.bucket.WriteCas(k, flags, exp, cas, v, opt)
}
func (b *LeakyBucket) Update(k string, exp int, callback sgbucket.UpdateFunc) (err error) {
	return b.bucket.Update(k, exp, callback)
}
func (b *LeakyBucket) WriteUpdate(k string, exp int, callback sgbucket.WriteUpdateFunc) (err error) {
	return b.bucket.WriteUpdate(k, exp, callback)
}
func (b *LeakyBucket) SetBulk(entries []*sgbucket.BulkSetEntry) (err error) {
	panic("SetBulk not implemented")
}

func (b *LeakyBucket) Incr(k string, amt, def uint64, exp int) (uint64, error) {

	if b.config.IncrTemporaryFailCount > 0 {
		if b.incrCount < b.config.IncrTemporaryFailCount {
			b.incrCount++
			return 0, errors.New(fmt.Sprintf("Incr forced abort (%d/%d), try again maybe?", b.incrCount, b.config.IncrTemporaryFailCount))
		}
		b.incrCount = 0

	}
	return b.bucket.Incr(k, amt, def, exp)
}

func (b *LeakyBucket) GetDDoc(docname string, value interface{}) error {
	return b.bucket.GetDDoc(docname, value)
}
func (b *LeakyBucket) PutDDoc(docname string, value interface{}) error {
	return b.bucket.PutDDoc(docname, value)
}
func (b *LeakyBucket) DeleteDDoc(docname string) error {
	return b.bucket.DeleteDDoc(docname)
}
func (b *LeakyBucket) View(ddoc, name string, params map[string]interface{}) (sgbucket.ViewResult, error) {
	return b.bucket.View(ddoc, name, params)
}
func (b *LeakyBucket) ViewCustom(ddoc, name string, params map[string]interface{}, vres interface{}) error {
	return b.bucket.ViewCustom(ddoc, name, params, vres)
}

func (b *LeakyBucket) Refresh() error {
	return b.bucket.Refresh()
}

func (b *LeakyBucket) StartTapFeed(args sgbucket.TapArguments) (sgbucket.TapFeed, error) {

	if b.config.TapFeedDeDuplication {
		return b.wrapFeedForDeduplication(args)
	} else if len(b.config.TapFeedMissingDocs) > 0 {
		callback := func(event *sgbucket.TapEvent) bool {
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
		// this is the sgbucket.TapFeed impl we'll return to callers, which
		// will add vbucket information
		channel := make(chan sgbucket.TapEvent, 10)
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

type EventUpdateFunc func(event *sgbucket.TapEvent) bool

func (b *LeakyBucket) wrapFeed(args sgbucket.TapArguments, callback EventUpdateFunc) (sgbucket.TapFeed, error) {

	// kick off the wrapped sgbucket tap feed
	walrusTapFeed, err := b.bucket.StartTapFeed(args)
	if err != nil {
		return walrusTapFeed, err
	}

	// create an output channel
	channel := make(chan sgbucket.TapEvent, 10)

	// this is the sgbucket.TapFeed impl we'll return to callers, which
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

func (b *LeakyBucket) wrapFeedForDeduplication(args sgbucket.TapArguments) (sgbucket.TapFeed, error) {
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
	channel := make(chan sgbucket.TapEvent, 10)

	// this is the sgbucket.TapFeed impl we'll return to callers, which
	// will reead from the de-duplicated events channel
	dupeTapFeed := &wrappedTapFeedImpl{
		channel:        channel,
		wrappedTapFeed: walrusTapFeed,
	}

	go func() {

		// the buffer to hold tap events that are candidates for de-duplication
		deDupeBuffer := []sgbucket.TapEvent{}

		for {
			select {
			case tapEvent, ok := <-walrusTapFeed.Events():
				if !ok {
					// channel closed, goroutine is done
					// dedupe and send what we currently have
					dedupeAndForward(deDupeBuffer, channel)
					deDupeBuffer = []sgbucket.TapEvent{}
					return
				}
				deDupeBuffer = append(deDupeBuffer, tapEvent)

				// if we've collected enough, dedeupe and send what we have,
				// and reset buffer.
				if len(deDupeBuffer) >= deDuplicationWindowSize {
					dedupeAndForward(deDupeBuffer, channel)
					deDupeBuffer = []sgbucket.TapEvent{}
				}

			case <-time.After(deDuplicationTimeoutMs):

				// give up on waiting for the buffer to fill up,
				// de-dupe and send what we currently have
				dedupeAndForward(deDupeBuffer, channel)
				deDupeBuffer = []sgbucket.TapEvent{}

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

// An implementation of a sgbucket tap feed that wraps
// tap events on the upstream tap feed to better emulate real world
// TAP/DCP behavior.
type wrappedTapFeedImpl struct {
	channel        chan sgbucket.TapEvent
	wrappedTapFeed sgbucket.TapFeed
}

func (feed *wrappedTapFeedImpl) Close() error {
	return feed.wrappedTapFeed.Close()
}

func (feed *wrappedTapFeedImpl) Events() <-chan sgbucket.TapEvent {
	return feed.channel
}

func (feed *wrappedTapFeedImpl) WriteEvents() chan<- sgbucket.TapEvent {
	return feed.channel
}

func dedupeAndForward(tapEvents []sgbucket.TapEvent, destChannel chan<- sgbucket.TapEvent) {

	deduped := dedupeTapEvents(tapEvents)

	for _, tapEvent := range deduped {
		destChannel <- tapEvent
	}

}

func dedupeTapEvents(tapEvents []sgbucket.TapEvent) []sgbucket.TapEvent {

	// For each document key, keep track of the latest seen tapEvent
	// doc1 -> tapEvent with Seq=1
	// doc2 -> tapEvent with Seq=5
	// (if tapEvent with Seq=7 comes in for doc1, it will clobber existing)
	latestTapEventPerKey := map[string]sgbucket.TapEvent{}

	for _, tapEvent := range tapEvents {
		key := string(tapEvent.Key)
		latestTapEventPerKey[key] = tapEvent
	}

	// Iterate over the original tapEvents, and only keep what
	// is in latestTapEventPerKey, and discard all previous mutations
	// of that doc.  This will preserve the original
	// sequence order as read off the feed.
	deduped := []sgbucket.TapEvent{}
	for _, tapEvent := range tapEvents {
		key := string(tapEvent.Key)
		latestTapEventForKey := latestTapEventPerKey[key]
		if tapEvent.Sequence == latestTapEventForKey.Sequence {
			deduped = append(deduped, tapEvent)
		}
	}

	return deduped

}

// VBHash finds the vbucket for the given key.
func VBHash(key string, numVb int) uint32 {
	return sgbucket.VBHash(key, uint16(numVb))
}
