package base

import (
	"context"
	"expvar"
	"sync"
	"time"

	sgbucket "github.com/couchbase/sg-bucket"
)

// A wrapper around a Bucket that transparently adds logging of all the API calls.
type LoggingBucket struct {
	bucket     Bucket
	logCtx     context.Context
	logCtxOnce sync.Once
}

func (b *LoggingBucket) ctx() context.Context {
	b.logCtxOnce.Do(func() {
		b.logCtx = bucketCtx(context.Background(), b)
	})
	return b.logCtx
}

func (b *LoggingBucket) log(start time.Time, args ...interface{}) {
	caller := GetCallersName(1, false)
	TracefCtx(b.ctx(), KeyBucket, "%s(%v) [%v]", caller, UD(args), time.Since(start))
}

func (b *LoggingBucket) GetName() string {
	// b.log() depends on this, so don't log here otherwise we'd stack overflow
	return b.bucket.GetName()
}
func (b *LoggingBucket) Get(k string, rv interface{}) (uint64, error) {
	defer b.log(time.Now(), k)
	return b.bucket.Get(k, rv)
}
func (b *LoggingBucket) GetRaw(k string) (v []byte, cas uint64, err error) {
	defer b.log(time.Now(), k)
	return b.bucket.GetRaw(k)
}
func (b *LoggingBucket) GetAndTouchRaw(k string, exp uint32) (v []byte, cas uint64, err error) {
	defer b.log(time.Now(), k, exp)
	return b.bucket.GetAndTouchRaw(k, exp)
}
func (b *LoggingBucket) Touch(k string, exp uint32) (cas uint64, err error) {
	defer b.log(time.Now(), k, exp)
	return b.bucket.Touch(k, exp)
}
func (b *LoggingBucket) GetBulkRaw(keys []string) (map[string][]byte, error) {
	defer b.log(time.Now(), keys)
	return b.bucket.GetBulkRaw(keys)
}
func (b *LoggingBucket) Add(k string, exp uint32, v interface{}) (added bool, err error) {
	defer b.log(time.Now(), k, exp)
	return b.bucket.Add(k, exp, v)
}
func (b *LoggingBucket) AddRaw(k string, exp uint32, v []byte) (added bool, err error) {
	defer b.log(time.Now(), k, exp)
	return b.bucket.AddRaw(k, exp, v)
}
func (b *LoggingBucket) Append(k string, data []byte) error {
	defer b.log(time.Now(), k)
	return b.bucket.Append(k, data)
}
func (b *LoggingBucket) Set(k string, exp uint32, v interface{}) error {
	defer b.log(time.Now(), k, exp)
	return b.bucket.Set(k, exp, v)
}
func (b *LoggingBucket) SetRaw(k string, exp uint32, v []byte) error {
	defer b.log(time.Now(), k, exp)
	return b.bucket.SetRaw(k, exp, v)
}
func (b *LoggingBucket) Delete(k string) error {
	defer b.log(time.Now(), k)
	return b.bucket.Delete(k)
}
func (b *LoggingBucket) Remove(k string, cas uint64) (casOut uint64, err error) {
	defer b.log(time.Now(), k, cas)
	return b.bucket.Remove(k, cas)
}
func (b *LoggingBucket) Write(k string, flags int, exp uint32, v interface{}, opt sgbucket.WriteOptions) error {
	defer b.log(time.Now(), k, flags, exp, opt)
	return b.bucket.Write(k, flags, exp, v, opt)
}
func (b *LoggingBucket) WriteCas(k string, flags int, exp uint32, cas uint64, v interface{}, opt sgbucket.WriteOptions) (uint64, error) {
	defer b.log(time.Now(), k, flags, exp, cas, opt)
	return b.bucket.WriteCas(k, flags, exp, cas, v, opt)
}
func (b *LoggingBucket) Update(k string, exp uint32, callback sgbucket.UpdateFunc) (casOut uint64, err error) {
	defer b.log(time.Now(), k, exp)
	return b.bucket.Update(k, exp, callback)
}
func (b *LoggingBucket) WriteUpdate(k string, exp uint32, callback sgbucket.WriteUpdateFunc) (casOut uint64, err error) {
	defer b.log(time.Now(), k, exp)
	return b.bucket.WriteUpdate(k, exp, callback)
}

func (b *LoggingBucket) Incr(k string, amt, def uint64, exp uint32) (uint64, error) {
	defer b.log(time.Now(), k, amt, def, exp)
	return b.bucket.Incr(k, amt, def, exp)
}
func (b *LoggingBucket) WriteCasWithXattr(k string, xattr string, exp uint32, cas uint64, v interface{}, xv interface{}) (casOut uint64, err error) {
	defer b.log(time.Now(), k, xattr, exp, cas)
	return b.bucket.WriteCasWithXattr(k, xattr, exp, cas, v, xv)
}
func (b *LoggingBucket) WriteUpdateWithXattr(k string, xattr string, exp uint32, previous *sgbucket.BucketDocument, callback sgbucket.WriteUpdateWithXattrFunc) (casOut uint64, err error) {
	defer b.log(time.Now(), k, xattr, exp)
	return b.bucket.WriteUpdateWithXattr(k, xattr, exp, previous, callback)
}
func (b *LoggingBucket) GetWithXattr(k string, xattr string, rv interface{}, xv interface{}) (cas uint64, err error) {
	defer b.log(time.Now(), k, xattr)
	return b.bucket.GetWithXattr(k, xattr, rv, xv)
}
func (b *LoggingBucket) DeleteWithXattr(k string, xattr string) error {
	defer b.log(time.Now(), k, xattr)
	return b.bucket.DeleteWithXattr(k, xattr)
}
func (b *LoggingBucket) GetXattr(k string, xattr string, xv interface{}) (cas uint64, err error) {
	defer b.log(time.Now(), k, xattr)
	return b.bucket.GetXattr(k, xattr, xv)
}
func (b *LoggingBucket) GetDDocs(value interface{}) error {
	defer b.log(time.Now())
	return b.bucket.GetDDocs(value)
}
func (b *LoggingBucket) GetDDoc(docname string, value interface{}) error {
	defer b.log(time.Now(), docname)
	return b.bucket.GetDDoc(docname, value)
}
func (b *LoggingBucket) PutDDoc(docname string, value interface{}) error {
	defer b.log(time.Now(), docname)
	return b.bucket.PutDDoc(docname, value)
}
func (b *LoggingBucket) DeleteDDoc(docname string) error {
	defer b.log(time.Now(), docname)
	return b.bucket.DeleteDDoc(docname)
}
func (b *LoggingBucket) View(ddoc, name string, params map[string]interface{}) (sgbucket.ViewResult, error) {
	defer b.log(time.Now(), ddoc, name)
	return b.bucket.View(ddoc, name, params)
}

func (b *LoggingBucket) ViewCustom(ddoc, name string, params map[string]interface{}, vres interface{}) error {
	defer b.log(time.Now(), ddoc, name)
	return b.bucket.ViewCustom(ddoc, name, params, vres)
}

func (b *LoggingBucket) ViewQuery(ddoc, name string, params map[string]interface{}) (sgbucket.QueryResultIterator, error) {
	defer b.log(time.Now(), ddoc, name)
	return b.bucket.ViewQuery(ddoc, name, params)
}

func (b *LoggingBucket) SetBulk(entries []*sgbucket.BulkSetEntry) (err error) {
	defer b.log(time.Now(), entries)
	return b.bucket.SetBulk(entries)
}

func (b *LoggingBucket) Refresh() error {
	defer b.log(time.Now())
	return b.bucket.Refresh()
}

func (b *LoggingBucket) StartTapFeed(args sgbucket.FeedArguments, dbStats *expvar.Map) (sgbucket.MutationFeed, error) {
	defer b.log(time.Now())
	return b.bucket.StartTapFeed(args, dbStats)
}

func (b *LoggingBucket) StartDCPFeed(args sgbucket.FeedArguments, callback sgbucket.FeedEventCallbackFunc, dbStats *expvar.Map) error {
	defer b.log(time.Now())
	return b.bucket.StartDCPFeed(args, callback, dbStats)
}

func (b *LoggingBucket) Close() {
	defer b.log(time.Now())
	b.bucket.Close()
}
func (b *LoggingBucket) Dump() {
	defer b.log(time.Now())
	b.bucket.Dump()
}
func (b *LoggingBucket) VBHash(docID string) uint32 {
	defer b.log(time.Now())
	return b.bucket.VBHash(docID)
}

func (b *LoggingBucket) GetMaxVbno() (uint16, error) {
	defer b.log(time.Now())
	return b.bucket.GetMaxVbno()
}

func (b *LoggingBucket) CouchbaseServerVersion() (major uint64, minor uint64, micro string) {
	defer b.log(time.Now())
	return b.bucket.CouchbaseServerVersion()
}

func (b *LoggingBucket) UUID() (string, error) {
	defer b.log(time.Now())
	return b.bucket.UUID()
}

func (b *LoggingBucket) GetStatsVbSeqno(maxVbno uint16, useAbsHighSeqNo bool) (uuids map[uint16]uint64, highSeqnos map[uint16]uint64, seqErr error) {
	defer b.log(time.Now())
	return b.bucket.GetStatsVbSeqno(maxVbno, useAbsHighSeqNo)
}

// GetUnderlyingBucket returns the underlying bucket for the LoggingBucket.
func (b *LoggingBucket) GetUnderlyingBucket() Bucket {
	defer b.log(time.Now())
	return b.bucket
}

func (b *LoggingBucket) IsSupported(feature sgbucket.BucketFeature) bool {
	defer b.log(time.Now())
	return b.bucket.IsSupported(feature)
}
