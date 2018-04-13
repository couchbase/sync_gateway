package base

import (
	"time"

	sgbucket "github.com/couchbase/sg-bucket"
)

// A wrapper around a Bucket that transparently adds logging of all the API calls.
type LoggingBucket struct {
	bucket Bucket
}

func (b *LoggingBucket) GetName() string {
	//LogTo("Bucket", "GetName()")
	return b.bucket.GetName()
}
func (b *LoggingBucket) Get(k string, rv interface{}) (uint64, error) {
	start := time.Now()
	defer func() { LogToR("Bucket", "Get(%q) [%v]", UD(k), time.Since(start)) }()
	return b.bucket.Get(k, rv)
}
func (b *LoggingBucket) GetRaw(k string) (v []byte, cas uint64, err error) {
	start := time.Now()
	defer func() { LogToR("Bucket", "GetRaw(%q) [%v]", UD(k), time.Since(start)) }()
	return b.bucket.GetRaw(k)
}
func (b *LoggingBucket) GetAndTouchRaw(k string, exp uint32) (v []byte, cas uint64, err error) {
	start := time.Now()
	defer func() { LogToR("Bucket", "GetAndTouchRaw(%q) [%v]", UD(k), time.Since(start)) }()
	return b.bucket.GetAndTouchRaw(k, exp)
}
func (b *LoggingBucket) GetBulkRaw(keys []string) (map[string][]byte, error) {
	start := time.Now()
	defer func() { LogToR("Bucket", "GetBulkRaw(%q) [%v]", UD(keys), time.Since(start)) }()
	return b.bucket.GetBulkRaw(keys)
}
func (b *LoggingBucket) Add(k string, exp uint32, v interface{}) (added bool, err error) {
	start := time.Now()
	defer func() { LogToR("Bucket", "Add(%q, %d, ...) [%v]", UD(k), exp, time.Since(start)) }()
	return b.bucket.Add(k, exp, v)
}
func (b *LoggingBucket) AddRaw(k string, exp uint32, v []byte) (added bool, err error) {
	start := time.Now()
	defer func() { LogToR("Bucket", "AddRaw(%q, %d, ...) [%v]", UD(k), exp, time.Since(start)) }()
	return b.bucket.AddRaw(k, exp, v)
}
func (b *LoggingBucket) Append(k string, data []byte) error {
	start := time.Now()
	defer func() { LogToR("Bucket", "Append(%q, ...) [%v]", UD(k), time.Since(start)) }()
	return b.bucket.Append(k, data)
}
func (b *LoggingBucket) Set(k string, exp uint32, v interface{}) error {
	start := time.Now()
	defer func() { LogToR("Bucket", "Set(%q, %d, ...) [%v]", UD(k), exp, time.Since(start)) }()
	return b.bucket.Set(k, exp, v)
}
func (b *LoggingBucket) SetRaw(k string, exp uint32, v []byte) error {
	start := time.Now()
	defer func() { LogToR("Bucket", "SetRaw(%q, %d, ...) [%v]", UD(k), exp, time.Since(start)) }()
	return b.bucket.SetRaw(k, exp, v)
}
func (b *LoggingBucket) Delete(k string) error {
	start := time.Now()
	defer func() { LogToR("Bucket", "Delete(%q) [%v]", UD(k), time.Since(start)) }()
	return b.bucket.Delete(k)
}
func (b *LoggingBucket) Remove(k string, cas uint64) (casOut uint64, err error) {
	start := time.Now()
	defer func() { LogToR("Bucket", "Remove(%q) [%v]", UD(k), time.Since(start)) }()
	return b.bucket.Remove(k, cas)
}
func (b *LoggingBucket) Write(k string, flags int, exp uint32, v interface{}, opt sgbucket.WriteOptions) error {
	start := time.Now()
	defer func() {
		LogToR("Bucket", "Write(%q, 0x%x, %d, ..., 0x%x) [%v]", UD(k), flags, exp, opt, time.Since(start))
	}()
	return b.bucket.Write(k, flags, exp, v, opt)
}
func (b *LoggingBucket) WriteCas(k string, flags int, exp uint32, cas uint64, v interface{}, opt sgbucket.WriteOptions) (uint64, error) {
	start := time.Now()
	defer func() {
		LogToR("Bucket", "WriteCas(%q, 0x%x, %d, %d, ..., 0x%x) [%v]", UD(k), flags, exp, cas, opt, time.Since(start))
	}()
	return b.bucket.WriteCas(k, flags, exp, cas, v, opt)
}
func (b *LoggingBucket) Update(k string, exp uint32, callback sgbucket.UpdateFunc) (err error) {
	start := time.Now()
	defer func() { LogToR("Bucket", "Update(%q, %d, ...) --> %v [%v]", UD(k), exp, err, time.Since(start)) }()
	return b.bucket.Update(k, exp, callback)
}
func (b *LoggingBucket) WriteUpdate(k string, exp uint32, callback sgbucket.WriteUpdateFunc) (err error) {
	start := time.Now()
	defer func() { LogToR("Bucket", "WriteUpdate(%q, %d, ...) --> %v [%v]", UD(k), exp, err, time.Since(start)) }()
	return b.bucket.WriteUpdate(k, exp, callback)
}
func (b *LoggingBucket) WriteUpdateAndTouch(k string, exp uint32, callback sgbucket.WriteUpdateFunc) (err error) {
	start := time.Now()
	defer func() { LogTo("Bucket", "WriteUpdateAndTouch(%q, %d, ...) --> %v [%v]", k, exp, err, time.Since(start)) }()
	return b.bucket.WriteUpdateAndTouch(k, exp, callback)
}

func (b *LoggingBucket) Incr(k string, amt, def uint64, exp uint32) (uint64, error) {
	start := time.Now()
	defer func() { LogToR("Bucket", "Incr(%q, %d, %d, %d) [%v]", UD(k), amt, def, exp, time.Since(start)) }()
	return b.bucket.Incr(k, amt, def, exp)
}
func (b *LoggingBucket) WriteCasWithXattr(k string, xattr string, exp uint32, cas uint64, v interface{}, xv interface{}) (casOut uint64, err error) {
	start := time.Now()
	defer func() { LogToR("Bucket", "WriteCasWithXattr(%q, ...) [%v]", UD(k), time.Since(start)) }()
	return b.bucket.WriteCasWithXattr(k, xattr, exp, cas, v, xv)
}
func (b *LoggingBucket) WriteUpdateWithXattr(k string, xattr string, exp uint32, previous *sgbucket.BucketDocument, callback sgbucket.WriteUpdateWithXattrFunc) (casOut uint64, err error) {
	start := time.Now()
	defer func() {
		LogToR("Bucket", "WriteUpdateWithXattr(%q, %d, ...) --> %v [%v]", UD(k), exp, err, time.Since(start))
	}()
	return b.bucket.WriteUpdateWithXattr(k, xattr, exp, previous, callback)
}
func (b *LoggingBucket) GetWithXattr(k string, xattr string, rv interface{}, xv interface{}) (cas uint64, err error) {
	start := time.Now()
	defer func() { LogToR("Bucket", "GetWithXattr(%q, ...) [%v]", UD(k), time.Since(start)) }()
	return b.bucket.GetWithXattr(k, xattr, rv, xv)
}
func (b *LoggingBucket) DeleteWithXattr(k string, xattr string) error {
	start := time.Now()
	defer func() { LogToR("Bucket", "DeleteWithXattr(%q, ...) [%v]", UD(k), time.Since(start)) }()
	return b.bucket.DeleteWithXattr(k, xattr)
}
func (b *LoggingBucket) GetDDoc(docname string, value interface{}) error {
	start := time.Now()
	defer func() { LogToR("Bucket", "GetDDoc(%q, ...) [%v]", UD(docname), time.Since(start)) }()
	return b.bucket.GetDDoc(docname, value)
}
func (b *LoggingBucket) PutDDoc(docname string, value interface{}) error {
	start := time.Now()
	defer func() { LogToR("Bucket", "PutDDoc(%q, ...) [%v]", UD(docname), time.Since(start)) }()
	return b.bucket.PutDDoc(docname, value)
}
func (b *LoggingBucket) DeleteDDoc(docname string) error {
	start := time.Now()
	defer func() { LogToR("Bucket", "DeleteDDoc(%q, ...) [%v]", UD(docname), time.Since(start)) }()
	return b.bucket.DeleteDDoc(docname)
}
func (b *LoggingBucket) View(ddoc, name string, params map[string]interface{}) (sgbucket.ViewResult, error) {
	start := time.Now()
	defer func() { LogToR("Bucket", "View(%q, %q, ...) [%v]", UD(ddoc), UD(name), time.Since(start)) }()
	return b.bucket.View(ddoc, name, params)
}

func (b *LoggingBucket) ViewCustom(ddoc, name string, params map[string]interface{}, vres interface{}) error {
	start := time.Now()
	defer func() { LogToR("Bucket", "ViewCustom(%q, %q, ...) [%v]", UD(ddoc), UD(name), time.Since(start)) }()
	return b.bucket.ViewCustom(ddoc, name, params, vres)
}

func (b *LoggingBucket) ViewQuery(ddoc, name string, params map[string]interface{}) (sgbucket.QueryResultIterator, error) {
	start := time.Now()
	defer func() { LogToR("Bucket", "ViewQuery(%q, %q, ...) [%v]", UD(ddoc), UD(name), time.Since(start)) }()
	return b.bucket.ViewQuery(ddoc, name, params)
}

func (b *LoggingBucket) SetBulk(entries []*sgbucket.BulkSetEntry) (err error) {
	start := time.Now()
	defer func() { LogToR("Bucket", "SetBulk(%q, ...) --> %v [%v]", UD(entries), err, time.Since(start)) }()
	return b.bucket.SetBulk(entries)
}

func (b *LoggingBucket) Refresh() error {
	start := time.Now()
	defer func() { LogToR("Bucket", "Refresh() [%v]", time.Since(start)) }()
	return b.bucket.Refresh()
}

func (b *LoggingBucket) StartTapFeed(args sgbucket.FeedArguments) (sgbucket.MutationFeed, error) {
	start := time.Now()
	defer func() { LogToR("Bucket", "StartTapFeed(...) [%v]", time.Since(start)) }()
	return b.bucket.StartTapFeed(args)
}

func (b *LoggingBucket) StartDCPFeed(args sgbucket.FeedArguments, callback sgbucket.FeedEventCallbackFunc) error {
	start := time.Now()
	defer func() { LogToR("Bucket", "StartDcpFeed(...) [%v]", time.Since(start)) }()
	return b.bucket.StartDCPFeed(args, callback)
}

func (b *LoggingBucket) Close() {
	start := time.Now()
	defer func() { LogToR("Bucket", "Close() [%v]", time.Since(start)) }()
	b.bucket.Close()
}
func (b *LoggingBucket) Dump() {
	LogToR("Bucket", "Dump()")
	b.bucket.Dump()
}
func (b *LoggingBucket) VBHash(docID string) uint32 {
	LogToR("Bucket", "VBHash()")
	return b.bucket.VBHash(docID)
}

func (b *LoggingBucket) GetMaxVbno() (uint16, error) {
	return b.bucket.GetMaxVbno()
}

func (b *LoggingBucket) CouchbaseServerVersion() (major uint64, minor uint64, micro string, err error) {
	return b.bucket.CouchbaseServerVersion()
}

func (b *LoggingBucket) UUID() (string, error) {
	return b.bucket.UUID()
}

func (b *LoggingBucket) GetStatsVbSeqno(maxVbno uint16, useAbsHighSeqNo bool) (uuids map[uint16]uint64, highSeqnos map[uint16]uint64, seqErr error) {
	return b.GetStatsVbSeqno(maxVbno, useAbsHighSeqNo)
}

func (b *LoggingBucket) SetTestCallback(fn sgbucket.TestCallbackFn) {
	b.bucket.SetTestCallback(fn)
}

func (b *LoggingBucket) GetTestCallback() (fn sgbucket.TestCallbackFn) {
	return b.bucket.GetTestCallback()
}