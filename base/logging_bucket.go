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
	defer func() { LogTo("Bucket", "Get(%q) [%v]", k, time.Since(start)) }()
	return b.bucket.Get(k, rv)
}
func (b *LoggingBucket) GetRaw(k string) (v []byte, cas uint64, err error) {
	start := time.Now()
	defer func() { LogTo("Bucket", "GetRaw(%q) [%v]", k, time.Since(start)) }()
	return b.bucket.GetRaw(k)
}
func (b *LoggingBucket) GetAndTouchRaw(k string, exp uint32) (v []byte, cas uint64, err error) {
	start := time.Now()
	defer func() { LogTo("Bucket", "GetAndTouchRaw(%q) [%v]", k, time.Since(start)) }()
	return b.bucket.GetAndTouchRaw(k, exp)
}
func (b *LoggingBucket) GetBulkRaw(keys []string) (map[string][]byte, error) {
	start := time.Now()
	defer func() { LogTo("Bucket", "GetBulkRaw(%q) [%v]", keys, time.Since(start)) }()
	return b.bucket.GetBulkRaw(keys)
}
func (b *LoggingBucket) Add(k string, exp uint32, v interface{}) (added bool, err error) {
	start := time.Now()
	defer func() { LogTo("Bucket", "Add(%q, %d, ...) [%v]", k, exp, time.Since(start)) }()
	return b.bucket.Add(k, exp, v)
}
func (b *LoggingBucket) AddRaw(k string, exp uint32, v []byte) (added bool, err error) {
	start := time.Now()
	defer func() { LogTo("Bucket", "AddRaw(%q, %d, ...) [%v]", k, exp, time.Since(start)) }()
	return b.bucket.AddRaw(k, exp, v)
}
func (b *LoggingBucket) Append(k string, data []byte) error {
	start := time.Now()
	defer func() { LogTo("Bucket", "Append(%q, ...) [%v]", k, time.Since(start)) }()
	return b.bucket.Append(k, data)
}
func (b *LoggingBucket) Set(k string, exp uint32, v interface{}) error {
	start := time.Now()
	defer func() { LogTo("Bucket", "Set(%q, %d, ...) [%v]", k, exp, time.Since(start)) }()
	return b.bucket.Set(k, exp, v)
}
func (b *LoggingBucket) SetRaw(k string, exp uint32, v []byte) error {
	start := time.Now()
	defer func() { LogTo("Bucket", "SetRaw(%q, %d, ...) [%v]", k, exp, time.Since(start)) }()
	return b.bucket.SetRaw(k, exp, v)
}
func (b *LoggingBucket) Delete(k string) error {
	start := time.Now()
	defer func() { LogTo("Bucket", "Delete(%q) [%v]", k, time.Since(start)) }()
	return b.bucket.Delete(k)
}
func (b *LoggingBucket) Remove(k string, cas uint64) (casOut uint64, err error) {
	start := time.Now()
	defer func() { LogTo("Bucket", "Remove(%q) [%v]", k, time.Since(start)) }()
	return b.bucket.Remove(k, cas)
}
func (b *LoggingBucket) Write(k string, flags int, exp uint32, v interface{}, opt sgbucket.WriteOptions) error {
	start := time.Now()
	defer func() { LogTo("Bucket", "Write(%q, 0x%x, %d, ..., 0x%x) [%v]", k, flags, exp, opt, time.Since(start)) }()
	return b.bucket.Write(k, flags, exp, v, opt)
}
func (b *LoggingBucket) WriteCas(k string, flags int, exp uint32, cas uint64, v interface{}, opt sgbucket.WriteOptions) (uint64, error) {
	start := time.Now()
	defer func() {
		LogTo("Bucket", "WriteCas(%q, 0x%x, %d, %d, ..., 0x%x) [%v]", k, flags, exp, cas, opt, time.Since(start))
	}()
	return b.bucket.WriteCas(k, flags, exp, cas, v, opt)
}
func (b *LoggingBucket) Update(k string, exp uint32, callback sgbucket.UpdateFunc) (err error) {
	start := time.Now()
	defer func() { LogTo("Bucket", "Update(%q, %d, ...) --> %v [%v]", k, exp, err, time.Since(start)) }()
	return b.bucket.Update(k, exp, callback)
}
func (b *LoggingBucket) WriteUpdate(k string, exp uint32, callback sgbucket.WriteUpdateFunc) (err error) {
	start := time.Now()
	defer func() { LogTo("Bucket", "WriteUpdate(%q, %d, ...) --> %v [%v]", k, exp, err, time.Since(start)) }()
	return b.bucket.WriteUpdate(k, exp, callback)
}

func (b *LoggingBucket) Incr(k string, amt, def uint64, exp uint32) (uint64, error) {
	start := time.Now()
	defer func() { LogTo("Bucket", "Incr(%q, %d, %d, %d) [%v]", k, amt, def, exp, time.Since(start)) }()
	return b.bucket.Incr(k, amt, def, exp)
}
func (b *LoggingBucket) WriteCasWithXattr(k string, xattr string, exp uint32, cas uint64, v interface{}, xv interface{}) (casOut uint64, err error) {
	start := time.Now()
	defer func() { LogTo("Bucket", "WriteCasWithXattr(%q, ...) [%v]", k, time.Since(start)) }()
	return b.bucket.WriteCasWithXattr(k, xattr, exp, cas, v, xv)
}
func (b *LoggingBucket) WriteUpdateWithXattr(k string, xattr string, exp uint32, previous *sgbucket.BucketDocument, callback sgbucket.WriteUpdateWithXattrFunc) (casOut uint64, err error) {
	start := time.Now()
	defer func() {
		LogTo("Bucket", "WriteUpdateWithXattr(%q, %d, ...) --> %v [%v]", k, exp, err, time.Since(start))
	}()
	return b.bucket.WriteUpdateWithXattr(k, xattr, exp, previous, callback)
}
func (b *LoggingBucket) GetWithXattr(k string, xattr string, rv interface{}, xv interface{}) (cas uint64, err error) {
	start := time.Now()
	defer func() { LogTo("Bucket", "GetWithXattr(%q, ...) [%v]", k, time.Since(start)) }()
	return b.bucket.GetWithXattr(k, xattr, rv, xv)
}
func (b *LoggingBucket) DeleteWithXattr(k string, xattr string) error {
	start := time.Now()
	defer func() { LogTo("Bucket", "DeleteWithXattr(%q, ...) [%v]", k, time.Since(start)) }()
	return b.bucket.DeleteWithXattr(k, xattr)
}
func (b *LoggingBucket) GetDDoc(docname string, value interface{}) error {
	start := time.Now()
	defer func() { LogTo("Bucket", "GetDDoc(%q, ...) [%v]", docname, time.Since(start)) }()
	return b.bucket.GetDDoc(docname, value)
}
func (b *LoggingBucket) PutDDoc(docname string, value interface{}) error {
	start := time.Now()
	defer func() { LogTo("Bucket", "PutDDoc(%q, ...) [%v]", docname, time.Since(start)) }()
	return b.bucket.PutDDoc(docname, value)
}
func (b *LoggingBucket) DeleteDDoc(docname string) error {
	start := time.Now()
	defer func() { LogTo("Bucket", "DeleteDDoc(%q, ...) [%v]", docname, time.Since(start)) }()
	return b.bucket.DeleteDDoc(docname)
}
func (b *LoggingBucket) View(ddoc, name string, params map[string]interface{}) (sgbucket.ViewResult, error) {
	start := time.Now()
	defer func() { LogTo("Bucket", "View(%q, %q, ...) [%v]", ddoc, name, time.Since(start)) }()
	return b.bucket.View(ddoc, name, params)
}

func (b *LoggingBucket) ViewCustom(ddoc, name string, params map[string]interface{}, vres interface{}) error {
	start := time.Now()
	defer func() { LogTo("Bucket", "ViewCustom(%q, %q, ...) [%v]", ddoc, name, time.Since(start)) }()
	return b.bucket.ViewCustom(ddoc, name, params, vres)
}

func (b *LoggingBucket) ViewQuery(ddoc, name string, params map[string]interface{}) (sgbucket.QueryResultIterator, error) {
	start := time.Now()
	defer func() { LogTo("Bucket", "ViewQuery(%q, %q, ...) [%v]", ddoc, name, time.Since(start)) }()
	return b.bucket.ViewQuery(ddoc, name, params)
}

func (b *LoggingBucket) SetBulk(entries []*sgbucket.BulkSetEntry) (err error) {
	start := time.Now()
	defer func() { LogTo("Bucket", "SetBulk(%q, ...) --> %v [%v]", entries, err, time.Since(start)) }()
	return b.bucket.SetBulk(entries)
}

func (b *LoggingBucket) Refresh() error {
	start := time.Now()
	defer func() { LogTo("Bucket", "Refresh() [%v]", time.Since(start)) }()
	return b.bucket.Refresh()
}

func (b *LoggingBucket) StartTapFeed(args sgbucket.FeedArguments) (sgbucket.MutationFeed, error) {
	start := time.Now()
	defer func() { LogTo("Bucket", "StartTapFeed(...) [%v]", time.Since(start)) }()
	return b.bucket.StartTapFeed(args)
}

func (b *LoggingBucket) StartDCPFeed(args sgbucket.FeedArguments, callback sgbucket.FeedEventCallbackFunc) error {
	start := time.Now()
	defer func() { LogTo("Bucket", "StartDcpFeed(...) [%v]", time.Since(start)) }()
	return b.bucket.StartDCPFeed(args, callback)
}

func (b *LoggingBucket) Close() {
	start := time.Now()
	defer func() { LogTo("Bucket", "Close() [%v]", time.Since(start)) }()
	b.bucket.Close()
}
func (b *LoggingBucket) Dump() {
	LogTo("Bucket", "Dump()")
	b.bucket.Dump()
}
func (b *LoggingBucket) VBHash(docID string) uint32 {
	LogTo("Bucket", "VBHash()")
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
