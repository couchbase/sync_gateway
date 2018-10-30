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
	//Tracef(KeyBucket, "GetName()")
	return b.bucket.GetName()
}
func (b *LoggingBucket) Get(k string, rv interface{}) (uint64, error) {
	start := time.Now()
	defer func() { Tracef(KeyBucket, "Get(%q) [%v]", UD(k), time.Since(start)) }()
	return b.bucket.Get(k, rv)
}
func (b *LoggingBucket) GetRaw(k string) (v []byte, cas uint64, err error) {
	start := time.Now()
	defer func() { Tracef(KeyBucket, "GetRaw(%q) [%v]", UD(k), time.Since(start)) }()
	return b.bucket.GetRaw(k)
}
func (b *LoggingBucket) GetAndTouchRaw(k string, exp uint32) (v []byte, cas uint64, err error) {
	start := time.Now()
	defer func() { Tracef(KeyBucket, "GetAndTouchRaw(%q) [%v]", UD(k), time.Since(start)) }()
	return b.bucket.GetAndTouchRaw(k, exp)
}
func (b *LoggingBucket) GetBulkRaw(keys []string) (map[string][]byte, error) {
	start := time.Now()
	defer func() { Tracef(KeyBucket, "GetBulkRaw(%q) [%v]", UD(keys), time.Since(start)) }()
	return b.bucket.GetBulkRaw(keys)
}
func (b *LoggingBucket) Add(k string, exp uint32, v interface{}) (added bool, err error) {
	start := time.Now()
	defer func() { Tracef(KeyBucket, "Add(%q, %d, ...) [%v]", UD(k), exp, time.Since(start)) }()
	return b.bucket.Add(k, exp, v)
}
func (b *LoggingBucket) AddRaw(k string, exp uint32, v []byte) (added bool, err error) {
	start := time.Now()
	defer func() { Tracef(KeyBucket, "AddRaw(%q, %d, ...) [%v]", UD(k), exp, time.Since(start)) }()
	return b.bucket.AddRaw(k, exp, v)
}
func (b *LoggingBucket) Append(k string, data []byte) error {
	start := time.Now()
	defer func() { Tracef(KeyBucket, "Append(%q, ...) [%v]", UD(k), time.Since(start)) }()
	return b.bucket.Append(k, data)
}
func (b *LoggingBucket) Set(k string, exp uint32, v interface{}) error {
	start := time.Now()
	defer func() { Tracef(KeyBucket, "Set(%q, %d, ...) [%v]", UD(k), exp, time.Since(start)) }()
	return b.bucket.Set(k, exp, v)
}
func (b *LoggingBucket) SetRaw(k string, exp uint32, v []byte) error {
	start := time.Now()
	defer func() { Tracef(KeyBucket, "SetRaw(%q, %d, ...) [%v]", UD(k), exp, time.Since(start)) }()
	return b.bucket.SetRaw(k, exp, v)
}
func (b *LoggingBucket) Delete(k string) error {
	start := time.Now()
	defer func() { Tracef(KeyBucket, "Delete(%q) [%v]", UD(k), time.Since(start)) }()
	return b.bucket.Delete(k)
}
func (b *LoggingBucket) Remove(k string, cas uint64) (casOut uint64, err error) {
	start := time.Now()
	defer func() { Tracef(KeyBucket, "Remove(%q) [%v]", UD(k), time.Since(start)) }()
	return b.bucket.Remove(k, cas)
}
func (b *LoggingBucket) Write(k string, flags int, exp uint32, v interface{}, opt sgbucket.WriteOptions) error {
	start := time.Now()
	defer func() {
		Tracef(KeyBucket, "Write(%q, 0x%x, %d, ..., 0x%x) [%v]", UD(k), flags, exp, opt, time.Since(start))
	}()
	return b.bucket.Write(k, flags, exp, v, opt)
}
func (b *LoggingBucket) WriteCas(k string, flags int, exp uint32, cas uint64, v interface{}, opt sgbucket.WriteOptions) (uint64, error) {
	start := time.Now()
	defer func() {
		Tracef(KeyBucket, "WriteCas(%q, 0x%x, %d, %d, ..., 0x%x) [%v]", UD(k), flags, exp, cas, opt, time.Since(start))
	}()
	return b.bucket.WriteCas(k, flags, exp, cas, v, opt)
}
func (b *LoggingBucket) Update(k string, exp uint32, callback sgbucket.UpdateFunc) (casOut uint64, err error) {
	start := time.Now()
	defer func() { Tracef(KeyBucket, "Update(%q, %d, ...) --> %v [%v]", UD(k), exp, err, time.Since(start)) }()
	return b.bucket.Update(k, exp, callback)
}
func (b *LoggingBucket) WriteUpdate(k string, exp uint32, callback sgbucket.WriteUpdateFunc) (casOut uint64, err error) {
	start := time.Now()
	defer func() { Tracef(KeyBucket, "WriteUpdate(%q, %d, ...) --> %v [%v]", UD(k), exp, err, time.Since(start)) }()
	return b.bucket.WriteUpdate(k, exp, callback)
}

func (b *LoggingBucket) Incr(k string, amt, def uint64, exp uint32) (uint64, error) {
	start := time.Now()
	defer func() { Tracef(KeyBucket, "Incr(%q, %d, %d, %d) [%v]", UD(k), amt, def, exp, time.Since(start)) }()
	return b.bucket.Incr(k, amt, def, exp)
}
func (b *LoggingBucket) WriteCasWithXattr(k string, xattr string, exp uint32, cas uint64, v interface{}, xv interface{}) (casOut uint64, err error) {
	start := time.Now()
	defer func() { Tracef(KeyBucket, "WriteCasWithXattr(%q, ...) [%v]", UD(k), time.Since(start)) }()
	return b.bucket.WriteCasWithXattr(k, xattr, exp, cas, v, xv)
}
func (b *LoggingBucket) WriteUpdateWithXattr(k string, xattr string, exp uint32, previous *sgbucket.BucketDocument, callback sgbucket.WriteUpdateWithXattrFunc) (casOut uint64, err error) {
	start := time.Now()
	defer func() {
		Tracef(KeyBucket, "WriteUpdateWithXattr(%q, %d, ...) --> %v [%v]", UD(k), exp, err, time.Since(start))
	}()
	return b.bucket.WriteUpdateWithXattr(k, xattr, exp, previous, callback)
}
func (b *LoggingBucket) GetWithXattr(k string, xattr string, rv interface{}, xv interface{}) (cas uint64, err error) {
	start := time.Now()
	defer func() { Tracef(KeyBucket, "GetWithXattr(%q, ...) [%v]", UD(k), time.Since(start)) }()
	return b.bucket.GetWithXattr(k, xattr, rv, xv)
}
func (b *LoggingBucket) DeleteWithXattr(k string, xattr string) error {
	start := time.Now()
	defer func() { Tracef(KeyBucket, "DeleteWithXattr(%q, ...) [%v]", UD(k), time.Since(start)) }()
	return b.bucket.DeleteWithXattr(k, xattr)
}
func (b *LoggingBucket) GetDDoc(docname string, value interface{}) error {
	start := time.Now()
	defer func() { Tracef(KeyBucket, "GetDDoc(%q, ...) [%v]", UD(docname), time.Since(start)) }()
	return b.bucket.GetDDoc(docname, value)
}
func (b *LoggingBucket) PutDDoc(docname string, value interface{}) error {
	start := time.Now()
	defer func() { Tracef(KeyBucket, "PutDDoc(%q, ...) [%v]", UD(docname), time.Since(start)) }()
	return b.bucket.PutDDoc(docname, value)
}
func (b *LoggingBucket) DeleteDDoc(docname string) error {
	start := time.Now()
	defer func() { Tracef(KeyBucket, "DeleteDDoc(%q, ...) [%v]", UD(docname), time.Since(start)) }()
	return b.bucket.DeleteDDoc(docname)
}
func (b *LoggingBucket) View(ddoc, name string, params map[string]interface{}) (sgbucket.ViewResult, error) {
	start := time.Now()
	defer func() { Tracef(KeyBucket, "View(%q, %q, ...) [%v]", MD(ddoc), UD(name), time.Since(start)) }()
	return b.bucket.View(ddoc, name, params)
}

func (b *LoggingBucket) ViewCustom(ddoc, name string, params map[string]interface{}, vres interface{}) error {
	start := time.Now()
	defer func() { Tracef(KeyBucket, "ViewCustom(%q, %q, ...) [%v]", MD(ddoc), UD(name), time.Since(start)) }()
	return b.bucket.ViewCustom(ddoc, name, params, vres)
}

func (b *LoggingBucket) ViewQuery(ddoc, name string, params map[string]interface{}) (sgbucket.QueryResultIterator, error) {
	start := time.Now()
	defer func() { Tracef(KeyBucket, "ViewQuery(%q, %q, ...) [%v]", MD(ddoc), UD(name), time.Since(start)) }()
	return b.bucket.ViewQuery(ddoc, name, params)
}

func (b *LoggingBucket) SetBulk(entries []*sgbucket.BulkSetEntry) (err error) {
	start := time.Now()
	defer func() { Tracef(KeyBucket, "SetBulk(%q, ...) --> %v [%v]", UD(entries), err, time.Since(start)) }()
	return b.bucket.SetBulk(entries)
}

func (b *LoggingBucket) Refresh() error {
	start := time.Now()
	defer func() { Tracef(KeyBucket, "Refresh() [%v]", time.Since(start)) }()
	return b.bucket.Refresh()
}

func (b *LoggingBucket) StartTapFeed(args sgbucket.FeedArguments) (sgbucket.MutationFeed, error) {
	start := time.Now()
	defer func() { Tracef(KeyBucket, "StartTapFeed(...) [%v]", time.Since(start)) }()
	return b.bucket.StartTapFeed(args)
}

func (b *LoggingBucket) StartDCPFeed(args sgbucket.FeedArguments, callback sgbucket.FeedEventCallbackFunc) error {
	start := time.Now()
	defer func() { Tracef(KeyBucket, "StartDcpFeed(...) [%v]", time.Since(start)) }()
	return b.bucket.StartDCPFeed(args, callback)
}

func (b *LoggingBucket) Close() {
	start := time.Now()
	defer func() { Tracef(KeyBucket, "Close() [%v]", time.Since(start)) }()
	b.bucket.Close()
}
func (b *LoggingBucket) Dump() {
	Tracef(KeyBucket, "Dump()")
	b.bucket.Dump()
}
func (b *LoggingBucket) VBHash(docID string) uint32 {
	Tracef(KeyBucket, "VBHash()")
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
	return b.bucket.GetStatsVbSeqno(maxVbno, useAbsHighSeqNo)
}

// GetUnderlyingBucket returns the underlying bucket for the LoggingBucket.
func (b *LoggingBucket) GetUnderlyingBucket() Bucket {
	return b.bucket
}
