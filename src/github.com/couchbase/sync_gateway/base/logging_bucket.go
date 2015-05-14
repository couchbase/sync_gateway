package base

import (
	"time"

	"github.com/couchbase/sg-bucket"
)

// A wrapper around a Bucket that transparently adds logging of all the API calls.
type LoggingBucket struct {
	bucket Bucket
}

func (b *LoggingBucket) GetName() string {
	//LogTo("Bucket", "GetName()")
	return b.bucket.GetName()
}
func (b *LoggingBucket) Get(k string, rv interface{}) error {
	start := time.Now()
	defer func() { LogTo("Bucket", "Get(%q) [%v]", k, time.Since(start)) }()
	return b.bucket.Get(k, rv)
}
func (b *LoggingBucket) GetRaw(k string) ([]byte, error) {
	start := time.Now()
	defer func() { LogTo("Bucket", "GetRaw(%q) [%v]", k, time.Since(start)) }()
	return b.bucket.GetRaw(k)
}
func (b *LoggingBucket) Add(k string, exp int, v interface{}) (added bool, err error) {
	start := time.Now()
	defer func() { LogTo("Bucket", "Add(%q, %d, ...) [%v]", k, exp, time.Since(start)) }()
	return b.bucket.Add(k, exp, v)
}
func (b *LoggingBucket) AddRaw(k string, exp int, v []byte) (added bool, err error) {
	start := time.Now()
	defer func() { LogTo("Bucket", "AddRaw(%q, %d, ...) [%v]", k, exp, time.Since(start)) }()
	return b.bucket.AddRaw(k, exp, v)
}
func (b *LoggingBucket) Append(k string, data []byte) error {
	start := time.Now()
	defer func() { LogTo("Bucket", "Append(%q, ...) [%v]", k, time.Since(start)) }()
	return b.bucket.Append(k, data)
}
func (b *LoggingBucket) Set(k string, exp int, v interface{}) error {
	start := time.Now()
	defer func() { LogTo("Bucket", "Set(%q, %d, ...) [%v]", k, exp, time.Since(start)) }()
	return b.bucket.Set(k, exp, v)
}
func (b *LoggingBucket) SetRaw(k string, exp int, v []byte) error {
	start := time.Now()
	defer func() { LogTo("Bucket", "SetRaw(%q, %d, ...) [%v]", k, exp, time.Since(start)) }()
	return b.bucket.SetRaw(k, exp, v)
}
func (b *LoggingBucket) Delete(k string) error {
	start := time.Now()
	defer func() { LogTo("Bucket", "Delete(%q) [%v]", k, time.Since(start)) }()
	return b.bucket.Delete(k)
}
func (b *LoggingBucket) Write(k string, flags int, exp int, v interface{}, opt sgbucket.WriteOptions) error {
	start := time.Now()
	defer func() { LogTo("Bucket", "Write(%q, 0x%x, %d, ..., 0x%x) [%v]", k, flags, exp, opt, time.Since(start)) }()
	return b.bucket.Write(k, flags, exp, v, opt)
}
func (b *LoggingBucket) Update(k string, exp int, callback sgbucket.UpdateFunc) (err error) {
	start := time.Now()
	defer func() { LogTo("Bucket", "Update(%q, %d, ...) --> %v [%v]", k, exp, err, time.Since(start)) }()
	return b.bucket.Update(k, exp, callback)
}
func (b *LoggingBucket) WriteUpdate(k string, exp int, callback sgbucket.WriteUpdateFunc) (err error) {
	start := time.Now()
	defer func() { LogTo("Bucket", "WriteUpdate(%q, %d, ...) --> %v [%v]", k, exp, err, time.Since(start)) }()
	return b.bucket.WriteUpdate(k, exp, callback)
}
func (b *LoggingBucket) Incr(k string, amt, def uint64, exp int) (uint64, error) {
	start := time.Now()
	defer func() { LogTo("Bucket", "Incr(%q, %d, %d, %d) [%v]", k, amt, def, exp, time.Since(start)) }()
	return b.bucket.Incr(k, amt, def, exp)
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
func (b *LoggingBucket) StartTapFeed(args sgbucket.TapArguments) (sgbucket.TapFeed, error) {
	start := time.Now()
	defer func() { LogTo("Bucket", "StartTapFeed(...) [%v]", time.Since(start)) }()
	return b.bucket.StartTapFeed(args)
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
