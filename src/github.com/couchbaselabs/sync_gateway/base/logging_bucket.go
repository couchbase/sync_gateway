package base

import (
	"github.com/couchbaselabs/walrus"
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
	LogTo("Bucket", "Get(%q)", k)
	return b.bucket.Get(k, rv)
}
func (b *LoggingBucket) GetRaw(k string) ([]byte, error) {
	LogTo("Bucket", "GetRaw(%q)", k)
	return b.bucket.GetRaw(k)
}
func (b *LoggingBucket) Add(k string, exp int, v interface{}) (added bool, err error) {
	LogTo("Bucket", "Add(%q, %d, ...)", k, exp)
	return b.bucket.Add(k, exp, v)
}
func (b *LoggingBucket) AddRaw(k string, exp int, v []byte) (added bool, err error) {
	LogTo("Bucket", "AddRaw(%q, %d, ...)", k, exp)
	return b.bucket.AddRaw(k, exp, v)
}
func (b *LoggingBucket) Append(k string, data []byte) error {
	LogTo("Bucket", "Append(%q, ...)", k)
	return b.bucket.Append(k, data)
}
func (b *LoggingBucket) Set(k string, exp int, v interface{}) error {
	LogTo("Bucket", "Set(%q, %d, ...)", k, exp)
	return b.bucket.Set(k, exp, v)
}
func (b *LoggingBucket) SetRaw(k string, exp int, v []byte) error {
	LogTo("Bucket", "SetRaw(%q, %d, ...)", k, exp)
	return b.bucket.SetRaw(k, exp, v)
}
func (b *LoggingBucket) Delete(k string) error {
	LogTo("Bucket", "Delete(%q)", k)
	return b.bucket.Delete(k)
}
func (b *LoggingBucket) Write(k string, flags int, exp int, v interface{}, opt walrus.WriteOptions) error {
	LogTo("Bucket", "Write(%q, 0x%x, %d, ..., 0x%x)", k, flags, exp, opt)
	return b.bucket.Write(k, flags, exp, v, opt)
}
func (b *LoggingBucket) Update(k string, exp int, callback walrus.UpdateFunc) error {
	LogTo("Bucket", "Update(%q, %d, ...)", k, exp)
	return b.bucket.Update(k, exp, callback)
}
func (b *LoggingBucket) WriteUpdate(k string, exp int, callback walrus.WriteUpdateFunc) error {
	LogTo("Bucket", "WriteUpdate(%q, %d, ...)", k, exp)
	return b.bucket.WriteUpdate(k, exp, callback)
}
func (b *LoggingBucket) Incr(k string, amt, def uint64, exp int) (uint64, error) {
	LogTo("Bucket", "Incr(%q, %d, %d, %d)", k, amt, def, exp)
	return b.bucket.Incr(k, amt, def, exp)
}
func (b *LoggingBucket) PutDDoc(docname string, value interface{}) error {
	LogTo("Bucket", "PutDDoc(%q, ...)", docname)
	return b.bucket.PutDDoc(docname, value)
}
func (b *LoggingBucket) View(ddoc, name string, params map[string]interface{}) (walrus.ViewResult, error) {
	LogTo("Bucket", "View(%q, %q, ...)", ddoc, name)
	return b.bucket.View(ddoc, name, params)
}
func (b *LoggingBucket) ViewCustom(ddoc, name string, params map[string]interface{}, vres interface{}) error {
	LogTo("Bucket", "ViewCustom(%q, %q, ...)", ddoc, name)
	return b.bucket.ViewCustom(ddoc, name, params, vres)
}
func (b *LoggingBucket) StartTapFeed(args walrus.TapArguments) (walrus.TapFeed, error) {
	LogTo("Bucket", "StartTapFeed(...)")
	return b.bucket.StartTapFeed(args)
}
func (b *LoggingBucket) Close() {
	LogTo("Bucket", "Close()")
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
