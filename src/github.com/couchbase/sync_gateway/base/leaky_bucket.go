package base

import (
	"errors"
	"fmt"
	"github.com/couchbaselabs/walrus"
)

const (
	maxIncrFailures = 2
)

// A wrapper around a Bucket to support forced errors.  For testing use only
type LeakyBucket struct {
	bucket    Bucket
	incrCount uint16
}

func NewLeakyBucket(bucket Bucket) Bucket {
	return &LeakyBucket{bucket: bucket}
}
func (b *LeakyBucket) GetName() string {
	return b.bucket.GetName()
}
func (b *LeakyBucket) Get(k string, rv interface{}) error {
	return b.bucket.Get(k, rv)
}
func (b *LeakyBucket) GetRaw(k string) ([]byte, error) {
	return b.bucket.GetRaw(k)
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
func (b *LeakyBucket) Write(k string, flags int, exp int, v interface{}, opt walrus.WriteOptions) error {
	return b.bucket.Write(k, flags, exp, v, opt)
}
func (b *LeakyBucket) Update(k string, exp int, callback walrus.UpdateFunc) (err error) {
	return b.bucket.Update(k, exp, callback)
}
func (b *LeakyBucket) WriteUpdate(k string, exp int, callback walrus.WriteUpdateFunc) (err error) {
	return b.bucket.WriteUpdate(k, exp, callback)
}
func (b *LeakyBucket) Incr(k string, amt, def uint64, exp int) (uint64, error) {

	if b.incrCount < maxIncrFailures {
		b.incrCount++
		return 0, errors.New(fmt.Sprintf("Incr forced fail (%d/%d), try again maybe?", b.incrCount, maxIncrFailures))
	}
	b.incrCount = 0
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
func (b *LeakyBucket) View(ddoc, name string, params map[string]interface{}) (walrus.ViewResult, error) {
	return b.bucket.View(ddoc, name, params)
}
func (b *LeakyBucket) ViewCustom(ddoc, name string, params map[string]interface{}, vres interface{}) error {
	return b.bucket.ViewCustom(ddoc, name, params, vres)
}
func (b *LeakyBucket) StartTapFeed(args walrus.TapArguments) (walrus.TapFeed, error) {
	return b.bucket.StartTapFeed(args)
}
func (b *LeakyBucket) Close() {
	b.bucket.Close()
}
func (b *LeakyBucket) Dump() {
	b.bucket.Dump()
}
func (b *LeakyBucket) VBHash(docID string) uint32 {
	return b.bucket.VBHash(docID)
}
