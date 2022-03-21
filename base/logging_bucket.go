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
func (b *LoggingBucket) Add(k string, exp uint32, v interface{}) (added bool, err error) {
	defer b.log(time.Now(), k, exp)
	return b.bucket.Add(k, exp, v)
}
func (b *LoggingBucket) AddRaw(k string, exp uint32, v []byte) (added bool, err error) {
	defer b.log(time.Now(), k, exp)
	return b.bucket.AddRaw(k, exp, v)
}
func (b *LoggingBucket) Set(k string, exp uint32, opts *sgbucket.UpsertOptions, v interface{}) error {
	defer b.log(time.Now(), k, exp)
	return b.bucket.Set(k, exp, opts, v)
}
func (b *LoggingBucket) SetRaw(k string, exp uint32, opts *sgbucket.UpsertOptions, v []byte) error {
	defer b.log(time.Now(), k, exp)
	return b.bucket.SetRaw(k, exp, opts, v)
}
func (b *LoggingBucket) Delete(k string) error {
	defer b.log(time.Now(), k)
	return b.bucket.Delete(k)
}
func (b *LoggingBucket) Remove(k string, cas uint64) (casOut uint64, err error) {
	defer b.log(time.Now(), k, cas)
	return b.bucket.Remove(k, cas)
}
func (b *LoggingBucket) WriteCas(k string, flags int, exp uint32, cas uint64, v interface{}, opt sgbucket.WriteOptions) (uint64, error) {
	defer b.log(time.Now(), k, flags, exp, cas, opt)
	return b.bucket.WriteCas(k, flags, exp, cas, v, opt)
}
func (b *LoggingBucket) Update(k string, exp uint32, callback sgbucket.UpdateFunc) (casOut uint64, err error) {
	defer b.log(time.Now(), k, exp)
	return b.bucket.Update(k, exp, callback)
}

func (b *LoggingBucket) Incr(k string, amt, def uint64, exp uint32) (uint64, error) {
	defer b.log(time.Now(), k, amt, def, exp)
	return b.bucket.Incr(k, amt, def, exp)
}

func (b *LoggingBucket) WriteCasWithXattr(k string, xattr string, exp uint32, cas uint64, opts *sgbucket.MutateInOptions, v interface{}, xv interface{}) (casOut uint64, err error) {
	defer b.log(time.Now(), k, xattr, exp, cas)
	return b.bucket.WriteCasWithXattr(k, xattr, exp, cas, opts, v, xv)
}

func (b *LoggingBucket) WriteWithXattr(k string, xattrKey string, exp uint32, cas uint64, opts *sgbucket.MutateInOptions, value []byte, xattrValue []byte, isDelete bool, deleteBody bool) (casOut uint64, err error) {
	defer b.log(time.Now(), k, xattrKey, exp, cas, value, xattrValue, isDelete, deleteBody)
	return b.bucket.WriteWithXattr(k, xattrKey, exp, cas, opts, value, xattrValue, isDelete, deleteBody)
}

func (b *LoggingBucket) WriteUpdateWithXattr(k string, xattr string, userXattrKey string, exp uint32, opts *sgbucket.MutateInOptions, previous *sgbucket.BucketDocument, callback sgbucket.WriteUpdateWithXattrFunc) (casOut uint64, err error) {
	defer b.log(time.Now(), k, xattr, exp)
	return b.bucket.WriteUpdateWithXattr(k, xattr, userXattrKey, exp, opts, previous, callback)
}

func (b *LoggingBucket) SetXattr(k string, xattrKey string, xv []byte) (casOut uint64, err error) {
	defer b.log(time.Now(), k, xattrKey)
	return b.bucket.SetXattr(k, xattrKey, xv)
}

func (b *LoggingBucket) RemoveXattr(k string, xattrKey string, cas uint64) (err error) {
	defer b.log(time.Now(), k, xattrKey)
	return b.bucket.RemoveXattr(k, xattrKey, cas)
}

func (b *LoggingBucket) DeleteXattrs(k string, xattrKey ...string) (err error) {
	defer b.log(time.Now(), k, xattrKey)
	return b.bucket.DeleteXattrs(k, xattrKey...)
}

func (b *LoggingBucket) SubdocInsert(docID string, fieldPath string, cas uint64, value interface{}) error {
	defer b.log(time.Now(), docID, fieldPath)
	return b.bucket.SubdocInsert(docID, fieldPath, cas, value)
}

func (b *LoggingBucket) GetWithXattr(k string, xattr string, userXattrKey string, rv interface{}, xv interface{}, uxv interface{}) (cas uint64, err error) {
	defer b.log(time.Now(), k, xattr, userXattrKey)
	return b.bucket.GetWithXattr(k, xattr, userXattrKey, rv, xv, uxv)
}
func (b *LoggingBucket) DeleteWithXattr(k string, xattr string) error {
	defer b.log(time.Now(), k, xattr)
	return b.bucket.DeleteWithXattr(k, xattr)
}
func (b *LoggingBucket) GetXattr(k string, xattr string, xv interface{}) (cas uint64, err error) {
	defer b.log(time.Now(), k, xattr)
	return b.bucket.GetXattr(k, xattr, xv)
}

func (b *LoggingBucket) GetSubDocRaw(k string, subdocKey string) ([]byte, uint64, error) {
	defer b.log(time.Now(), k, subdocKey)
	return b.bucket.GetSubDocRaw(k, subdocKey)
}

func (b *LoggingBucket) WriteSubDoc(k string, subdocKey string, cas uint64, value []byte) (uint64, error) {
	defer b.log(time.Now(), k, subdocKey)
	return b.bucket.WriteSubDoc(k, subdocKey, cas, value)
}

func (b *LoggingBucket) GetDDocs() (map[string]sgbucket.DesignDoc, error) {
	defer b.log(time.Now())
	return b.bucket.GetDDocs()
}
func (b *LoggingBucket) GetDDoc(docname string) (sgbucket.DesignDoc, error) {
	defer b.log(time.Now(), docname)
	return b.bucket.GetDDoc(docname)
}
func (b *LoggingBucket) PutDDoc(docname string, value *sgbucket.DesignDoc) error {
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

func (b *LoggingBucket) ViewQuery(ddoc, name string, params map[string]interface{}) (sgbucket.QueryResultIterator, error) {
	defer b.log(time.Now(), ddoc, name)
	return b.bucket.ViewQuery(ddoc, name, params)
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

/*
func (b *LoggingBucket) VBHash(docID string) uint32 {
	defer b.log(time.Now())
	return b.bucket.VBHash(docID)
}
*/

func (b *LoggingBucket) GetMaxVbno() (uint16, error) {
	defer b.log(time.Now())
	return b.bucket.GetMaxVbno()
}

func (b *LoggingBucket) UUID() (string, error) {
	defer b.log(time.Now())
	return b.bucket.UUID()
}

// GetUnderlyingBucket returns the underlying bucket for the LoggingBucket.
func (b *LoggingBucket) GetUnderlyingBucket() Bucket {
	defer b.log(time.Now())
	return b.bucket
}

func (b *LoggingBucket) IsSupported(feature sgbucket.DataStoreFeature) bool {
	defer b.log(time.Now())
	return b.bucket.IsSupported(feature)
}

func (b *LoggingBucket) IsError(err error, errorType sgbucket.DataStoreErrorType) bool {
	return b.bucket.IsError(err, errorType)
}
