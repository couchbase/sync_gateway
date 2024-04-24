// Copyright 2022-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package base

import (
	"context"
	"errors"
	"fmt"

	sgbucket "github.com/couchbase/sg-bucket"
)

type LeakyDataStore struct {
	dataStore DataStore
	incrCount uint16
	bucket    Bucket
	config    *LeakyBucketConfig
}

var (
	_ DataStore         = &LeakyDataStore{}
	_ WrappingDatastore = &LeakyDataStore{}
	// _ N1QLStore = &LeakyDataStore{} // TODO: Not implemented
)

func NewLeakyDataStore(bucket *LeakyBucket, dataStore DataStore, config *LeakyBucketConfig) *LeakyDataStore {
	return &LeakyDataStore{
		dataStore: dataStore,
		bucket:    bucket,
		config:    config,
	}
}

// AsLeakyDataStore returns the given DataStore as a LeakyDataStore, if possible.
func AsLeakyDataStore(ds DataStore) (*LeakyDataStore, bool) {
	lds, ok := ds.(*LeakyDataStore)
	return lds, ok
}

func (lds *LeakyDataStore) GetUnderlyingDataStore() DataStore {
	return lds.dataStore
}

func (lds *LeakyDataStore) SetDDocDeleteErrorCount(i int) {
	lds.config.DDocDeleteErrorCount = i
}

func (lds *LeakyDataStore) SetDDocGetErrorCount(i int) {
	lds.config.DDocGetErrorCount = i
}

func (lds *LeakyDataStore) GetExpiry(ctx context.Context, k string) (expiry uint32, err error) {
	return lds.dataStore.GetExpiry(ctx, k)
}

func (lds *LeakyDataStore) Exists(k string) (exists bool, err error) {
	return lds.dataStore.Exists(k)
}

func (lds *LeakyDataStore) GetName() string {
	return lds.dataStore.GetName()
}

func (lds *LeakyDataStore) ScopeName() string {
	return lds.dataStore.ScopeName()
}

func (lds *LeakyDataStore) CollectionName() string {
	return lds.dataStore.CollectionName()
}

func (lds *LeakyDataStore) GetCollectionID() uint32 {
	return lds.dataStore.GetCollectionID()
}

func (lds *LeakyDataStore) Get(k string, rv interface{}) (cas uint64, err error) {
	return lds.dataStore.Get(k, rv)
}

func (lds *LeakyDataStore) SetGetRawCallback(callback func(string) error) {
	lds.config.GetRawCallback = callback
}

func (lds *LeakyDataStore) SetGetWithXattrCallback(callback func(string) error) {
	lds.config.GetWithXattrCallback = callback
}

func (lds *LeakyDataStore) GetRaw(k string) (v []byte, cas uint64, err error) {
	if lds.config.GetRawCallback != nil {
		err = lds.config.GetRawCallback(k)
		if err != nil {
			return nil, 0, err
		}
	}
	return lds.dataStore.GetRaw(k)
}
func (lds *LeakyDataStore) GetWithXattrs(ctx context.Context, k string, xattrKeys []string) (body []byte, xattrs map[string][]byte, cas uint64, err error) {
	if lds.config.GetWithXattrCallback != nil {
		if err := lds.config.GetWithXattrCallback(k); err != nil {
			return nil, nil, 0, err
		}
	}
	return lds.dataStore.GetWithXattrs(ctx, k, xattrKeys)
}

func (lds *LeakyDataStore) GetAndTouchRaw(k string, exp uint32) (v []byte, cas uint64, err error) {
	return lds.dataStore.GetAndTouchRaw(k, exp)
}
func (lds *LeakyDataStore) Touch(k string, exp uint32) (cas uint64, err error) {
	return lds.dataStore.Touch(k, exp)
}
func (lds *LeakyDataStore) Add(k string, exp uint32, v interface{}) (added bool, err error) {
	return lds.dataStore.Add(k, exp, v)
}
func (lds *LeakyDataStore) AddRaw(k string, exp uint32, v []byte) (added bool, err error) {
	return lds.dataStore.AddRaw(k, exp, v)
}
func (lds *LeakyDataStore) Set(k string, exp uint32, opts *sgbucket.UpsertOptions, v interface{}) error {
	return lds.dataStore.Set(k, exp, opts, v)
}
func (lds *LeakyDataStore) SetRaw(k string, exp uint32, opts *sgbucket.UpsertOptions, v []byte) error {
	for _, errorKey := range lds.config.ForceErrorSetRawKeys {
		if k == errorKey {
			return fmt.Errorf("Leaky bucket forced SetRaw error for key %s", k)
		}
	}
	return lds.dataStore.SetRaw(k, exp, opts, v)
}
func (lds *LeakyDataStore) Delete(k string) error {
	return lds.dataStore.Delete(k)
}
func (lds *LeakyDataStore) Remove(k string, cas uint64) (casOut uint64, err error) {
	return lds.dataStore.Remove(k, cas)
}
func (lds *LeakyDataStore) WriteCas(k string, exp uint32, cas uint64, v interface{}, opt sgbucket.WriteOptions) (uint64, error) {
	return lds.dataStore.WriteCas(k, exp, cas, v, opt)
}
func (lds *LeakyDataStore) Update(k string, exp uint32, callback sgbucket.UpdateFunc) (casOut uint64, err error) {
	if lds.config.UpdateCallback != nil {
		wrapperCallback := func(current []byte) (updated []byte, expiry *uint32, isDelete bool, err error) {
			updated, expiry, isDelete, err = callback(current)
			lds.config.UpdateCallback(k)
			return updated, expiry, isDelete, err
		}
		return lds.dataStore.Update(k, exp, wrapperCallback)
	}

	casOut, err = lds.dataStore.Update(k, exp, callback)

	if lds.config.PostUpdateCallback != nil {
		lds.config.PostUpdateCallback(k)
	}

	return casOut, err
}

func (lds *LeakyDataStore) Incr(k string, amt, def uint64, exp uint32) (uint64, error) {

	if lds.config.IncrTemporaryFailCount > 0 {
		if lds.incrCount < lds.config.IncrTemporaryFailCount {
			lds.incrCount++
			return 0, errors.New(fmt.Sprintf("Incr forced abort (%d/%d), try again maybe?", lds.incrCount, lds.config.IncrTemporaryFailCount))
		}
		lds.incrCount = 0

	}
	val, err := lds.dataStore.Incr(k, amt, def, exp)

	if lds.config.IncrCallback != nil {
		lds.config.IncrCallback()
	}
	return val, err
}

func (lds *LeakyDataStore) GetDDocs() (map[string]sgbucket.DesignDoc, error) {
	vs, ok := AsViewStore(lds.dataStore)
	if !ok {
		return nil, errors.New("bucket does not support views")
	}
	return vs.GetDDocs()
}

func (lds *LeakyDataStore) GetDDoc(docname string) (ddoc sgbucket.DesignDoc, err error) {
	vs, ok := AsViewStore(lds.dataStore)
	if !ok {
		return sgbucket.DesignDoc{}, errors.New("bucket does not support views")
	}
	if lds.config.DDocGetErrorCount > 0 {
		lds.config.DDocGetErrorCount--
		return ddoc, errors.New(fmt.Sprintf("Artificial leaky bucket error %d fails remaining", lds.config.DDocGetErrorCount))
	}
	return vs.GetDDoc(docname)
}

func (lds *LeakyDataStore) PutDDoc(ctx context.Context, docname string, value *sgbucket.DesignDoc) error {
	vs, ok := AsViewStore(lds.dataStore)
	if !ok {
		return errors.New("bucket does not support views")
	}
	return vs.PutDDoc(ctx, docname, value)
}

func (lds *LeakyDataStore) DeleteDDoc(docname string) error {
	vs, ok := AsViewStore(lds.dataStore)
	if !ok {
		return errors.New("bucket does not support views")
	}
	if lds.config.DDocDeleteErrorCount > 0 {
		lds.config.DDocDeleteErrorCount--
		return errors.New(fmt.Sprintf("Artificial leaky bucket error %d fails remaining", lds.config.DDocDeleteErrorCount))
	}
	return vs.DeleteDDoc(docname)
}

func (lds *LeakyDataStore) View(ctx context.Context, ddoc, name string, params map[string]interface{}) (sgbucket.ViewResult, error) {
	vs, ok := AsViewStore(lds.dataStore)
	if !ok {
		return sgbucket.ViewResult{}, errors.New("bucket does not support views")
	}
	return vs.View(ctx, ddoc, name, params)
}

func (lds *LeakyDataStore) ViewQuery(ctx context.Context, ddoc, name string, params map[string]interface{}) (sgbucket.QueryResultIterator, error) {
	vs, ok := AsViewStore(lds.dataStore)
	if !ok {
		return nil, errors.New("bucket does not support views")
	}
	iterator, err := vs.ViewQuery(ctx, ddoc, name, params)

	if lds.config.FirstTimeViewCustomPartialError {
		lds.config.FirstTimeViewCustomPartialError = !lds.config.FirstTimeViewCustomPartialError
		err = ErrPartialViewErrors
	}

	if lds.config.PostQueryCallback != nil {
		lds.config.PostQueryCallback(ddoc, name, params)
	}
	return iterator, err
}

func (lds *LeakyDataStore) GetMaxVbno() (uint16, error) {
	return lds.bucket.GetMaxVbno()
}

func (lds *LeakyDataStore) WriteWithXattrs(ctx context.Context, k string, exp uint32, cas uint64, value []byte, xattrs map[string][]byte, xattrsToDelete []string, opts *sgbucket.MutateInOptions) (casOut uint64, err error) {
	if lds.config.WriteWithXattrCallback != nil {
		lds.config.WriteWithXattrCallback(k)
	}
	return lds.dataStore.WriteWithXattrs(ctx, k, exp, cas, value, xattrs, xattrsToDelete, opts)
}

func (lds *LeakyDataStore) WriteUpdateWithXattrs(ctx context.Context, k string, xattrKeys []string, exp uint32, previous *sgbucket.BucketDocument, opts *sgbucket.MutateInOptions, callback sgbucket.WriteUpdateWithXattrsFunc) (casOut uint64, err error) {
	if lds.config.UpdateCallback != nil {
		wrapperCallback := func(current []byte, xattrs map[string][]byte, cas uint64) (sgbucket.UpdatedDoc, error) {
			updatedDoc, err := callback(current, xattrs, cas)
			lds.config.UpdateCallback(k)
			return updatedDoc, err
		}
		return lds.dataStore.WriteUpdateWithXattrs(ctx, k, xattrKeys, exp, previous, opts, wrapperCallback)
	}
	return lds.dataStore.WriteUpdateWithXattrs(ctx, k, xattrKeys, exp, previous, opts, callback)
}

func (lds *LeakyDataStore) SetXattrs(ctx context.Context, k string, xv map[string][]byte) (casOut uint64, err error) {
	if lds.config.SetXattrCallback != nil {
		if err := lds.config.SetXattrCallback(k); err != nil {
			return 0, err
		}
	}
	return lds.dataStore.SetXattrs(ctx, k, xv)
}

func (lds *LeakyDataStore) RemoveXattrs(ctx context.Context, k string, xattrKeys []string, cas uint64) (err error) {
	return lds.dataStore.RemoveXattrs(ctx, k, xattrKeys, cas)
}

func (lds *LeakyDataStore) DeleteSubDocPaths(ctx context.Context, k string, xattrKeys ...string) (err error) {
	return lds.dataStore.DeleteSubDocPaths(ctx, k, xattrKeys...)
}

func (lds *LeakyDataStore) SubdocInsert(ctx context.Context, docID string, fieldPath string, cas uint64, value interface{}) error {
	return lds.dataStore.SubdocInsert(ctx, docID, fieldPath, cas, value)
}

func (lds *LeakyDataStore) DeleteWithXattrs(ctx context.Context, k string, xattrKeys []string) error {
	return lds.dataStore.DeleteWithXattrs(ctx, k, xattrKeys)
}

func (lds *LeakyDataStore) GetXattrs(ctx context.Context, k string, xattrKeys []string) (xattrs map[string][]byte, cas uint64, err error) {
	return lds.dataStore.GetXattrs(ctx, k, xattrKeys)
}

func (lds *LeakyDataStore) GetSubDocRaw(ctx context.Context, k string, subdocKey string) ([]byte, uint64, error) {
	return lds.dataStore.GetSubDocRaw(ctx, k, subdocKey)
}

func (lds *LeakyDataStore) WriteSubDoc(ctx context.Context, k string, subdocKey string, cas uint64, value []byte) (uint64, error) {
	return lds.dataStore.WriteSubDoc(ctx, k, subdocKey, cas, value)
}

// Accessors to set leaky bucket config for a running bucket.  Used to tune properties on a walrus bucket created as part of rest tester - it will
// be a leaky bucket (due to DCP support), but there's no mechanism to pass in a leaky bucket config to a RestTester bucket at bucket creation time.
func (lds *LeakyDataStore) SetFirstTimeViewCustomPartialError(val bool) {
	lds.config.FirstTimeViewCustomPartialError = val
}

func (lds *LeakyDataStore) SetPostQueryCallback(callback func(ddoc, viewName string, params map[string]interface{})) {
	lds.config.PostQueryCallback = callback
}

func (lds *LeakyDataStore) SetPostN1QLQueryCallback(callback func()) {
	lds.config.PostN1QLQueryCallback = callback
}

func (lds *LeakyDataStore) SetPostUpdateCallback(callback func(key string)) {
	lds.config.PostUpdateCallback = callback
}

func (lds *LeakyDataStore) SetUpdateCallback(callback func(key string)) {
	lds.config.UpdateCallback = callback
}

func (lds *LeakyDataStore) IsError(err error, errorType sgbucket.DataStoreErrorType) bool {
	return lds.dataStore.IsError(err, errorType)
}

func (lds *LeakyDataStore) IsSupported(feature sgbucket.BucketStoreFeature) bool {
	return lds.dataStore.IsSupported(feature)
}

func (lds *LeakyDataStore) UpdateXattrs(ctx context.Context, k string, exp uint32, cas uint64, xv map[string][]byte, opts *sgbucket.MutateInOptions) (casOut uint64, err error) {
	return lds.dataStore.UpdateXattrs(ctx, k, exp, cas, xv, opts)
}

func (lds *LeakyDataStore) WriteTombstoneWithXattrs(ctx context.Context, k string, exp uint32, cas uint64, xv map[string][]byte, xattrsToDelete []string, deleteBody bool, opts *sgbucket.MutateInOptions) (casOut uint64, err error) {
	return lds.dataStore.WriteTombstoneWithXattrs(ctx, k, exp, cas, xv, xattrsToDelete, deleteBody, opts)

}

func (lds *LeakyDataStore) WriteResurrectionWithXattrs(ctx context.Context, k string, exp uint32, body []byte, xv map[string][]byte, opts *sgbucket.MutateInOptions) (casOut uint64, err error) {
	return lds.dataStore.WriteResurrectionWithXattrs(ctx, k, exp, body, xv, opts)
}

func (lds *LeakyDataStore) GetSpec() BucketSpec {
	if b, ok := AsCouchbaseBucketStore(lds.bucket); ok {
		return b.GetSpec()
	} else {
		// Return a minimal struct:
		return BucketSpec{
			BucketName: lds.bucket.GetName(),
			UseXattrs:  true,
		}
	}
}

// Assert interface compliance:
var (
	_ sgbucket.DataStore = &LeakyDataStore{}
)
