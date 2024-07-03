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

func (lds *LeakyDataStore) Get(k string, rv interface{}) (cas uint64, err error) {
	return lds.dataStore.Get(k, rv)
}

func (lds *LeakyDataStore) SetGetRawCallback(callback func(string) error) {
	lds.config.GetRawCallback = callback
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
func (lds *LeakyDataStore) GetWithXattr(ctx context.Context, k string, xattr string, userXattrKey string, rv interface{}, xv interface{}, uxv interface{}) (cas uint64, err error) {
	if lds.config.GetWithXattrCallback != nil {
		if err := lds.config.GetWithXattrCallback(k); err != nil {
			return 0, err
		}
	}
	return lds.dataStore.GetWithXattr(ctx, k, xattr, userXattrKey, rv, xv, uxv)
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
func (lds *LeakyDataStore) WriteCas(k string, flags int, exp uint32, cas uint64, v interface{}, opt sgbucket.WriteOptions) (uint64, error) {
	return lds.dataStore.WriteCas(k, flags, exp, cas, v, opt)
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
		return fmt.Errorf("bucket %T does not support views", lds.dataStore)
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

func (lds *LeakyDataStore) WriteCasWithXattr(ctx context.Context, k string, xattr string, exp uint32, cas uint64, opts *sgbucket.MutateInOptions, v interface{}, xv interface{}) (casOut uint64, err error) {
	return lds.dataStore.WriteCasWithXattr(ctx, k, xattr, exp, cas, opts, v, xv)
}

func (lds *LeakyDataStore) WriteWithXattr(ctx context.Context, k string, xattrKey string, exp uint32, cas uint64, opts *sgbucket.MutateInOptions, value []byte, xattrValue []byte, isDelete bool, deleteBody bool) (casOut uint64, err error) {
	if lds.config.WriteWithXattrCallback != nil {
		lds.config.WriteWithXattrCallback(k)
	}
	return lds.dataStore.WriteWithXattr(ctx, k, xattrKey, exp, cas, opts, value, xattrValue, isDelete, deleteBody)
}

func (lds *LeakyDataStore) WriteUpdateWithXattr(ctx context.Context, k string, xattr string, userXattrKey string, exp uint32, opts *sgbucket.MutateInOptions, previous *sgbucket.BucketDocument, callback sgbucket.WriteUpdateWithXattrFunc) (casOut uint64, err error) {
	if lds.config.UpdateCallback != nil {
		wrapperCallback := func(current []byte, xattr []byte, userXattr []byte, cas uint64) (updated []byte, updatedXattr []byte, deletedDoc bool, expiry *uint32, err error) {
			updated, updatedXattr, deletedDoc, expiry, err = callback(current, xattr, userXattr, cas)
			lds.config.UpdateCallback(k)
			return updated, updatedXattr, deletedDoc, expiry, err
		}
		return lds.dataStore.WriteUpdateWithXattr(ctx, k, xattr, userXattrKey, exp, opts, previous, wrapperCallback)
	}
	return lds.dataStore.WriteUpdateWithXattr(ctx, k, xattr, userXattrKey, exp, opts, previous, callback)
}

func (lds *LeakyDataStore) SetXattr(ctx context.Context, k string, xattrKey string, xv []byte) (casOut uint64, err error) {
	if lds.config.SetXattrCallback != nil {
		if err := lds.config.SetXattrCallback(k); err != nil {
			return 0, err
		}
	}
	return lds.dataStore.SetXattr(ctx, k, xattrKey, xv)
}

func (lds *LeakyDataStore) RemoveXattr(ctx context.Context, k string, xattrKey string, cas uint64) (err error) {
	return lds.dataStore.RemoveXattr(ctx, k, xattrKey, cas)
}

func (lds *LeakyDataStore) DeleteXattrs(ctx context.Context, k string, xattrKeys ...string) (err error) {
	return lds.dataStore.DeleteXattrs(ctx, k, xattrKeys...)
}

func (lds *LeakyDataStore) SubdocInsert(ctx context.Context, docID string, fieldPath string, cas uint64, value interface{}) error {
	return lds.dataStore.SubdocInsert(ctx, docID, fieldPath, cas, value)
}

func (lds *LeakyDataStore) DeleteWithXattr(ctx context.Context, k string, xattr string) error {
	return lds.dataStore.DeleteWithXattr(ctx, k, xattr)
}

func (lds *LeakyDataStore) GetXattr(ctx context.Context, k string, xattr string, xv interface{}) (cas uint64, err error) {
	return lds.dataStore.GetXattr(ctx, k, xattr, xv)
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

// getN1QLStore abstracts getting an N1QLStore from the underlying DataStore.
func (lds *LeakyDataStore) getN1QLStore() (N1QLStore, error) {
	// rosmar doesn't implement N1QLStore
	n1qlStore, ok := AsN1QLStore(lds.dataStore)
	if !ok {
		return nil, fmt.Errorf("bucket %T does not support N1QL", lds.dataStore)
	}
	return n1qlStore, nil
}
func (lds *LeakyDataStore) BuildDeferredIndexes(ctx context.Context, indexSet []string) error {
	n1qlStore, err := lds.getN1QLStore()
	if err != nil {
		return err
	}
	return n1qlStore.BuildDeferredIndexes(ctx, indexSet)
}

func (lds *LeakyDataStore) CreateIndex(ctx context.Context, indexName string, expression string, filterExpression string, options *N1qlIndexOptions) error {
	n1qlStore, err := lds.getN1QLStore()
	if err != nil {
		return err
	}
	return n1qlStore.CreateIndex(ctx, indexName, expression, filterExpression, options)
}

func (lds *LeakyDataStore) CreatePrimaryIndex(ctx context.Context, indexName string, options *N1qlIndexOptions) error {
	n1qlStore, err := lds.getN1QLStore()
	if err != nil {
		return err
	}
	return n1qlStore.CreatePrimaryIndex(ctx, indexName, options)
}

func (lds *LeakyDataStore) DropIndex(ctx context.Context, indexName string) error {
	n1qlStore, err := lds.getN1QLStore()
	if err != nil {
		return err
	}
	return n1qlStore.DropIndex(ctx, indexName)
}

func (lds *LeakyDataStore) ExplainQuery(ctx context.Context, statement string, params map[string]interface{}) (plan map[string]interface{}, err error) {
	n1qlStore, err := lds.getN1QLStore()
	if err != nil {
		return nil, err
	}
	return n1qlStore.ExplainQuery(ctx, statement, params)
}

func (lds *LeakyDataStore) EscapedKeyspace() string {
	n1qlStore, err := lds.getN1QLStore()
	if err != nil {
		return fmt.Sprintf("Calling LeakyDataStore.EscapedKeyspace is not supported: %s", err.Error())
	}
	return n1qlStore.EscapedKeyspace()
}

func (lds *LeakyDataStore) GetIndexMeta(ctx context.Context, indexName string) (exists bool, meta *IndexMeta, err error) {
	n1qlStore, err := lds.getN1QLStore()
	if err != nil {
		return false, nil, err
	}
	return n1qlStore.GetIndexMeta(ctx, indexName)
}

func (lds *LeakyDataStore) Query(ctx context.Context, statement string, params map[string]interface{}, consistency ConsistencyMode, adhoc bool) (results sgbucket.QueryResultIterator, err error) {
	n1qlStore, err := lds.getN1QLStore()
	if err != nil {
		return nil, err
	}
	iterator, err := n1qlStore.Query(ctx, statement, params, consistency, adhoc)

	if lds.config.PostN1QLQueryCallback != nil {
		lds.config.PostN1QLQueryCallback()
	}
	return iterator, err
}

func (lds *LeakyDataStore) IsErrNoResults(err error) bool {
	n1qlStore, getN1QLStoreErr := lds.getN1QLStore()
	if getN1QLStoreErr != nil {
		return false
	}
	return n1qlStore.IsErrNoResults(err)
}

func (lds *LeakyDataStore) IndexMetaBucketID() string {
	n1qlStore, err := lds.getN1QLStore()
	if err != nil {
		return fmt.Sprintf("Calling LeakyDataStore.IndexMetaBucketID is not supported: %s", err.Error())
	}
	return n1qlStore.IndexMetaBucketID()
}

func (lds *LeakyDataStore) IndexMetaScopeID() string {
	n1qlStore, err := lds.getN1QLStore()
	if err != nil {
		return fmt.Sprintf("Calling LeakyDataStore.IndexMetaScopeID is not supported: %s", err.Error())
	}
	return n1qlStore.IndexMetaScopeID()
}

func (lds *LeakyDataStore) IndexMetaKeyspaceID() string {
	n1qlStore, err := lds.getN1QLStore()
	if err != nil {
		return fmt.Sprintf("Calling LeakyDataStore.IndexMetaKeyspaceID is not supported: %s", err.Error())
	}
	return n1qlStore.IndexMetaKeyspaceID()
}

func (lds *LeakyDataStore) WaitForIndexesOnline(ctx context.Context, indexNames []string, option WaitForIndexesOnlineOption) error {
	n1qlStore, err := lds.getN1QLStore()
	if err != nil {
		return err
	}
	return n1qlStore.WaitForIndexesOnline(ctx, indexNames, option)
}

func (lds *LeakyDataStore) executeQuery(statement string) (sgbucket.QueryResultIterator, error) {
	n1qlStore, err := lds.getN1QLStore()
	if err != nil {
		return nil, err
	}
	return n1qlStore.executeQuery(statement)
}

func (lds *LeakyDataStore) executeStatement(statement string) error {
	n1qlStore, err := lds.getN1QLStore()
	if err != nil {
		return err
	}
	return n1qlStore.executeStatement(statement)
}

func (lds *LeakyDataStore) waitUntilQueryServiceReady(timeout time.Duration) error {
	n1qlStore, err := lds.getN1QLStore()
	if err != nil {
		return err
	}
	return n1qlStore.waitUntilQueryServiceReady(timeout)
}

func (lds *LeakyDataStore) GetIndexes() (indexes []string, err error) {
	n1qlStore, err := lds.getN1QLStore()
	if err != nil {
		return nil, err
	}
	return n1qlStore.GetIndexes()
}

// Assert interface compliance:
var (
	_ sgbucket.DataStore = &LeakyDataStore{}
)
