package base

import (
	"math/rand"

	sgbucket "github.com/couchbase/sg-bucket"
)

// A wrapper around a gocb bucket that supports multiple gocb bucket instances.  All operations except Close will be randomly routed through one of the
// underlying gocb bucket instances.  Close() will close all gocb bucket instances.
type CouchbaseBucketGoCBMulti struct {
	numBuckets int
	buckets    []*CouchbaseBucketGoCB
}

func (b *CouchbaseBucketGoCBMulti) getBucket() *CouchbaseBucketGoCB {
	// TODO: I don't expect the low-level locking in rand.Intn to be a significant performance consideration.  Could revisit if that's
	//       not the case and instead do something based on a mod of time.UnixNano
	return b.buckets[rand.Intn(len(b.buckets))]
}

// Creates a Bucket that talks to a real live Couchbase server.
func GetCouchbaseBucketGoCBMulti(spec BucketSpec) (bucket *CouchbaseBucketGoCBMulti, err error) {

	Logf("Using %d gocb instances for bucket %s.", spec.NumSDKClients, spec.BucketName)
	numBuckets := spec.NumSDKClients
	bucket = &CouchbaseBucketGoCBMulti{
		numBuckets: spec.NumSDKClients,
		buckets:    make([]*CouchbaseBucketGoCB, numBuckets),
	}

	for i := 0; i < numBuckets; i++ {
		bucket.buckets[i], err = GetCouchbaseBucketGoCB(spec)
		if err != nil {
			return bucket, err
		}
	}

	return bucket, err
}

func (b *CouchbaseBucketGoCBMulti) GetName() string {
	return b.getBucket().GetName()
}
func (b *CouchbaseBucketGoCBMulti) Get(k string, rv interface{}) (uint64, error) {
	return b.getBucket().Get(k, rv)
}
func (b *CouchbaseBucketGoCBMulti) GetRaw(k string) (v []byte, cas uint64, err error) {
	return b.getBucket().GetRaw(k)
}
func (b *CouchbaseBucketGoCBMulti) GetAndTouchRaw(k string, exp int) (v []byte, cas uint64, err error) {
	return b.getBucket().GetAndTouchRaw(k, exp)
}
func (b *CouchbaseBucketGoCBMulti) GetBulkRaw(keys []string) (map[string][]byte, error) {
	return b.getBucket().GetBulkRaw(keys)
}
func (b *CouchbaseBucketGoCBMulti) Add(k string, exp int, v interface{}) (added bool, err error) {
	return b.getBucket().Add(k, exp, v)
}
func (b *CouchbaseBucketGoCBMulti) AddRaw(k string, exp int, v []byte) (added bool, err error) {
	return b.getBucket().AddRaw(k, exp, v)
}
func (b *CouchbaseBucketGoCBMulti) Append(k string, data []byte) error {
	return b.getBucket().Append(k, data)
}
func (b *CouchbaseBucketGoCBMulti) Set(k string, exp int, v interface{}) error {
	return b.getBucket().Set(k, exp, v)
}
func (b *CouchbaseBucketGoCBMulti) SetRaw(k string, exp int, v []byte) error {
	return b.getBucket().SetRaw(k, exp, v)
}
func (b *CouchbaseBucketGoCBMulti) Delete(k string) error {
	return b.getBucket().Delete(k)
}
func (b *CouchbaseBucketGoCBMulti) Remove(k string, cas uint64) (casOut uint64, err error) {
	return b.getBucket().Remove(k, cas)
}
func (b *CouchbaseBucketGoCBMulti) Write(k string, flags int, exp int, v interface{}, opt sgbucket.WriteOptions) error {
	return b.getBucket().Write(k, flags, exp, v, opt)
}
func (b *CouchbaseBucketGoCBMulti) WriteCas(k string, flags int, exp int, cas uint64, v interface{}, opt sgbucket.WriteOptions) (uint64, error) {
	return b.getBucket().WriteCas(k, flags, exp, cas, v, opt)
}
func (b *CouchbaseBucketGoCBMulti) Update(k string, exp int, callback sgbucket.UpdateFunc) (err error) {
	return b.getBucket().Update(k, exp, callback)
}
func (b *CouchbaseBucketGoCBMulti) WriteUpdate(k string, exp int, callback sgbucket.WriteUpdateFunc) (err error) {
	return b.getBucket().WriteUpdate(k, exp, callback)
}

func (b *CouchbaseBucketGoCBMulti) Incr(k string, amt, def uint64, exp int) (uint64, error) {
	return b.getBucket().Incr(k, amt, def, exp)
}
func (b *CouchbaseBucketGoCBMulti) WriteCasWithXattr(k string, xattr string, exp int, cas uint64, v interface{}, xv interface{}) (casOut uint64, err error) {
	return b.getBucket().WriteCasWithXattr(k, xattr, exp, cas, v, xv)
}
func (b *CouchbaseBucketGoCBMulti) WriteUpdateWithXattr(k string, xattr string, exp int, previous *sgbucket.BucketDocument, callback sgbucket.WriteUpdateWithXattrFunc) (casOut uint64, err error) {
	return b.getBucket().WriteUpdateWithXattr(k, xattr, exp, previous, callback)
}
func (b *CouchbaseBucketGoCBMulti) GetWithXattr(k string, xattr string, rv interface{}, xv interface{}) (cas uint64, err error) {
	return b.getBucket().GetWithXattr(k, xattr, rv, xv)
}
func (b *CouchbaseBucketGoCBMulti) DeleteWithXattr(k string, xattr string) error {
	return b.getBucket().DeleteWithXattr(k, xattr)
}
func (b *CouchbaseBucketGoCBMulti) GetDDoc(docname string, value interface{}) error {
	return b.getBucket().GetDDoc(docname, value)
}
func (b *CouchbaseBucketGoCBMulti) PutDDoc(docname string, value interface{}) error {
	return b.getBucket().PutDDoc(docname, value)
}
func (b *CouchbaseBucketGoCBMulti) DeleteDDoc(docname string) error {
	return b.getBucket().DeleteDDoc(docname)
}
func (b *CouchbaseBucketGoCBMulti) View(ddoc, name string, params map[string]interface{}) (sgbucket.ViewResult, error) {
	return b.getBucket().View(ddoc, name, params)
}

func (b *CouchbaseBucketGoCBMulti) ViewCustom(ddoc, name string, params map[string]interface{}, vres interface{}) error {
	return b.getBucket().ViewCustom(ddoc, name, params, vres)
}

func (b *CouchbaseBucketGoCBMulti) SetBulk(entries []*sgbucket.BulkSetEntry) (err error) {
	return b.getBucket().SetBulk(entries)
}

func (b *CouchbaseBucketGoCBMulti) Refresh() error {
	return b.getBucket().Refresh()
}

func (b *CouchbaseBucketGoCBMulti) StartTapFeed(args sgbucket.FeedArguments) (sgbucket.MutationFeed, error) {
	return b.getBucket().StartTapFeed(args)
}

func (b *CouchbaseBucketGoCBMulti) StartDCPFeed(args sgbucket.FeedArguments, callback sgbucket.FeedEventCallbackFunc) error {
	return b.getBucket().StartDCPFeed(args, callback)
}

func (b *CouchbaseBucketGoCBMulti) Close() {
	for i := 0; i < b.numBuckets; i++ {
		b.buckets[i].Close()
	}
}

func (b *CouchbaseBucketGoCBMulti) Dump() {
	b.getBucket().Dump()
}
func (b *CouchbaseBucketGoCBMulti) VBHash(docID string) uint32 {
	return b.getBucket().VBHash(docID)
}

func (b *CouchbaseBucketGoCBMulti) GetMaxVbno() (uint16, error) {
	return b.getBucket().GetMaxVbno()
}

func (b *CouchbaseBucketGoCBMulti) CouchbaseServerVersion() (major uint64, minor uint64, micro string, err error) {
	return b.getBucket().CouchbaseServerVersion()
}

func (b *CouchbaseBucketGoCBMulti) UUID() (string, error) {
	return b.getBucket().UUID()
}

func (b *CouchbaseBucketGoCBMulti) GetStatsVbSeqno(maxVbno uint16, useAbsHighSeqNo bool) (uuids map[uint16]uint64, highSeqnos map[uint16]uint64, seqErr error) {
	return b.GetStatsVbSeqno(maxVbno, useAbsHighSeqNo)
}
