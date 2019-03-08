package base

import (
	"encoding/json"
	"fmt"
	"sync/atomic"

	sgbucket "github.com/couchbase/sg-bucket"
)

// A wrapper around a Bucket that tracks bucket usage statistics as basic read/write counts.  Doesn't break
// down by operation type, to better identify counts for bulk operations
type StatsBucket struct {
	bucket             Bucket
	docsRead           uint64
	docsWritten        uint64
	bytesRead          uint64
	bytesWritten       uint64
	unknownSizeRead    uint64
	unknownSizeWritten uint64
}

type StatsBucketStats struct {
	DocsRead           uint64
	DocsWritten        uint64
	BytesRead          uint64
	BytesWritten       uint64
	UnknownSizeRead    uint64
	UnknownSizeWritten uint64
}

func (sbs *StatsBucketStats) String() string {
	return fmt.Sprintf("\nDocs Read:          %12d \nDocs Written:       %12d \nBytes Read:         %12d \nBytes Written:      %12d \nUnknown Size Reads: %12d \nUnknown Size Writes:%12d",
		sbs.DocsRead, sbs.DocsWritten, sbs.BytesRead, sbs.BytesWritten, sbs.UnknownSizeRead, sbs.UnknownSizeWritten)
}

func (sbs *StatsBucketStats) PerIteration(iterationCount uint64) string {
	return fmt.Sprintf("\nDocs Read:          %12d \nDocs Written:       %12d \nBytes Read:         %12d \nBytes Written:      %12d \nUnknown Size Reads: %12d \nUnknown Size Writes:%12d",
		sbs.DocsRead/iterationCount,
		sbs.DocsWritten/iterationCount,
		sbs.BytesRead/iterationCount,
		sbs.BytesWritten/iterationCount,
		sbs.UnknownSizeRead/iterationCount,
		sbs.UnknownSizeWritten/iterationCount)
}

func NewStatsBucket(bucket Bucket) *StatsBucket {
	return &StatsBucket{
		bucket: bucket,
	}
}

func (b *StatsBucket) docRead(count, bytesRead int) {
	atomic.AddUint64(&b.docsRead, uint64(count))
	if bytesRead == -1 {
		atomic.AddUint64(&b.unknownSizeRead, uint64(1))
	} else {
		atomic.AddUint64(&b.bytesRead, uint64(bytesRead))
	}
}

func (b *StatsBucket) docWrite(count, bytesWritten int) {
	atomic.AddUint64(&b.docsWritten, uint64(count))
	if bytesWritten == -1 {
		atomic.AddUint64(&b.unknownSizeWritten, uint64(1))
	} else {
		atomic.AddUint64(&b.bytesWritten, uint64(bytesWritten))

	}
}

func (b *StatsBucket) GetStats() StatsBucketStats {
	return StatsBucketStats{
		DocsRead:           atomic.LoadUint64(&b.docsRead),
		DocsWritten:        atomic.LoadUint64(&b.docsWritten),
		BytesRead:          atomic.LoadUint64(&b.bytesRead),
		BytesWritten:       atomic.LoadUint64(&b.bytesWritten),
		UnknownSizeRead:    atomic.LoadUint64(&b.unknownSizeRead),
		UnknownSizeWritten: atomic.LoadUint64(&b.unknownSizeWritten),
	}
}

func (b *StatsBucket) GetName() string {
	return b.bucket.GetName()
}
func (b *StatsBucket) Get(k string, rv interface{}) (uint64, error) {

	cas, err := b.bucket.Get(k, rv)
	if vBytes, ok := rv.([]byte); ok {
		defer b.docRead(1, len(vBytes))
	} else if marshalledJSON, marshalErr := json.Marshal(rv); marshalErr == nil {
		defer b.docRead(1, len(marshalledJSON))
	} else {
		defer b.docRead(1, -1)
	}
	return cas, err
}
func (b *StatsBucket) GetRaw(k string) (v []byte, cas uint64, err error) {
	v, cas, err = b.bucket.GetRaw(k)
	b.docRead(1, len(v))
	return v, cas, err
}
func (b *StatsBucket) GetAndTouchRaw(k string, exp uint32) (v []byte, cas uint64, err error) {
	v, cas, err = b.bucket.GetAndTouchRaw(k, exp)
	b.docRead(1, len(v))
	return v, cas, err
}
func (b *StatsBucket) Touch(k string, exp uint32) (cas uint64, err error) {
	return b.bucket.Touch(k, exp)
}
func (b *StatsBucket) GetBulkRaw(keys []string) (map[string][]byte, error) {
	results, err := b.bucket.GetBulkRaw(keys)
	for _, value := range results {
		b.docRead(1, len(value))
	}
	return results, err
}
func (b *StatsBucket) Add(k string, exp uint32, v interface{}) (added bool, err error) {
	if vBytes, ok := v.([]byte); ok {
		defer b.docWrite(1, len(vBytes))
	} else {
		defer b.docWrite(1, -1)
	}
	return b.bucket.Add(k, exp, v)
}
func (b *StatsBucket) AddRaw(k string, exp uint32, v []byte) (added bool, err error) {
	defer b.docWrite(1, len(v))
	return b.bucket.AddRaw(k, exp, v)
}
func (b *StatsBucket) Append(k string, data []byte) error {
	defer b.docWrite(1, len(data))
	return b.bucket.Append(k, data)
}
func (b *StatsBucket) Set(k string, exp uint32, v interface{}) error {
	if vBytes, ok := v.([]byte); ok {
		defer b.docWrite(1, len(vBytes))
	} else {
		defer b.docWrite(1, -1)
	}
	return b.bucket.Set(k, exp, v)
}
func (b *StatsBucket) SetRaw(k string, exp uint32, v []byte) error {
	defer b.docWrite(1, len(v))
	return b.bucket.SetRaw(k, exp, v)
}
func (b *StatsBucket) Delete(k string) error {
	return b.bucket.Delete(k)
}
func (b *StatsBucket) Remove(k string, cas uint64) (casOut uint64, err error) {
	return b.bucket.Remove(k, cas)
}
func (b *StatsBucket) Write(k string, flags int, exp uint32, v interface{}, opt sgbucket.WriteOptions) error {
	if vBytes, ok := v.([]byte); ok {
		defer b.docWrite(1, len(vBytes))
	} else {
		defer b.docWrite(1, -1)
	}
	return b.bucket.Write(k, flags, exp, v, opt)
}
func (b *StatsBucket) WriteCas(k string, flags int, exp uint32, cas uint64, v interface{}, opt sgbucket.WriteOptions) (uint64, error) {
	if vBytes, ok := v.([]byte); ok {
		defer b.docWrite(1, len(vBytes))
	} else {
		defer b.docWrite(1, -1)
	}
	return b.bucket.WriteCas(k, flags, exp, cas, v, opt)
}
func (b *StatsBucket) Update(k string, exp uint32, callback sgbucket.UpdateFunc) (casOut uint64, err error) {
	defer b.docWrite(1, -1)
	return b.bucket.Update(k, exp, callback)
}
func (b *StatsBucket) WriteUpdate(k string, exp uint32, callback sgbucket.WriteUpdateFunc) (casOut uint64, err error) {
	defer b.docWrite(1, -1)
	return b.bucket.WriteUpdate(k, exp, callback)
}
func (b *StatsBucket) Incr(k string, amt, def uint64, exp uint32) (uint64, error) {
	return b.bucket.Incr(k, amt, def, exp)
}
func (b *StatsBucket) WriteCasWithXattr(k string, xattr string, exp uint32, cas uint64, v interface{}, xv interface{}) (casOut uint64, err error) {
	if vBytes, ok := v.([]byte); ok {
		defer b.docWrite(1, len(vBytes))
	} else {
		defer b.docWrite(1, -1)
	}
	return b.bucket.WriteCasWithXattr(k, xattr, exp, cas, v, xv)
}
func (b *StatsBucket) WriteUpdateWithXattr(k string, xattr string, exp uint32, previous *sgbucket.BucketDocument, callback sgbucket.WriteUpdateWithXattrFunc) (casOut uint64, err error) {
	defer b.docWrite(1, -1)
	return b.bucket.WriteUpdateWithXattr(k, xattr, exp, previous, callback)
}
func (b *StatsBucket) GetWithXattr(k string, xattr string, rv interface{}, xv interface{}) (cas uint64, err error) {
	cas, err = b.bucket.GetWithXattr(k, xattr, rv, xv)
	if vBytes, ok := rv.([]byte); ok {
		defer b.docRead(1, len(vBytes))
	} else if marshalledJSON, marshalErr := json.Marshal(rv); marshalErr == nil {
		defer b.docRead(1, len(marshalledJSON))
	} else {
		defer b.docRead(1, -1)
	}
	return cas, err
}
func (b *StatsBucket) DeleteWithXattr(k string, xattr string) error {
	return b.bucket.DeleteWithXattr(k, xattr)
}
func (b *StatsBucket) GetXattr(k string, xattr string, xv interface{}) (cas uint64, err error) {
	return b.bucket.GetXattr(k, xattr, xv)
}
func (b *StatsBucket) GetDDoc(docname string, value interface{}) error {
	return b.bucket.GetDDoc(docname, value)
}
func (b *StatsBucket) PutDDoc(docname string, value interface{}) error {
	return b.bucket.PutDDoc(docname, value)
}
func (b *StatsBucket) DeleteDDoc(docname string) error {
	return b.bucket.DeleteDDoc(docname)
}
func (b *StatsBucket) View(ddoc, name string, params map[string]interface{}) (sgbucket.ViewResult, error) {
	return b.bucket.View(ddoc, name, params)
}

func (b *StatsBucket) ViewCustom(ddoc, name string, params map[string]interface{}, vres interface{}) error {
	return b.bucket.ViewCustom(ddoc, name, params, vres)
}

func (b *StatsBucket) ViewQuery(ddoc, name string, params map[string]interface{}) (sgbucket.QueryResultIterator, error) {
	return b.bucket.ViewQuery(ddoc, name, params)
}

func (b *StatsBucket) SetBulk(entries []*sgbucket.BulkSetEntry) (err error) {
	defer b.docWrite(len(entries), 0)
	return b.bucket.SetBulk(entries)
}

func (b *StatsBucket) Refresh() error {
	return b.bucket.Refresh()
}

func (b *StatsBucket) StartTapFeed(args sgbucket.FeedArguments) (sgbucket.MutationFeed, error) {
	return b.bucket.StartTapFeed(args)
}

func (b *StatsBucket) StartDCPFeed(args sgbucket.FeedArguments, callback sgbucket.FeedEventCallbackFunc) error {
	return b.bucket.StartDCPFeed(args, callback)
}

func (b *StatsBucket) Close() {
	b.bucket.Close()
}
func (b *StatsBucket) Dump() {
	b.bucket.Dump()
}
func (b *StatsBucket) VBHash(docID string) uint32 {
	return b.bucket.VBHash(docID)
}

func (b *StatsBucket) GetMaxVbno() (uint16, error) {
	return b.bucket.GetMaxVbno()
}

func (b *StatsBucket) CouchbaseServerVersion() (major uint64, minor uint64, micro string, err error) {
	return b.bucket.CouchbaseServerVersion()
}

func (b *StatsBucket) UUID() (string, error) {
	return b.bucket.UUID()
}

func (b *StatsBucket) GetStatsVbSeqno(maxVbno uint16, useAbsHighSeqNo bool) (uuids map[uint16]uint64, highSeqnos map[uint16]uint64, seqErr error) {
	return b.GetStatsVbSeqno(maxVbno, useAbsHighSeqNo)
}
