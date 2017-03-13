package base

import (
	"github.com/couchbase/sg-bucket"
	"log"
)

// Defaults to gocb bucket, falls back to go-couchbase bucket
type CouchbaseBucketGoCBGoCouchbaseHybrid struct {

	// Embedded struct so that all calls will call down to GoCB bucket by default
	*CouchbaseBucketGoCB

	// Ability to explicitly call down to go-couchbase bucket when needed
	GoCouchbaseBucket *CouchbaseBucket

}

// Creates a Bucket that talks to a real live Couchbase server.
func NewCouchbaseBucketGoCBGoCouchbaseHybrid(spec BucketSpec, callback sgbucket.BucketNotifyFn) (bucket *CouchbaseBucketGoCBGoCouchbaseHybrid, err error) {

	log.Printf("NewCouchbaseBucketGoCBGoCouchbaseHybrid")

	hybrid := CouchbaseBucketGoCBGoCouchbaseHybrid{}

	// Create GoCB bucket
	hybrid.CouchbaseBucketGoCB, err = GetCouchbaseBucketGoCB(spec)
	if err != nil {
		return nil, err
	}

	// Create go-couchbase bucket
	hybrid.GoCouchbaseBucket, err = GetCouchbaseBucket(spec, callback)
	if err != nil {
		return nil, err
	}

	return &hybrid, nil

}

func (bucket CouchbaseBucketGoCBGoCouchbaseHybrid) PutDDoc(docname string, value interface{}) error {
	return bucket.GoCouchbaseBucket.PutDDoc(docname, value)
}

func (bucket CouchbaseBucketGoCBGoCouchbaseHybrid) GetDDoc(docname string, into interface{}) error {
	return bucket.GoCouchbaseBucket.GetDDoc(docname, into)
}

func (bucket CouchbaseBucketGoCBGoCouchbaseHybrid) DeleteDDoc(docname string) error {
	return bucket.GoCouchbaseBucket.DeleteDDoc(docname)
}

func (bucket CouchbaseBucketGoCBGoCouchbaseHybrid) View(ddoc, name string, params map[string]interface{}) (sgbucket.ViewResult, error) {
	return bucket.GoCouchbaseBucket.View(ddoc, name, params)
}

func (bucket CouchbaseBucketGoCBGoCouchbaseHybrid) ViewCustom(ddoc, name string, params map[string]interface{}, vres interface{}) error {
	return bucket.GoCouchbaseBucket.ViewCustom(ddoc, name, params, vres)
}

func (bucket CouchbaseBucketGoCBGoCouchbaseHybrid) Refresh() error {
	return bucket.GoCouchbaseBucket.Refresh()
}

func (bucket CouchbaseBucketGoCBGoCouchbaseHybrid) StartTapFeed(args sgbucket.TapArguments) (sgbucket.TapFeed, error) {
	return bucket.GoCouchbaseBucket.StartTapFeed(args)
}

func (bucket CouchbaseBucketGoCBGoCouchbaseHybrid) Dump() {
	bucket.GoCouchbaseBucket.Dump()
}

