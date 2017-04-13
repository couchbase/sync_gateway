package base

import "github.com/couchbase/sg-bucket"

// Defaults to gocb bucket, but uses a go-couchbase bucket for mutation feed functionality.
// See comments on StartTapFeed for rationale
type CouchbaseBucketGoCBGoCouchbaseHybrid struct {

	// Embedded struct so that all calls will call down to GoCB bucket by default
	*CouchbaseBucketGoCB

	// Ability to explicitly call down to go-couchbase bucket when needed
	GoCouchbaseBucket *CouchbaseBucket
}

// Creates a Bucket that talks to a real live Couchbase server.
func NewCouchbaseBucketGoCBGoCouchbaseHybrid(spec BucketSpec, callback sgbucket.BucketNotifyFn) (bucket *CouchbaseBucketGoCBGoCouchbaseHybrid, err error) {

	hybrid := CouchbaseBucketGoCBGoCouchbaseHybrid{}

	// Create GoCB bucket
	hybrid.CouchbaseBucketGoCB, err = GetCouchbaseBucketGoCB(spec)
	if err != nil {
		return nil, err
	}
	
	// Set transcoder to SGTranscoder to avoid cases where it tries to write docs as []byte without setting
	// the proper doctype flag and then later read them as JSON, which fails because it gets back a []byte
	// initially this was using SGTranscoder for all GoCB buckets, but due to
	// https://github.com/couchbase/sync_gateway/pull/2416#issuecomment-288882896
	// it's only being set for hybrid buckets
	hybrid.CouchbaseBucketGoCB.SetTranscoder(SGTranscoder{})

	// Create go-couchbase bucket
	hybrid.GoCouchbaseBucket, err = GetCouchbaseBucket(spec, callback)
	if err != nil {
		return nil, err
	}

	return &hybrid, nil

}


func (bucket CouchbaseBucketGoCBGoCouchbaseHybrid) Refresh() error {
	return bucket.GoCouchbaseBucket.Refresh()
}

// It would be a considerable amount of work to add mutation feed support using pure GoCB buckets,
// especially in the CBGT usage where we depend on cbdatasource features.
func (bucket CouchbaseBucketGoCBGoCouchbaseHybrid) StartTapFeed(args sgbucket.TapArguments) (sgbucket.TapFeed, error) {
	return bucket.GoCouchbaseBucket.StartTapFeed(args)
}

func (bucket CouchbaseBucketGoCBGoCouchbaseHybrid) Dump() {
	bucket.GoCouchbaseBucket.Dump()
}

func (bucket CouchbaseBucketGoCBGoCouchbaseHybrid) UUID() (string, error) {
	// Since the GoCB bucket doesn't have an implementation for UUID() yet, this needs to be
	// forced to use the go-couchbase bucket
	return bucket.GoCouchbaseBucket.UUID()
}


