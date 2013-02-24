// bucket.go

package base

import (
	"log"
	"strings"
	"sync"

	"github.com/couchbaselabs/go-couchbase"
	"github.com/couchbaselabs/walrus"
)

type Bucket walrus.Bucket

// Implementation of walrus.Bucket that talks to a Couchbase server
type couchbaseBucket struct {
	*couchbase.Bucket
}

func (bucket couchbaseBucket) GetName() string {
	return bucket.Name
}

func (bucket couchbaseBucket) Update(k string, exp int, callback walrus.UpdateFunc) error {
	return bucket.Bucket.Update(k, exp, couchbase.UpdateFunc(callback))
}

func (bucket couchbaseBucket) View(ddoc, name string, params map[string]interface{}) (walrus.ViewResult, error) {
	vres := walrus.ViewResult{}
	return vres, bucket.Bucket.ViewCustom(ddoc, name, params, &vres)
}

// Creates a Bucket that talks to a real live Couchbase server.
func GetCouchbaseBucket(couchbaseURL, poolName, bucketName string) (bucket Bucket, err error) {
	cbbucket, err := couchbase.GetBucket(couchbaseURL, poolName, bucketName)
	if err == nil {
		bucket = couchbaseBucket{cbbucket}
	}
	return
}

var buckets map[[3]string]Bucket
var bucketsLock sync.Mutex

func GetWalrusBucket(couchbaseURL, poolName, bucketName string) (Bucket, error) {
	bucketsLock.Lock()
	defer bucketsLock.Unlock()

	if buckets == nil {
		buckets = make(map[[3]string]Bucket)
	}
	key := [3]string{couchbaseURL, poolName, bucketName}
	bucket := buckets[key]
	if bucket == nil {
		bucket = walrus.NewBucket(bucketName)
		buckets[key] = bucket
		log.Printf("Created Walrus bucket %+v", key)
	}
	return bucket, nil
}

func GetBucket(url, poolName, bucketName string) (bucket Bucket, err error) {
	if strings.HasPrefix(url, "walrus:") {
		return GetWalrusBucket(url, poolName, bucketName)
	}
	return GetCouchbaseBucket(url, poolName, bucketName)
}
