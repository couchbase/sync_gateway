package db

import (
	"log"
	"testing"
	"time"

	"github.com/couchbaselabs/go.assert"

	"github.com/couchbaselabs/sync_gateway/base"
)

func makeExternalBucket() base.Bucket {
	bucket, err := ConnectToBucket(base.BucketSpec{
		Server:     "walrus:",
		BucketName: "external_bucket"})
	if err != nil {
		log.Fatalf("Couldn't connect to bucket: %v", err)
	}
	return bucket
}

func TestShadowerPull(t *testing.T) {
	bucket := makeExternalBucket()
	defer bucket.Close()
	bucket.Set("key1", 0, Body{"foo": 1})
	bucket.Set("key2", 0, Body{"bar": -1})

	db := setupTestDB(t)
	defer tearDownTestDB(t, db)

	shadower, err := NewShadower(db.DatabaseContext, bucket)
	assertNoError(t, err, "NewShadower")
	defer shadower.Stop()

	base.Log("Waiting for shadower to catch up...")
	var doc1, doc2 *document
	var start = time.Now()
	for {
		seq, _ := db.LastSequence()
		if seq >= 2 {
			break
		}
		if time.Since(start) >= 3*time.Second {
			assertFailed(t, "Timeout!")
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	doc1, _ = db.GetDoc("key1")
	doc2, _ = db.GetDoc("key2")
	assert.DeepEquals(t, doc1.body, Body{"foo": float64(1)})
	assert.DeepEquals(t, doc2.body, Body{"bar": float64(-1)})
}

func TestShadowerPush(t *testing.T) {
	//base.LogKeys["Shadow"] = true
	bucket := makeExternalBucket()
	defer bucket.Close()

	db := setupTestDB(t)
	defer tearDownTestDB(t, db)

	var err error
	db.Shadower, err = NewShadower(db.DatabaseContext, bucket)
	assertNoError(t, err, "NewShadower")

	time.Sleep(1 * time.Millisecond) //TEMP
	_, err = db.Put("key1", Body{"aaa": "bbb"})
	assertNoError(t, err, "Put")
	_, err = db.Put("key2", Body{"ccc": "ddd"})
	assertNoError(t, err, "Put")

	base.Log("Waiting for shadower to catch up...")
	var doc1, doc2 Body
	var start = time.Now()
	for {
		if bucket.Get("key1", &doc1) == nil && bucket.Get("key2", &doc2) == nil {
			break
		}
		if time.Since(start) >= 3*time.Second {
			assertFailed(t, "Timeout!")
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	assert.DeepEquals(t, doc1, Body{"aaa": "bbb"})
	assert.DeepEquals(t, doc2, Body{"ccc": "ddd"})
}
