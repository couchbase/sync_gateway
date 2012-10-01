// database_test.go

package couchglue

import (
    "log"
    "testing"

    "github.com/couchbaselabs/go-couchbase"
    "github.com/sdegutis/go.assert"
)

var gTestBucket *couchbase.Bucket

func init() {
    var err error
    gTestBucket, err = ConnectToBucket("http://localhost:8091", "default", "couchdb")
    if err != nil {log.Fatalf("Couldn't connect to bucket: %v", err)}
}


func TestDatabase(t *testing.T) {
    db, err := CreateDatabase(gTestBucket, "testdb")
    assertNoError(t, err, "Couldn't create database")
    defer func() {
        err = db.Delete()
        assertNoError(t, err, "Couldn't delete database")
    }()
    
    body := Body {"key1": "value1", "key2": 1234}
    revid, err := db.Put("doc1", body)
    assertNoError(t, err, "Couldn't create document")
    assert.Equals(t, revid, "1-6865ee3d9953ee26fa392728624f60a2806f0184")
}