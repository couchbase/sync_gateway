package db

import (
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"testing"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbaselabs/go.assert"
)

const (
	docIdProblematicRevTree = "docIdProblematicRevTree"
)

func testBucketWithViewsAndBrokenDoc() (bucket base.Bucket, numDocs int) {

	numDocsAdded := 0
	bucket = testBucket()
	log.Printf("installing views")
	installViews(bucket)
	log.Printf("installed views")

	// Add a harmless doc
	testSyncData := syncData{}
	bucket.Add("foo", 0, map[string]interface{}{"foo": "bar", "_sync": testSyncData})
	numDocsAdded++

	// Add a doc that should be repaired
	rawDoc, err := unmarshalDocument(docIdProblematicRevTree, []byte(testdocProblematicRevTree))
	if err != nil {
		panic(fmt.Sprintf("Error unmarshalling doc: %v", err))
	}
	bucket.Add(docIdProblematicRevTree, 0, rawDoc)
	numDocsAdded++

	return bucket, numDocsAdded

}

func TestRepairBucket(t *testing.T) {

	base.EnableLogKey("RepairBucket")

	bucket, numdocs := testBucketWithViewsAndBrokenDoc()

	repairJobWaitGroup := sync.WaitGroup{}

	repairJob := func(doc *document) (transformedDoc *document, transformed bool, err error) {
		defer repairJobWaitGroup.Done()
		log.Printf("repairJob called back")
		return nil, true, nil
	}
	repairBucket := NewRepairBucket(bucket).
		SetDryRun(true).
		AddRepairJob(repairJob)

	repairJobWaitGroup.Add(numdocs)
	err := repairBucket.RepairBucket()

	assertNoError(t, err, fmt.Sprintf("Unexpected error: %v", err))

	log.Printf("waiting for waitgroup to be finished")
	repairJobWaitGroup.Wait()

}

func TestRepairBucketRevTreeCycles(t *testing.T) {

	base.EnableLogKey("RepairBucket")

	bucket, _ := testBucketWithViewsAndBrokenDoc()

	repairJob := RepairJobRevTreeCycles

	repairBucket := NewRepairBucket(bucket).
		SetDryRun(false).
		AddRepairJob(repairJob)

	err := repairBucket.RepairBucket()

	assertNoError(t, err, fmt.Sprintf("Error repairing bucket: %v", err))

	// Now get the doc from the bucket
	var value interface{}
	_, errGetDoc := bucket.Get(docIdProblematicRevTree, &value)
	assertNoError(t, errGetDoc, fmt.Sprintf("Error getting doc: %v", errGetDoc))

	marshalled, errMarshal := json.Marshal(value)
	assertNoError(t, errMarshal, fmt.Sprintf("Error marshalling doc: %v", errMarshal))

	repairedDoc, errUnmarshal := unmarshalDocument(docIdProblematicRevTree, marshalled)
	assertNoError(t, errUnmarshal, fmt.Sprintf("Error unmarshalling doc: %v", errUnmarshal))

	// Since doc was repaired, should contain no cycles
	assert.False(t, repairedDoc.History.ContainsCycles())

}
