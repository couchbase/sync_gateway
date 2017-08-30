package db

import (
	"encoding/json"
	"fmt"
	"log"
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

	base.EnableLogKey("CRUD")

	bucket, _ := testBucketWithViewsAndBrokenDoc()

	repairJob := func(docId string, originalCBDoc []byte) (transformedCBDoc []byte, transformed bool, err error) {
		log.Printf("repairJob called back")
		return nil, true, nil
	}
	repairBucket := NewRepairBucket(bucket).
		SetDryRun(true).
		AddRepairJob(repairJob)

	repairedDocs, err := repairBucket.RepairBucket()

	assertNoError(t, err, fmt.Sprintf("Unexpected error: %v", err))

	// Both docs will be repaired due to the repairJob function that indiscriminately repairs all docs
	assert.True(t, len(repairedDocs) == 2)

}

func TestRepairBucketRevTreeCycles(t *testing.T) {

	base.EnableLogKey("CRUD")

	bucket, _ := testBucketWithViewsAndBrokenDoc()

	repairBucket := NewRepairBucket(bucket)

	repairBucket.InitFrom(RepairBucketParams{
		DryRun: false,
		RepairJobs: []RepairJobParams{
			{
				RepairJobType: RepairRevTreeCycles,
			},
		},
	})

	repairedDocs, err := repairBucket.RepairBucket()

	assertNoError(t, err, fmt.Sprintf("Error repairing bucket: %v", err))
	assert.True(t, len(repairedDocs) == 1)

	// Now get the doc from the bucket
	var value interface{}
	_, errGetDoc := bucket.Get(docIdProblematicRevTree, &value)
	assertNoError(t, errGetDoc, fmt.Sprintf("Error getting doc: %v", errGetDoc))
	log.Printf("Got doc with docIdProblematicRevTree: %+v", value)


	marshalled, errMarshal := json.Marshal(value)
	log.Printf("marshalled docIdProblematicRevTree: %s", marshalled)
	assertNoError(t, errMarshal, fmt.Sprintf("Error marshalling doc: %v", errMarshal))

	repairedDoc, errUnmarshal := unmarshalDocument(docIdProblematicRevTree, marshalled)
	assertNoError(t, errUnmarshal, fmt.Sprintf("Error unmarshalling doc: %v", errUnmarshal))

	// Since doc was repaired, should contain no cycles
	assert.False(t, repairedDoc.History.ContainsCycles())

}
