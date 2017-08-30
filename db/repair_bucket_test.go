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
	docIdProblematicRevTree2 = "docIdProblematicRevTree2"
)

func testBucketWithViewsAndBrokenDoc() (bucket base.Bucket, numDocs int) {

	numDocsAdded := 0
	bucket = testBucket()
	installViews(bucket)

	// Add a harmless doc
	testSyncData := syncData{}
	bucket.Add("foo", 0, map[string]interface{}{"foo": "bar", "_sync": testSyncData})
	numDocsAdded++

	// Add doc that should be repaired
	rawDoc, err := unmarshalDocument(docIdProblematicRevTree, []byte(testdocProblematicRevTree))
	if err != nil {
		panic(fmt.Sprintf("Error unmarshalling doc: %v", err))
	}
	bucket.Add(docIdProblematicRevTree, 0, rawDoc)
	numDocsAdded++

	// Add 2nd doc that should be repaired
	rawDoc, err = unmarshalDocument(docIdProblematicRevTree2, []byte(testdocProblematicRevTree))
	if err != nil {
		panic(fmt.Sprintf("Error unmarshalling doc: %v", err))
	}
	bucket.Add(docIdProblematicRevTree2, 0, rawDoc)
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
	assert.True(t, len(repairedDocs) == 2)

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

	// There should be a backup doc in the bucket with ID _sync:repair:backup:docIdProblematicRevTree
	var backupDocRaw interface{}
	_, errGetDoc = bucket.Get("_sync:repair:backup:docIdProblematicRevTree", &backupDocRaw)
	assertNoError(t, errGetDoc, fmt.Sprintf("Error getting backup doc: %v", errGetDoc))

	marshalledBackup, _ := json.MarshalIndent(backupDocRaw, "", "")

	backupDoc, errUnmarshalBackup := unmarshalDocument(docIdProblematicRevTree, marshalledBackup)
	assertNoError(t, errUnmarshalBackup, fmt.Sprintf("Error umarshalling backup doc: %v", errUnmarshalBackup))

	// The backup doc should contain revtree cycles
	assert.True(t, backupDoc.History.ContainsCycles())

}
