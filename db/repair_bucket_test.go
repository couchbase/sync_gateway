package db

import (
	"fmt"
	"log"
	"testing"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbaselabs/go.assert"
)

const (
	docIdProblematicRevTree  = "docIdProblematicRevTree"
	docIdProblematicRevTree2 = "docIdProblematicRevTree2"
)

func testBucketWithViewsAndBrokenDoc() (bucket base.Bucket, numDocs int) {

	numDocsAdded := 0
	bucket = testBucket()
	installViews(bucket, false)

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

	// All docs will be repaired due to the repairJob function that indiscriminately repairs all docs
	assert.True(t, len(repairedDocs) == 3)

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
	rawVal, _, errGetDoc := bucket.GetRaw(docIdProblematicRevTree)
	assertNoError(t, errGetDoc, fmt.Sprintf("Error getting doc: %v", errGetDoc))

	repairedDoc, errUnmarshal := unmarshalDocument(docIdProblematicRevTree, rawVal)
	assertNoError(t, errUnmarshal, fmt.Sprintf("Error unmarshalling doc: %v", errUnmarshal))

	// Since doc was repaired, should contain no cycles
	assert.False(t, repairedDoc.History.ContainsCycles())

	// There should be a backup doc in the bucket with ID _sync:repair:backup:docIdProblematicRevTree
	rawVal, _, errGetDoc = bucket.GetRaw("_sync:repair:backup:docIdProblematicRevTree")
	assertNoError(t, errGetDoc, fmt.Sprintf("Error getting backup doc: %v", errGetDoc))

	backupDoc, errUnmarshalBackup := unmarshalDocument(docIdProblematicRevTree, rawVal)
	assertNoError(t, errUnmarshalBackup, fmt.Sprintf("Error umarshalling backup doc: %v", errUnmarshalBackup))

	// The backup doc should contain revtree cycles
	assert.True(t, backupDoc.History.ContainsCycles())

}

// Make sure docs not modified during dry run
func TestRepairBucketDryRun(t *testing.T) {

	base.EnableLogKey("CRUD")

	bucket, _ := testBucketWithViewsAndBrokenDoc()

	repairBucket := NewRepairBucket(bucket)

	repairBucket.InitFrom(RepairBucketParams{
		DryRun: true,
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
	rawVal, _, errGetDoc := bucket.GetRaw(docIdProblematicRevTree2)
	assertNoError(t, errGetDoc, fmt.Sprintf("Error getting doc: %v", errGetDoc))

	repairedDoc, errUnmarshal := unmarshalDocument(docIdProblematicRevTree2, rawVal)
	assertNoError(t, errUnmarshal, fmt.Sprintf("Error unmarshalling doc: %v", errUnmarshal))

	// Since doc was not repaired due to dry, should still contain cycles
	assert.True(t, repairedDoc.History.ContainsCycles())

	rawVal, _, errGetDoc = bucket.GetRaw("_sync:repair:dryrun:docIdProblematicRevTree2")
	assertNoError(t, errGetDoc, fmt.Sprintf("Error getting backup doc: %v", errGetDoc))

	backupDoc, errUnmarshalBackup := unmarshalDocument(docIdProblematicRevTree2, rawVal)
	assertNoError(t, errUnmarshalBackup, fmt.Sprintf("Error umarshalling backup doc: %v", errUnmarshalBackup))

	// The dry run fixed doc should not contain revtree cycles
	assert.False(t, backupDoc.History.ContainsCycles())

}
