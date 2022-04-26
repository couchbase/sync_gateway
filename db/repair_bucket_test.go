/*
Copyright 2017-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package db

import (
	"fmt"
	"testing"

	"github.com/couchbase/sync_gateway/base"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	docIdProblematicRevTree  = "docIdProblematicRevTree"
	docIdProblematicRevTree2 = "docIdProblematicRevTree2"
)

func testBucketWithViewsAndBrokenDoc(t testing.TB) (bucket *base.TestBucket, numDocs int) {

	numDocsAdded := 0
	bucket = base.GetTestBucket(t)

	err := installViews(bucket)
	require.NoError(t, err)

	// Add harmless docs
	for i := 0; i < base.DefaultViewQueryPageSize+1; i++ {
		testSyncData := SyncData{}
		_, err = bucket.Add(fmt.Sprintf("foo-%d", i), 0, map[string]interface{}{"foo": "bar", base.SyncPropertyName: testSyncData})
		require.NoError(t, err)
		numDocsAdded++
	}

	// Add doc that should be repaired
	rawDoc, err := unmarshalDocument(docIdProblematicRevTree, []byte(testdocProblematicRevTree1))
	if err != nil {
		panic(fmt.Sprintf("Error unmarshalling doc: %v", err))
	}
	_, err = bucket.Add(docIdProblematicRevTree, 0, rawDoc)
	require.NoError(t, err)
	numDocsAdded++

	// Add 2nd doc that should be repaired
	rawDoc, err = unmarshalDocument(docIdProblematicRevTree2, []byte(testdocProblematicRevTree1))
	if err != nil {
		panic(fmt.Sprintf("Error unmarshalling doc: %v", err))
	}
	_, err = bucket.Add(docIdProblematicRevTree2, 0, rawDoc)
	require.NoError(t, err)
	numDocsAdded++

	return bucket, numDocsAdded

}

func TestRepairBucket(t *testing.T) {
	if !base.UnitTestUrlIsWalrus() {
		t.Skip("This test only works against walrus (requires views)")
	}

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyCRUD)

	bucket, numDocs := testBucketWithViewsAndBrokenDoc(t)
	defer bucket.Close()

	repairJob := func(docId string, originalCBDoc []byte) (transformedCBDoc []byte, transformed bool, err error) {
		return nil, true, nil
	}
	repairBucket := NewRepairBucket(bucket).
		SetDryRun(true).
		AddRepairJob(repairJob)

	repairedDocs, err := repairBucket.RepairBucket()

	assert.NoError(t, err, fmt.Sprintf("Unexpected error: %v", err))

	// All docs will be repaired due to the repairJob function that indiscriminately repairs all docs
	assert.True(t, len(repairedDocs) == numDocs)

}

func TestRepairBucketRevTreeCycles(t *testing.T) {

	// Disabled due to failure described #3267
	t.Skip("WARNING: TEST DISABLED")

	if !base.UnitTestUrlIsWalrus() {
		t.Skip("This test only works against walrus (requires views)")
	}

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyCRUD)

	bucket, _ := testBucketWithViewsAndBrokenDoc(t)
	defer bucket.Close()

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

	assert.NoError(t, err, fmt.Sprintf("Error repairing bucket: %v", err))
	assert.True(t, len(repairedDocs) == 2)

	// Now get the doc from the bucket
	rawVal, _, errGetDoc := bucket.GetRaw(docIdProblematicRevTree)
	assert.NoError(t, errGetDoc, fmt.Sprintf("Error getting doc: %v", errGetDoc))

	repairedDoc, errUnmarshal := unmarshalDocument(docIdProblematicRevTree, rawVal)
	assert.NoError(t, errUnmarshal, fmt.Sprintf("Error unmarshalling doc: %v", errUnmarshal))

	// Since doc was repaired, should contain no cycles
	assert.False(t, repairedDoc.History.ContainsCycles())

	// There should be a backup doc in the bucket with ID _sync:repair:backup:docIdProblematicRevTree
	rawVal, _, errGetDoc = bucket.GetRaw(base.RepairBackup + "docIdProblematicRevTree")
	assert.NoError(t, errGetDoc, fmt.Sprintf("Error getting backup doc: %v", errGetDoc))

	backupDoc, errUnmarshalBackup := unmarshalDocument(docIdProblematicRevTree, rawVal)
	assert.NoError(t, errUnmarshalBackup, fmt.Sprintf("Error umarshalling backup doc: %v", errUnmarshalBackup))

	// The backup doc should contain revtree cycles
	assert.True(t, backupDoc.History.ContainsCycles())

}

// Make sure docs not modified during dry run
func TestRepairBucketDryRun(t *testing.T) {
	if !base.UnitTestUrlIsWalrus() {
		t.Skip("This test only works against walrus (requires views)")
	}

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyCRUD)

	bucket, _ := testBucketWithViewsAndBrokenDoc(t)
	defer bucket.Close()

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

	assert.NoError(t, err, fmt.Sprintf("Error repairing bucket: %v", err))
	assert.True(t, len(repairedDocs) == 2)

	// Now get the doc from the bucket
	rawVal, _, errGetDoc := bucket.GetRaw(docIdProblematicRevTree2)
	assert.NoError(t, errGetDoc, fmt.Sprintf("Error getting doc: %v", errGetDoc))

	repairedDoc, errUnmarshal := unmarshalDocument(docIdProblematicRevTree2, rawVal)
	assert.NoError(t, errUnmarshal, fmt.Sprintf("Error unmarshalling doc: %v", errUnmarshal))

	// Since doc was not repaired due to dry, should still contain cycles
	assert.True(t, repairedDoc.History.ContainsCycles())

	rawVal, _, errGetDoc = bucket.GetRaw(base.RepairDryRun + "docIdProblematicRevTree2")
	assert.NoError(t, errGetDoc, fmt.Sprintf("Error getting backup doc: %v", errGetDoc))

	backupDoc, errUnmarshalBackup := unmarshalDocument(docIdProblematicRevTree2, rawVal)
	assert.NoError(t, errUnmarshalBackup, fmt.Sprintf("Error umarshalling backup doc: %v", errUnmarshalBackup))

	// The dry run fixed doc should not contain revtree cycles
	assert.False(t, backupDoc.History.ContainsCycles())

}
