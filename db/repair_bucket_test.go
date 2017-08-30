package db

import (
	"testing"
	"fmt"
	"sync"
)



func TestRepairBucket(t *testing.T) {

	bucket := testBucket()
	bucket.Add("foo", 0, map[string]interface{}{"foo": "bar"})

	repairJobWaitGroup := sync.WaitGroup{}

	repairJob := func(doc *document) (transformedDoc *document, transformed bool, err error) {
		defer repairJobWaitGroup.Done()
		return nil, false, nil
	}
	repairBucket := NewRepairBucket(bucket).
		SetDryRun(true).
		AddRepairJob(repairJob)

	repairJobWaitGroup.Add(1)
	err := repairBucket.RepairBucket()

	assertNoError(t, err, fmt.Sprintf("Unexpected error: %v", err))

	repairJobWaitGroup.Wait()



}
