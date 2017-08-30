package db

import (
	"io/ioutil"

	"fmt"

	"github.com/couchbase/go-couchbase"
	"github.com/couchbase/sync_gateway/base"
	"encoding/json"
)

// Enum for the different repair jobs (eg, repairing rev tree cycles)
type RepairJobType string

const (
	RepairRevTreeCycles = RepairJobType("RepairRevTreeCycles")
)

// Params suitable for external (eg, HTTP) invocations to describe a RepairBucket operation
type RepairBucketParams struct {
	DryRun     bool              `json:"dry_run"`
	RepairJobs []RepairJobParams `json:"repair_jobs"`
}

// Params suitable for external (eg, HTTP) invocations to describe a specific RepairJob operation
type RepairJobParams struct {
	RepairJobType   RepairJobType          `json:"type"`
	RepairJobParams map[string]interface{} `json:"params"`
}

// Given a Couchbase Bucket doc, transform the doc in some way to produce a new doc.
// Also return a boolean to indicate whether a transformation took place, or any errors occurred.
type DocTransformer func(docId string, originalCBDoc []byte) (transformedCBDoc []byte, transformed bool, err error)

// A RepairBucket struct is the main API entrypoint to call for repairing documents in buckets
type RepairBucket struct {
	DryRun                  bool // If true, will only output what changes it *would* have made, but not make any changes
	WriteRepairedDocsToDisk bool // If true, write the before and after doc to temp files
	Bucket                  base.Bucket
	RepairJobs              []DocTransformer
}

func NewRepairBucket(bucket base.Bucket) *RepairBucket {
	return &RepairBucket{
		Bucket: bucket,
	}
}

func (r *RepairBucket) SetDryRun(dryRun bool) *RepairBucket {
	r.DryRun = dryRun
	return r
}

func (r *RepairBucket) AddRepairJob(repairJob DocTransformer) *RepairBucket {
	r.RepairJobs = append(r.RepairJobs, repairJob)
	return r
}

func (r *RepairBucket) InitFrom(params RepairBucketParams) *RepairBucket {

	r.SetDryRun(params.DryRun)
	r.WriteRepairedDocsToDisk = true
	for _, repairJobParams := range params.RepairJobs {
		switch repairJobParams.RepairJobType {
		case RepairRevTreeCycles:
			r.AddRepairJob(RepairJobRevTreeCycles)
		}
	}

	return r
}

func (r RepairBucket) RepairBucket() (repairedDocIds []string, err error) {

	options := Body{"stale": false, "reduce": false}
	options["startkey"] = []interface{}{true}

	vres, err := r.Bucket.View(DesignDocSyncHousekeeping, ViewImport, options)
	if err != nil {
		return repairedDocIds, err
	}

	for _, row := range vres.Rows {
		rowKey := row.Key.([]interface{})
		docid := rowKey[1].(string)
		key := realDocID(docid)

		err = r.Bucket.Update(key, 0, func(currentValue []byte) ([]byte, error) {
			// Be careful: this block can be invoked multiple times if there are races!
			if currentValue == nil {
				return nil, couchbase.UpdateCancel // someone deleted it?!
			}

			updatedDoc, shouldUpdate, err := r.TransformBucketDoc(key, currentValue)
			if err != nil {
				return nil, err
			}

			switch shouldUpdate {
			case true:

				if r.WriteRepairedDocsToDisk {
					origDocTempFile, writeTempFileErr := writeDocToTempFile(key, currentValue)
					if writeTempFileErr != nil {
						base.Warn("Error writing orig doc to tmp file.  Doc: %v.  Error: %v", string(currentValue), writeTempFileErr)
					}

					updatedDocTempFile, writeTempFileErr := writeDocToTempFile(key, updatedDoc)
					if writeTempFileErr != nil {
						base.Warn("Error writing update doc to tmp file.  UpdatedDoc: %v.  Error: %v", string(updatedDoc), writeTempFileErr)
					}

					base.LogTo("CRUD", "Repair Doc (dry_run=%v).  Original doc written to: %+v, Post-repair doc written to: %+v", r.DryRun, origDocTempFile, updatedDocTempFile)

				} else {

					base.LogTo("CRUD", "Repair Doc (dry_run=%v)  Dumping doc contents directly into logs since WriteRepairedDocsToDisk disabled.", r.DryRun)

					base.LogTo("CRUD", "Original Doc before repair (dry_run=%v) %s", r.DryRun, currentValue)
					base.LogTo("CRUD", "Updated doc after repair (dry_run=%v) %s", r.DryRun, updatedDoc)

				}

				repairedDocIds = append(repairedDocIds, key)
				if r.DryRun {
					return nil, couchbase.UpdateCancel
				} else {
					return updatedDoc, nil
				}
			default:
				return nil, couchbase.UpdateCancel
			}

		})

	}

	return repairedDocIds, nil
}

func writeDocToTempFile(docId string, doc []byte) (tmpFileName string, err error) {

	if doc == nil {
		return "", fmt.Errorf("Cannot write nil doc to temp file")
	}

	tmpfile, err := ioutil.TempFile("", docId)
	if err != nil {
		return "", err
	}

	if _, err := tmpfile.Write(doc); err != nil {
		return "", err
	}

	if err := tmpfile.Close(); err != nil {
		return "", err
	}

	return tmpfile.Name(), nil
}

func (r RepairBucket) TransformBucketDoc(docId string, originalCBDoc []byte) (transformedCBDoc []byte, transformed bool, err error) {

	transformed = false
	for _, repairJob := range r.RepairJobs {

		repairedDoc, repairedDocTxformed, repairDocErr := repairJob(docId, originalCBDoc)
		if repairDocErr != nil {
			return nil, false, repairDocErr
		}

		if !repairedDocTxformed {
			continue
		}

		// Update output value to indicate this doc was transformed by at least one of the underlying repair jobs
		transformed = true

		// Update output value with latest result from repair job, which may be overwritten by later loop iterations
		transformedCBDoc = repairedDoc

		// Update doc that is being transformed to be the output of the last repair job, in order
		// that the next iteration of the loop use this as input
		originalCBDoc = repairedDoc

	}

	return transformedCBDoc, transformed, nil
}

// Repairs rev tree cycles (see SG issue #2847)
func RepairJobRevTreeCycles(docId string, originalCBDoc []byte) (transformedCBDoc []byte, transformed bool, err error) {

	doc, errUnmarshal := unmarshalDocument(docId, originalCBDoc)
	if errUnmarshal != nil {
		return nil, false, errUnmarshal
	}

	// Check if rev history has cycles
	containsCycles := doc.History.ContainsCycles()

	if !containsCycles {
		// nothing to repair
		return nil, false, nil
	}

	// Repair it
	if err := doc.History.Repair(); err != nil {
		return nil, false, err
	}

	transformedCBDoc, errMarshal := json.Marshal(doc)
	if errMarshal != nil {
		return nil, false, errMarshal
	}

	// Return original doc pointer since it was repaired in place
	return transformedCBDoc, true, nil

}
