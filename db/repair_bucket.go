package db

import (
	"encoding/json"

	"io/ioutil"

	"fmt"

	"github.com/couchbase/go-couchbase"
	"github.com/couchbase/sync_gateway/base"
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
type DocTransformer func(doc *document) (transformedDoc *document, transformed bool, err error)

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

func (r RepairBucket) RepairBucket() (repairedDocs []*document, err error) {

	options := Body{"stale": false, "reduce": false}
	options["startkey"] = []interface{}{true}

	vres, err := r.Bucket.View(DesignDocSyncHousekeeping, ViewImport, options)
	if err != nil {
		return repairedDocs, err
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
			doc, err := unmarshalDocument(docid, currentValue)
			if err != nil {
				return nil, err
			}
			updatedDoc, shouldUpdate, err := r.TransformBucketDoc(doc)
			if err != nil {
				return nil, err
			}

			switch shouldUpdate {
			case true:

				if r.WriteRepairedDocsToDisk {
					origDocTempFile, writeTempFileErr := writeDocToTempFile(doc)
					if writeTempFileErr != nil {
						base.Warn("Error writing orig doc to tmp file.  Doc: %v.  Error: %v", doc, writeTempFileErr)
					}

					updatedDocTempFile, writeTempFileErr := writeDocToTempFile(updatedDoc)
					if writeTempFileErr != nil {
						base.Warn("Error writing update doc to tmp file.  UpdatedDoc: %v.  Error: %v", updatedDoc, writeTempFileErr)
					}

					base.LogTo("CRUD", "Repair Doc (dry_run=%v).  Original doc written to: %+v, Post-repair doc written to: %+v", r.DryRun, origDocTempFile, updatedDocTempFile)

				} else {

					base.LogTo("CRUD", "Repair Doc (dry_run=%v)  Dumping doc contents directly into logs since WriteRepairedDocsToDisk disabled.", r.DryRun)

					contentOrig, err := json.MarshalIndent(doc, "", "    ")
					if err == nil {
						base.LogTo("CRUD", "Original Doc before repair (dry_run=%v) %s", r.DryRun, contentOrig)
					}

					contentUpdated, err := json.MarshalIndent(doc, "", "    ")
					if err == nil {
						base.LogTo("CRUD", "Updated doc after repair (dry_run=%v) %s", r.DryRun, contentUpdated)
					}

				}

				repairedDocs = append(repairedDocs, updatedDoc)
				if r.DryRun {
					return nil, couchbase.UpdateCancel
				} else {
					return json.Marshal(updatedDoc)
				}
			default:
				return nil, couchbase.UpdateCancel
			}

		})

	}

	return repairedDocs, nil
}

func writeDocToTempFile(doc *document) (tmpFileName string, err error) {

	if doc == nil {
		return "", fmt.Errorf("Cannot write nil doc to temp file")
	}

	tmpfile, err := ioutil.TempFile("", doc.ID)
	if err != nil {
		return "", err
	}

	content, err := json.MarshalIndent(doc, "", "    ")
	if err != nil {
		return "", err
	}

	if _, err := tmpfile.Write(content); err != nil {
		return "", err
	}

	if err := tmpfile.Close(); err != nil {
		return "", err
	}

	return tmpfile.Name(), nil
}

func (r RepairBucket) TransformBucketDoc(doc *document) (transformedDoc *document, transformed bool, err error) {

	transformed = false
	for _, repairJob := range r.RepairJobs {

		repairedDoc, repairedDocTxformed, repairDocErr := repairJob(doc)
		if repairDocErr != nil {
			return nil, false, repairDocErr
		}

		if !repairedDocTxformed {
			continue
		}

		// Update output value to indicate this doc was transformed by at least one of the underlying repair jobs
		transformed = true

		// Update output value with latest result from repair job, which may be overwritten by later loop iterations
		transformedDoc = repairedDoc

		// Update doc that is being transformed to be the output of the last repair job, in order
		// that the next iteration of the loop use this as input
		doc = repairedDoc

	}

	return transformedDoc, transformed, nil
}

// Repairs rev tree cycles (see SG issue #2847)
func RepairJobRevTreeCycles(doc *document) (transformedDoc *document, transformed bool, err error) {

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

	// Return original doc pointer since it was repaired in place
	return doc, true, nil

}
