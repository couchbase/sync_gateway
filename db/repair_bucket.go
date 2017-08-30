package db

import (
	"encoding/json"

	"github.com/couchbase/go-couchbase"
	"github.com/couchbase/sync_gateway/base"
	"log"
)

type RepairBucket struct {
	DryRun     bool
	Bucket     base.Bucket
	RepairJobs []DocTransformer
}

// Given a Couchbase Bucket doc, transform the doc in some way to produce a new doc.
// Also return a boolean to indicate whether a transformation took place, or any errors occurred.
type DocTransformer func(doc *document) (transformedDoc *document, transformed bool, err error)

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

func (r RepairBucket) RepairBucket() (err error) {

	options := Body{"stale": false, "reduce": false}
	options["startkey"] = []interface{}{true}

	vres, err := r.Bucket.View(DesignDocSyncHousekeeping, ViewImport, options)
	if err != nil {
		return err
	}

	log.Printf("view query returned vres: %+v", vres	)

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
			log.Printf("TransformBucketDoc returned updatedDoc: %v, shouldUpdate: %v, err: %v.  dryRun: %v", updatedDoc, shouldUpdate, err, r.DryRun)
			if err != nil {
				return nil, err
			}
			if shouldUpdate && r.DryRun {
				// TODO: write marshalled val to temp files?
				base.LogTo("RepairBucket", "Update disabled for dry run.  Original doc: %+v, Post-repair doc: %+v", doc, updatedDoc)
			}
			if shouldUpdate && !r.DryRun {
				return json.Marshal(updatedDoc)
			} else {
				return nil, couchbase.UpdateCancel
			}
		})

	}

	return nil
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
