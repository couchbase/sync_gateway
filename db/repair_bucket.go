package db

import (
	"encoding/json"
	"time"

	"github.com/couchbase/sync_gateway/base"
	pkgerrors "github.com/pkg/errors"
)

// Enum for the different repair jobs (eg, repairing rev tree cycles)
type RepairJobType string

const kDefaultRepairedFileTTL = 60 * 60 * 24 * time.Second // 24 hours

const (
	RepairRevTreeCycles = RepairJobType("RepairRevTreeCycles")
)

// Params suitable for external (eg, HTTP) invocations to describe a RepairBucket operation
type RepairBucketParams struct {
	DryRun            bool              `json:"dry_run"`
	ViewQueryPageSize *int              `json:"view_query_page_size"`
	RepairedFileTTL   *int              `json:"repair_output_ttl_seconds"`
	RepairJobs        []RepairJobParams `json:"repair_jobs"`
}

// Params suitable for external (eg, HTTP) invocations to describe a specific RepairJob operation
type RepairJobParams struct {
	RepairJobType   RepairJobType          `json:"type"`
	RepairJobParams map[string]interface{} `json:"params"`
}

// Record details about the result of a bucket repair that was made on a doc
type RepairBucketResult struct {
	DryRun              bool            `json:"dry_run"`
	BackupOrDryRunDocId string          `json:"backup_or_dryrun_doc_id"`
	DocId               string          `json:"id"`
	RepairJobTypes      []RepairJobType `json:"repair_job_type"`
}

// Given a Couchbase Bucket doc, transform the doc in some way to produce a new doc.
// Also return a boolean to indicate whether a transformation took place, or any errors occurred.
type DocTransformer func(docId string, originalCBDoc []byte) (transformedCBDoc []byte, transformed bool, err error)

// A RepairBucket struct is the main API entrypoint to call for repairing documents in buckets
type RepairBucket struct {
	DryRun            bool // If true, will only output what changes it *would* have made, but not make any changes
	RepairedFileTTL   time.Duration
	ViewQueryPageSize int
	Bucket            base.Bucket
	RepairJobs        []DocTransformer
}

func NewRepairBucket(bucket base.Bucket) *RepairBucket {
	return &RepairBucket{
		Bucket:            bucket,
		ViewQueryPageSize: base.DefaultViewQueryPageSize,
		RepairedFileTTL:   kDefaultRepairedFileTTL,
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
	if params.ViewQueryPageSize != nil && *params.ViewQueryPageSize > 0 {
		r.ViewQueryPageSize = *params.ViewQueryPageSize
	}

	if params.RepairedFileTTL != nil && *params.RepairedFileTTL >= 0 {
		r.RepairedFileTTL = time.Duration(*params.RepairedFileTTL) * time.Second
	}

	for _, repairJobParams := range params.RepairJobs {
		switch repairJobParams.RepairJobType {
		case RepairRevTreeCycles:
			r.AddRepairJob(RepairJobRevTreeCycles)
		}
	}

	return r
}

/*

This is how the view is iterated:

┌ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─
                ┌ ─ ─ ─│─ ─ ─ ─ ─ ─ ─ ─
│                               ┌ ─ ─ ─│─ ─ ─ ─ ─ ─ ─ ┐
                │      │                ┌ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┐
│┌────┐  ┌────┐  ┌────┐  ┌────┐ │┌────┐│ ┌────┐       │
 │doc1│  │doc2│ ││doc3││ │doc4│  │doc5│ ││doc6│               │
│└────┘  └────┘  └────┘  └────┘ │└────┘│ └────┘       │
                │      │                │                     │
└ ─ ─ ─ ─ ─ ▲ ─ ─ ─ ─ ─         │      │              │
            │   └ ─ ─ ─ ─ ─ ▲ ─ ─ ─ ─ ─ │                     │
            │               │   └ ─ ─ ─ ─ ─▲─ ─ ─ ─ ─ ┘
      StartKey: ""          │           └ ─│─ ─ ─ ─ ─ ─ ─ ─ ─ ┘
        Limit: 3            │              │       ▲
    NumProcessed: 3         │              │       │
                    StartKey: "doc3"       │       │
                        Limit: 3           │       │
                    NumProcessed: 2        │       └────────┐
                                           │                │
                                   StartKey: "doc5"         │
                                       Limit: 3             │
                                   NumProcessed: 1          │
                                                            │
                                                    StartKey: "doc6"
                                                        Limit: 3
                                                    NumProcessed: 0

* It starts with an empty start key
* For the next page, it uses the last key processed as the new start key
* Since the start key is inclusive, it will see the start key twice (on first page, and on next page)
* If it's iterating a result page and sees a doc with the start key (eg, doc3 in above), it will ignore it so it doesn't process it twice
* Stop condition: if NumProcessed is 0, because the only doc in result set had already been processed.
*
*/
func (r RepairBucket) RepairBucket() (results []RepairBucketResult, err error) {

	base.Infof(base.KeyCRUD, "RepairBucket() invoked")
	defer base.Infof(base.KeyCRUD, "RepairBucket() finished")

	startKey := ""
	results = []RepairBucketResult{}
	numDocsProcessed := 0

	for {

		options := Body{"stale": false, "reduce": false}
		options["startkey"] = []interface{}{
			true,
			startKey,
		}
		options["limit"] = r.ViewQueryPageSize

		base.Infof(base.KeyCRUD, "RepairBucket() querying view with options: %+v", options)
		vres, err := r.Bucket.View(DesignDocSyncHousekeeping(), ViewImport, options)
		base.Infof(base.KeyCRUD, "RepairBucket() queried view and got %d results", len(vres.Rows))
		if err != nil {
			// TODO: Maybe we could retry if the view timed out (as seen in #3267)
			return results, err
		}

		// Check
		if len(vres.Rows) == 0 {
			// No more results.  Return
			return results, nil
		}

		// Keep a counter of how many results were processed, since if none were processed it indicates that
		// we hit the last (empty) page of data.  This is needed because the start key is inclusive, and
		// so even on the last page of results, the view query will return a single result with the start key doc.
		numResultsProcessed := 0

		for _, row := range vres.Rows {

			rowKey := row.Key.([]interface{})
			docid := rowKey[1].(string)

			if docid == startKey {
				// Skip this, already processed in previous iteration.  Important to do this before numResultsProcessed
				// is incremented.
				continue
			}

			// The next page for viewquery should start at the last result in this page.  There is a de-duping mechanism
			// to avoid processing this doc twice.
			startKey = docid

			// Increment counter of how many results were processed for detecting stop condition
			numResultsProcessed += 1

			key := realDocID(docid)
			var backupOrDryRunDocId string

			_, err = r.Bucket.Update(key, 0, func(currentValue []byte) ([]byte, *uint32, error) {
				// Be careful: this block can be invoked multiple times if there are races!
				if currentValue == nil {
					return nil, nil, base.ErrUpdateCancel // someone deleted it?!
				}
				updatedDoc, shouldUpdate, repairJobs, err := r.TransformBucketDoc(key, currentValue)
				if err != nil {
					return nil, nil, err
				}

				switch shouldUpdate {
				case true:

					backupOrDryRunDocId, err = r.WriteRepairedDocsToBucket(key, currentValue, updatedDoc)
					if err != nil {
						base.Infof(base.KeyCRUD, "Repair Doc (dry_run=%v) Writing docs to bucket failed with error: %v.  Dumping raw contents.", r.DryRun, err)
						base.Infof(base.KeyCRUD, "Original Doc before repair: %s", base.UD(currentValue))
						base.Infof(base.KeyCRUD, "Updated doc after repair: %s", base.UD(updatedDoc))
					}

					result := RepairBucketResult{
						DryRun:              r.DryRun,
						BackupOrDryRunDocId: backupOrDryRunDocId,
						DocId:               key,
						RepairJobTypes:      repairJobs,
					}

					results = append(results, result)

					if r.DryRun {
						return nil, nil, base.ErrUpdateCancel
					} else {
						return updatedDoc, nil, nil
					}
				default:
					return nil, nil, base.ErrUpdateCancel
				}

			})

			if err != nil {
				// Ignore base.ErrUpdateCancel (Cas.QUIT) errors.  Any other errors should be returned to caller
				if pkgerrors.Cause(err) != base.ErrUpdateCancel {
					return results, err
				}
			}

			if backupOrDryRunDocId != "" {
				if r.DryRun {
					base.Infof(base.KeyCRUD, "Repair Doc: dry run result available in Bucket Doc: %v (auto-deletes in 24 hours)", base.UD(backupOrDryRunDocId))
				} else {
					base.Infof(base.KeyCRUD, "Repair Doc: Doc repaired, original doc backed up in Bucket Doc: %v (auto-deletes in 24 hours)", base.UD(backupOrDryRunDocId))
				}
			}

		}

		numDocsProcessed += numResultsProcessed

		base.Infof(base.KeyCRUD, "RepairBucket() processed %d / %d", numDocsProcessed, vres.TotalRows)

		if numResultsProcessed == 0 {
			// No point in going to the next page, since this page had 0 results.  See method comments.
			return results, nil
		}

	}

	// Should never get here, due to early returns above
	return results, nil

}

func (r RepairBucket) WriteRepairedDocsToBucket(docId string, originalDoc, updatedDoc []byte) (backupOrDryRunDocId string, err error) {

	var contentToSave []byte

	if r.DryRun {
		backupOrDryRunDocId = base.RepairDryRun + docId
		contentToSave = updatedDoc
	} else {
		backupOrDryRunDocId = base.RepairBackup + docId
		contentToSave = originalDoc
	}

	doc, err := unmarshalDocument(docId, contentToSave)
	if err != nil {
		return backupOrDryRunDocId, err
	}

	//If the RepairedFileTTL is explicitly set to 0 then don't write the doc at all
	if int(r.RepairedFileTTL.Seconds()) == 0 {
		base.Infof(base.KeyCRUD, "Repair Doc: Doc %v repaired, TTL set to 0, doc will not be written to bucket", base.UD(backupOrDryRunDocId))
		return backupOrDryRunDocId, nil
	}

	if err := r.Bucket.Set(backupOrDryRunDocId, base.DurationToCbsExpiry(r.RepairedFileTTL), doc); err != nil {
		return backupOrDryRunDocId, err
	}

	return backupOrDryRunDocId, nil

}

// Loops over all repair jobs and applies them
func (r RepairBucket) TransformBucketDoc(docId string, originalCBDoc []byte) (transformedCBDoc []byte, transformed bool, repairJobs []RepairJobType, err error) {

	transformed = false
	for _, repairJob := range r.RepairJobs {

		repairedDoc, repairedDocTxformed, repairDocErr := repairJob(docId, originalCBDoc)
		if repairDocErr != nil {
			return nil, false, repairJobs, repairDocErr
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

		// Hack: since RepairRevTreeCycles is the only type of repair job, hardcode it to this
		// In the future, this will need to be updated so that that RepairJob is based on interfaces instead of functions
		// So that .JobType() can be called on it.  Currently there doesn't seem to be a way to do that.
		repairJobs = append(repairJobs, RepairRevTreeCycles)

	}

	return transformedCBDoc, transformed, repairJobs, nil
}

// Repairs rev tree cycles (see SG issue #2847)
func RepairJobRevTreeCycles(docId string, originalCBDoc []byte) (transformedCBDoc []byte, transformed bool, err error) {

	base.Debugf(base.KeyCRUD, "RepairJobRevTreeCycles() called with doc id: %v", base.UD(docId))
	defer base.Debugf(base.KeyCRUD, "RepairJobRevTreeCycles() finished.  Doc id: %v.  transformed: %v.  err: %v", base.UD(docId), base.UD(transformed), err)

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
	if err := doc.History.RepairCycles(); err != nil {
		return nil, false, err
	}

	transformedCBDoc, errMarshal := json.Marshal(doc)
	if errMarshal != nil {
		return nil, false, errMarshal
	}

	return transformedCBDoc, true, nil

}
