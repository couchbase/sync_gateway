package db

import (
	"errors"
	"fmt"
	"strconv"

	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"
	"github.com/robertkrimen/otto"
)

type ImportMode uint8

const (
	ImportFromFeed = ImportMode(iota) // Feed-based import.  Attempt to import once - cancels import on cas write failure of the imported doc.
	ImportOnDemand                    // On-demand import. Reattempt import on cas write failure of the imported doc until either the import succeeds, or existing doc is an SG write.
)

// Imports a document that was written by someone other than sync gateway, given the existing state of the doc in raw bytes
func (db *Database) ImportDocRaw(docid string, value []byte, xattrValue []byte, isDelete bool, cas uint64, expiry uint32, mode ImportMode) (docOut *document, err error) {

	var body Body
	if isDelete {
		body = Body{"_deleted": true}
	} else {
		err := body.Unmarshal(value)
		if err != nil {
			base.LogTo("Import", "Unmarshal error during importDoc %v", err)
			return nil, err
		}
	}

	existingBucketDoc := &sgbucket.BucketDocument{
		Body:   value,
		Xattr:  xattrValue,
		Cas:    cas,
		Expiry: expiry,
	}
	return db.importDoc(docid, body, isDelete, existingBucketDoc, mode)
}

// Import a document, given the existing state of the doc in *document format.
func (db *Database) ImportDoc(docid string, existingDoc *document, isDelete bool, mode ImportMode) (docOut *document, err error) {

	if existingDoc == nil {
		return nil, fmt.Errorf("No existing doc present when attempting to import %s", docid)
	}
	// TODO: We need to remarshal the existing doc into bytes.  Less performance overhead than the previous bucket op to get the value in WriteUpdateWithXattr,
	//       but should refactor import processing to support using the already-unmarshalled doc.
	rawValue, rawXattr, err := existingDoc.MarshalWithXattr()
	if err != nil {
		return nil, err
	}
	existingBucketDoc := &sgbucket.BucketDocument{
		Body:  rawValue,
		Xattr: rawXattr,
		Cas:   existingDoc.Cas,
	}

	return db.importDoc(docid, existingDoc.Body(), isDelete, existingBucketDoc, mode)
}

func (db *Database) importDoc(docid string, body Body, isDelete bool, existingDoc *sgbucket.BucketDocument, mode ImportMode) (docOut *document, err error) {

	base.LogTo("Import+", "Attempting to import doc %q...", docid)

	if existingDoc == nil {
		return nil, fmt.Errorf("No existing doc present when attempting to import %s", docid)
	}

	var newRev string
	var alreadyImportedDoc *document
	// TODO: what happens on cas retry?  It will need to reload the expiry.
	docOut, _, err = db.updateAndReturnDoc(docid, true, existingDoc.Expiry, existingDoc, func(doc *document) (Body, AttachmentData, error) {

		// Perform cas mismatch check first, as we want to identify cas mismatch before triggering migrate handling.
		// If there's a cas mismatch, the doc has been updated since the version that triggered the import.  Handling depends on import mode.
		if doc.Cas != existingDoc.Cas {
			// If this is a feed import, cancel on cas failure (doc has been updated )
			if mode == ImportFromFeed {
				return nil, nil, base.ErrImportCasFailure
			}
			// If this is an on-demand import, we want to continue to import the current version of the doc.  Re-initialize existing doc based on the latest doc
			if mode == ImportOnDemand {
				body = doc.Body()
				existingDoc = &sgbucket.BucketDocument{
					Cas: doc.Cas,
				}
			}
		}

		// If the existing doc is a legacy SG write (_sync in body), check for migrate instead of import.
		_, ok := body["_sync"]
		if ok {
			migratedDoc, requiresImport, migrateErr := db.migrateMetadata(docid, body, existingDoc)
			if migrateErr != nil {
				return nil, nil, migrateErr
			}
			// Migration successful, doesn't require import - return ErrDocumentMigrated to cancel import processing
			if !requiresImport {
				alreadyImportedDoc = migratedDoc
				return nil, nil, base.ErrDocumentMigrated
			}
			// If document still requires import post-migration attempt, continue with import processing based on the body returned by migrate
			doc = migratedDoc
			body = migratedDoc.Body()
			base.LogTo("Migrate", "Falling back to import with cas: %v", doc.Cas)
		}

		// Check if the doc has been deleted
		if doc.Cas == 0 {
			base.LogTo("Import+", "Document has been removed from the bucket before it could be imported - cancelling import.")
			return nil, nil, base.ErrImportCancelled
		}

		// If this is a delete, and there is no xattr on the existing doc,
		// we shouldn't import.  (SG purge arriving over DCP feed)
		if isDelete && doc.CurrentRev == "" {
			base.LogTo("Import+", "Import not required for delete mutation with no existing SG xattr (SG purge): %s", docid)
			return nil, nil, base.ErrImportCancelled
		}

		// If the current version of the doc is an SG write, document has been updated by SG subsequent to the update that triggered this import.
		// Cancel import
		if doc.IsSGWrite() {
			base.LogTo("Import+", "During import, existing doc (%s) identified as SG write.  Canceling import.", docid)
			alreadyImportedDoc = doc
			return nil, nil, base.ErrAlreadyImported
		}

		// If there's a filter function defined, evaluate to determine whether we should import this doc
		if db.DatabaseContext.Options.ImportOptions.ImportFilter != nil {
			shouldImport, err := db.DatabaseContext.Options.ImportOptions.ImportFilter.EvaluateFunction(body)
			if err != nil {
				base.LogTo("Import+", "Error returned for doc %s while evaluating import function - will not be imported.", docid)
				return nil, nil, base.ErrImportCancelledFilter
			}
			if shouldImport == false {
				base.LogTo("Import+", "Doc %s excluded by document import function - will not be imported.", docid)
				// TODO: If this document has a current revision (this is a document that was previously mobile-enabled), do additional opt-out processing
				// pending https://github.com/couchbase/sync_gateway/issues/2750
				return nil, nil, base.ErrImportCancelledFilter
			}
		}

		// The active rev is the parent for an import
		parentRev := doc.CurrentRev
		generation, _ := ParseRevID(parentRev)
		generation++
		newRev = createRevID(generation, parentRev, body)
		base.LogTo("Import", "Created new rev ID %v", newRev)
		body["_rev"] = newRev
		doc.History.addRevision(docid, RevInfo{ID: newRev, Parent: parentRev, Deleted: isDelete})

		// During import, oldDoc (doc.Body) is nil (since it's no longer available)
		doc.RemoveBody()

		// Note - no attachments processing is done during ImportDoc.  We don't (currently) support writing attachments through anything but SG.

		return body, nil, nil
	})

	switch err {
	case base.ErrAlreadyImported, base.ErrDocumentMigrated:
		// If the doc was already imported, we want to return the imported version
		docOut = alreadyImportedDoc
	case nil:
		base.LogTo("Import+", "Imported %s (delete=%v) as rev %s", docid, isDelete, newRev)
	case base.ErrImportCancelled:
		// Import was cancelled (SG purge) - don't return error.
	case base.ErrImportCancelledFilter:
		// Import was cancelled based on import filter.  Return error but don't log as error/warning.
		return nil, err
	case base.ErrImportCasFailure:
		// Import was cancelled due to CAS failure.
		return nil, err
	default:
		base.LogTo("Import", "Error importing doc %q: %v", docid, err)
		return nil, err

	}

	return docOut, nil
}

// Migrates document metadata from document body to system xattr.  On CAS failure, retrieves current doc body and retries
// migration if _sync property exists.  If _sync property is not found, returns doc and sets requiresImport to true
func (db *Database) migrateMetadata(docid string, body Body, existingDoc *sgbucket.BucketDocument) (docOut *document, requiresImport bool, err error) {

	// TODO: deal with reloading scenarios
	expiry := existingDoc.Expiry

	for {
		// Reload existing doc, if not present
		if len(existingDoc.Body) == 0 {
			cas, getErr := db.Bucket.GetWithXattr(docid, KSyncXattrName, &existingDoc.Body, &existingDoc.Xattr)
			base.LogTo("Migrate", "Reload in migrate got cas: %d", cas)
			if getErr != nil {
				return nil, false, getErr
			}

			// If an xattr exists on the doc, someone has already migrated.  Cancel and return for potential import checking.
			if len(existingDoc.Xattr) > 0 {
				updatedDoc, unmarshalErr := unmarshalDocumentWithXattr(docid, existingDoc.Body, existingDoc.Xattr, cas, DocUnmarshalAll)
				if unmarshalErr != nil {
					return nil, false, unmarshalErr
				}
				base.LogTo("Migrate", "Returning updated doc because xattr exists (cas=%d): %s", updatedDoc.Cas, existingDoc.Xattr)
				return updatedDoc, true, nil
			}
			existingDoc.Cas = cas
		}

		// Unmarshal the existing doc in legacy SG format
		doc, unmarshalErr := unmarshalDocument(docid, existingDoc.Body)
		if err != nil {
			return nil, false, unmarshalErr
		}
		doc.Cas = existingDoc.Cas

		// If no sync metadata is present, return for import handling
		if !doc.HasValidSyncData(false) {
			base.LogTo("Migrate", "During migrate, doc %q doesn't have valid sync data.  Falling back to import handling.  (cas=%d)", docid, doc.Cas)
			return doc, true, nil
		}

		// Move any large revision bodies to external storage
		doc.migrateRevisionBodies(db.Bucket)

		// Persist the document in xattr format
		value, xattrValue, marshalErr := doc.MarshalWithXattr()
		if marshalErr != nil {
			return nil, false, marshalErr
		}

		// TODO: Could refactor migrateMetadata to use WriteUpdateWithXattr for both CAS retry and general write handling, and avoid cast to CouchbaseBucketGoCB
		gocbBucket, ok := db.Bucket.(*base.CouchbaseBucketGoCB)
		if !ok {
			return nil, false, fmt.Errorf("Metadata migration requires gocb bucket (%T)", db.Bucket)
		}

		// Use WriteWithXattr to handle both normal migration and tombstone migration (xattr creation, body delete)
		isDelete := doc.hasFlag(channels.Deleted)
		deleteBody := isDelete && len(existingDoc.Body) > 0
		casOut, writeErr := gocbBucket.WriteWithXattr(docid, KSyncXattrName, expiry, existingDoc.Cas, value, xattrValue, isDelete, deleteBody)
		if writeErr == nil {
			doc.Cas = casOut
			base.LogTo("Migrate", "Successfully migrated doc %q", docid)
			return doc, false, nil
		}

		// On any error other than cas mismatch, return error
		if !base.IsCasMismatch(db.Bucket, writeErr) {
			return nil, false, writeErr
		}

		base.LogTo("Migrate", "CAS mismatch, retrying")
		// On cas mismatch, reset existingDoc.Body to trigger reload
		existingDoc = &sgbucket.BucketDocument{}
	}
}

//////// Import Filter Function

// A compiled JavaScript event function.
type jsImportFilterRunner struct {
	sgbucket.JSRunner
	response bool
}

// Compiles a JavaScript event function to a jsImportFilterRunner object.
func newImportFilterRunner(funcSource string) (sgbucket.JSServerTask, error) {
	importFilterRunner := &jsEventTask{}
	err := importFilterRunner.Init(funcSource)
	if err != nil {
		return nil, err
	}

	importFilterRunner.After = func(result otto.Value, err error) (interface{}, error) {
		nativeValue, _ := result.Export()
		return nativeValue, err
	}

	return importFilterRunner, nil
}

type ImportFilterFunction struct {
	*sgbucket.JSServer
}

func NewImportFilterFunction(fnSource string) *ImportFilterFunction {

	base.LogTo("Import+", "Creating new ImportFilterFunction")
	return &ImportFilterFunction{
		JSServer: sgbucket.NewJSServer(fnSource, kTaskCacheSize,
			func(fnSource string) (sgbucket.JSServerTask, error) {
				return newImportFilterRunner(fnSource)
			}),
	}
}

// Calls a jsEventFunction returning an interface{}
func (i *ImportFilterFunction) EvaluateFunction(doc Body) (bool, error) {

	result, err := i.Call(doc)
	if err != nil {
		base.Warn("Unexpected error invoking import filter for document %s - processing aborted, document will not be imported.  Error: %v", err)
		return false, err
	}
	switch result := result.(type) {
	case bool:
		return result, nil
	case string:
		boolResult, err := strconv.ParseBool(result)
		if err != nil {
			return false, err
		}
		return boolResult, nil
	default:
		base.Warn("Import filter function returned non-boolean result %v Type: %T", result, result)
		return false, errors.New("Import filter function returned non-boolean value.")
	}
}
