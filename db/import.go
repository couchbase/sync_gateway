package db

import (
	"errors"
	"fmt"
	"strconv"
	"time"

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
func (db *Database) ImportDocRaw(docid string, value []byte, xattrValue []byte, isDelete bool, cas uint64, expiry *uint32, mode ImportMode) (docOut *Document, err error) {

	var body Body
	if isDelete {
		body = Body{}
	} else {
		err := body.Unmarshal(value)
		if err != nil {
			base.Infof(base.KeyImport, "Unmarshal error during importDoc %v", err)
			return nil, err
		}
		if body == nil {
			return nil, base.ErrEmptyDocument
		}
	}

	if isPurged, ok := body[BodyPurged].(bool); ok && isPurged {
		return nil, base.ErrImportCancelledPurged
	}

	// Get the doc expiry if it wasn't passed in
	if expiry == nil {
		gocbBucket, _ := base.AsGoCBBucket(db.Bucket)
		getExpiry, getExpiryErr := gocbBucket.GetExpiry(docid)
		if getExpiryErr != nil {
			return nil, getExpiryErr
		}
		expiry = &getExpiry
	}

	existingBucketDoc := &sgbucket.BucketDocument{
		Body:   value,
		Xattr:  xattrValue,
		Cas:    cas,
		Expiry: *expiry,
	}
	return db.importDoc(docid, body, isDelete, existingBucketDoc, mode)
}

// Import a document, given the existing state of the doc in *document format.
func (db *Database) ImportDoc(docid string, existingDoc *Document, isDelete bool, expiry *uint32, mode ImportMode) (docOut *Document, err error) {

	if existingDoc == nil {
		return nil, base.RedactErrorf("No existing doc present when attempting to import %s", base.UD(docid))
	}

	// Get the doc expiry if it wasn't passed in
	if expiry == nil {
		gocbBucket, _ := base.AsGoCBBucket(db.Bucket)
		getExpiry, getExpiryErr := gocbBucket.GetExpiry(docid)
		if getExpiryErr != nil {
			return nil, getExpiryErr
		}
		expiry = &getExpiry
	}

	// TODO: We need to remarshal the existing doc into bytes.  Less performance overhead than the previous bucket op to get the value in WriteUpdateWithXattr,
	//       but should refactor import processing to support using the already-unmarshalled doc.
	existingBucketDoc := &sgbucket.BucketDocument{
		Cas:    existingDoc.Cas,
		Expiry: *expiry,
	}

	// If we marked this as having inline Sync Data ensure that the existingBucketDoc we pass to importDoc has syncData
	// in the body so we can detect this and perform the migrate
	if existingDoc.inlineSyncData {
		existingBucketDoc.Body, err = existingDoc.MarshalJSON()
		existingBucketDoc.Xattr = nil
	} else {
		existingBucketDoc.Body, existingBucketDoc.Xattr, err = existingDoc.MarshalWithXattr()
	}

	if err != nil {
		return nil, err
	}

	return db.importDoc(docid, existingDoc.Body(), isDelete, existingBucketDoc, mode)
}

// Import document
//   docid  - document key
//   body - marshalled body of document to be imported
//   isDelete - whether the document to be imported is a delete
//   existingDoc - bytes/cas/expiry of the  document to be imported (including xattr when available)
//   mode - ImportMode - ImportFromFeed or ImportOnDemand
func (db *Database) importDoc(docid string, body Body, isDelete bool, existingDoc *sgbucket.BucketDocument, mode ImportMode) (docOut *Document, err error) {

	base.Debugf(base.KeyImport, "Attempting to import doc %q...", base.UD(docid))
	importStartTime := time.Now()

	if existingDoc == nil {
		return nil, base.RedactErrorf("No existing doc present when attempting to import %s", base.UD(docid))
	} else if body == nil {
		if !isDelete {
			// only deletes can have an empty (null) body and be imported
			return nil, base.ErrEmptyDocument
		}
		body = Body{}
	}

	newDoc := &Document{
		ID:      docid,
		Deleted: isDelete,
	}

	var newRev string
	var alreadyImportedDoc *Document
	docOut, _, err = db.updateAndReturnDoc(newDoc.ID, true, existingDoc.Expiry, existingDoc, func(doc *Document) (resultDocument *Document, resultAttachmentData AttachmentData, updatedExpiry *uint32, resultErr error) {

		// Perform cas mismatch check first, as we want to identify cas mismatch before triggering migrate handling.
		// If there's a cas mismatch, the doc has been updated since the version that triggered the import.  Handling depends on import mode.
		if doc.Cas != existingDoc.Cas {
			// If this is a feed import, cancel on cas failure (doc has been updated )
			if mode == ImportFromFeed {
				return nil, nil, nil, base.ErrImportCasFailure
			}

			// If this is an on-demand import, we want to continue to import the current version of the doc.  Re-initialize existing doc based on the latest doc
			if mode == ImportOnDemand {
				body = doc.Body()
				if body == nil {
					return nil, nil, nil, base.ErrEmptyDocument
				}

				// Reload the doc expiry
				gocbBucket, _ := base.AsGoCBBucket(db.Bucket)
				expiry, getExpiryErr := gocbBucket.GetExpiry(newDoc.ID)
				if getExpiryErr != nil {
					return nil, nil, nil, getExpiryErr
				}

				existingDoc = &sgbucket.BucketDocument{
					Cas:    doc.Cas,
					Expiry: expiry,
				}

				if doc.inlineSyncData {
					existingDoc.Body, err = doc.MarshalBodyAndSync()
				} else {
					existingDoc.Body, err = doc.BodyBytes()
				}

				if err != nil {
					return nil, nil, nil, err
				}

				updatedExpiry = &expiry
			}
		}

		// If the existing doc is a legacy SG write (_sync in body), check for migrate instead of import.
		_, ok := body[base.SyncPropertyName]
		if ok || doc.inlineSyncData {
			migratedDoc, requiresImport, migrateErr := db.migrateMetadata(newDoc.ID, body, existingDoc)
			if migrateErr != nil {
				return nil, nil, updatedExpiry, migrateErr
			}
			// Migration successful, doesn't require import - return ErrDocumentMigrated to cancel import processing
			if !requiresImport {
				alreadyImportedDoc = migratedDoc
				return nil, nil, updatedExpiry, base.ErrDocumentMigrated
			}

			// If document still requires import post-migration attempt, continue with import processing based on the body returned by migrate
			doc = migratedDoc
			body = migratedDoc.Body()
			base.Infof(base.KeyMigrate, "Falling back to import with cas: %v", doc.Cas)
		}

		// Check if the doc has been deleted
		if doc.Cas == 0 {
			base.Debugf(base.KeyImport, "Document has been removed from the bucket before it could be imported - cancelling import.")
			return nil, nil, updatedExpiry, base.ErrImportCancelled
		}

		// If this is a delete, and there is no xattr on the existing doc,
		// we shouldn't import.  (SG purge arriving over DCP feed)
		if isDelete && doc.CurrentRev == "" {
			base.Debugf(base.KeyImport, "Import not required for delete mutation with no existing SG xattr (SG purge): %s", base.UD(newDoc.ID))
			return nil, nil, updatedExpiry, base.ErrImportCancelled
		}

		// Is this doc an SG Write?
		isSgWrite, crc32Match := doc.IsSGWrite(existingDoc.Body)
		if crc32Match {
			db.DbStats.StatsDatabase().Add(base.StatKeyCrc32cMatchCount, 1)
		}

		// If the current version of the doc is an SG write, document has been updated by SG subsequent to the update that triggered this import.
		// Cancel import
		if isSgWrite {
			base.Debugf(base.KeyImport, "During import, existing doc (%s) identified as SG write.  Canceling import.", base.UD(docid))
			alreadyImportedDoc = doc
			return nil, nil, updatedExpiry, base.ErrAlreadyImported
		}

		// If there's a filter function defined, evaluate to determine whether we should import this doc
		if db.DatabaseContext.Options.ImportOptions.ImportFilter != nil {
			var shouldImport bool
			var importErr error

			if isDelete && body == nil {
				deleteBody := Body{BodyDeleted: true}
				shouldImport, importErr = db.DatabaseContext.Options.ImportOptions.ImportFilter.EvaluateFunction(deleteBody)
			} else if isDelete && body != nil {
				deleteBody := body.ShallowCopy()
				deleteBody[BodyDeleted] = true
				shouldImport, importErr = db.DatabaseContext.Options.ImportOptions.ImportFilter.EvaluateFunction(deleteBody)
			} else {
				shouldImport, importErr = db.DatabaseContext.Options.ImportOptions.ImportFilter.EvaluateFunction(body)
			}

			if importErr != nil {
				base.Debugf(base.KeyImport, "Error returned for doc %s while evaluating import function - will not be imported.", base.UD(docid))
				return nil, nil, updatedExpiry, base.ErrImportCancelledFilter
			}
			if !shouldImport {
				base.Debugf(base.KeyImport, "Doc %s excluded by document import function - will not be imported.", base.UD(docid))
				// TODO: If this document has a current revision (this is a document that was previously mobile-enabled), do additional opt-out processing
				// pending https://github.com/couchbase/sync_gateway/issues/2750
				return nil, nil, updatedExpiry, base.ErrImportCancelledFilter
			}
		}

		// The active rev is the parent for an import
		parentRev := doc.CurrentRev
		generation, _ := ParseRevID(parentRev)
		generation++
		var rawBodyForRevID []byte
		var wasStripped bool
		if len(existingDoc.Body) > 0 {
			rawBodyForRevID = existingDoc.Body
		} else {
			var bodyWithoutSpecialProps Body
			bodyWithoutSpecialProps, wasStripped = stripSpecialProperties(body)
			rawBodyForRevID, err = base.JSONMarshalCanonical(bodyWithoutSpecialProps)
			if err != nil {
				return nil, nil, nil, err
			}
		}

		newRev = CreateRevIDWithBytes(generation, parentRev, rawBodyForRevID)
		if err != nil {
			return nil, nil, updatedExpiry, err
		}
		base.DebugfCtx(db.Ctx, base.KeyImport, "Created new rev ID for doc %q / %q", base.UD(newDoc.ID), newRev)
		// body[BodyRev] = newRev
		newDoc.RevID = newRev
		err := doc.History.addRevision(newDoc.ID, RevInfo{ID: newRev, Parent: parentRev, Deleted: isDelete})
		if err != nil {
			base.InfofCtx(db.Ctx, base.KeyImport, "Error adding new rev ID for doc %q / %q, Error: %v", base.UD(newDoc.ID), newRev, err)
		}

		// If the previous revision body is available in the rev cache,
		// make a temporary copy in the bucket for other nodes/clusters
		if db.DatabaseContext.Options.ImportOptions.BackupOldRev && doc.CurrentRev != "" {
			backupErr := db.backupPreImportRevision(newDoc.ID, doc.CurrentRev)
			if backupErr != nil {
				base.Infof(base.KeyImport, "Optimistic backup of previous revision failed due to %s", backupErr)
			}
		}

		// During import, oldDoc (doc.Body) is nil (since it's not guaranteed to be available)
		doc.RemoveBody()

		newDoc.UpdateBody(body)
		if !wasStripped && !isDelete {
			newDoc._rawBody = rawBodyForRevID
		}

		// Note - no attachments processing is done during ImportDoc.  We don't (currently) support writing attachments through anything but SG.

		return newDoc, nil, updatedExpiry, nil
	})

	switch err {
	case base.ErrAlreadyImported, base.ErrDocumentMigrated:
		// If the doc was already imported, we want to return the imported version
		docOut = alreadyImportedDoc
	case nil:
		db.DbStats.NewStats.SharedBucketImport().ImportCount.Add(1)
		db.DbStats.NewStats.SharedBucketImport().ImportHighSeq.Set(int64(docOut.SyncData.Sequence))
		db.DbStats.NewStats.SharedBucketImport().ImportProcessingTime.Add(time.Since(importStartTime).Nanoseconds())
		base.Debugf(base.KeyImport, "Imported %s (delete=%v) as rev %s", base.UD(newDoc.ID), isDelete, newRev)
	case base.ErrImportCancelled:
		// Import was cancelled (SG purge) - don't return error.
	case base.ErrImportCancelledFilter:
		// Import was cancelled based on import filter.  Return error (required for on-demand write import logic), but don't log as error/warning.
		return nil, err
	case base.ErrImportCasFailure:
		// Import was cancelled due to CAS failure.
		db.DbStats.NewStats.SharedBucketImport().ImportCancelCAS.Add(1)
		return nil, err
	case base.ErrImportCancelledPurged:
		// Import ignored
		return nil, err
	default:
		base.Infof(base.KeyImport, "Error importing doc %q: %v", base.UD(newDoc.ID), err)
		db.DbStats.NewStats.SharedBucketImport().ImportErrorCount.Add(1)
		return nil, err

	}

	return docOut, nil
}

// Migrates document metadata from document body to system xattr.  On CAS failure, retrieves current doc body and retries
// migration if _sync property exists.  If _sync property is not found, returns doc and sets requiresImport to true
func (db *Database) migrateMetadata(docid string, body Body, existingDoc *sgbucket.BucketDocument) (docOut *Document, requiresImport bool, err error) {

	// Unmarshal the existing doc in legacy SG format
	doc, unmarshalErr := unmarshalDocument(docid, existingDoc.Body)
	if unmarshalErr != nil {
		return nil, false, unmarshalErr
	}
	doc.Cas = existingDoc.Cas

	// If no sync metadata is present, return for import handling
	if !doc.HasValidSyncData() {
		base.Infof(base.KeyMigrate, "During migrate, doc %q doesn't have valid sync data.  Falling back to import handling.  (cas=%d)", base.UD(docid), doc.Cas)
		return doc, true, nil
	}

	// Move any large revision bodies to external storage
	err = doc.migrateRevisionBodies(db.Bucket)
	if err != nil {
		base.Infof(base.KeyMigrate, "Error migrating revision bodies to external storage, doc %q, (cas=%d), Error: %v", base.UD(docid), doc.Cas, err)
	}

	// Persist the document in xattr format
	value, xattrValue, marshalErr := doc.MarshalWithXattr()
	if marshalErr != nil {
		return nil, false, marshalErr
	}

	// Use WriteWithXattr to handle both normal migration and tombstone migration (xattr creation, body delete)
	isDelete := doc.hasFlag(channels.Deleted)
	deleteBody := isDelete && len(existingDoc.Body) > 0
	casOut, writeErr := db.Bucket.WriteWithXattr(docid, base.SyncXattrName, existingDoc.Expiry, existingDoc.Cas, value, xattrValue, isDelete, deleteBody)
	if writeErr == nil {
		doc.Cas = casOut
		base.Infof(base.KeyMigrate, "Successfully migrated doc %q", base.UD(docid))
		return doc, false, nil
	}

	// If it was a cas mismatch, propagate an error as far up the stack as possible to force a full refresh + retry
	if base.IsCasMismatch(writeErr) {
		return nil, false, base.ErrCasFailureShouldRetry
	}

	// On any other error, return it as-is since it shouldn't necessarily retry in this case
	return nil, false, writeErr

}

// backupPreImportRev attempts to make a temporary backup of a revision body if the
// revision is currently resident in the revision cache.  This is the import parallel for
// the temporary revision bodies made during SG writes.  Allows in-flight replications on
// other Sync Gateway nodes to serve the previous revision
// (https://github.com/couchbase/sync_gateway/issues/3740)
func (db *Database) backupPreImportRevision(docid, revid string) error {

	// If Delta Sync is enabled, this will already be handled by the backup handling used for delta generation
	if db.DeltaSyncEnabled() {
		return nil
	}

	previousRev, ok := db.revisionCache.Peek(docid, revid)
	if !ok {
		return nil
	}

	var kvPairs []base.KVPair
	if len(previousRev.Attachments) > 0 {
		kvPairs = append(kvPairs, base.KVPair{Key: BodyAttachments, Val: previousRev.Attachments})
	}

	if previousRev.Deleted {
		kvPairs = append(kvPairs, base.KVPair{Key: BodyDeleted, Val: true})
	}

	// Stamp _attachments and _deleted into backup
	oldRevJSON, err := base.InjectJSONProperties(previousRev.BodyBytes, kvPairs...)
	if err != nil {
		return err
	}

	setOldRevErr := db.setOldRevisionJSON(docid, revid, oldRevJSON, db.Options.OldRevExpirySeconds)
	if setOldRevErr != nil {
		return fmt.Errorf("Persistence error: %v", setOldRevErr)
	}

	return nil
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
	err := importFilterRunner.InitWithLogging(funcSource,
		func(s string) { base.Errorf(base.KeyJavascript.String()+": Import %s", base.UD(s)) },
		func(s string) { base.Infof(base.KeyJavascript, "Import %s", base.UD(s)) })
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

	base.Debugf(base.KeyImport, "Creating new ImportFilterFunction")
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
		base.Warnf("Unexpected error invoking import filter for document %s - processing aborted, document will not be imported.  Error: %v", base.UD(doc), err)
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
		base.Warnf("Import filter function returned non-boolean result %v Type: %T", result, result)
		return false, errors.New("Import filter function returned non-boolean value.")
	}
}
