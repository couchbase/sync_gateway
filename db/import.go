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
	"context"
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
func (db *Database) ImportDocRaw(docid string, value []byte, xattrValue []byte, userXattrValue []byte, isDelete bool, cas uint64, expiry *uint32, mode ImportMode) (docOut *Document, err error) {

	var body Body
	if isDelete {
		body = Body{}
	} else {
		err := body.Unmarshal(value)
		if err != nil {
			base.InfofCtx(db.Ctx, base.KeyImport, "Unmarshal error during importDoc %v", err)
			return nil, err
		}

		err = validateImportBody(body)
		if err != nil {
			return nil, err
		}
		delete(body, BodyPurged)
	}

	existingBucketDoc := &sgbucket.BucketDocument{
		Body:      value,
		Xattr:     xattrValue,
		UserXattr: userXattrValue,
		Cas:       cas,
	}

	return db.importDoc(docid, body, expiry, isDelete, existingBucketDoc, mode)
}

// Import a document, given the existing state of the doc in *document format.
func (db *Database) ImportDoc(docid string, existingDoc *Document, isDelete bool, expiry *uint32, mode ImportMode) (docOut *Document, err error) {

	if existingDoc == nil {
		return nil, base.RedactErrorf("No existing doc present when attempting to import %s", base.UD(docid))
	}

	// TODO: We need to remarshal the existing doc into bytes.  Less performance overhead than the previous bucket op to get the value in WriteUpdateWithXattr,
	//       but should refactor import processing to support using the already-unmarshalled doc.
	existingBucketDoc := &sgbucket.BucketDocument{
		Cas:       existingDoc.Cas,
		UserXattr: existingDoc.rawUserXattr,
	}

	// If we marked this as having inline Sync Data ensure that the existingBucketDoc we pass to importDoc has syncData
	// in the body so we can detect this and perform the migrate
	if existingDoc.inlineSyncData {
		existingBucketDoc.Body, err = existingDoc.MarshalJSON()
		existingBucketDoc.Xattr = nil
	} else {
		if existingDoc.Deleted {
			existingBucketDoc.Xattr, err = base.JSONMarshal(existingDoc.SyncData)
		} else {
			existingBucketDoc.Body, existingBucketDoc.Xattr, err = existingDoc.MarshalWithXattr()
		}
	}

	if err != nil {
		return nil, err
	}

	return db.importDoc(docid, existingDoc.Body(), expiry, isDelete, existingBucketDoc, mode)
}

// Import document
//   docid  - document key
//   body - marshalled body of document to be imported
//   isDelete - whether the document to be imported is a delete
//   existingDoc - bytes/cas/expiry of the  document to be imported (including xattr when available)
//   mode - ImportMode - ImportFromFeed or ImportOnDemand
func (db *Database) importDoc(docid string, body Body, expiry *uint32, isDelete bool, existingDoc *sgbucket.BucketDocument, mode ImportMode) (docOut *Document, err error) {

	base.DebugfCtx(db.Ctx, base.KeyImport, "Attempting to import doc %q...", base.UD(docid))
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

	mutationOptions := sgbucket.MutateInOptions{}
	if db.Bucket.IsSupported(sgbucket.DataStoreFeaturePreserveExpiry) {
		mutationOptions.PreserveExpiry = true
	} else {
		// Get the doc expiry if it wasn't passed in and preserve expiry is not supported
		if expiry == nil {
			cbStore, _ := base.AsCouchbaseStore(db.Bucket)
			getExpiry, getExpiryErr := cbStore.GetExpiry(docid)
			if getExpiryErr != nil {
				return nil, getExpiryErr
			}
			expiry = &getExpiry
		}
		existingDoc.Expiry = *expiry
	}

	docOut, _, err = db.updateAndReturnDoc(newDoc.ID, true, existingDoc.Expiry, &mutationOptions, existingDoc, func(doc *Document) (resultDocument *Document, resultAttachmentData AttachmentData, createNewRevIDSkipped bool, updatedExpiry *uint32, resultErr error) {
		// Perform cas mismatch check first, as we want to identify cas mismatch before triggering migrate handling.
		// If there's a cas mismatch, the doc has been updated since the version that triggered the import.  Handling depends on import mode.
		if doc.Cas != existingDoc.Cas {
			// If this is a feed import, cancel on cas failure (doc has been updated )
			if mode == ImportFromFeed {
				return nil, nil, false, nil, base.ErrImportCasFailure
			}

			// If this is an on-demand import, we want to continue to import the current version of the doc.  Re-initialize existing doc based on the latest doc
			if mode == ImportOnDemand {
				body = doc.Body()
				if body == nil {
					return nil, nil, false, nil, base.ErrEmptyDocument
				}

				existingDoc = &sgbucket.BucketDocument{
					Cas: doc.Cas,
				}

				if !mutationOptions.PreserveExpiry {
					// Reload the doc expiry if GoCB is not preserving expiry
					cbStore, _ := base.AsCouchbaseStore(db.Bucket)
					expiry, getExpiryErr := cbStore.GetExpiry(newDoc.ID)
					if getExpiryErr != nil {
						return nil, nil, false, nil, getExpiryErr
					}
					existingDoc.Expiry = expiry
					updatedExpiry = &expiry
				}

				if doc.inlineSyncData {
					existingDoc.Body, err = doc.MarshalBodyAndSync()
				} else {
					existingDoc.Body, err = doc.BodyBytes()
				}

				if err != nil {
					return nil, nil, false, nil, err
				}
			}
		}

		// If the existing doc is a legacy SG write (_sync in body), check for migrate instead of import.
		_, ok := body[base.SyncPropertyName]
		if ok || doc.inlineSyncData {
			migratedDoc, requiresImport, migrateErr := db.migrateMetadata(newDoc.ID, body, existingDoc, &mutationOptions)
			if migrateErr != nil {
				return nil, nil, false, updatedExpiry, migrateErr
			}
			// Migration successful, doesn't require import - return ErrDocumentMigrated to cancel import processing
			if !requiresImport {
				alreadyImportedDoc = migratedDoc
				return nil, nil, false, updatedExpiry, base.ErrDocumentMigrated
			}

			// If document still requires import post-migration attempt, continue with import processing based on the body returned by migrate
			doc = migratedDoc
			body = migratedDoc.Body()
			base.InfofCtx(db.Ctx, base.KeyMigrate, "Falling back to import with cas: %v", doc.Cas)
		}

		// Check if the doc has been deleted
		if doc.Cas == 0 {
			base.DebugfCtx(db.Ctx, base.KeyImport, "Document has been removed from the bucket before it could be imported - cancelling import.")
			return nil, nil, false, updatedExpiry, base.ErrImportCancelled
		}

		// If this is a delete, and there is no xattr on the existing doc,
		// we shouldn't import.  (SG purge arriving over DCP feed)
		if isDelete && doc.CurrentRev == "" {
			base.DebugfCtx(db.Ctx, base.KeyImport, "Import not required for delete mutation with no existing SG xattr (SG purge): %s", base.UD(newDoc.ID))
			return nil, nil, false, updatedExpiry, base.ErrImportCancelled
		}

		// Is this doc an SG Write?
		isSgWrite, crc32Match, bodyChanged := doc.IsSGWrite(db.Ctx, existingDoc.Body)
		if crc32Match {
			db.DbStats.Database().Crc32MatchCount.Add(1)
		}

		// If the current version of the doc is an SG write, document has been updated by SG subsequent to the update that triggered this import.
		// Cancel import
		if isSgWrite {
			base.DebugfCtx(db.Ctx, base.KeyImport, "During import, existing doc (%s) identified as SG write.  Canceling import.", base.UD(docid))
			alreadyImportedDoc = doc
			return nil, nil, false, updatedExpiry, base.ErrAlreadyImported
		}

		// If there's a filter function defined, evaluate to determine whether we should import this doc
		if db.DatabaseContext.Options.ImportOptions.ImportFilter != nil {
			var shouldImport bool
			var importErr error

			if isDelete && body == nil {
				deleteBody := Body{BodyDeleted: true}
				shouldImport, importErr = db.DatabaseContext.Options.ImportOptions.ImportFilter.EvaluateFunction(db.Ctx, deleteBody)
			} else if isDelete && body != nil {
				deleteBody := body.ShallowCopy()
				deleteBody[BodyDeleted] = true
				shouldImport, importErr = db.DatabaseContext.Options.ImportOptions.ImportFilter.EvaluateFunction(db.Ctx, deleteBody)
			} else {
				shouldImport, importErr = db.DatabaseContext.Options.ImportOptions.ImportFilter.EvaluateFunction(db.Ctx, body)
			}

			if importErr != nil {
				base.DebugfCtx(db.Ctx, base.KeyImport, "Error returned for doc %s while evaluating import function - will not be imported.", base.UD(docid))
				return nil, nil, false, updatedExpiry, base.ErrImportCancelledFilter
			}
			if !shouldImport {
				base.DebugfCtx(db.Ctx, base.KeyImport, "Doc %s excluded by document import function - will not be imported.", base.UD(docid))
				// TODO: If this document has a current revision (this is a document that was previously mobile-enabled), do additional opt-out processing
				// pending https://github.com/couchbase/sync_gateway/issues/2750
				return nil, nil, false, updatedExpiry, base.ErrImportCancelledFilter
			}
		}

		var rawBodyForRevID []byte
		var wasStripped bool
		if len(existingDoc.Body) > 0 {
			rawBodyForRevID = existingDoc.Body
		} else {
			var bodyWithoutInternalProps Body
			bodyWithoutInternalProps, wasStripped = stripInternalProperties(body)
			rawBodyForRevID, err = base.JSONMarshalCanonical(bodyWithoutInternalProps)
			if err != nil {
				return nil, nil, false, nil, err
			}
		}

		shouldGenerateNewRev := bodyChanged

		// If the body has changed then the document has been updated and we should generate a new revision. Otherwise
		// the import was triggered by a user xattr mutation and therefore should not generate a new revision.
		if shouldGenerateNewRev {
			// The active rev is the parent for an import
			parentRev := doc.CurrentRev
			generation, _ := ParseRevID(parentRev)
			generation++
			newRev = CreateRevIDWithBytes(generation, parentRev, rawBodyForRevID)
			if err != nil {
				return nil, nil, false, updatedExpiry, err
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
					base.InfofCtx(db.Ctx, base.KeyImport, "Optimistic backup of previous revision failed due to %s", backupErr)
				}
			}
		} else {
			newDoc.RevID = doc.CurrentRev
		}

		// During import, oldDoc (doc.Body) is nil (since it's not guaranteed to be available)
		doc.RemoveBody()

		newDoc.UpdateBody(body)
		if !wasStripped && !isDelete {
			newDoc._rawBody = rawBodyForRevID
		}

		// Existing attachments are preserved while importing an updated body - we don't (currently) support changing
		// attachments through anything but SG. When importing a "delete" mutation, existing attachments are removed
		// to ensure obsolete attachments are removed from the bucket.
		if isDelete {
			doc.SyncData.Attachments = nil
		} else {
			newDoc.DocAttachments = doc.SyncData.Attachments
		}

		return newDoc, nil, !shouldGenerateNewRev, updatedExpiry, nil
	})

	switch err {
	case base.ErrAlreadyImported, base.ErrDocumentMigrated:
		// If the doc was already imported, we want to return the imported version
		docOut = alreadyImportedDoc
	case nil:
		db.DbStats.SharedBucketImport().ImportCount.Add(1)
		db.DbStats.SharedBucketImport().ImportHighSeq.Set(int64(docOut.SyncData.Sequence))
		db.DbStats.SharedBucketImport().ImportProcessingTime.Add(time.Since(importStartTime).Nanoseconds())
		base.DebugfCtx(db.Ctx, base.KeyImport, "Imported %s (delete=%v) as rev %s", base.UD(newDoc.ID), isDelete, newRev)
	case base.ErrImportCancelled:
		// Import was cancelled (SG purge) - don't return error.
	case base.ErrImportCancelledFilter:
		// Import was cancelled based on import filter.  Return error (required for on-demand write import logic), but don't log as error/warning.
		return nil, err
	case base.ErrImportCasFailure:
		// Import was cancelled due to CAS failure.
		db.DbStats.SharedBucketImport().ImportCancelCAS.Add(1)
		return nil, err
	case base.ErrImportCancelledPurged:
		// Import ignored
		return nil, err
	default:
		base.InfofCtx(db.Ctx, base.KeyImport, "Error importing doc %q: %v", base.UD(newDoc.ID), err)
		db.DbStats.SharedBucketImport().ImportErrorCount.Add(1)
		return nil, err

	}

	return docOut, nil
}

// Migrates document metadata from document body to system xattr.  On CAS failure, retrieves current doc body and retries
// migration if _sync property exists.  If _sync property is not found, returns doc and sets requiresImport to true
func (db *Database) migrateMetadata(docid string, body Body, existingDoc *sgbucket.BucketDocument, opts *sgbucket.MutateInOptions) (docOut *Document, requiresImport bool, err error) {

	// Unmarshal the existing doc in legacy SG format
	doc, unmarshalErr := unmarshalDocument(docid, existingDoc.Body)
	if unmarshalErr != nil {
		return nil, false, unmarshalErr
	}
	doc.Cas = existingDoc.Cas

	// If no sync metadata is present, return for import handling
	if !doc.HasValidSyncData() {
		base.InfofCtx(db.Ctx, base.KeyMigrate, "During migrate, doc %q doesn't have valid sync data.  Falling back to import handling.  (cas=%d)", base.UD(docid), doc.Cas)
		return doc, true, nil
	}

	// Move any large revision bodies to external storage
	err = doc.migrateRevisionBodies(db.Bucket)
	if err != nil {
		base.InfofCtx(db.Ctx, base.KeyMigrate, "Error migrating revision bodies to external storage, doc %q, (cas=%d), Error: %v", base.UD(docid), doc.Cas, err)
	}

	// Persist the document in xattr format
	value, xattrValue, marshalErr := doc.MarshalWithXattr()
	if marshalErr != nil {
		return nil, false, marshalErr
	}

	// Use WriteWithXattr to handle both normal migration and tombstone migration (xattr creation, body delete)
	isDelete := doc.hasFlag(channels.Deleted)
	deleteBody := isDelete && len(existingDoc.Body) > 0
	casOut, writeErr := db.Bucket.WriteWithXattr(docid, base.SyncXattrName, existingDoc.Expiry, existingDoc.Cas, opts, value, xattrValue, isDelete, deleteBody)
	if writeErr == nil {
		doc.Cas = casOut
		base.InfofCtx(db.Ctx, base.KeyMigrate, "Successfully migrated doc %q", base.UD(docid))
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

	previousRev, ok := db.revisionCache.Peek(db.Ctx, docid, revid)
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

// ////// Import Filter Function

// A compiled JavaScript event function.
type jsImportFilterRunner struct {
	sgbucket.JSRunner
	response bool
}

// Compiles a JavaScript event function to a jsImportFilterRunner object.
func newImportFilterRunner(funcSource string) (sgbucket.JSServerTask, error) {
	importFilterRunner := &jsEventTask{}
	err := importFilterRunner.InitWithLogging(funcSource,
		func(s string) {
			base.ErrorfCtx(context.Background(), base.KeyJavascript.String()+": Import %s", base.UD(s))
		},
		func(s string) { base.InfofCtx(context.Background(), base.KeyJavascript, "Import %s", base.UD(s)) })
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

	base.DebugfCtx(context.Background(), base.KeyImport, "Creating new ImportFilterFunction")
	return &ImportFilterFunction{
		JSServer: sgbucket.NewJSServer(fnSource, kTaskCacheSize,
			func(fnSource string) (sgbucket.JSServerTask, error) {
				return newImportFilterRunner(fnSource)
			}),
	}
}

// Calls a jsEventFunction returning an interface{}
func (i *ImportFilterFunction) EvaluateFunction(ctx context.Context, doc Body) (bool, error) {

	result, err := i.Call(doc)
	if err != nil {
		base.WarnfCtx(ctx, "Unexpected error invoking import filter for document %s - processing aborted, document will not be imported.  Error: %v", base.UD(doc), err)
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
		base.WarnfCtx(ctx, "Import filter function returned non-boolean result %v Type: %T", result, result)
		return false, errors.New("Import filter function returned non-boolean value.")
	}
}
