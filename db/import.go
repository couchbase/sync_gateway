package db

import (
	"errors"
	"strconv"

	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/base"
	"github.com/robertkrimen/otto"
)

type ImportMode uint8

const (
	ImportFromFeed = ImportMode(iota) // Feed-based import.  Attempt to import once - cancels import on cas write failure of the imported doc.
	ImportOnDemand                    // On-demand import. Reattempt import on cas write failure of the imported doc until either the import succeeds, or existing doc is an SG write.
)

// Imports a document that was written by someone other than sync gateway.
func (db *Database) ImportDocRaw(docid string, value []byte, isDelete bool, cas uint64, mode ImportMode) (docOut *document, err error) {

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

	return db.ImportDoc(docid, body, isDelete, cas, mode)
}

func (db *Database) ImportDoc(docid string, body Body, isDelete bool, importCas uint64, mode ImportMode) (docOut *document, err error) {

	base.LogTo("Import+", "Attempting to import doc %q...", docid)

	var newRev string
	var alreadyImportedDoc *document
	docOut, _, err = db.updateAndReturnDoc(docid, true, 0, func(doc *document) (Body, AttachmentData, error) {

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
		// Cancel update
		if doc.IsSGWrite() {
			base.LogTo("Import+", "During import, existing doc (%s) identified as SG write.  Canceling import.", docid)
			alreadyImportedDoc = doc
			return nil, nil, base.ErrAlreadyImported
		}

		// If there's a cas mismatch, the doc has been updated since the version that triggered the import.  This is an SDK write (since we checked
		// for SG write above).  How to handle depends on import mode.
		if doc.Cas != importCas {
			// If this is a feed import, cancel on cas failure (doc has been updated )
			if mode == ImportFromFeed {
				return nil, nil, base.ErrImportCasFailure
			}
			// If this is an on-demand import, we want to switch to importing the current version doc
			if mode == ImportOnDemand {
				body = doc.body
			}
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
		doc.body = nil

		// Note - no attachments processing is done during ImportDoc.  We don't (currently) support writing attachments through anything but SG.

		return body, nil, nil
	})

	switch err {
	case base.ErrAlreadyImported:
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
