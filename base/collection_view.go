package base

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/couchbase/gocb"
	sgbucket "github.com/couchbase/sg-bucket"
	pkgerrors "github.com/pkg/errors"
)

// View-related functionality for collections.  View operations are currently only supported
// by Couchbase Server at the bucket or default collection level, so all view operations here
// target the parent bucket for the collection.

// Metadata is returned as rawBytes when using ViewResultRaw.  viewMetadata used to retrieve
// TotalRows
type viewMetadata struct {
	TotalRows int `json:"total_rows,omitempty"`
}

func (c *Collection) GetDDoc(docname string) (ddoc sgbucket.DesignDoc, err error) {
	manager := c.Bucket().ViewIndexes()
	designDoc, err := manager.GetDesignDocument(docname, gocb.DesignDocumentNamespaceProduction, nil)
	if err != nil {
		if strings.Contains(err.Error(), "not_found") {
			return ddoc, ErrNotFound
		}
		return ddoc, err
	}

	// Serialize/deserialize to convert to sgbucket.DesignDoc
	designDocBytes, marshalErr := JSONMarshal(designDoc)
	if marshalErr != nil {
		return ddoc, marshalErr
	}
	err = JSONUnmarshal(designDocBytes, &ddoc)
	return ddoc, err
}

func (c *Collection) GetDDocs() (ddocs map[string]sgbucket.DesignDoc, err error) {
	manager := c.Bucket().ViewIndexes()
	gocbDDocs, getErr := manager.GetAllDesignDocuments(gocb.DesignDocumentNamespaceProduction, nil)
	if getErr != nil {
		return nil, getErr
	}

	result := make(map[string]gocb.DesignDocument, len(gocbDDocs))
	for _, ddoc := range gocbDDocs {
		result[ddoc.Name] = ddoc
	}

	// Serialize/deserialize to convert to sgbucket.DesignDoc
	resultBytes, marshalErr := JSONMarshal(result)
	if marshalErr != nil {
		return nil, marshalErr
	}
	err = JSONUnmarshal(resultBytes, &ddocs)
	return ddocs, err
}

func (c *Collection) PutDDoc(docname string, sgDesignDoc *sgbucket.DesignDoc) error {
	manager := c.Bucket().ViewIndexes()
	gocbDesignDoc := gocb.DesignDocument{
		Name:  docname,
		Views: make(map[string]gocb.View),
	}

	for viewName, view := range sgDesignDoc.Views {
		gocbView := gocb.View{
			Map:    view.Map,
			Reduce: view.Reduce,
		}
		gocbDesignDoc.Views[viewName] = gocbView
	}

	// If design doc needs to be tombstone-aware, requires custom creation*
	if sgDesignDoc.Options != nil && sgDesignDoc.Options.IndexXattrOnTombstones {
		return c.putDDocForTombstones(&gocbDesignDoc)
	}

	return manager.UpsertDesignDocument(gocbDesignDoc, gocb.DesignDocumentNamespaceProduction, nil)
}

// gocb doesn't have built-in support for the internal index_xattr_on_deleted_docs
// design doc property. XattrEnabledDesignDocV2 extends gocb.DesignDocument to support
// use of putDDocForTombstones
type XattrEnabledDesignDocV2 struct {
	*gocb.DesignDocument
	IndexXattrOnTombstones bool `json:"index_xattr_on_deleted_docs,omitempty"`
}

func (c *Collection) putDDocForTombstones(ddoc *gocb.DesignDocument) error {
	username, password, _ := c.Spec.Auth.GetCredentials()
	agent, err := c.Bucket().Internal().IORouter()
	if err != nil {
		return fmt.Errorf("Unable to get handle for bucket router: %v", err)
	}

	xattrEnabledDesignDoc := XattrEnabledDesignDocV2{
		DesignDocument:         ddoc,
		IndexXattrOnTombstones: true,
	}
	data, err := JSONMarshal(&xattrEnabledDesignDoc)
	if err != nil {
		return err
	}

	return putDDocForTombstones(ddoc.Name, data, agent.CapiEps(), agent.HTTPClient(), username, password)

}

func (c *Collection) DeleteDDoc(docname string) error {
	return c.Bucket().ViewIndexes().DropDesignDocument(docname, gocb.DesignDocumentNamespaceProduction, nil)
}

func (c *Collection) View(ddoc, name string, params map[string]interface{}) (sgbucket.ViewResult, error) {

	var viewResult sgbucket.ViewResult
	gocbViewResult, err := c.executeViewQuery(ddoc, name, params)
	if err != nil {
		return viewResult, err
	}

	if gocbViewResult != nil {
		viewResultIterator := &gocbRawIterator{
			rawResult: gocbViewResult,
		}
		for {
			viewRow := sgbucket.ViewRow{}
			if gotRow := viewResultIterator.Next(&viewRow); gotRow == false {
				break
			}
			viewResult.Rows = append(viewResult.Rows, &viewRow)
		}

		// Check for errors
		err = gocbViewResult.Err()
		if err != nil {
			viewErr := sgbucket.ViewError{
				Reason: err.Error(),
			}
			viewResult.Errors = append(viewResult.Errors, viewErr)
		}

		viewMeta, err := unmarshalViewMetadata(gocbViewResult)
		if err != nil {
			Warnf("Unable to type get metadata for gocb ViewResult - the total rows count will be missing.")
		} else {
			viewResult.TotalRows = viewMeta.TotalRows
		}
		_ = gocbViewResult.Close()

	}

	// Indicate the view response contained partial errors so consumers can determine
	// if the result is valid to their particular use-case (see SG issue #2383)
	if len(viewResult.Errors) > 0 {
		return viewResult, ErrPartialViewErrors
	}

	return viewResult, nil
}

func unmarshalViewMetadata(viewResult *gocb.ViewResultRaw) (viewMetadata, error) {
	var viewMeta viewMetadata
	rawMeta, err := viewResult.MetaData()
	if err == nil {
		err = JSONUnmarshal(rawMeta, &viewMeta)
	}
	return viewMeta, err
}

func (c *Collection) ViewQuery(ddoc, name string, params map[string]interface{}) (sgbucket.QueryResultIterator, error) {

	gocbViewResult, err := c.executeViewQuery(ddoc, name, params)
	if err != nil {
		return nil, err
	}
	return &gocbRawIterator{rawResult: gocbViewResult}, nil
}

func (c *Collection) executeViewQuery(ddoc, name string, params map[string]interface{}) (*gocb.ViewResultRaw, error) {
	c.waitForAvailViewOp()
	defer c.releaseViewOp()
	viewResult := sgbucket.ViewResult{}
	viewResult.Rows = sgbucket.ViewRows{}

	viewOpts, optsErr := createViewOptions(params)
	if optsErr != nil {
		return nil, optsErr
	}

	goCbViewResult, err := c.Bucket().ViewQuery(ddoc, name, viewOpts)

	// On timeout, return an typed error
	if isGoCBQueryTimeoutError(err) {
		return nil, ErrViewTimeoutError
	} else if err != nil {
		return nil, pkgerrors.WithStack(err)
	}

	return goCbViewResult.Raw(), nil
}

type gocbResultRaw interface {

	// NextBytes returns the next row as bytes.
	NextBytes() []byte

	// Err returns any errors that have occurred on the stream
	Err() error

	// Close marks the results as closed, returning any errors that occurred during reading the results.
	Close() error

	// MetaData returns any meta-data that was available from this query as bytes.
	MetaData() ([]byte, error)
}

// GoCBQueryIterator wraps a gocb v2 ViewResultRaw to implement sgbucket.QueryResultIterator
type gocbRawIterator struct {
	rawResult gocbResultRaw
}

func NewGoCBQueryIterator(viewResult *gocb.ViewResultRaw) *gocbRawIterator {
	return &gocbRawIterator{
		rawResult: viewResult,
	}
}

// Unmarshal a single result row into valuePtr, and then close the iterator
func (i *gocbRawIterator) One(valuePtr interface{}) error {
	if !i.Next(valuePtr) {
		err := i.Close()
		if err != nil {
			return nil
		}
		return gocb.ErrNoResult
	}

	// Ignore any errors occurring after we already have our result
	//  - follows approach used by gocb v1 One() implementation
	_ = i.Close()
	return nil
}

// Unmarshal the next result row into valuePtr.  Returns false when reaching end of result set
func (i *gocbRawIterator) Next(valuePtr interface{}) bool {

	nextBytes := i.rawResult.NextBytes()
	if nextBytes == nil {
		return false
	}

	err := JSONUnmarshal(nextBytes, &valuePtr)
	if err != nil {
		Warnf("Unable to marshal view result row into value: %v", err)
		return false
	}
	return true
}

// Retrieve raw bytes for the next result row
func (i *gocbRawIterator) NextBytes() []byte {
	return i.rawResult.NextBytes()
}

// Closes the iterator.  Returns any row-level errors seen during iteration.
func (i *gocbRawIterator) Close() error {

	// check for errors before closing?
	closeErr := i.rawResult.Close()
	if closeErr != nil {
		return closeErr
	}
	resultErr := i.rawResult.Err()
	return resultErr
}

// waitForAvailableViewOp prevents Sync Gateway from having too many concurrent
// view queries against Couchbase Server
func (c *Collection) waitForAvailViewOp() {
	c.viewOps <- struct{}{}
}

func (c *Collection) releaseViewOp() {
	<-c.viewOps
}

// Applies the viewquery options as specified in the params map to the gocb.ViewOptions
func createViewOptions(params map[string]interface{}) (viewOpts *gocb.ViewOptions, err error) {

	viewOpts = &gocb.ViewOptions{}
	for optionName, optionValue := range params {
		switch optionName {
		case ViewQueryParamStale:
			viewOpts.ScanConsistency = asViewConsistency(optionValue)
		case ViewQueryParamReduce:
			viewOpts.Reduce = asBool(optionValue)
		case ViewQueryParamLimit:
			uintVal, err := normalizeIntToUint(optionValue)
			if err != nil {
				Warnf("ViewQueryParamLimit error: %v", err)
			}
			viewOpts.Limit = uint32(uintVal)
		case ViewQueryParamDescending:
			if asBool(optionValue) == true {
				viewOpts.Order = gocb.ViewOrderingDescending
			}
		case ViewQueryParamSkip:
			uintVal, err := normalizeIntToUint(optionValue)
			if err != nil {
				Warnf("ViewQueryParamSkip error: %v", err)
			}
			viewOpts.Skip = uint32(uintVal)
		case ViewQueryParamGroup:
			viewOpts.Group = asBool(optionValue)
		case ViewQueryParamGroupLevel:
			uintVal, err := normalizeIntToUint(optionValue)
			if err != nil {
				Warnf("ViewQueryParamGroupLevel error: %v", err)
			}
			viewOpts.GroupLevel = uint32(uintVal)
		case ViewQueryParamKey:
			viewOpts.Key = optionValue
		case ViewQueryParamKeys:
			keys, err := ConvertToEmptyInterfaceSlice(optionValue)
			if err != nil {
				return nil, err
			}
			viewOpts.Keys = keys
		case ViewQueryParamStartKey, ViewQueryParamEndKey, ViewQueryParamInclusiveEnd, ViewQueryParamStartKeyDocId, ViewQueryParamEndKeyDocId:
			// These are dealt with outside of this case statement to build ranges
		case ViewQueryParamIncludeDocs:
			// Ignored -- see https://forums.couchbase.com/t/do-the-viewquery-options-omit-include-docs-on-purpose/12399
		default:
			return nil, fmt.Errorf("Unexpected view query param: %v.  This will be ignored", optionName)
		}
	}

	// Range: startkey, endkey, inclusiveend
	var startKey, endKey interface{}
	if _, ok := params[ViewQueryParamStartKey]; ok {
		startKey = params[ViewQueryParamStartKey]
	}
	if _, ok := params[ViewQueryParamEndKey]; ok {
		endKey = params[ViewQueryParamEndKey]
	}

	// Default value of inclusiveEnd in Couchbase Server is true (if not specified)
	inclusiveEnd := true
	if _, ok := params[ViewQueryParamInclusiveEnd]; ok {
		inclusiveEnd = asBool(params[ViewQueryParamInclusiveEnd])
	}
	viewOpts.StartKey = startKey
	viewOpts.EndKey = endKey
	viewOpts.InclusiveEnd = inclusiveEnd

	// IdRange: startKeyDocId, endKeyDocId
	startKeyDocId := ""
	endKeyDocId := ""
	if _, ok := params[ViewQueryParamStartKeyDocId]; ok {
		startKeyDocId = params[ViewQueryParamStartKeyDocId].(string)
	}
	if _, ok := params[ViewQueryParamEndKeyDocId]; ok {
		endKeyDocId = params[ViewQueryParamEndKeyDocId].(string)
	}
	viewOpts.StartKeyDocID = startKeyDocId
	viewOpts.EndKeyDocID = endKeyDocId
	return viewOpts, nil
}

// Used to convert the stale view parameter to a gocb ViewScanConsistency
func asViewConsistency(value interface{}) gocb.ViewScanConsistency {

	switch typeValue := value.(type) {
	case string:
		if typeValue == "ok" {
			return gocb.ViewScanConsistencyNotBounded
		}
		if typeValue == "update_after" {
			return gocb.ViewScanConsistencyUpdateAfter
		}
		parsedVal, err := strconv.ParseBool(typeValue)
		if err != nil {
			Warnf("asStale called with unknown value: %v.  defaulting to stale=false", typeValue)
			return gocb.ViewScanConsistencyRequestPlus
		}
		if parsedVal {
			return gocb.ViewScanConsistencyNotBounded
		} else {
			return gocb.ViewScanConsistencyRequestPlus
		}
	case bool:
		if typeValue {
			return gocb.ViewScanConsistencyNotBounded
		} else {
			return gocb.ViewScanConsistencyRequestPlus
		}
	default:
		Warnf("asViewConsistency called with unknown type: %T.  defaulting to RequestPlus", typeValue)
		return gocb.ViewScanConsistencyRequestPlus
	}

}
