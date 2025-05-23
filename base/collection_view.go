/*
Copyright 2021-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package base

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/couchbase/gocb/v2"
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
	if !c.IsDefaultScopeCollection() {
		return ddoc, fmt.Errorf("views not supported for non-default collection")
	}

	manager := c.Collection.Bucket().ViewIndexes()
	designDoc, err := manager.GetDesignDocument(docname, gocb.DesignDocumentNamespaceProduction, nil)
	if err != nil {
		if strings.Contains(err.Error(), "not found") {
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
	if !c.IsDefaultScopeCollection() {
		return nil, fmt.Errorf("views not supported for non-default collection")
	}

	manager := c.Collection.Bucket().ViewIndexes()
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

func (c *Collection) PutDDoc(ctx context.Context, docname string, sgDesignDoc *sgbucket.DesignDoc) error {
	if !c.IsDefaultScopeCollection() {
		return fmt.Errorf("views not supported for non-default collection")
	}

	manager := c.Collection.Bucket().ViewIndexes()
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
		return c.Bucket.putDDocForTombstones(ctx, &gocbDesignDoc)
	}

	// Retry for all errors (The view service sporadically returns 500 status codes with Erlang errors (for unknown reasons) - E.g: 500 {"error":"case_clause","reason":"false"})
	worker := func() (bool, error, interface{}) {
		err := manager.UpsertDesignDocument(gocbDesignDoc, gocb.DesignDocumentNamespaceProduction, nil)
		if err != nil {
			WarnfCtx(ctx, "Got error from UpsertDesignDocument: %v - Retrying...", err)
			return true, err, nil
		}
		return false, nil, nil
	}

	err, _ := RetryLoop(ctx, "PutDDocRetryLoop", worker, CreateSleeperFunc(5, 100))
	return err
}

// gocb doesn't have built-in support for the internal index_xattr_on_deleted_docs
// design doc property. XattrEnabledDesignDocV2 extends gocb.DesignDocument to support
// use of putDDocForTombstones
type XattrEnabledDesignDocV2 struct {
	*jsonDesignDocument
	IndexXattrOnTombstones bool `json:"index_xattr_on_deleted_docs,omitempty"`
}

// gocb's DesignDocument and View aren't directly marshallable for use in viewEp requests - they
// copy into *private* structs with the correct json annotations.  Cloning those here to support
// use of index_xattr_on_deleted_docs.
type jsonView struct {
	Map    string `json:"map,omitempty"`
	Reduce string `json:"reduce,omitempty"`
}

type jsonDesignDocument struct {
	Views map[string]jsonView `json:"views,omitempty"`
}

func asJsonDesignDocument(ddoc *gocb.DesignDocument) *jsonDesignDocument {
	jsonDDoc := &jsonDesignDocument{}
	jsonDDoc.Views = make(map[string]jsonView, 0)
	for name, view := range ddoc.Views {
		jsonDDoc.Views[name] = jsonView{
			Map:    view.Map,
			Reduce: view.Reduce,
		}
	}
	return jsonDDoc
}

type NoNameView struct {
	Map    string `json:"map,omitempty"`
	Reduce string `json:"reduce,omitempty"`
}

type NoNameDesignDocument struct {
	Name  string                `json:"-"`
	Views map[string]NoNameView `json:"views"`
}

func (b *GocbV2Bucket) putDDocForTombstones(ctx context.Context, ddoc *gocb.DesignDocument) error {
	username, password, _ := b.Spec.Auth.GetCredentials()
	agent, err := b.GetGoCBAgent()
	if err != nil {
		return fmt.Errorf("Unable to get handle for bucket router: %v", err)
	}

	jsonDdoc := asJsonDesignDocument(ddoc)

	xattrEnabledDesignDoc := XattrEnabledDesignDocV2{
		jsonDesignDocument:     jsonDdoc,
		IndexXattrOnTombstones: true,
	}
	data, err := JSONMarshal(&xattrEnabledDesignDoc)
	if err != nil {
		return err
	}

	return putDDocForTombstones(ctx, ddoc.Name, data, agent.CapiEps(), agent.HTTPClient(), username, password)

}

func (c *Collection) DeleteDDoc(docname string) error {
	if !c.IsDefaultScopeCollection() {
		return fmt.Errorf("views not supported for non-default collection")
	}

	return c.Collection.Bucket().ViewIndexes().DropDesignDocument(docname, gocb.DesignDocumentNamespaceProduction, nil)
}

func (c *Collection) View(ctx context.Context, ddoc, name string, params map[string]interface{}) (sgbucket.ViewResult, error) {
	var viewResult sgbucket.ViewResult
	gocbViewResult, err := c.executeViewQuery(ctx, ddoc, name, params)
	if err != nil {
		return viewResult, err
	}

	if gocbViewResult != nil {
		viewResultIterator := &gocbRawIterator{
			rawResult:                  gocbViewResult,
			concurrentQueryOpLimitChan: c.Bucket.queryOps,
		}
		for {
			viewRow := sgbucket.ViewRow{}
			if gotRow := viewResultIterator.Next(ctx, &viewRow); gotRow == false {
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
			WarnfCtx(ctx, "Unable to type get metadata for gocb ViewResult - the total rows count will be missing.")
		} else {
			viewResult.TotalRows = viewMeta.TotalRows
		}
		_ = viewResultIterator.Close()

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

func (c *Collection) ViewQuery(ctx context.Context, ddoc, name string, params map[string]interface{}) (sgbucket.QueryResultIterator, error) {
	gocbViewResult, err := c.executeViewQuery(ctx, ddoc, name, params)
	if err != nil {
		return nil, err
	}
	return &gocbRawIterator{rawResult: gocbViewResult, concurrentQueryOpLimitChan: c.Bucket.queryOps}, nil
}

func (c *Collection) executeViewQuery(ctx context.Context, ddoc, name string, params map[string]interface{}) (*gocb.ViewResultRaw, error) {
	viewResult := sgbucket.ViewResult{}
	viewResult.Rows = sgbucket.ViewRows{}

	viewOpts, optsErr := createViewOptions(ctx, params)
	if optsErr != nil {
		return nil, optsErr
	}

	c.Bucket.waitForAvailQueryOp()
	goCbViewResult, err := c.Collection.Bucket().ViewQuery(ddoc, name, viewOpts)

	// On timeout, return an typed error
	if isGoCBQueryTimeoutError(err) {
		c.Bucket.releaseQueryOp()
		return nil, ErrViewTimeoutError
	} else if err != nil {
		c.Bucket.releaseQueryOp()
		return nil, pkgerrors.WithStack(err)
	}

	return goCbViewResult.Raw(), nil
}

// Applies the viewquery options as specified in the params map to the gocb.ViewOptions
func createViewOptions(ctx context.Context, params map[string]interface{}) (viewOpts *gocb.ViewOptions, err error) {

	viewOpts = &gocb.ViewOptions{}
	for optionName, optionValue := range params {
		switch optionName {
		case ViewQueryParamStale:
			viewOpts.ScanConsistency = asViewConsistency(ctx, optionValue)
		case ViewQueryParamReduce:
			viewOpts.Reduce = asBool(ctx, optionValue)
		case ViewQueryParamLimit:
			uintVal, err := normalizeIntToUint(optionValue)
			if err != nil {
				WarnfCtx(ctx, "ViewQueryParamLimit error: %v", err)
			}
			viewOpts.Limit = uint32(uintVal)
		case ViewQueryParamDescending:
			if asBool(ctx, optionValue) == true {
				viewOpts.Order = gocb.ViewOrderingDescending
			}
		case ViewQueryParamSkip:
			uintVal, err := normalizeIntToUint(optionValue)
			if err != nil {
				WarnfCtx(ctx, "ViewQueryParamSkip error: %v", err)
			}
			viewOpts.Skip = uint32(uintVal)
		case ViewQueryParamGroup:
			viewOpts.Group = asBool(ctx, optionValue)
		case ViewQueryParamGroupLevel:
			uintVal, err := normalizeIntToUint(optionValue)
			if err != nil {
				WarnfCtx(ctx, "ViewQueryParamGroupLevel error: %v", err)
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
		inclusiveEnd = asBool(ctx, params[ViewQueryParamInclusiveEnd])
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
func asViewConsistency(ctx context.Context, value interface{}) gocb.ViewScanConsistency {

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
			WarnfCtx(ctx, "asStale called with unknown value: %v.  defaulting to stale=false", typeValue)
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
		WarnfCtx(ctx, "asViewConsistency called with unknown type: %T.  defaulting to RequestPlus", typeValue)
		return gocb.ViewScanConsistencyRequestPlus
	}

}
