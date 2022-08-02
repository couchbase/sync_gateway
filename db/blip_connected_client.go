/*
Copyright 2022-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package db

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/couchbase/go-blip"
	"github.com/couchbase/sync_gateway/base"
)

// Handles a Connected-Client "getRev" request.
func (bh *blipHandler) handleGetRev(rq *blip.Message) error {
	docID := rq.Properties[GetRevMessageId]
	ifNotRev := rq.Properties[GetRevIfNotRev]

	rev, err := bh.db.GetRev(docID, "", false, nil)
	if err != nil {
		status, reason := base.ErrorAsHTTPStatus(err)
		return &base.HTTPError{Status: status, Message: reason}
	} else if ifNotRev == rev.RevID {
		return base.HTTPErrorf(http.StatusNotModified, "Not Modified")
	} else if rev.Deleted {
		return base.HTTPErrorf(http.StatusNotFound, "Deleted")
	}

	body, err := rev.Body()
	if err != nil {
		return base.HTTPErrorf(http.StatusInternalServerError, "Couldn't read document body: %s", err)
	}

	bh.logEndpointEntry(rq.Profile(), fmt.Sprintf("doc: %s, ifNotRev: %s", docID, ifNotRev))

	// Still need to stamp _attachments into BLIP messages
	if len(rev.Attachments) > 0 {
		DeleteAttachmentVersion(rev.Attachments)
		body[BodyAttachments] = rev.Attachments
	}

	bodyBytes, err := base.JSONMarshalCanonical(body)
	if err != nil {
		return base.HTTPErrorf(http.StatusInternalServerError, "Couldn't encode document: %s", err)
	}

	response := rq.Response()
	response.SetCompressed(true)
	response.Properties[GetRevRevId] = rev.RevID
	response.SetBody(bodyBytes)
	bh.replicationStats.HandleGetRevCount.Add(1)
	return nil
}

// Handles a Connected-Client "putRev" request.
func (bh *blipHandler) handlePutRev(rq *blip.Message) error {
	stats := processRevStats{
		count:           bh.replicationStats.HandlePutRevCount,
		errorCount:      bh.replicationStats.HandlePutRevErrorCount,
		deltaRecvCount:  bh.replicationStats.HandlePutRevDeltaRecvCount,
		bytes:           bh.replicationStats.HandlePutRevBytes,
		processingTime:  bh.replicationStats.HandlePutRevProcessingTime,
		docsPurgedCount: bh.replicationStats.HandlePutRevDocsPurgedCount,
	}
	return bh.processRev(rq, &stats)
}

// Handles a Connected-Client "function" request.
func (bh *blipHandler) handleFunction(rq *blip.Message) error {
	name, requestParams, err := bh.parseQueryNameAndParams(rq)
	if err != nil {
		return err
	}
	bh.logEndpointEntry(rq.Profile(), fmt.Sprintf("name: %s", name))
	results, err := bh.db.CallUserFunction(name, requestParams, true)
	if err != nil {
		return base.HTTPErrorf(http.StatusInternalServerError, "Error running function: %v", err)
	}

	// Write the result to the response:
	response := rq.Response()
	response.SetCompressed(true)
	response.SetJSONBody(results)
	return nil
}

// Handles a Connected-Client "query" request.
func (bh *blipHandler) handleQuery(rq *blip.Message) error {
	name, requestParams, err := bh.parseQueryNameAndParams(rq)
	if err != nil {
		return err
	}
	bh.logEndpointEntry(rq.Profile(), fmt.Sprintf("name: %s", name))

	// Run the query:
	results, err := bh.db.UserN1QLQuery(name, requestParams)
	if err != nil {
		return err
	}

	// Write the results to the response:
	var out bytes.Buffer
	enc := base.JSONEncoder(&out)
	var row interface{}
	for results.Next(&row) {
		enc.Encode(row) // always ends with a newline
	}
	if err = results.Close(); err != nil {
		return err
	}
	response := rq.Response()
	response.SetCompressed(true)
	response.SetJSONBodyAsBytes(out.Bytes())
	return nil
}

func (bh *blipHandler) parseQueryNameAndParams(rq *blip.Message) (name string, params map[string]interface{}, err error) {
	var found bool
	name, found = rq.Properties[QueryName]
	if !found {
		err = base.HTTPErrorf(http.StatusBadRequest, "Missing 'name'")
		return
	}
	var bodyBytes []byte
	bodyBytes, err = rq.Body()
	if len(bodyBytes) > 0 && err == nil {
		if json.Unmarshal(bodyBytes, &params) != nil {
			err = base.HTTPErrorf(http.StatusBadRequest, "Invalid function parameters")
		}
	}
	return
}

// Handles a Connected-Client "graphql" request.
// - Request string is in the body of the request.
// - Response JSON is returned in the body of the response in GraphQL format (including errors.)
func (bh *blipHandler) handleGraphQL(rq *blip.Message) error {
	query, found := rq.Properties[GraphQLQuery]
	if !found {
		return base.HTTPErrorf(http.StatusBadRequest, "Missing 'query'")
	}
	operationName := rq.Properties[GraphQLOperationName]

	bodyBytes, err := rq.Body()
	if err != nil {
		return err
	}
	var variables map[string]interface{}
	if len(bodyBytes) > 0 {
		if err = json.Unmarshal(bodyBytes, &variables); err != nil {
			return base.HTTPErrorf(http.StatusBadRequest, "Invalid body JSON")
		}
	}

	bh.logEndpointEntry(rq.Profile(), fmt.Sprintf("query: %s", query))
	result, err := bh.db.UserGraphQLQuery(query, operationName, variables, true)
	if err != nil {
		base.WarnfCtx(bh.loggingCtx, "Error running GraphQL query %q: %v", query, err)
		return base.HTTPErrorf(http.StatusInternalServerError, "Internal GraphQL error")
	}
	response := rq.Response()
	response.SetCompressed(true)
	response.SetJSONBody(result)
	return nil
}
