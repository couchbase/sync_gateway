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
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/couchbase/go-blip"
	"github.com/couchbase/sync_gateway/base"
)

//////// GETREV:

// Handles a Connected-Client "getRev" request.
func (bh *blipHandler) handleGetRev(rq *blip.Message) error {
	docID := rq.Properties[GetRevMessageId]
	ifNotRev := rq.Properties[GetRevIfNotRev]

	rev, err := bh.collection.GetRev(bh.loggingCtx, docID, "", false, nil)
	if err != nil {
		status, reason := base.ErrorAsHTTPStatus(err)
		return &base.HTTPError{Status: status, Message: reason}
	}
	if ifNotRev == rev.RevID {
		return base.HTTPErrorf(http.StatusNotModified, "Not Modified")
	}
	if rev.Deleted {
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

//////// PUTREV:

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

//////// FUNCTION:

// Handles a Connected-Client "function" request.
// - Function name is in the "name" property.
// - Arguments (optional) are JSON-encoded in the body.
func (bh *blipHandler) handleFunction(rq *blip.Message) error {
	name, found := rq.Properties[QueryName]
	if !found {
		return base.HTTPErrorf(http.StatusBadRequest, "Missing 'name'")
	}

	requestParams, err := bh.parseJsonBody(rq, bh.db.UserFunctions.MaxRequestSize)
	if err != nil {
		return err
	}

	bh.logEndpointEntry(rq.Profile(), fmt.Sprintf("name: %s", name))
	return WithTimeout(bh.loggingCtx, UserFunctionTimeout, func(ctx context.Context) error {
		// Call the function:
		fn, err := bh.db.GetUserFunction(name, requestParams, true, ctx)
		if err != nil {
			return err
		}

		if iter, err := fn.Iterate(); err != nil {
			return err
		} else if iter != nil {
			// Write each iterated result to the response:
			defer func() {
				if iter != nil {
					_ = iter.Close()
				}
			}()
			var out bytes.Buffer
			enc := base.JSONEncoder(&out)
			var row interface{}
			for iter.Next(&row) {
				if err = enc.Encode(row); err != nil { // always ends with a newline
					return err
				}
				if err = CheckTimeout(ctx); err != nil {
					return err
				}
			}
			err = iter.Close()
			iter = nil
			if err != nil {
				return err
			}
			response := rq.Response()
			response.SetCompressed(true)
			response.SetJSONBodyAsBytes(out.Bytes())
			return nil

		} else {
			// Write the single result to the response:
			result, err := fn.RunAsJSON()
			if err != nil {
				return err
			}
			response := rq.Response()
			response.SetCompressed(true)
			response.SetBody(result)
			return nil
		}
	})
}

//////// GRAPHQL:

// Handles a Connected-Client "graphql" request.
// - GraphQL query string is in the "query" property.
// - GraphQL operationName (optional) is in the "operationName" property.
// - Variables (optional) are JSON-encoded in the body of the request.
// - Response JSON is returned in the body of the response in GraphQL format (including errors.)
func (bh *blipHandler) handleGraphQL(rq *blip.Message) error {
	query, found := rq.Properties[GraphQLQuery]
	if !found {
		return base.HTTPErrorf(http.StatusBadRequest, "Missing 'query'")
	}
	operationName := rq.Properties[GraphQLOperationName]

	variables, err := bh.parseJsonBody(rq, bh.db.GraphQL.MaxRequestSize())
	if err != nil {
		return err
	}

	bh.logEndpointEntry(rq.Profile(), fmt.Sprintf("query: %s", query))
	return WithTimeout(bh.loggingCtx, UserFunctionTimeout, func(ctx context.Context) error {
		result, err := bh.db.UserGraphQLQuery(query, operationName, variables, true, ctx)
		if err != nil {
			return err
		}
		response := rq.Response()
		response.SetCompressed(true)
		_ = response.SetJSONBody(result)
		return nil
	})
}

func (bh *blipHandler) parseJsonBody(rq *blip.Message, maxSize *int) (map[string]any, error) {
	bodyBytes, err := rq.Body()
	if err != nil {
		return nil, err
	}
	if err := CheckRequestSize(len(bodyBytes), bh.db.UserFunctions.MaxRequestSize); err != nil {
		return nil, err
	}
	var parsedBody map[string]any
	if len(bodyBytes) > 0 {
		if json.Unmarshal(bodyBytes, &parsedBody) != nil {
			return nil, base.HTTPErrorf(http.StatusBadRequest, "Invalid function parameters")
		}
	}
	return parsedBody, nil
}
