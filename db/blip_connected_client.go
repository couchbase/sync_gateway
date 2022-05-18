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
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/couchbase/go-blip"
	"github.com/couchbase/gocb/v2"
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

// Handles a Connected-Client "query" request.
func (bh *blipHandler) handleQuery(rq *blip.Message) error {
	if _, found := rq.Properties[QuerySource]; found {
		return base.HTTPErrorf(http.StatusForbidden, "Only named queries allowed")
	}
	name, found := rq.Properties[QueryName]
	if !found {
		return base.HTTPErrorf(http.StatusBadRequest, "Missing 'name'")
	}
	query, found := bh.db.Options.ConnectedClientQueries[name]
	if !found {
		return base.HTTPErrorf(http.StatusNotFound, "No such query '%s'", name)
	}

	var params map[string]interface{}
	bodyBytes, err := rq.Body()
	if err != nil {
		return err
	}
	if len(bodyBytes) > 0 {
		if err = json.Unmarshal(bodyBytes, &params); err != nil {
			return base.HTTPErrorf(http.StatusBadRequest, "Invalid query parameters")
		}
	}
	for _, paramName := range query.Parameters {
		if _, found := params[paramName]; !found {
			return base.HTTPErrorf(http.StatusBadRequest, "Parameter '%s' is missing", paramName)
		}
	}

	bh.logEndpointEntry(rq.Profile(), fmt.Sprintf("name: %s", name))

	// In the N1QL string, replace "BUCKET" with the actual bucket name, wrapped in back-quotes in
	// case it's not a valid N1QL identifier (e.g. `travel-sample`.)
	// TODO: This is a workaround for the inability to use back-quotes for this in an SG config
	// file, because those are already reserved for quoting long literals like the sync function.
	statement := strings.ReplaceAll(query.Statement, "BUCKET", "`"+bh.db.Name+"`")

	// Run the query:
	results, err := bh.db.N1QLQueryWithStats(bh.db.Ctx, QueryTypeConnectedClientPrefix+name, statement, params,
		base.RequestPlus, false)
	if err != nil {
		var qe *gocb.QueryError
		if errors.As(err, &qe) {
			base.WarnfCtx(bh.loggingCtx, "Error running query %q: %v", name, err)
			return base.HTTPErrorf(http.StatusInternalServerError, "Query error: %s", qe.Errors[0].Message)
		} else {
			base.WarnfCtx(bh.loggingCtx, "Unknown error running query %q: %T %#v", name, err, err)
			return base.HTTPErrorf(http.StatusInternalServerError, "Unknown error running query")
		}
	}

	// Write the results to the response:
	var out bytes.Buffer
	enc := json.NewEncoder(&out)
	var row interface{}
	for results.Next(&row) {
		enc.Encode(row) // always ends with a newline
	}
	response := rq.Response()
	response.SetCompressed(true)
	response.SetJSONBodyAsBytes(out.Bytes())
	return nil
}
