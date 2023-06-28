// Copyright 2023-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package rest

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/couchbase/sync_gateway/channels"
	"github.com/stretchr/testify/require"
)

// TestRequirePlusSkippedSequence
func TestRequestPlusSkippedSequence(t *testing.T) {

	restTesterConfig := RestTesterConfig{SyncFn: channels.DocChannelsSyncFunction}

	// JWT claim based grants do not support named collections
	rt := NewRestTester(t, &restTesterConfig)
	defer rt.Close()

	const (
		username = "alice"
		channel  = "foo"
	)
	rt.CreateUser(username, []string{channel})

	// try directly using bearer token in a keyspace request
	resp := rt.SendAdminRequest(http.MethodPut, "/{{.keyspace}}/doc1", fmt.Sprintf(`{"channels":"%s"}`, channel))
	RequireStatus(t, resp, http.StatusCreated)
	docSeq := rt.GetDocumentSequence("doc1")

	require.NoError(t, rt.WaitForPendingChanges())

	resp = rt.SendUserRequest(http.MethodGet, fmt.Sprintf("/{{.keyspace}}/_changes?since=%d&request_plus=true", docSeq), "", username)
	RequireStatus(t, resp, http.StatusOK)
	var changesResp ChangesResults
	require.NoError(t, json.Unmarshal(resp.BodyBytes(), &changesResp))
	// QUESTION: this returns alice doc, which makes sense because we did a channel grant?, but why didn't it catch up with `WaitForPendingChanges`
	require.Len(t, changesResp.Results, 0)
}
