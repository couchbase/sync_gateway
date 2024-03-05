/*
Copyright 2024-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package rest

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"

	"github.com/couchbase/sync_gateway/db"

	"github.com/stretchr/testify/assert"
)

func TestGetAlldocChannels(t *testing.T) {
	rt := NewRestTester(t, &RestTesterConfig{SyncFn: `function(doc) {channel(doc.channel);}`})
	defer rt.Close()

	version := rt.PutDoc("doc", `{"channel":["CHAN1"]}`)
	updatedVersion := rt.UpdateDoc("doc", version, `{"channel":["CHAN2"]}`)
	updatedVersion = rt.UpdateDoc("doc", updatedVersion, `{"channel":["CHAN1"]}`)
	updatedVersion = rt.UpdateDoc("doc", updatedVersion, `{"channel":["CHAN1", "CHAN2"]}`)
	updatedVersion = rt.UpdateDoc("doc", updatedVersion, `{"channel":["CHAN3"]}`)
	updatedVersion = rt.UpdateDoc("doc", updatedVersion, `{"channel":["CHAN1"]}`)

	response := rt.SendDiagnosticRequest("GET", "/{{.keyspace}}/doc/_all_channels", "")
	RequireStatus(t, response, http.StatusOK)

	var channelMap map[string][]string
	err := json.Unmarshal(response.BodyBytes(), &channelMap)
	assert.NoError(t, err)
	assert.ElementsMatch(t, channelMap["CHAN1"], []string{"6-0", "1-2", "3-5"})
	assert.ElementsMatch(t, channelMap["CHAN2"], []string{"4-5", "2-3"})
	assert.ElementsMatch(t, channelMap["CHAN3"], []string{"5-6"})

	for i := 1; i <= 10; i++ {
		updatedVersion = rt.UpdateDoc("doc", updatedVersion, `{}`)
		updatedVersion = rt.UpdateDoc("doc", updatedVersion, `{"channel":["CHAN3"]}`)
	}
	response = rt.SendAdminRequest("GET", "/{{.keyspace}}/doc", "")
	RequireStatus(t, response, http.StatusOK)
	response = rt.SendDiagnosticRequest("GET", "/{{.keyspace}}/doc/_all_channels", "")
	RequireStatus(t, response, http.StatusOK)

	err = json.Unmarshal(response.BodyBytes(), &channelMap)
	assert.NoError(t, err)

	// If the channel is still in channel_set, then the total will be 5 entries in history and 1 in channel_set
	assert.Equal(t, len(channelMap["CHAN3"]), db.DocumentHistoryMaxEntriesPerChannel+1)

}

func TestGetDocDryRuns(t *testing.T) {
	rt := NewRestTester(t, &RestTesterConfig{PersistentConfig: true})
	defer rt.Close()
	ImportFilter := `"function(doc) { if (doc.user.num) { return true; } else { return false; } }"`
	SyncFn := `"function(doc) {channel(doc.channel); access(doc.accessUser, doc.accessChannel);role(doc.accessUser, doc.role); }"`
	resp := rt.SendAdminRequest("PUT", "/db/", fmt.Sprintf(
		`{ "num_index_replicas": 0, "enable_shared_bucket_access": %t, "sync":%s, "import_filter":%s}`,
		base.TestUseXattrs(), SyncFn, ImportFilter))
	RequireStatus(t, resp, http.StatusCreated)
	response := rt.SendDiagnosticRequest("GET", "/{{.keyspace}}/sync/doc", "{\"accessChannel\": [\"dynamicChan5412\"],\"accessUser\": \"user\",\"channel\": [\"dynamicChan222\"],\"user\":{\"num\":0}}")
	RequireStatus(t, response, http.StatusOK)

	var respMap SyncFnDryRun
	err := json.Unmarshal(response.BodyBytes(), &respMap)
	assert.NoError(t, err)
	assert.ElementsMatch(t, respMap.Exception, nil)
	assert.Equal(t, respMap.Roles, channels.AccessMap{})
	assert.Equal(t, respMap.Access, channels.AccessMap{"user": channels.BaseSetOf(t, "dynamicChan5412")})
	response = rt.SendDiagnosticRequest("GET", "/{{.keyspace}}/sync/doc", "{\"role\": [\"role:role1\"], \"accessUser\": \"user\"}")
	RequireStatus(t, response, http.StatusOK)

	err = json.Unmarshal(response.BodyBytes(), &respMap)
	assert.NoError(t, err)
	assert.Equal(t, respMap.Roles, channels.AccessMap{"user": channels.BaseSetOf(t, "role1")})
	newSyncFn := `"function(doc) {if (doc.user.num >= \"100\") {channel(doc.channel);} else {throw({forbidden: \"user num too low\"});}}"`
	resp = rt.SendAdminRequest("PUT", "/db/_config", fmt.Sprintf(
		`{"num_index_replicas": 0, "enable_shared_bucket_access": %t, "sync":%s, "import_filter":%s}`,
		base.TestUseXattrs(), newSyncFn, ImportFilter))
	RequireStatus(t, resp, http.StatusCreated)

	response = rt.SendDiagnosticRequest("GET", "/{{.keyspace}}/sync/doc", `{"user":{"num":23}}`)
	RequireStatus(t, response, http.StatusOK)

	err = json.Unmarshal(response.BodyBytes(), &respMap)
	assert.NoError(t, err)
	assert.Equal(t, respMap.Exception, "403 user num too low")
	assert.ElementsMatch(t, respMap.Channels, []string{})

	// Import filter import=false and type error
	response = rt.SendDiagnosticRequest("GET", "/{{.keyspace}}/import_filter/doc", `{"accessUser": "user"}`)
	RequireStatus(t, response, http.StatusOK)

	var respMap2 ImportFilterDryRun
	err = json.Unmarshal(response.BodyBytes(), &respMap2)
	assert.NoError(t, err)
	assert.Equal(t, respMap2.Error, "TypeError: Cannot access member 'num' of undefined")
	assert.False(t, respMap2.ShouldImport)

	// Import filter import=true and no error
	response = rt.SendDiagnosticRequest("GET", "/{{.keyspace}}/import_filter/doc", `{"user":{"num":23}}`)
	RequireStatus(t, response, http.StatusOK)

	err = json.Unmarshal(response.BodyBytes(), &respMap2)
	assert.NoError(t, err)
	assert.Equal(t, respMap2.Error, "")
	assert.True(t, respMap2.ShouldImport)
}
