/*
Copyright 2018-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package rest

import (
	"fmt"
	"log"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDocumentUnmarshal(t *testing.T) {

	jsonContent := `
{
   "_id":"docid",
   "_rev":"1-rev",
   "foo":"bar",
   "_attachments":{
      "myattachment":{
         "content_type":"text",
         "digest":"987u98u",
         "length":10,
         "revpos":1,
         "stub":true
      }
   }
}
`

	doc := RestDocument{}
	err := base.JSONUnmarshal([]byte(jsonContent), &doc)
	if err != nil {
		log.Printf("Error: %v", err)
	}
	assert.True(t, err == nil)
	log.Printf("doc: %+v", doc)

	assert.True(t, doc.ID() == "docid")

	docFooField, hasFoo := doc["foo"]
	assert.True(t, hasFoo)
	log.Printf("docFooField: %v", docFooField)

	attachments, err := doc.GetAttachments()
	assert.True(t, err == nil)

	assert.Equal(t, 1, len(attachments))
	myattachment := attachments["myattachment"]
	assert.Equal(t, "text", myattachment.ContentType)

}

func TestAttachmentRoundTrip(t *testing.T) {

	doc := RestDocument{}
	attachmentMap := db.AttachmentMap{
		"foo": &db.DocAttachment{
			ContentType: "application/octet-stream",
			Digest:      "WHATEVER",
		},
		"bar": &db.DocAttachment{
			ContentType: "text/plain",
			Digest:      "something",
		},
		"baz": &db.DocAttachment{
			Data: []byte(""),
		},
	}

	doc.SetAttachments(attachmentMap)

	attachments, err := doc.GetAttachments()
	require.NoError(t, err)
	require.Equal(t, 3, len(attachments))

	require.NotNil(t, attachments["foo"])
	assert.Equal(t, "application/octet-stream", attachments["foo"].ContentType)
	assert.Equal(t, "WHATEVER", attachments["foo"].Digest)

	require.NotNil(t, attachments["bar"])
	assert.Equal(t, "text/plain", attachments["bar"].ContentType)
	assert.Equal(t, "something", attachments["bar"].Digest)

	require.NotNil(t, attachments["baz"])
	assert.Equal(t, "", attachments["baz"].ContentType)
	assert.Equal(t, "", attachments["baz"].Digest)
	assert.Equal(t, []byte{}, attachments["baz"].Data) // data field is explicitly ignored

}

// TestRestTesterInvalidPathVariable ensures that invalid path variables return an error instead of silently returning "<no value>" or empty string.
func TestRestTesterInvalidPathVariable(t *testing.T) {
	const dbName = "dbname"
	rt := NewRestTester(t, &RestTesterConfig{
		DatabaseConfig: &DatabaseConfig{
			DbConfig: DbConfig{
				Name: dbName,
			},
		},
	})
	defer rt.Close()

	uri, err := rt.templateResource("/foo/{{.invalid}}/bar")
	assert.Errorf(t, err, "Expected error for invalid path variable")
	assert.Equalf(t, "", uri, "Expected empty URI for invalid path variable")
	assert.NotContainsf(t, uri, "<no value>", "Expected URI to not contain \"<no value>\" for invalid path variable")

	uri, err = rt.templateResource("/foo/{{.db}}/bar")
	assert.NoError(t, err)
	assert.Equalf(t, "/foo/"+dbName+"/bar", uri, "Expected valid URI for valid path variable")
}

func TestCECheck(t *testing.T) {
	if base.UnitTestUrlIsWalrus() {
		t.Skip("Only works with CBS")
	}
	if base.TestsUseServerCE() {
		t.Skip("test only runs with CE server")
	}
	rt := NewRestTester(t, nil)
	defer rt.Close()
	form := url.Values{}
	form.Add("password", "password")
	form.Add("roles", "[mobile_sync_Gateway]")
	eps, _, err := rt.ServerContext().ObtainManagementEndpointsAndHTTPClient()
	require.NoError(t, err)

	req, err := http.NewRequest("PUT", fmt.Sprintf("%s/settings/rbac/users/local/%s", eps[0], "username"), strings.NewReader(form.Encode()))
	require.Error(t, err)
	require.Equal(t, req, http.StatusBadRequest)

}
