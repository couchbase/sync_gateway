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

	assert.Len(t, attachments, 1)
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
	require.Len(t, attachments, 3)

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
	if !base.TestsUseServerCE() {
		t.Skip("test only runs with CE server")
	}
	rt := NewRestTester(t, nil)
	defer rt.Close()
	eps, _, err := rt.ServerContext().ObtainManagementEndpointsAndHTTPClient()
	require.NoError(t, err)

	form := url.Values{}
	form.Add("password", "password")
	form.Add("roles", "[mobile_sync_Gateway]")

	req, err := http.NewRequest("PUT", fmt.Sprintf("%s/settings/rbac/users/local/%s", eps[0], "username"), strings.NewReader(form.Encode()))
	require.NoError(t, err)

	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.SetBasicAuth(base.TestClusterUsername(), base.TestClusterPassword())

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	require.Equal(t, resp.StatusCode, http.StatusBadRequest)
}

func TestRestTesterTemplateMultipleDatabases(t *testing.T) {
	rt := NewRestTester(t, &RestTesterConfig{
		PersistentConfig: true,
	})
	defer rt.Close()
	testCases := []struct {
		input     string
		output    string
		errString string
	}{
		{
			input:     "/{{.db}}/",
			output:    "",
			errString: `map has no entry for key "db"`,
		},
		{
			input:     "/{{.db1}}/",
			output:    "",
			errString: `map has no entry for key "db1"`,
		},
		{
			input:     "/{{.keyspace}}/",
			output:    "",
			errString: `map has no entry for key "keyspace"`,
		},
		{
			input:     "/{{.keyspace1}}/",
			output:    "",
			errString: `map has no entry for key "keyspace1"`,
		},
		{
			input:  "/passthrough/",
			output: "/passthrough/",
		},
	}
	for _, test := range testCases {
		rt.Run(test.input, func(t *testing.T) {
			output, err := rt.templateResource(test.input)
			if test.errString == "" {
				require.NoError(t, err)
			} else {
				require.Contains(t, fmt.Sprintf("%s", err), test.errString)
			}
			require.Equal(t, test.output, output)
		})
	}
	numCollections := 2
	base.RequireNumTestDataStores(t, numCollections)
	dbConfig := rt.NewDbConfig()
	dbConfig.Scopes = GetCollectionsConfig(rt.TB(), rt.TestBucket, numCollections)
	dbOne := "dbone"
	bucket1Datastore1, err := rt.TestBucket.GetNamedDataStore(0)
	require.NoError(t, err)
	bucket1Datastore2, err := rt.TestBucket.GetNamedDataStore(1)
	require.NoError(t, err)
	resp := rt.CreateDatabase(dbOne, dbConfig)
	RequireStatus(t, resp, http.StatusCreated)
	testCases = []struct {
		input     string
		output    string
		errString string
	}{
		{
			input:  "/{{.db}}/",
			output: fmt.Sprintf("/%s/", dbOne),
		},
		{
			input:     "/{{.db1}}/",
			output:    "",
			errString: `map has no entry for key "db1"`,
		},
		{
			input:     "/{{.keyspace}}/",
			output:    "",
			errString: `map has no entry for key "keyspace"`,
		},
		{
			input:  "/{{.keyspace1}}/",
			output: fmt.Sprintf("/%s.%s.%s/", dbOne, bucket1Datastore1.ScopeName(), bucket1Datastore1.CollectionName()),
		},
		{
			input:  "/{{.keyspace2}}/",
			output: fmt.Sprintf("/%s.%s.%s/", dbOne, bucket1Datastore2.ScopeName(), bucket1Datastore2.CollectionName()),
		},
	}
	for _, test := range testCases {
		rt.Run("dbone_"+test.input, func(t *testing.T) {
			output, err := rt.templateResource(test.input)
			if test.errString == "" {
				require.NoError(t, err)
			} else {
				require.Contains(t, fmt.Sprintf("%s", err), test.errString)
			}
			require.Equal(t, test.output, output)
		})
	}
	base.RequireNumTestBuckets(t, 2)
	ctx := base.TestCtx(t)
	bucket2 := base.GetTestBucket(t)
	defer bucket2.Close(ctx)
	dbConfig = rt.NewDbConfig()
	dbConfig.Scopes = GetCollectionsConfig(rt.TB(), bucket2, numCollections)
	dbConfig.BucketConfig = BucketConfig{
		Bucket: base.Ptr(bucket2.GetName()),
	}
	dbTwo := "dbtwo"
	bucket2Datastore1, err := rt.TestBucket.GetNamedDataStore(0)
	require.NoError(t, err)
	bucket2Datastore2, err := rt.TestBucket.GetNamedDataStore(1)
	require.NoError(t, err)
	resp = rt.CreateDatabase(dbTwo, dbConfig)
	RequireStatus(t, resp, http.StatusCreated)
	testCases = []struct {
		input     string
		output    string
		errString string
	}{
		{
			input:     "/{{.db}}/",
			errString: `map has no entry for key "db"`,
		},
		{
			input:  "/{{.db1}}/",
			output: fmt.Sprintf("/%s/", dbOne),
		},
		{
			input:  "/{{.db2}}/",
			output: fmt.Sprintf("/%s/", dbTwo),
		},
		{
			input:     "/{{.keyspace}}/",
			errString: `map has no entry for key "keyspace"`,
		},
		{
			input:     "/{{.keyspace1}}/",
			errString: `map has no entry for key "keyspace1"`,
		},
		{
			input:     "/{{.keyspace2}}/",
			errString: `map has no entry for key "keyspace2"`,
		},
		{
			input:  "/{{.db1keyspace1}}/",
			output: fmt.Sprintf("/%s.%s.%s/", dbOne, bucket1Datastore1.ScopeName(), bucket2Datastore1.CollectionName()),
		},
		{
			input:  "/{{.db1keyspace2}}/",
			output: fmt.Sprintf("/%s.%s.%s/", dbOne, bucket1Datastore2.ScopeName(), bucket2Datastore2.CollectionName()),
		},

		{
			input:  "/{{.db2keyspace1}}/",
			output: fmt.Sprintf("/%s.%s.%s/", dbTwo, bucket2Datastore1.ScopeName(), bucket2Datastore1.CollectionName()),
		},
		{
			input:  "/{{.db2keyspace2}}/",
			output: fmt.Sprintf("/%s.%s.%s/", dbTwo, bucket2Datastore2.ScopeName(), bucket2Datastore2.CollectionName()),
		},
	}
	for _, test := range testCases {
		rt.Run("twodb_"+test.input, func(t *testing.T) {
			output, err := rt.templateResource(test.input)
			if test.errString == "" {
				require.NoError(t, err)
			} else {
				require.Contains(t, fmt.Sprintf("%s", err), test.errString)
			}
			require.Equal(t, test.output, output)
		})
	}

}

func TestRequest(t *testing.T) {
	assert.PanicsWithValue(t, `http.NewRequest failed: parse "http://localhost%": invalid URL escape "%"`, func() {
		// invalid URL escape - should panic (with a prefix in message)
		_ = Request(http.MethodGet, "%", "")
	})
}
