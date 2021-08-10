/*
Copyright 2019-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package rest

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"mime/multipart"
	"net/http"
	"strconv"
	"strings"
	"testing"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db"
	"github.com/stretchr/testify/assert"
)

func TestWriteMultipartDocument(t *testing.T) {
	rt := NewRestTester(t, nil)
	defer rt.Close()

	reqHeaders := map[string]string{
		"MIME-Version": "1.0",
		"Content-Type": "multipart/related; boundary=0123456789"}

	bodyText := `--0123456789
Content-Type: application/json

{"key":"foo","value":"bar"}
--0123456789--`

	response := rt.SendAdminRequestWithHeaders(http.MethodPut, "/db/doc1", bodyText, reqHeaders)
	assertStatus(t, response, http.StatusCreated)

	response = rt.SendAdminRequestWithHeaders(http.MethodGet, "/db/doc1", "", reqHeaders)
	log.Printf("response: %v", string(response.BodyBytes()))
	assertStatus(t, response, http.StatusOK)
}

func TestReadJSONFromMIME(t *testing.T) {
	// Read JSON from MIME by specifying non-JSON content type
	header := http.Header{}
	header.Add("MIME-Version", "1.0")
	header.Add("Content-Type", "multipart/related; boundary=0123456789")
	input := ioutil.NopCloser(strings.NewReader(`{"key":"foo","value":"bar"}`))
	var body db.Body
	err := ReadJSONFromMIME(header, input, &body)
	assert.Error(t, err, "Can't read JSON from MIME by specifying non-JSON content type")
	assert.Contains(t, err.Error(), strconv.Itoa(http.StatusUnsupportedMediaType))

	// Read valid JSON from MIME
	header = http.Header{}
	header.Add("MIME-Version", "1.0")
	header.Add("Content-Type", "application/json")
	input = ioutil.NopCloser(strings.NewReader(`{"key":"foo","value":"bar"}`))
	err = ReadJSONFromMIME(header, input, &body)
	assert.NoError(t, err, "Should read valid JSON from MIME")
	assert.Equal(t, "foo", body["key"])
	assert.Equal(t, "bar", body["value"])

	// Read JSON from MIME with illegal JSON body content
	header = http.Header{}
	header.Add("MIME-Version", "1.0")
	header.Add("Content-Type", "application/json")
	input = ioutil.NopCloser(strings.NewReader(`"key":"foo","value":"bar"`))
	err = ReadJSONFromMIME(header, input, &body)
	assert.Error(t, err, "Can't read JSON from MIME with illegal JSON body content")
	assert.Contains(t, err.Error(), strconv.Itoa(http.StatusBadRequest))

	// Read JSON from MIME with gzip content encoding and illegal content type (application/json)
	header = http.Header{}
	header.Add("MIME-Version", "1.0")
	header.Add("Content-Type", "application/json")
	header.Add("Content-Encoding", "gzip")
	input = ioutil.NopCloser(strings.NewReader(`{"key":"foo","value":"bar"}`))
	err = ReadJSONFromMIME(header, input, &body)
	assert.Error(t, err, "Can't read JSON from MIME with gzip content encoding and illegal content type")
	assert.Contains(t, err.Error(), "invalid header")

	// Read JSON from MIME with unsupported content encoding.
	header = http.Header{}
	header.Add("MIME-Version", "1.0")
	header.Add("Content-Encoding", "zip")
	input = ioutil.NopCloser(strings.NewReader(`{"key":"foo","value":"bar"}`))
	err = ReadJSONFromMIME(header, input, &body)
	assert.Error(t, err, "Can't read JSON from MIME with unsupported content encoding")
	assert.Contains(t, err.Error(), strconv.Itoa(http.StatusUnsupportedMediaType))

	// Read JSON from MIME with gzip content encoding.
	header = http.Header{}
	header.Add("MIME-Version", "1.0")
	header.Add("Content-Encoding", "gzip")
	var buffer bytes.Buffer

	gz := gzip.NewWriter(&buffer)
	_, err = gz.Write([]byte(`{"key":"foo","value":"bar"}`))
	assert.NoError(t, err, "Writes a compressed form of bytes to the underlying io.Writer")
	assert.NoError(t, gz.Flush(), "Flushes any pending compressed data to the underlying writer")
	assert.NoError(t, gz.Close(), "Closes the Writer by flushing any unwritten data")

	input = ioutil.NopCloser(&buffer)
	err = ReadJSONFromMIME(header, input, &body)
	assert.NoError(t, err, "Should read JSON from MIME with gzip content encoding")
	assert.Equal(t, "foo", body["key"])
	assert.Equal(t, "bar", body["value"])
}

func TestReadMultipartDocument(t *testing.T) {
	rt := NewRestTester(t, nil)
	defer rt.Close()

	reqHeaders := map[string]string{
		"MIME-Version": "1.0",
		"Content-Type": "multipart/related; boundary=123"}

	bodyText := `--123
Content-Type: application/json

{"key":"foo","value":"bar","_attachments":{"att.txt": {"type": "text/plain", "length": 35, "follows": true, "digest":"sha1-6RU4WkyC+YYARHkO052YJ/dw1Zk="}}}
--123  
Content-Type: application/json
Content-Disposition: attachment; filename=att.txt

{"root":"Jacques' JSON attachment"}
--123--`

	response := rt.SendAdminRequestWithHeaders(http.MethodPut, "/db/doc1", bodyText, reqHeaders)
	assertStatus(t, response, http.StatusCreated)

	response = rt.SendAdminRequestWithHeaders(http.MethodGet, "/db/doc1", "", reqHeaders)
	assertStatus(t, response, http.StatusOK)

	var body db.Body
	assert.NoError(t, base.JSONUnmarshal(response.BodyBytes(), &body))
	log.Printf("body: %v", body)
	assert.Equal(t, "doc1", body["_id"])
	assert.Equal(t, "foo", body["key"])
	assert.Equal(t, "bar", body["value"])

	attachments := body["_attachments"].(map[string]interface{})
	attachment := attachments["att.txt"].(map[string]interface{})
	assert.Equal(t, float64(35), attachment["length"])
	assert.Equal(t, float64(1), attachment["revpos"])
	assert.True(t, attachment["stub"].(bool))
	assert.Equal(t, "sha1-6RU4WkyC+YYARHkO052YJ/dw1Zk=", attachment["digest"])
}

func TestWriteJSONPart(t *testing.T) {
	// writeJSONPart toggles compression to false if the incoming body is less than 300 bytes, so creating
	// a body larger than 300 bytes to test writeJSONPart with compression=true and compression=false
	mockFakeBody := func() db.Body {
		bytes := make([]byte, 139)
		rand.Read(bytes)
		value := fmt.Sprintf("%x", bytes)
		return db.Body{"key": "foo", "value": value}
	}

	body := mockFakeBody()
	log.Printf("body: %v", body)
	buffer := &bytes.Buffer{}
	writer := multipart.NewWriter(buffer)
	bytes, _ := base.JSONMarshalCanonical(body)
	log.Printf("len(bytes): %v", len(bytes))
	assert.NoError(t, writeJSONPart(writer, "application/json", body, true))
	assert.NoError(t, writeJSONPart(writer, "application/json", body, false))
}

func TestMd5DigestKey(t *testing.T) {
	assert.Equal(t, "md5-rL0Y20zC+Fzt72VPzMSk2A==", md5DigestKey([]byte("foo")))
	assert.Equal(t, "md5-N7UdGUp1E+RbVvZSTy1R8g==", md5DigestKey([]byte("bar")))
	assert.Equal(t, "md5-xWvVSA9uVBPLYqCtlmZhOg==", md5DigestKey([]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}))
}
