package rest

import (
	"bytes"
	"compress/gzip"
	"log"
	"net/http"
	"strconv"
	"strings"
	"testing"

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

	response := rt.SendRequestWithHeaders(http.MethodPut, "/db/doc1", bodyText, reqHeaders)
	assertStatus(t, response, http.StatusCreated)

	response = rt.SendRequestWithHeaders(http.MethodGet, "/db/doc1", "", reqHeaders)
	log.Printf("response: %v", string(response.BodyBytes()))
	assertStatus(t, response, http.StatusOK)
}

func TestReadJSONFromMIME(t *testing.T) {
	// Read JSON from MIME by specifying non-JSON content type
	header := http.Header{}
	header.Add("MIME-Version", "1.0")
	header.Add("Content-Type", "multipart/related; boundary=0123456789")
	input := strings.NewReader(`{"key":"foo","value":"bar"}`)
	var body db.Body
	err := ReadJSONFromMIME(header, input, &body)
	assert.Error(t, err, "Can't read JSON from MIME by specifying non-JSON content type")
	assert.Contains(t, err.Error(), strconv.Itoa(http.StatusUnsupportedMediaType))

	// Read valid JSON from MIME
	header = http.Header{}
	header.Add("MIME-Version", "1.0")
	header.Add("Content-Type", "application/json")
	input = strings.NewReader(`{"key":"foo","value":"bar"}`)
	err = ReadJSONFromMIME(header, input, &body)
	assert.NoError(t, err, "Should read valid JSON from MIME")
	assert.Equal(t, "foo", body["key"])
	assert.Equal(t, "bar", body["value"])

	// Read JSON from MIME with illegal JSON body content
	header = http.Header{}
	header.Add("MIME-Version", "1.0")
	header.Add("Content-Type", "application/json")
	input = strings.NewReader(`"key":"foo","value":"bar"`)
	err = ReadJSONFromMIME(header, input, &body)
	assert.Error(t, err, "Can't read JSON from MIME with illegal JSON body content")
	assert.Contains(t, err.Error(), strconv.Itoa(http.StatusBadRequest))

	// Read JSON from MIME with gzip content encoding and illegal content type (application/json)
	header = http.Header{}
	header.Add("MIME-Version", "1.0")
	header.Add("Content-Type", "application/json")
	header.Add("Content-Encoding", "gzip")
	input = strings.NewReader(`{"key":"foo","value":"bar"}`)
	err = ReadJSONFromMIME(header, input, &body)
	assert.Error(t, err, "Can't read JSON from MIME with gzip content encoding and illegal content type")
	assert.Contains(t, err.Error(), "invalid header")

	// Read JSON from MIME with unsupported content encoding.
	header = http.Header{}
	header.Add("MIME-Version", "1.0")
	header.Add("Content-Encoding", "zip")
	input = strings.NewReader(`{"key":"foo","value":"bar"}`)
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

	input = strings.NewReader(string(buffer.Bytes()))
	err = ReadJSONFromMIME(header, input, &body)
	assert.NoError(t, err, "Should read JSON from MIME with gzip content encoding")
	assert.Equal(t, "foo", body["key"])
	assert.Equal(t, "bar", body["value"])
}
