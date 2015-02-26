//  Copyright (c) 2013 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package rest

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"net/http"
	"testing"

	"github.com/couchbaselabs/go.assert"
)

// Tests the ConfigServer feature.
func TestConfigServer(t *testing.T) {
	fakeConfigURL := "http://example.com/config"
	mockClient := NewMockClient()
	mockClient.RespondToGET(fakeConfigURL+"/db2", MakeResponse(200, nil,
		`{
			"bucket": "fivez",
			"server": "walrus:/fake",
			"users": {
				"GUEST": {"disabled": false, "admin_channels": ["*"] }
			}
		}`))

	var rt restTester
	sc := rt.ServerContext()
	sc.HTTPClient = mockClient.Client
	sc.config.ConfigServer = &fakeConfigURL

	dbc, err := sc.GetDatabase("db")
	assert.Equals(t, err, nil)
	assert.Equals(t, dbc.Name, "db")

	dbc, err = sc.GetDatabase("db2")
	assert.Equals(t, err, nil)
	assert.Equals(t, dbc.Name, "db2")
	assert.Equals(t, dbc.Bucket.GetName(), "fivez")

	rt.bucket() // no-op that just keeps rt from being GC'd/finalized (bug CBL-9)
}

//////// MOCK HTTP CLIENT: (TODO: Move this into a separate package)

// Creates a filled-in http.Response from minimal details
func MakeResponse(status int, headers map[string]string, body string) *http.Response {
	return &http.Response{
		StatusCode:    status,
		Status:        fmt.Sprintf("%d", status),
		Body:          ioutil.NopCloser(bytes.NewBufferString(body)),
		ContentLength: int64(len(body)),
		Proto:         "HTTP/1.1",
		ProtoMajor:    1,
		ProtoMinor:    1,
	}
}

// Implementation of http.RoundTripper that does the actual work
type mockTripper struct {
	getURLs map[string]*http.Response
}

func (m *mockTripper) RoundTrip(rq *http.Request) (*http.Response, error) {
	response := m.getURLs[rq.URL.String()]
	if response == nil {
		response = MakeResponse(http.StatusNotFound, nil, "Not Found")
	}
	return response, nil
}

// Fake http.Client that returns canned responses.
type MockClient struct {
	*http.Client
}

// Creates a new MockClient.
func NewMockClient() *MockClient {
	tripper := mockTripper{
		getURLs: map[string]*http.Response{},
	}
	return &MockClient{
		Client: &http.Client{Transport: &tripper},
	}
}

// Adds a canned response. The Client will respond to a GET of the given URL with the response.
func (client *MockClient) RespondToGET(url string, response *http.Response) {
	tripper := client.Transport.(*mockTripper)
	tripper.getURLs[url] = response
}
