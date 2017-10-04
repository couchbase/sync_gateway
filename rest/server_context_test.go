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
	"log"
	"net/http"
	"testing"
	"time"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db"
	"github.com/couchbaselabs/go.assert"
)

// Tests the ConfigServer feature.
func TestConfigServer(t *testing.T) {
	fakeConfigURL := "http://example.com/config"
	mockClient := NewMockClient()
	mockClient.RespondToGET(fakeConfigURL+"/db2", MakeResponse(200, nil,
		`{
			"bucket": "fivez",
			"server": "walrus:/fake"
		}`))

	var rt RestTester
	defer rt.Close()

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

	rt.Bucket() // no-op that just keeps rt from being GC'd/finalized (bug CBL-9)
}

// Tests the ConfigServer feature.
func TestConfigServerWithSyncFunction(t *testing.T) {
	fakeConfigURL := "http://example.com/config"
	fakeConfig := `{
			"bucket": "fivez",
			"server": "walrus:/fake",
			"sync":%s%s%s
		}`

	fakeSyncFunction := `
      function(doc, oldDoc) {
        if (doc.type == "reject_me") {
	      throw({forbidden : "Rejected document"})
        } else if (doc.type == "bar") {
	  // add "bar" docs to the "important" channel
            channel("important");
	} else if (doc.type == "secret") {
          if (!doc.owner) {
            throw({forbidden : "Secret documents must have an owner field"})
          }
	} else {
	    // all other documents just go into all channels listed in the doc["channels"] field
	    channel(doc.channels)
	}
      }
    `
	//Create config with embedded sync function in back quotes
	responseBody := fmt.Sprintf(fakeConfig, "`", fakeSyncFunction, "`")

	mockClient := NewMockClient()
	mockClient.RespondToGET(fakeConfigURL+"/db2", MakeResponse(200, nil, responseBody))

	var rt RestTester
	defer rt.Close()

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

	rt.Bucket() // no-op that just keeps rt from being GC'd/finalized (bug CBL-9)

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

// If no users are defined, expect a warning
func TestCollectAccessWarningsNoUsers(t *testing.T) {

	sc := NewServerContext(&ServerConfig{})
	defer sc.Close()

	dbServer := "walrus:"
	dbConfig := &DbConfig{
		BucketConfig: BucketConfig{Server: &dbServer},
		Name:         "db",
	}
	_, err := sc.AddDatabaseFromConfig(dbConfig)
	if err != nil {
		panic(fmt.Sprintf("Error from AddDatabaseFromConfig: %v", err))
	}
	dbContext := sc.Database("db")
	warnings := collectAccessRelatedWarnings(dbConfig, dbContext)
	assert.True(t, len(warnings) > 0)

}

// If a guest user is defined but has no channels, expect a warning
func TestCollectAccessWarningsGuestNoChans(t *testing.T) {

	sc := NewServerContext(&ServerConfig{})
	defer sc.Close()

	dbServer := "walrus:"
	dbConfig := &DbConfig{
		BucketConfig: BucketConfig{Server: &dbServer},
		Name:         "db",
		Users: map[string]*db.PrincipalConfig{
			base.GuestUsername: {
				Disabled: false,
			},
		},
	}
	_, err := sc.AddDatabaseFromConfig(dbConfig)
	if err != nil {
		panic(fmt.Sprintf("Error from AddDatabaseFromConfig: %v", err))
	}
	dbContext := sc.Database("db")
	warnings := collectAccessRelatedWarnings(dbConfig, dbContext)
	assert.True(t, len(warnings) > 0)

}

// If a guest user and has channels, expect no warnings
func TestCollectAccessWarningsGuestWithChans(t *testing.T) {

	sc := NewServerContext(&ServerConfig{})
	defer sc.Close()

	dbServer := "walrus:"
	dbConfig := &DbConfig{
		BucketConfig: BucketConfig{Server: &dbServer},
		Name:         "db",
		Users: map[string]*db.PrincipalConfig{
			base.GuestUsername: {
				Disabled:         false,
				ExplicitChannels: base.SetFromArray([]string{"*"}),
			},
		},
	}
	_, err := sc.AddDatabaseFromConfig(dbConfig)
	if err != nil {
		panic(fmt.Sprintf("Error from AddDatabaseFromConfig: %v", err))
	}
	dbContext := sc.Database("db")
	warnings := collectAccessRelatedWarnings(dbConfig, dbContext)
	assert.Equals(t, len(warnings), 0)

}

// As long as there is one user in the db, there should be no warnings
func TestCollectAccessWarningsUsersInDb(t *testing.T) {

	sc := NewServerContext(&ServerConfig{})
	defer sc.Close()

	dbServer := "walrus:"
	dbConfig := &DbConfig{
		BucketConfig: BucketConfig{Server: &dbServer},
		Name:         "db",
	}
	_, err := sc.AddDatabaseFromConfig(dbConfig)
	if err != nil {
		panic(fmt.Sprintf("Error from AddDatabaseFromConfig: %v", err))
	}
	dbContext := sc.Database("db")

	password := "bar"
	// create user
	spec := map[string]*db.PrincipalConfig{
		"foo": {
			Password:         &password,
			Disabled:         false,
			ExplicitChannels: base.SetFromArray([]string{"*"}),
		},
	}

	// add a user to the db
	err = sc.installPrincipals(dbContext, spec, "user")
	assertNoError(t, err, "Error installing principal")

	var warnings []string
	for i := 1; i <= 10; i++ {
		log.Printf("View attempt %d/10", i)
		warnings = collectAccessRelatedWarnings(dbConfig, dbContext)
		if len(warnings) == 0 {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	assert.Equals(t, len(warnings), 0)

}
