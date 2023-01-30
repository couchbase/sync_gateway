// Copyright (c) 2012 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.
package rest

import (
	"fmt"
	"log"
	"net/http"
	"strings"
	"testing"

	goassert "github.com/couchbaselabs/go.assert"
	"github.com/tleyden/fakehttp"
)

func TestVerifyFacebook(t *testing.T) {
	// TODO: Disabled due to https://github.com/couchbase/sync_gateway/issues/1659
	t.Skip("WARNING: TEST DISABLED")

	testServer := fakehttp.NewHTTPServer()
	testServer.Start()

	fakeResponse := `{"id": "801878789",
                          "name": "Alice",
                          "email": "alice@dot.com"}`

	testServer.Response(200, nil, fakeResponse)

	urlString := fmt.Sprintf("%s/foo.html", testServer.URL)
	facebookResponse, err := verifyFacebook(urlString, "fake_access_token")

	if err != nil {
		log.Panicf("Got error: %v", err)
	}

	log.Printf("facebookResponse: %s, err: %s", facebookResponse, err)
	goassert.True(t, true)
	goassert.Equals(t, facebookResponse.Email, "alice@dot.com")

}

// This test exists because there have been problems with builds of Go being unable to make HTTPS
// connections due to the TLS package missing the Cgo bits needed to load system root certs.
func TestVerifyHTTPSSupport(t *testing.T) {

	if testing.Short() {
		t.Skip("skipping test in short mode")
	}

	resp, err := http.Get("https://google.com")
	defer func() { _ = resp.Body.Close() }()

	if err != nil {
		// Skip test if dial tcp fails with no such host.
		// This is to allow tests to be run offline/without third-party dependencies.
		if strings.Contains(err.Error(), "no such host") {
			t.Skipf("WARNING: Host could not be reached: %s", err)
		}
		t.Errorf("Error making HTTPS connection: %v", err)
	}
}
