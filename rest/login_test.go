//  Copyright 2012-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.
package rest

import (
	"fmt"
	"log"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
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
	assert.True(t, true)
	assert.Equal(t, "alice@dot.com", facebookResponse.Email)

}

// This test exists because there have been problems with builds of Go being unable to make HTTPS
// connections due to the TLS package missing the Cgo bits needed to load system root certs.
func TestVerifyHTTPSSupport(t *testing.T) {

	if testing.Short() {
		t.Skip("skipping test in short mode")
	}

	resp, err := http.Get("https://google.com")
	defer func() {
		if resp != nil {
			_ = resp.Body.Close()
		}
	}()

	if err != nil {
		// Skip test if dial tcp fails with no such host.
		// This is to allow tests to be run offline/without third-party dependencies.
		if strings.Contains(err.Error(), "no such host") {
			t.Skipf("WARNING: Host could not be reached: %s", err)
		}
		t.Errorf("Error making HTTPS connection: %v", err)
	}
}
