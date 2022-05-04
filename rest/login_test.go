//  Copyright 2012-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.
package rest

import (
	"net/http"
	"strings"
	"testing"
)

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
