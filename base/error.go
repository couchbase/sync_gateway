//  Copyright (c) 2012 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package base

import (
	"fmt"
	"log"
	"net/http"

	"github.com/couchbaselabs/walrus"
	"github.com/dustin/gomemcached"
)

// Simple error implementation wrapping an HTTP response status.
type HTTPError struct {
	Status  int
	Message string
}

func (err *HTTPError) Error() string {
	return err.Message
}

// Attempts to map an error to an HTTP status code and message.
// Defaults to 500 if it doesn't recognize the error. Returns 200 for a nil error.
func ErrorAsHTTPStatus(err error) (int, string) {
	if err == nil {
		return 200, "OK"
	}
	switch err := err.(type) {
	case *HTTPError:
		return err.Status, err.Message
	case *gomemcached.MCResponse:
		switch err.Status {
		case gomemcached.KEY_ENOENT:
			return http.StatusNotFound, "missing"
		case gomemcached.KEY_EEXISTS:
			return http.StatusConflict, "Conflict"
		case gomemcached.E2BIG:
			return http.StatusRequestEntityTooLarge, "Too Large"
		default:
			return http.StatusBadGateway, fmt.Sprintf("MC status %s", err.Status.String())
		}
	case walrus.MissingError:
		return http.StatusNotFound, "missing"
	}
	log.Printf("WARNING: Couldn't interpret error type %T, value %v", err, err)
	return http.StatusInternalServerError, fmt.Sprintf("Internal error: %v", err)
}

// Returns true if an error is a Couchbase doc-not-found error
func IsDocNotFoundError(err error) bool {
	switch err := err.(type) {
	case *gomemcached.MCResponse:
		return err.Status == gomemcached.KEY_ENOENT
	case walrus.MissingError:
		return true
	}
	return false
}
