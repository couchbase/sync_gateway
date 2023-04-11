// Copyright 2023-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package auth

import (
	"net/http"
	"strings"
)

// Configuration for Cross-Origin Resource Sharing
// <https://en.wikipedia.org/wiki/Cross-origin_resource_sharing>
type CORSConfig struct {
	Origin      []string `json:"origin,omitempty"       help:"List of allowed origins, use ['*'] to allow access from everywhere"`
	LoginOrigin []string `json:"login_origin,omitempty" help:"List of allowed login origins"`
	Headers     []string `json:"headers,omitempty"      help:"List of allowed headers"`
	MaxAge      int      `json:"max_age,omitempty"      help:"Maximum age of the CORS Options request"`
}

// Adds Access-Control-Allow-Origin, Access-Control-Allow-Credentials, Access-Control-Allow-Headers headers to an HTTP response.
func (cors *CORSConfig) AddResponseHeaders(request *http.Request, response http.ResponseWriter) {
	if originHeader := request.Header["Origin"]; len(originHeader) > 0 {
		origin := MatchedOrigin(cors.Origin, originHeader)
		response.Header().Add("Access-Control-Allow-Origin", origin)
		response.Header().Add("Access-Control-Allow-Credentials", "true")
		response.Header().Add("Access-Control-Allow-Headers", strings.Join(cors.Headers, ", "))
	}
}

func MatchedOrigin(allowOrigins []string, rqOrigins []string) string {
	for _, rv := range rqOrigins {
		for _, av := range allowOrigins {
			if rv == av {
				return av
			}
		}
	}
	for _, av := range allowOrigins {
		if av == "*" {
			return "*"
		}
	}
	return ""
}
