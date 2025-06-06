// Copyright 2025-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package base

import (
	"fmt"
	"runtime"
	"strings"
)

// NewSGProcessUserAgent returns a new user agent string for the given process name.
// A process may be something like "ISGR" and is used in place of "Platform" when compared with CBL user agents.
//
// Additional information may be provided which will be appended to the comments section of the UA string.
// Example: "CouchbaseSyncGateway/3.2.1.4@33-EE (ISGR; darwin/arm64)"
func NewSGProcessUserAgent(process string, extraComments ...string) string {
	productName := removeAllWhitespace(ProductNameString)
	productVersion := removeAllWhitespace(ProductVersion.String())

	comment := fmt.Sprintf("%s; %s/%s", process, runtime.GOOS, runtime.GOARCH)
	if len(extraComments) > 0 {
		comment += "; " + strings.Join(extraComments, "; ")
	}

	return fmt.Sprintf(`%s/%s (%s)`, productName, productVersion, comment)
}
