// Copyright 2023-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package functions

// Name and capabilities of the current user. (Admin is represented by a nil `*userCredentials`.)
type userCredentials struct {
	Name     string
	Roles    []string
	Channels []string
}
