// Copyright 2024-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package base

// IsDevMode returns true when compiled with the `cb_sg_devmode` build tag, and false otherwise.
//
// The compiler will remove this check and all code invoked inside it in non-dev mode, avoiding any impact on production code.
// https://godbolt.org/z/f1K8a96rE
func IsDevMode() bool {
	return cbSGDevModeBuildTagSet
}
