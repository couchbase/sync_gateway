/*
Copyright 2019-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package channels

import (
	"testing"

	"github.com/couchbase/sync_gateway/base"
)

// Creates a set from zero or more inline string arguments.
// Channel names must be valid, else the function will panic, so this should only be called
// with hardcoded known-valid strings.
func SetOf(tb testing.TB, names ...string) base.Set {
	set, err := SetFromArray(names, KeepStar)
	if err != nil {
		tb.Fatalf("channels.SetOf failed: %v", err)
	}
	return set
}
