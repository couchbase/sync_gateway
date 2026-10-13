// Copyright 2026-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package sgtest

import (
	"fmt"
	"runtime/pprof"
	"strings"

	"github.com/couchbase/sync_gateway/testing/require"
)

// CountLabeledGoroutines returns the number of goroutines carrying the pprof label key=value.
func CountLabeledGoroutines(t require.TestingT, key, value string) int {
	var profile strings.Builder
	require.NoError(t, pprof.Lookup("goroutine").WriteTo(&profile, 1))
	label := fmt.Sprintf("%q:%q", key, value)
	count := 0
	// debug=1 output groups identical goroutines into blank-line separated blocks headed by "<count> @ <pcs>". The
	// "goroutine profile: total N" header has no blank line after it, so drop it before splitting.
	_, records, _ := strings.Cut(profile.String(), "\n")
	for _, group := range strings.Split(records, "\n\n") {
		if !strings.Contains(group, "# labels: ") || !strings.Contains(group, label) {
			continue
		}
		var n int
		_, err := fmt.Sscanf(group, "%d @", &n)
		require.NoError(t, err)
		count += n
	}
	return count
}
