// Copyright 2026-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package main

import (
	"os/exec"
	"time"
)

// killProcessTreeOnCancel kills only gotestsum on cancellation, and WaitDelay stops cmd.Wait() from
// blocking on descendants that still hold the output pipe.
func killProcessTreeOnCancel(cmd *exec.Cmd) {
	cmd.WaitDelay = 30 * time.Second
}
