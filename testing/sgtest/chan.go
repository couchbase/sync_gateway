// Copyright 2026-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package sgtest

import (
	"testing"
	"time"
)

// ChanCallbackTimeout is the timeout used by RequireChanClosedFromCallback.
const ChanCallbackTimeout = 30 * time.Second

// RequireChanClosedFromCallback waits for ch to be closed, similar to base.RequireChanClosed, but
// is safe to call from a goroutine other than the one running the test (e.g. a DCP worker or
// background manager goroutine invoking a test callback). Unlike base.RequireChanClosed, it
// reports a timeout via t.Errorf instead of t.FailNow, since FailNow must only be called from the
// test's own goroutine — calling it elsewhere can hang the test binary instead of failing the test.
func RequireChanClosedFromCallback[T any](t testing.TB, ch <-chan T) {
	for {
		select {
		case _, ok := <-ch:
			if ok {
				continue
			}
			return
		case <-time.After(ChanCallbackTimeout):
			t.Errorf("timed out after %v waiting for channel to close (called from non-test goroutine)", ChanCallbackTimeout)
			return
		}
	}
}
