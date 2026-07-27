// Copyright 2026-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

//go:build ruleguard
// +build ruleguard

//nolint:unused // functions in here are invoked by ruleguard, but aren't imported/used by anything Go can detect.
package ruleguard

import (
	"github.com/quasilyte/go-ruleguard/dsl"
)

// withcancel finds uses of context.WithCancel. WithCancelCause carries a reason through to
// ctx.Err()/context.Cause(), which is what makes a cancelled operation diagnosable from a log line.
// Deliberate exceptions (e.g. testing the causeless cancellation path) should carry a //nolint:gocritic
// with the reason.
func withcancel(m dsl.Matcher) {
	m.
		Match(`context.WithCancel($ctx)`).
		Report("use context.WithCancelCause instead of context.WithCancel, so the cancellation reason propagates")
}
