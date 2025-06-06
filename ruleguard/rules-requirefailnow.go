// Copyright 2025-Present Couchbase, Inc.
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

func failnow(m dsl.Matcher) {
	m.Match(
		`require.FailNow($t, $msg, $*_)`,
		`require.FailNow($t, $msg)`,
		`require.FailNowf($t, $msg)`,
		`require.FailNowf($t, $msg, $*_)`,
		`require.Fail($t, $msg)`,
		`require.Fail($t, $msg, $*_)`,
		`require.Failf($t, $msg)`,
		`require.Failf($t, $msg, $*_)`,
		`assert.FailNow($t, $msg, $*_)`,
		`assert.FailNow($t, $msg)`,
		`assert.FailNowf($t, $msg)`,
		`assert.FailNowf($t, $msg, $*_)`,
		`assert.Fail($t, $msg)`,
		`assert.Fail($t, $msg, $*_)`,
		`assert.Failf($t, $msg)`,
		`assert.Failf($t, $msg, $*_)`,
	).Where(m["msg"].Pure && m["msg"].Type.Is("string") && m["msg"].Text.Matches(".*%[A-Za-z]")).Report("second argument can not contain format verbs starting with %, wrap this argument in fmt.Sprintf() if you want to use format verbs")
}
