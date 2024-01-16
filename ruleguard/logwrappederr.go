// Copyright 2024-Present Couchbase, Inc.
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

// logwrappederr finds instances of Sync Gateway logging where we are attempting to use %w on an error. Should use %s or %v.
func logwrappederr(m dsl.Matcher) {
	m.
		Match(
			// this is horrible but the variadics after $err don't match with the optional wildcards $*_
			// also no way to regex match to provide non-package qualified versions
			`base.PanicfCtx($ctx, $format, $*_)`,
			`PanicfCtx($ctx, $format, $*_)`,
			`base.FatalfCtx($ctx, $format, $*_)`,
			`FatalfCtx($ctx, $format, $*_)`,
			`base.ErrorfCtx($ctx, $format, $*_)`,
			`ErrorfCtx($ctx, $format, $*_)`,
			`base.WarnfCtx($ctx, $format, $*_)`,
			`WarnfCtx($ctx, $format, $*_)`,
			`base.InfofCtx($ctx, $logkey, $format, $*_)`,
			`InfofCtx($ctx, $logkey, $format, $*_)`,
			`base.DebugfCtx($ctx, $logkey, $format, $*_)`,
			`DebugfCtx($ctx, $logkey, $format, $*_)`,
			`base.TracefCtx($ctx, $logkey, $format, $*_)`,
			`TracefCtx($ctx, $logkey, $format, $*_)`,
			`fmt.Printf($format, $*_)`,
			`log.Printf($format, $*_)`,
		).
		Where(
			m["format"].Type.Is("string") && m["format"].Text.Matches(`.*%w.*`)).
		Report("cannot use error wrapping verb %w outside of fmt.Errorf() - use %s or %v instead?")
}
