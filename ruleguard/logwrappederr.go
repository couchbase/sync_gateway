// Copyright 2024-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

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
			`base.PanicfCtx($ctx, $format, $err)`,
			`base.PanicfCtx($ctx, $format, $*_, $err)`,
			`base.PanicfCtx($ctx, $format, $*_, $err, $_)`,
			`base.PanicfCtx($ctx, $format, $*_, $err, $_, $_)`,
			`base.PanicfCtx($ctx, $format, $*_, $err, $_, $_, $_)`,
			`base.PanicfCtx($ctx, $format, $*_, $err, $_, $_, $_, $_)`,
			`base.PanicfCtx($ctx, $format, $*_, $err, $_, $_, $_, $_, $_)`,
			`PanicfCtx($ctx, $format, $err)`,
			`PanicfCtx($ctx, $format, $*_, $err)`,
			`PanicfCtx($ctx, $format, $*_, $err, $_)`,
			`PanicfCtx($ctx, $format, $*_, $err, $_, $_)`,
			`PanicfCtx($ctx, $format, $*_, $err, $_, $_, $_)`,
			`PanicfCtx($ctx, $format, $*_, $err, $_, $_, $_, $_)`,
			`PanicfCtx($ctx, $format, $*_, $err, $_, $_, $_, $_, $_)`,
			`base.FatalfCtx($ctx, $format, $err)`,
			`base.FatalfCtx($ctx, $format, $*_, $err)`,
			`base.FatalfCtx($ctx, $format, $*_, $err, $_)`,
			`base.FatalfCtx($ctx, $format, $*_, $err, $_, $_)`,
			`base.FatalfCtx($ctx, $format, $*_, $err, $_, $_, $_)`,
			`base.FatalfCtx($ctx, $format, $*_, $err, $_, $_, $_, $_)`,
			`base.FatalfCtx($ctx, $format, $*_, $err, $_, $_, $_, $_, $_)`,
			`FatalfCtx($ctx, $format, $err)`,
			`FatalfCtx($ctx, $format, $*_, $err)`,
			`FatalfCtx($ctx, $format, $*_, $err, $_)`,
			`FatalfCtx($ctx, $format, $*_, $err, $_, $_)`,
			`FatalfCtx($ctx, $format, $*_, $err, $_, $_, $_)`,
			`FatalfCtx($ctx, $format, $*_, $err, $_, $_, $_, $_)`,
			`FatalfCtx($ctx, $format, $*_, $err, $_, $_, $_, $_, $_)`,
			`base.ErrorfCtx($ctx, $format, $err)`,
			`base.ErrorfCtx($ctx, $format, $*_, $err)`,
			`base.ErrorfCtx($ctx, $format, $*_, $err, $_)`,
			`base.ErrorfCtx($ctx, $format, $*_, $err, $_, $_)`,
			`base.ErrorfCtx($ctx, $format, $*_, $err, $_, $_, $_)`,
			`base.ErrorfCtx($ctx, $format, $*_, $err, $_, $_, $_, $_)`,
			`base.ErrorfCtx($ctx, $format, $*_, $err, $_, $_, $_, $_, $_)`,
			`ErrorfCtx($ctx, $format, $err)`,
			`ErrorfCtx($ctx, $format, $*_, $err)`,
			`ErrorfCtx($ctx, $format, $*_, $err, $_)`,
			`ErrorfCtx($ctx, $format, $*_, $err, $_, $_)`,
			`ErrorfCtx($ctx, $format, $*_, $err, $_, $_, $_)`,
			`ErrorfCtx($ctx, $format, $*_, $err, $_, $_, $_, $_)`,
			`ErrorfCtx($ctx, $format, $*_, $err, $_, $_, $_, $_, $_)`,
			`base.WarnfCtx($ctx, $format, $err)`,
			`base.WarnfCtx($ctx, $format, $*_, $err)`,
			`base.WarnfCtx($ctx, $format, $*_, $err, $_)`,
			`base.WarnfCtx($ctx, $format, $*_, $err, $_, $_)`,
			`base.WarnfCtx($ctx, $format, $*_, $err, $_, $_, $_)`,
			`base.WarnfCtx($ctx, $format, $*_, $err, $_, $_, $_, $_)`,
			`base.WarnfCtx($ctx, $format, $*_, $err, $_, $_, $_, $_, $_)`,
			`WarnfCtx($ctx, $format, $err)`,
			`WarnfCtx($ctx, $format, $*_, $err)`,
			`WarnfCtx($ctx, $format, $*_, $err, $_)`,
			`WarnfCtx($ctx, $format, $*_, $err, $_, $_)`,
			`WarnfCtx($ctx, $format, $*_, $err, $_, $_, $_)`,
			`WarnfCtx($ctx, $format, $*_, $err, $_, $_, $_, $_)`,
			`WarnfCtx($ctx, $format, $*_, $err, $_, $_, $_, $_, $_)`,
			`base.InfofCtx($ctx, $logkey, $format, $err)`,
			`base.InfofCtx($ctx, $logkey, $format, $*_, $err)`,
			`base.InfofCtx($ctx, $logkey, $format, $*_, $err, $_)`,
			`base.InfofCtx($ctx, $logkey, $format, $*_, $err, $_, $_)`,
			`base.InfofCtx($ctx, $logkey, $format, $*_, $err, $_, $_, $_)`,
			`base.InfofCtx($ctx, $logkey, $format, $*_, $err, $_, $_, $_, $_)`,
			`base.InfofCtx($ctx, $logkey, $format, $*_, $err, $_, $_, $_, $_, $_)`,
			`InfofCtx($ctx, $logkey, $format, $err)`,
			`InfofCtx($ctx, $logkey, $format, $*_, $err)`,
			`InfofCtx($ctx, $logkey, $format, $*_, $err, $_)`,
			`InfofCtx($ctx, $logkey, $format, $*_, $err, $_, $_)`,
			`InfofCtx($ctx, $logkey, $format, $*_, $err, $_, $_, $_)`,
			`InfofCtx($ctx, $logkey, $format, $*_, $err, $_, $_, $_, $_)`,
			`InfofCtx($ctx, $logkey, $format, $*_, $err, $_, $_, $_, $_, $_)`,
			`base.DebugfCtx($ctx, $logkey, $format, $err)`,
			`base.DebugfCtx($ctx, $logkey, $format, $*_, $err)`,
			`base.DebugfCtx($ctx, $logkey, $format, $*_, $err, $_)`,
			`base.DebugfCtx($ctx, $logkey, $format, $*_, $err, $_, $_)`,
			`base.DebugfCtx($ctx, $logkey, $format, $*_, $err, $_, $_, $_)`,
			`base.DebugfCtx($ctx, $logkey, $format, $*_, $err, $_, $_, $_, $_)`,
			`base.DebugfCtx($ctx, $logkey, $format, $*_, $err, $_, $_, $_, $_, $_)`,
			`DebugfCtx($ctx, $logkey, $format, $err)`,
			`DebugfCtx($ctx, $logkey, $format, $*_, $err)`,
			`DebugfCtx($ctx, $logkey, $format, $*_, $err, $_)`,
			`DebugfCtx($ctx, $logkey, $format, $*_, $err, $_, $_)`,
			`DebugfCtx($ctx, $logkey, $format, $*_, $err, $_, $_, $_)`,
			`DebugfCtx($ctx, $logkey, $format, $*_, $err, $_, $_, $_, $_)`,
			`DebugfCtx($ctx, $logkey, $format, $*_, $err, $_, $_, $_, $_, $_)`,
			`base.TracefCtx($ctx, $logkey, $format, $err)`,
			`base.TracefCtx($ctx, $logkey, $format, $*_, $err)`,
			`base.TracefCtx($ctx, $logkey, $format, $*_, $err, $_)`,
			`base.TracefCtx($ctx, $logkey, $format, $*_, $err, $_, $_)`,
			`base.TracefCtx($ctx, $logkey, $format, $*_, $err, $_, $_, $_)`,
			`base.TracefCtx($ctx, $logkey, $format, $*_, $err, $_, $_, $_, $_)`,
			`base.TracefCtx($ctx, $logkey, $format, $*_, $err, $_, $_, $_, $_, $_)`,
			`TracefCtx($ctx, $logkey, $format, $err)`,
			`TracefCtx($ctx, $logkey, $format, $*_, $err)`,
			`TracefCtx($ctx, $logkey, $format, $*_, $err, $_)`,
			`TracefCtx($ctx, $logkey, $format, $*_, $err, $_, $_)`,
			`TracefCtx($ctx, $logkey, $format, $*_, $err, $_, $_, $_)`,
			`TracefCtx($ctx, $logkey, $format, $*_, $err, $_, $_, $_, $_)`,
			`TracefCtx($ctx, $logkey, $format, $*_, $err, $_, $_, $_, $_, $_)`,
			`fmt.Printf($format, $err)`,
			`fmt.Printf($format, $*_, $err)`,
			`fmt.Printf($format, $*_, $err, $_)`,
			`fmt.Printf($format, $*_, $err, $_, $_)`,
			`fmt.Printf($format, $*_, $err, $_, $_, $_)`,
			`fmt.Printf($format, $*_, $err, $_, $_, $_, $_)`,
			`fmt.Printf($format, $*_, $err, $_, $_, $_, $_, $_)`,
			`log.Printf($format, $err)`,
			`log.Printf($format, $*_, $err)`,
			`log.Printf($format, $*_, $err, $_)`,
			`log.Printf($format, $*_, $err, $_, $_)`,
			`log.Printf($format, $*_, $err, $_, $_, $_)`,
			`log.Printf($format, $*_, $err, $_, $_, $_, $_)`,
			`log.Printf($format, $*_, $err, $_, $_, $_, $_, $_)`,
		).
		Where(
			m["err"].Type.Is("error") &&
				m["format"].Type.Is("string") &&
				m["format"].Text.Matches(`.*%w.*`)).
		Report("cannot use error wrapping verb %w outside of fmt.Errorf() - use %s or %v instead?")
}
