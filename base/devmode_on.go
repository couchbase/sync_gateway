//go:build cb_sg_devmode
// +build cb_sg_devmode

// Copyright 2024-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package base

import (
	"context"
	"fmt"
)

const cbSGDevModeBuildTagSet = true

var assertLogFn logFn = PanicfCtx

func panicRecoveryLogFn(ctx context.Context, format string, args ...any) {
	// no-op call to enable golang.org/x/tools/go/analysis/passes/printf checking in go vet
	if false {
		_ = fmt.Sprintf(format, args...)
	}
	// add a warn count since the devmode=off path also adds a warn count
	SyncGatewayStats.GlobalStats.ResourceUtilization.WarnCount.Add(1)
	PanicfCtx(ctx, format, args...)
}
