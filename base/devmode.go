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
)

const (
	AssertionFailedPrefix = "Assertion failed: "
)

// IsDevMode returns true when compiled with the `cb_sg_devmode` build tag
func IsDevMode() bool {
	return cbSGDevModeBuildTagSet
}

// AssertfCtx logs an error message and continues execution, or when compiled with the `cb_sg_devmode` build tag panics for better dev-time visibility.
// The SG test harness will ensure AssertionFailCount is zero at the end of tests, even without devmode enabled.
// Note: Callers MUST ensure code is safe to continue executing after the Assert (e.g. by returning an error) and MUST NOT be used like a panic that will halt.
func AssertfCtx(ctx context.Context, format string, args ...any) {

	SyncGatewayStats.GlobalStats.ResourceUtilization.AssertionFailCount.Add(1)
	assertLogFn(ctx, AssertionFailedPrefix+format, args...)
}

// PanicRecoveryfCtx logs a warning message. This function is suitable for recovering from a panic in a location where
// it is expected to continue operation, like HTTP handlers.
// When compiled with the `cb_sg_devmode` build tag this function panics to fail the test harness for better dev-time visibility.
// In all cases, the ErrorCount stat is incremented.
func PanicRecoveryfCtx(ctx context.Context, format string, args ...any) {
	SyncGatewayStats.GlobalStats.ResourceUtilization.ErrorCount.Add(1)
	panicRecoveryLogFn(ctx, format, args...)
}
