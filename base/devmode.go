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
	"slices"
	"sync"
)

const (
	AssertionFailedPrefix = "Assertion failed: "
)

// assertionFailure is a single recorded assertion failure, and the name of the test that was running when it happened, when known.
type assertionFailure struct {
	testName string
	msg      string
}

// assertionFailures collects assertion failures in dev mode, so the test harness can report them in detail at the end of a run.
var assertionFailures = struct {
	lock     sync.Mutex
	failures []assertionFailure
}{}

// recordAssertionFailure stores an assertion failure message, along with the name of the test in ctx, if there is one. Safe for concurrent use.
func recordAssertionFailure(ctx context.Context, msg string) {
	failure := assertionFailure{testName: getLogCtx(ctx).TestName, msg: msg}
	assertionFailures.lock.Lock()
	defer assertionFailures.lock.Unlock()
	assertionFailures.failures = append(assertionFailures.failures, failure)
}

// getAssertionFailures returns a copy of the assertion failures recorded so far.
func getAssertionFailures() []assertionFailure {
	assertionFailures.lock.Lock()
	defer assertionFailures.lock.Unlock()
	return slices.Clone(assertionFailures.failures)
}

// AssertionFailures returns the assertion failure messages recorded so far, prefixed with the test that recorded them where known. Always empty unless compiled with the `cb_sg_devmode` build tag.
func AssertionFailures() []string {
	failures := getAssertionFailures()
	msgs := make([]string, 0, len(failures))
	for _, failure := range failures {
		if failure.testName == "" {
			msgs = append(msgs, failure.msg)
			continue
		}
		msgs = append(msgs, failure.testName+": "+failure.msg)
	}
	return msgs
}

// ClearAssertionFailures discards the recorded assertion failures, for tests that trigger assertions deliberately.
func ClearAssertionFailures() {
	assertionFailures.lock.Lock()
	defer assertionFailures.lock.Unlock()
	assertionFailures.failures = nil
}

// IsDevMode returns true when compiled with the `cb_sg_devmode` build tag
func IsDevMode() bool {
	return cbSGDevModeBuildTagSet
}

// AssertfCtx logs an error message and continues execution, or when compiled with the `cb_sg_devmode` build tag panics for better dev-time visibility.
// In dev mode the message is also recorded, and the SG test harness fails the run if any assertion failures were recorded.
// Note: Callers MUST ensure code is safe to continue executing after the Assert (e.g. by returning an error) and MUST NOT be used like a panic that will halt.
func AssertfCtx(ctx context.Context, format string, args ...any) {

	SyncGatewayStats.GlobalStats.ResourceUtilization.AssertionFailCount.Add(1)
	if IsDevMode() {
		recordAssertionFailure(ctx, fmt.Sprintf(AssertionFailedPrefix+format, args...))
	}
	assertLogFn(ctx, AssertionFailedPrefix+format, args...)
}

// PanicRecoveryfCtx logs a warning message. This function is suitable for recovering from a panic in a location where
// it is expected to continue operation, like HTTP handlers.
// When compiled with the `cb_sg_devmode` build tag this function panics to fail the test harness for better dev-time visibility.
// In all cases, the WarnCount stat is incremented.
func PanicRecoveryfCtx(ctx context.Context, format string, args ...any) {
	panicRecoveryLogFn(ctx, format, args...)
}
