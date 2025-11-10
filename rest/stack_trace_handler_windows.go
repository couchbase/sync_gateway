//go:build windows
// +build windows

/*
Copyright 2025-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package rest

// registerSignalHandlerForStackTrace will register a signal handler to capture stack traces
// - SIGUSR1 causes Sync Gateway to record a stack trace of all running goroutines.
func (sc *ServerContext) registerSignalHandlerForStackTrace(ctx context.Context) {
	// No-op on Windows
}
