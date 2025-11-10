//go:build !windows
// +build !windows

/*
Copyright 2025-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package rest

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/couchbase/sync_gateway/base"
)

// registerSignalHandlerForStackTrace will register a signal handler to capture stack traces
// - SIGUSR1 causes Sync Gateway to record a stack trace of all running goroutines.
func (sc *ServerContext) registerSignalHandlerForStackTrace(ctx context.Context) {
	signalChannel := make(chan os.Signal, 1)
	signal.Notify(signalChannel, syscall.SIGUSR1)

	defer func() {
		signal.Stop(signalChannel)
		close(signalChannel)
	}()

	go func() {
		select {
		case sig := <-signalChannel:
			base.InfofCtx(ctx, base.KeyAll, "Handling signal: %v", sig)
			switch sig {
			case syscall.SIGUSR1:
				// stack trace signal received
				currentTime := time.Now()
				timestamp := currentTime.Format(time.RFC3339)
				sc.logStackTraces(ctx, timestamp)
			default:
				// unhandled signal here
			}
		case <-ctx.Done():
			return
		}
	}()
}
