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

// RegisterSignalHandler invokes functions based on the given signals for unix environments:
// - SIGHUP causes Sync Gateway to rotate log files.
// - SIGINT or SIGTERM causes Sync Gateway to exit cleanly.
// - SIGKILL cannot be handled by the application.
// - SIGUSR1 causes Sync Gateway to log stack traces for all goroutines.
func RegisterSignalHandler(ctx context.Context, logDirectory string) chan os.Signal {
	signalChannel := make(chan os.Signal, 1)
	signal.Notify(signalChannel, syscall.SIGHUP, os.Interrupt, syscall.SIGTERM, syscall.SIGUSR1)

	go func() {
		for sig := range signalChannel {
			base.InfofCtx(ctx, base.KeyAll, "Handling signal: %v", sig)
			switch sig {
			case syscall.SIGHUP:
				HandleSighup(ctx)
			case syscall.SIGUSR1:
				stackTrace, err := base.GetStackTrace()
				if err != nil {
					base.WarnfCtx(ctx, "Error collecting stack trace: %v", err)
				} else {
					base.InfofCtx(ctx, base.KeyAll, "Collecting stack trace for all goroutines")
				}
				// log to console and log to file in the log directory
				currentTime := time.Now()
				timestamp := currentTime.Format(time.RFC3339)
				base.LogStackTraces(ctx, logDirectory, stackTrace, timestamp)
			default:
				// Ensure log buffers are flushed before exiting.
				base.FlushLogBuffers()
				os.Exit(130) // 130 == exit code 128 + 2 (interrupt)
			}
		}
	}()
	return signalChannel
}
