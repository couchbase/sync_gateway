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

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/couchbase/sync_gateway/base"
)

// RegisterSignalHandler invokes functions based on the given signals for windows environments:
// - SIGHUP causes Sync Gateway to rotate log files.
// - SIGINT or SIGTERM causes Sync Gateway to exit cleanly.
// - SIGKILL cannot be handled by the application.
func RegisterSignalHandler(ctx context.Context, logDirectory string) chan os.Signal {
	signalChannel := make(chan os.Signal, 1)
	signal.Notify(signalChannel, syscall.SIGHUP, os.Interrupt, syscall.SIGTERM)

	go func() {
		for sig := range signalChannel {
			base.InfofCtx(ctx, base.KeyAll, "Handling signal: %v", sig)
			switch sig {
			case syscall.SIGHUP:
				HandleSighup(ctx)
			default:
				// Ensure log buffers are flushed before exiting.
				base.FlushLogBuffers()
				os.Exit(130) // 130 == exit code 128 + 2 (interrupt)
			}
		}
	}()
	return signalChannel
}
