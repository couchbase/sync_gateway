// Copyright 2023-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package rest

import (
	"fmt"
	"log"
	"testing"
	"time"

	"github.com/couchbase/sync_gateway/testing/require"
)

// TestChannelTimeout can be increased to support step-through debugging
const TestChannelTimeout = 30 * time.Second

func WaitForChannel(t *testing.T, ch <-chan error, message string) {
	WaitForChannelWithDiagnostic(t, ch, message, nil)
}

// WaitForChannelWithDiagnostic behaves like WaitForChannel, but if the wait times out it calls onTimeout (when non-nil)
// and appends the returned string to the failure message. onTimeout is only evaluated on timeout, so it can render live
// state captured while the wait was in progress (for example a progress snapshot describing what the awaited operation
// was still doing) rather than a static, guessed-at explanation supplied up front.
func WaitForChannelWithDiagnostic(t *testing.T, ch <-chan error, message string, onTimeout func() string) {
	if message != "" {
		log.Printf("[%s] starting wait", message)
		defer func() {
			log.Printf("[%s] completed wait", message)
		}()
	}
	select {
	case err := <-ch:
		if err != nil {
			require.Fail(t, fmt.Sprintf("[%s] channel returned error: %v", message, err))
		}
		return
	case <-time.After(TestChannelTimeout):
		failureMessage := fmt.Sprintf("[%s] expected channel message did not arrive in %v", message, TestChannelTimeout)
		if onTimeout != nil {
			failureMessage += "\n" + onTimeout()
		}
		require.Fail(t, failureMessage)
	}
}

func waitForError(t *testing.T, ch <-chan error, message string) error {
	if message != "" {
		log.Printf("[%s] starting wait for error", message)
		defer func() {
			log.Printf("[%s] completed wait for error", message)
		}()
	}
	select {
	case err := <-ch:
		require.Error(t, err, "[%s] Expected error message on channel", message)
		return err
	case <-time.After(TestChannelTimeout):
		require.Fail(t, fmt.Sprintf("[%s] expected error message did not arrive in %v", message, TestChannelTimeout))
		return nil
	}
}

func notifyChannel(t *testing.T, ch chan<- error, message string) {
	if message != "" {
		log.Printf("[%s] starting notify", message)
		defer func() {
			log.Printf("[%s] completed notify", message)
		}()
	}
	select {
	case ch <- nil:
		return
	case <-time.After(TestChannelTimeout):
		require.Fail(t, fmt.Sprintf("[%s] unable to send channel notification within 10s", message))
	}
}
