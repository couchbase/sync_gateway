package rest

import (
	"fmt"
	"log"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestChannelTimeout can be increased to support step-through debugging
const TestChannelTimeout = 30 * time.Second

func WaitForChannel(t *testing.T, ch <-chan error, message string) {
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
		require.Fail(t, fmt.Sprintf("[%s] expected channel message did not arrive in %v", message, TestChannelTimeout))
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
		if err == nil {
			require.Fail(t, "[%s] Received non-error message on channel", message)
		}
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
