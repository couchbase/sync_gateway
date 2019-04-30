package db

import (
	"time"

	"github.com/couchbase/sync_gateway/base"
	"golang.org/x/net/context"
)

type BackgroundTaskFunc func(ctx context.Context) error

// backgroundTask runs task at the specified time interval in its own goroutine until stopped or an error is thrown by
// the BackgroundTaskFunc
func NewBackgroundTask(name string, contextID string, task BackgroundTaskFunc, interval time.Duration, c chan bool) {
	base.Infof(base.KeyAll, "Created background task: %q with interval %v", name, interval)
	go func() {
		for {
			select {
			case <-time.After(interval):
				ctx := context.WithValue(context.Background(), base.LogContextKey{}, base.LogContext{CorrelationID: base.NewTaskID(contextID, name)})
				base.DebugfCtx(ctx, base.KeyAll, "Running background task: %q", name)
				if err := task(ctx); err != nil {
					base.ErrorfCtx(ctx, base.KeyAll, "Background task %q returned error: %v", name, err)
					return
				}
			case <-c:
				base.Debugf(base.KeyAll, "Terminating background task: %q", name)
				return
			}
		}
	}()
}
