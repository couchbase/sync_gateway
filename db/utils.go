package db

import (
	"context"
	"time"

	"github.com/couchbase/sync_gateway/base"
)

type BackgroundTaskFunc func(ctx context.Context) error

// backgroundTask runs task at the specified time interval in its own goroutine until stopped or an error is thrown by
// the BackgroundTaskFunc
func NewBackgroundTask(taskName string, dbName string, task BackgroundTaskFunc, interval time.Duration, c chan bool) {
	base.Infof(base.KeyAll, "Created background task: %q with interval %v", taskName, interval)
	go func() {
		defer base.FatalPanicHandler()
		for {
			select {
			case <-time.After(interval):
				ctx := context.WithValue(context.Background(), base.LogContextKey{}, base.LogContext{CorrelationID: base.NewTaskID(dbName, taskName)})
				if err := task(ctx); err != nil {
					base.ErrorfCtx(ctx, "Background task returned error: %v", err)
					return
				}
			case <-c:
				base.Debugf(base.KeyAll, "Terminating background task: %q", taskName)
				return
			}
		}
	}()
}
