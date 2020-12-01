package db

import (
	"context"
	"fmt"
	"time"

	"github.com/couchbase/sync_gateway/base"
)

type BackgroundTaskFunc func(ctx context.Context) error

// backgroundTask runs task at the specified time interval in its own goroutine until stopped or an error is thrown by
// the BackgroundTaskFunc
func NewBackgroundTask(taskName string, dbName string, task BackgroundTaskFunc, interval time.Duration,
	c chan bool) (bgt BackgroundTask, err error) {
	if interval <= 0 {
		return BackgroundTask{}, &BackgroundTaskError{TaskName: taskName, Interval: interval}
	}
	bgt = BackgroundTask{
		taskName: taskName,
		doneChan: make(chan struct{}),
	}

	base.Infof(base.KeyAll, "Created background task: %q with interval %v", taskName, interval)
	go func() {
		defer close(bgt.doneChan)
		defer base.FatalPanicHandler()
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
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
	return bgt, nil
}

type BackgroundTaskError struct {
	TaskName string
	Interval time.Duration
}

func (err *BackgroundTaskError) Error() string {
	return fmt.Sprintf("Can't create background task: %q with interval %v", err.TaskName, err.Interval)
}

// BackgroundTask contains the name of the background task that is
// initiated and a channel that notifies background task termination.
type BackgroundTask struct {
	taskName string        // Name of the background task.
	doneChan chan struct{} // doneChan is closed when background task is terminated.
}
