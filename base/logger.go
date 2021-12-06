/*
Copyright 2018-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package base

import (
	"log"
	"math"
	"strings"
	"sync"
	"time"
)

var flushLogBuffersWaitGroup sync.WaitGroup
var flushLogMutex sync.Mutex

// FlushLogBuffers will cause all log collation buffers to be flushed to the output before returning.
func FlushLogBuffers() {
	flushLogMutex.Lock()
	defer flushLogMutex.Unlock()

	loggers := []*FileLogger{
		traceLogger,
		debugLogger,
		infoLogger,
		warnLogger,
		errorLogger,
		statsLogger,
		&consoleLogger.FileLogger,
	}

	for _, logger := range loggers {
		if logger != nil && cap(logger.collateBuffer) > 1 {
			logger.collateBufferWg.Wait()
			flushLogBuffersWaitGroup.Add(1)
			logger.flushChan <- struct{}{}
		}
	}

	flushLogBuffersWaitGroup.Wait()
}

// logCollationWorker will take log lines over the given channel, and buffer them until either the buffer is full, or the flushTimeout is exceeded.
// This is to reduce the number of writes to the log files, in order to batch them up as larger collated chunks, whilst maintaining a low-level of latency with the flush timeout.
func logCollationWorker(collateBuffer chan string, flushChan chan struct{}, collateBufferWg *sync.WaitGroup, logger *log.Logger, maxBufferSize int, collateFlushTimeout time.Duration) {

	// The initial duration of the timeout timer doesn't matter,
	// because we reset it whenever we buffer a log without flushing it.
	t := time.NewTimer(math.MaxInt64)
	logBuffer := make([]string, 0, maxBufferSize)

	for {
		select {
		case l := <-collateBuffer:
			logBuffer = append(logBuffer, l)
			collateBufferWg.Done()
			if len(logBuffer) >= maxBufferSize {
				// Flush if the buffer is full after this log
				logger.Print(strings.Join(logBuffer, "\n"))
				logBuffer = logBuffer[:0]
			} else {
				// Start the timeout timer to flush this partial buffer.
				// Note: We don't need to care about stopping the timer as per Go docs,
				// because we're not bothered about a double-firing of the timer,
				// since we check if there's anything to flush first.
				_ = t.Reset(collateFlushTimeout)
			}
		case <-flushChan:
			if len(logBuffer) > 0 {
				// We've sent an explicit "flush now" signal, and want to use a wait group to signal when we've actually performed the flush.
				logger.Print(strings.Join(logBuffer, "\n"))
				logBuffer = logBuffer[:0]
			}
			flushLogBuffersWaitGroup.Done()
		case <-t.C:
			if len(logBuffer) > 0 {
				// We've timed out waiting for more logs to be put into the buffer, so flush it now.
				logger.Print(strings.Join(logBuffer, "\n"))
				logBuffer = logBuffer[:0]
			}
		}
	}
}
