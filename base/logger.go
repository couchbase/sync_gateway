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
	"slices"
	"strings"
	"sync"
	"time"
)

var flushLogMutex sync.Mutex

// collateEntry is a unit of work for the log collation worker: a log line to buffer, or - when
// flushed is non-nil - a request to write the buffer out and close flushed once that is done.
// Flush requests travel through the same channel as the log lines, so a flush is guaranteed to
// write out everything that was queued ahead of it.
type collateEntry struct {
	msg     string
	flushed chan struct{}
}

// getFileLoggers returns a slice of all non-nil file loggers.
func getFileLoggers() []*FileLogger {
	loggers := []*FileLogger{
		traceLogger.Load(),
		debugLogger.Load(),
		infoLogger.Load(),
		warnLogger.Load(),
		errorLogger.Load(),
		statsLogger.Load(),
	}
	rawAuditLogger := auditLogger.Load()
	if rawAuditLogger != nil {
		loggers = append(loggers, &rawAuditLogger.FileLogger)
	}
	rawConsoleLogger := consoleLogger.Load()
	if rawConsoleLogger != nil {
		loggers = append(loggers, &rawConsoleLogger.FileLogger)
	}
	return slices.DeleteFunc(loggers, func(l *FileLogger) bool { return l == nil })
}

// FlushLogBuffers will cause all log collation buffers to be flushed to the output before returning.
func FlushLogBuffers() {
	flushLogMutex.Lock()
	defer flushLogMutex.Unlock()

	loggers := getFileLoggers()
	// Queue every flush request before waiting on any of them, so that a logger whose worker is
	// stalled writing its output cannot stop the remaining loggers from being flushed.
	flushed := make([]chan struct{}, len(loggers))
	for i, logger := range loggers {
		flushed[i] = logger.requestFlush()
	}
	for i, logger := range loggers {
		logger.awaitFlush(flushed[i])
	}
}

// logCollationWorker will take log lines over the given channel, and buffer them until either the buffer is full, or the flushTimeout is exceeded.
// This is to reduce the number of writes to the log files, in order to batch them up as larger collated chunks, whilst maintaining a low-level of latency with the flush timeout.
func logCollationWorker(loggerClosed, workerDone chan struct{}, collateBuffer chan collateEntry, logger *log.Logger, maxBufferSize int, collateFlushTimeout time.Duration) {
	defer close(workerDone)

	// The initial duration of the timeout timer doesn't matter,
	// because we reset it whenever we buffer a log without flushing it.
	t := time.NewTimer(math.MaxInt64)
	logBuffer := make([]string, 0, maxBufferSize)
	flush := func() {
		if len(logBuffer) > 0 {
			logger.Print(strings.Join(logBuffer, "\n"))
			logBuffer = logBuffer[:0]
		}
	}

	for {
		select {
		case entry := <-collateBuffer:
			if entry.flushed != nil {
				// An explicit "flush now" request. Closing the channel signals that we've actually
				// performed the flush.
				flush()
				close(entry.flushed)
				continue
			}
			logBuffer = append(logBuffer, entry.msg)
			if len(logBuffer) >= maxBufferSize {
				// Flush if the buffer is full after this log
				flush()
			} else {
				// Start the timeout timer to flush this partial buffer.
				// Note: We don't need to care about stopping the timer as per Go docs,
				// because we're not bothered about a double-firing of the timer,
				// since we check if there's anything to flush first.
				_ = t.Reset(collateFlushTimeout)
			}
		case <-t.C:
			// We've timed out waiting for more logs to be put into the buffer, so flush it now.
			flush()
		case <-loggerClosed:
			// Write out what is still buffered instead of discarding it on shutdown.
			flush()
			return
		}
	}
}
