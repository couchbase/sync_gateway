package base

import (
	"log"
	"math"
	"strings"
	"time"
)

// FlushLogBuffers will cause all log collation buffers to be flushed to the output.
func FlushLogBuffers() {
	time.Sleep(loggerCollateFlushDelay)
}

// logCollationWorker will take log lines over the given channel, and buffer them until either the buffer is full, or the flushTimeout is exceeded.
// This is to reduce the number of writes to the log files, in order to batch them up as larger collated chunks, whilst maintaining a low-level of latency with the flush timeout.
func logCollationWorker(collateBuffer chan string, logger *log.Logger, maxBufferSize int, collateFlushTimeout time.Duration) {

	// The initial duration of the timeout timer doesn't matter,
	// because we start it whenever we buffer a log without flushing it.
	t := time.NewTimer(math.MaxInt64)
	logBuffer := make([]string, 0, maxBufferSize)

	for {
		select {
		case l := <-collateBuffer:
			logBuffer = append(logBuffer, l)
			if len(logBuffer) >= maxBufferSize {
				// flush if the buffer is full after this log
				logger.Print(strings.Join(logBuffer, "\n"))
				logBuffer = logBuffer[:0]
			} else {
				// Start the timeout timer going to flush this partial buffer
				_ = t.Reset(collateFlushTimeout)
			}
		case <-t.C:
			if len(logBuffer) > 0 {
				// We've timed out waiting for more logs to be put into the buffer, so flush it now.
				logger.Print(strings.Join(logBuffer, "\n"))
				logBuffer = logBuffer[:0]
			}
		}
	}
}
