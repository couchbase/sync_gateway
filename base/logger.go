package base

import (
	"log"
	"strings"
	"time"
)

// FlushLogBuffers will cause all log collation buffers to be flushed to the output.
func FlushLogBuffers() {
	time.Sleep(loggerCollateFlushDelay)
}

func logCollationWorker(collateBuffer chan string, logger *log.Logger, bufferSize int) {
	// This is the temporary buffer we'll store logs in.
	logBuffer := []string{}
	for {
		select {
		// Add log to buffer and flush to output if it's full.
		case l := <-collateBuffer:
			logBuffer = append(logBuffer, l)
			if len(logBuffer) >= bufferSize {
				logger.Print(strings.Join(logBuffer, "\n"))
				// Empty buffer
				logBuffer = logBuffer[:0]
			}
		// Flush the buffer to the output after this time, even if we don't fill it.
		case <-time.After(loggerCollateFlushTimeout):
			if len(logBuffer) > 0 {
				logger.Print(strings.Join(logBuffer, "\n"))
				// Empty buffer
				logBuffer = logBuffer[:0]
			}
		}
	}
}
