package base

import (
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
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

//runLogDeletion will delete rotated logs for the supplied logLevel. It will only perform these deletions when the
//cumulative size of the logs are above the supplied sizeLimitMB.
//logDirectory is the supplied directory where the logs are stored.
func runLogDeletion(logDirectory string, logLevel string, sizeLimitMB int) {

	sizeLimitMB = sizeLimitMB * 1024 * 1024 //Convert MB input to bytes
	files, err := ioutil.ReadDir(logDirectory)

	if err != nil {
		Errorf(KeyAll, "Error reading log directory: %v", err)
	}

	totalSize := 0
	indexDeletePoint := -1
	for i := len(files) - 1; i >= 0; i-- {
		file := files[i]
		if strings.Contains(file.Name(), logLevel) && strings.HasSuffix(file.Name(), ".gz") {
			totalSize += int(file.Size())
			if totalSize > sizeLimitMB {
				indexDeletePoint = i
			}
			if i <= indexDeletePoint {
				err = os.Remove(filepath.Join(logDirectory, file.Name()))
				if err != nil {
					Errorf(KeyAll, "Error deleting stale log file: %v", err)
				}
			}
		}
	}
}
