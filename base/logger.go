package base

import (
	"fmt"
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

func runLogDeletion(logDirectory string, logLevel string, maxSize int) {

	maxSize = maxSize * 1048576 //Convert MB input to bytes

	logDirectory = logDirectory + "/"

	logDiff := 0 - maxSize

	files, err := ioutil.ReadDir(logDirectory)
	if err != nil {
		fmt.Println(err.Error())
	}

	for _, f := range files {
		var extension = filepath.Ext(logDirectory + f.Name())
		if extension == ".gz" && strings.Contains(f.Name(), logLevel) {
			logDiff += int(f.Size())
		}
	}

	var modTime time.Time
	var oldestFile string
	var fileSize int
	for logDiff > 0 {
		files, _ = ioutil.ReadDir(logDirectory)
		for _, f := range files {
			var extension = filepath.Ext(logDirectory + f.Name())
			if strings.Contains(f.Name(), logLevel) && extension == ".gz" {
				if modTime.Before(f.ModTime()) || modTime.IsZero() {
					modTime = f.ModTime()
					oldestFile = f.Name()
					fileSize = int(f.Size())
				}
			}
		}
		logDiff = logDiff - fileSize
		os.Remove(logDirectory + oldestFile)
		modTime = time.Time{}
	}

}
