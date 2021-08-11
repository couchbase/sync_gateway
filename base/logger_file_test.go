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
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var fileShouldLogTests = []struct {
	enabled     int32
	loggerLevel LogLevel
	logToLevel  LogLevel
	loggerKeys  []string
	logToKey    LogKey
	expected    bool
}{
	{
		// Log with matching log level
		enabled:     1,
		loggerLevel: LevelInfo,
		logToLevel:  LevelInfo,
		expected:    true,
	},
	{
		// Log with higher log level
		enabled:     1,
		loggerLevel: LevelInfo,
		logToLevel:  LevelWarn,
		expected:    true,
	},
	{
		// Log with lower log level
		enabled:     1,
		loggerLevel: LevelWarn,
		logToLevel:  LevelInfo,
		expected:    false,
	},
	{
		// Logger disabled (enabled = false)
		enabled:     0,
		loggerLevel: LevelNone,
		logToLevel:  LevelError,
		expected:    false,
	},
	{
		// Logger disabled (LevelNone)
		enabled:     1,
		loggerLevel: LevelNone,
		logToLevel:  LevelInfo,
		expected:    false,
	},
}

func TestFileShouldLog(t *testing.T) {
	for _, test := range fileShouldLogTests {
		name := fmt.Sprintf("logger{%s,%s}.shouldLog(%s,%s)",
			test.loggerLevel.StringShort(), test.loggerKeys,
			test.logToLevel.StringShort(), test.logToKey)

		l := FileLogger{
			Enabled: AtomicBool{test.enabled},
			level:   test.loggerLevel,
			output:  ioutil.Discard,
			logger:  log.New(ioutil.Discard, "", 0),
		}

		t.Run(name, func(ts *testing.T) {
			got := l.shouldLog(test.logToLevel)
			assert.Equal(ts, test.expected, got)
		})
	}
}

func BenchmarkFileShouldLog(b *testing.B) {
	for _, test := range fileShouldLogTests {
		name := fmt.Sprintf("logger{%s,%s}.shouldLog(%s,%s)",
			test.loggerLevel.StringShort(), test.loggerKeys,
			test.logToLevel.StringShort(), test.logToKey)

		l := FileLogger{
			Enabled: AtomicBool{test.enabled},
			level:   test.loggerLevel,
			output:  ioutil.Discard,
			logger:  log.New(ioutil.Discard, "", 0),
		}

		b.Run(name, func(bb *testing.B) {
			for i := 0; i < bb.N; i++ {
				l.shouldLog(test.logToLevel)
			}
		})
	}
}

func TestRotatedLogDeletion(t *testing.T) {
	var dirContents []os.FileInfo

	//Regular Test With multiple files above high and low watermark
	dir, _ := ioutil.TempDir("", "tempdir1")

	err := makeTestFile(2, logFilePrefix+"error-2019-02-01T12-00-00.log.gz", dir)
	assert.NoError(t, err)
	err = makeTestFile(2, logFilePrefix+"error-2019-02-01T12-10-00.log.gz", dir)
	assert.NoError(t, err)
	err = makeTestFile(2, logFilePrefix+"error-2019-02-01T12-20-00.log.gz", dir)
	assert.NoError(t, err)
	err = makeTestFile(2, logFilePrefix+"info-2019-02-01T12-00-00.log.gz", dir)
	assert.NoError(t, err)
	err = makeTestFile(2, logFilePrefix+"info-2019-02-01T12-01-00.log.gz", dir)
	assert.NoError(t, err)
	err = makeTestFile(2, logFilePrefix+"info-2019-02-02T12-00-00.log.gz", dir)
	assert.NoError(t, err)
	err = makeTestFile(2, logFilePrefix+"info-2019-02-02T12-10-00.log.gz", dir)
	assert.NoError(t, err)
	err = runLogDeletion(dir, "error", 3, 5)
	assert.NoError(t, err)
	err = runLogDeletion(dir, "info", 5, 7)
	assert.NoError(t, err)
	dirContents, err = ioutil.ReadDir(dir)
	require.Len(t, dirContents, 3)

	var fileNames = []string{}

	for fileIndex := range dirContents {
		fileNames = append(fileNames, dirContents[fileIndex].Name())
	}

	assert.Contains(t, fileNames, logFilePrefix+"error-2019-02-01T12-20-00.log.gz")
	assert.Contains(t, fileNames, logFilePrefix+"info-2019-02-02T12-00-00.log.gz")
	assert.Contains(t, fileNames, logFilePrefix+"info-2019-02-02T12-10-00.log.gz")

	assert.NoError(t, os.RemoveAll(dir))

	//Hit low watermark but not high watermark
	dir, _ = ioutil.TempDir("", "tempdir2")
	err = makeTestFile(3, logFilePrefix+"error.log.gz", dir)
	assert.NoError(t, err)
	err = runLogDeletion(dir, "error", 2, 4)
	assert.NoError(t, err)
	dirContents, err = ioutil.ReadDir(dir)
	require.Len(t, dirContents, 1)
	assert.NoError(t, os.RemoveAll(dir))

	//Single file hitting low and high watermark
	dir, _ = ioutil.TempDir("", "tempdir3")
	err = makeTestFile(5, logFilePrefix+"error.log.gz", dir)
	assert.NoError(t, err)
	err = runLogDeletion(dir, "error", 2, 4)
	assert.NoError(t, err)
	dirContents, err = ioutil.ReadDir(dir)
	assert.Empty(t, dirContents)
	assert.NoError(t, os.RemoveAll(dir))

	//Not hitting low or high therefore no deletion
	dir, _ = ioutil.TempDir("", "tempdir4")
	err = makeTestFile(1, logFilePrefix+"error.log.gz", dir)
	assert.NoError(t, err)
	err = runLogDeletion(dir, "error", 2, 4)
	assert.NoError(t, err)
	dirContents, err = ioutil.ReadDir(dir)
	require.Len(t, dirContents, 1)
	assert.NoError(t, os.RemoveAll(dir))

	//Test deletion with files at the end of date boundaries
	dir, _ = ioutil.TempDir("", "tempdir5")
	err = makeTestFile(1, logFilePrefix+"error-2018-12-31T23-59-59.log.gz", dir)
	assert.NoError(t, err)
	err = makeTestFile(1, logFilePrefix+"error-2019-01-01T00-00-00.log.gz", dir)
	assert.NoError(t, err)
	err = makeTestFile(1, logFilePrefix+"error-2019-01-31T23-59-59.log.gz", dir)
	assert.NoError(t, err)
	err = makeTestFile(1, logFilePrefix+"error-2019-01-01T12-00-00.log.gz", dir)
	assert.NoError(t, err)
	err = runLogDeletion(dir, "error", 2, 3)
	assert.NoError(t, err)

	dirContents, err = ioutil.ReadDir(dir)
	require.Len(t, dirContents, 2)

	fileNames = []string{}
	for fileIndex := range dirContents {
		fileNames = append(fileNames, dirContents[fileIndex].Name())
	}

	assert.Contains(t, fileNames, logFilePrefix+"error-2019-01-01T12-00-00.log.gz")
	assert.Contains(t, fileNames, logFilePrefix+"error-2019-01-31T23-59-59.log.gz")

	assert.NoError(t, os.RemoveAll(dir))

	//Test deletion with no .gz files to ensure nothing is deleted
	dir, _ = ioutil.TempDir("", "tempdir6")
	err = makeTestFile(1, logFilePrefix+"error", dir)
	assert.NoError(t, err)
	err = makeTestFile(1, logFilePrefix+"info", dir)
	assert.NoError(t, err)

	dirContents, err = ioutil.ReadDir(dir)
	require.Len(t, dirContents, 2)

	fileNames = []string{}
	for fileIndex := range dirContents {
		fileNames = append(fileNames, dirContents[fileIndex].Name())
	}

	assert.Contains(t, fileNames, logFilePrefix+"error")
	assert.Contains(t, fileNames, logFilePrefix+"info")

	assert.NoError(t, os.RemoveAll(dir))
}

func makeTestFile(sizeMB int, name string, dir string) (err error) {
	f, err := os.Create(filepath.Join(dir, name))
	if err != nil {
		return err
	}
	if err := f.Truncate(int64(sizeMB * 1024 * 1024)); err != nil {
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return nil
}
