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
	"io"
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
			output:  io.Discard,
			logger:  log.New(io.Discard, "", 0),
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
			output:  io.Discard,
			logger:  log.New(io.Discard, "", 0),
		}

		b.Run(name, func(bb *testing.B) {
			for i := 0; i < bb.N; i++ {
				l.shouldLog(test.logToLevel)
			}
		})
	}
}

func TestRotatedLogDeletion(t *testing.T) {
	type logFile struct {
		name string
		size int
	}
	testCases := []struct {
		name              string
		preFiles          []logFile
		postFileNames     []string
		logDeletionPrefix string
		lowWatermark      int
		highWatermark     int
	}{
		{
			name: "error-deletion",
			preFiles: []logFile{
				logFile{size: 2, name: "error-2019-02-01T12-00-00.000.log.gz"},
				logFile{size: 2, name: "error-2019-02-01T12-10-00.000.log.gz"},
				logFile{size: 2, name: "error-2019-02-01T12-20-00.000.log.gz"},
				logFile{size: 2, name: "info-2019-02-01T12-00-00.000.log.gz"},
				logFile{size: 2, name: "info-2019-02-01T12-01-00.000.log.gz"},
				logFile{size: 2, name: "info-2019-02-02T12-00-00.000.log.gz"},
				logFile{size: 2, name: "info-2019-02-02T12-10-00.000.log.gz"},
			},
			logDeletionPrefix: "error.log",
			lowWatermark:      3,
			highWatermark:     5,
			postFileNames: []string{
				"error-2019-02-01T12-20-00.000.log.gz",
				"info-2019-02-01T12-00-00.000.log.gz",
				"info-2019-02-01T12-01-00.000.log.gz",
				"info-2019-02-02T12-00-00.000.log.gz",
				"info-2019-02-02T12-10-00.000.log.gz",
			},
		},
		{
			name: "info-deletion",
			preFiles: []logFile{
				logFile{size: 2, name: "error-2019-02-01T12-00-00.000.log.gz"},
				logFile{size: 2, name: "error-2019-02-01T12-10-00.000.log.gz"},
				logFile{size: 2, name: "error-2019-02-01T12-20-00.000.log.gz"},
				logFile{size: 2, name: "info-2019-02-01T12-00-00.000.log.gz"},
				logFile{size: 2, name: "info-2019-02-01T12-01-00.000.log.gz"},
				logFile{size: 2, name: "info-2019-02-02T12-00-00.000.log.gz"},
				logFile{size: 2, name: "info-2019-02-02T12-10-00.000.log.gz"},
			},
			logDeletionPrefix: "info.log",
			lowWatermark:      5,
			highWatermark:     7,
			postFileNames: []string{
				"error-2019-02-01T12-00-00.000.log.gz",
				"error-2019-02-01T12-10-00.000.log.gz",
				"error-2019-02-01T12-20-00.000.log.gz",
				"info-2019-02-02T12-00-00.000.log.gz",
				"info-2019-02-02T12-10-00.000.log.gz",
			},
		},
		{
			name: "low but not high watermark",
			preFiles: []logFile{
				logFile{size: 3, name: "error"},
			},
			logDeletionPrefix: "error",
			lowWatermark:      2,
			highWatermark:     4,
			postFileNames: []string{
				"error",
			},
		},
		{
			name: "high and low watermark",
			preFiles: []logFile{
				logFile{size: 5, name: "error"},
			},
			logDeletionPrefix: "error",
			lowWatermark:      2,
			highWatermark:     4,
			postFileNames:     []string{},
		},
		{
			name: "date boundary deletion",
			preFiles: []logFile{
				logFile{size: 1, name: "error-2018-12-31T23-59-59.000.log.gz"},
				logFile{size: 1, name: "error-2019-01-01T00-00-00.000.log.gz"},
				logFile{size: 1, name: "error-2019-01-31T23-59-59.000.log.gz"},
				logFile{size: 1, name: "error-2019-01-01T12-00-00.000.log.gz"},
			},
			logDeletionPrefix: "error.log",
			lowWatermark:      2,
			highWatermark:     3,
			postFileNames: []string{
				"error-2019-01-01T12-00-00.000.log.gz",
				"error-2019-01-31T23-59-59.000.log.gz",
			},
		},
		{
			name: "base .log and .gz logs",
			preFiles: []logFile{
				logFile{size: 1, name: "error.log"},
				logFile{size: 4, name: "error-2019-01-01T00-00-00.000.log.gz"},
			},
			logDeletionPrefix: "error.log",
			lowWatermark:      2,
			highWatermark:     3,
			postFileNames: []string{
				"error.log",
			},
		},
		{
			name: "base .log, gunzipped file and .gz logs",
			preFiles: []logFile{
				logFile{size: 1, name: "error.log"},
				logFile{size: 4, name: "error-2019-01-01T00-00-00.000.log"},
				logFile{size: 4, name: "error-2019-01-02T00-00-00.000.log.gz"},
			},
			logDeletionPrefix: "error.log",
			lowWatermark:      2,
			highWatermark:     3,
			postFileNames: []string{
				"error.log",
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {

			dir := t.TempDir()

			for _, f := range tc.preFiles {
				require.NoError(t, makeTestFile(f.size, f.name, dir))
			}

			ctx := TestCtx(t)
			logFilePath := filepath.Join(dir, tc.logDeletionPrefix)
			foundDir, logPattern := getDeletionDirAndRegexp(logFilePath)
			require.Equal(t, filepath.Clean(dir), filepath.Clean(foundDir))

			require.NoError(t, runLogDeletion(ctx, dir, logPattern, tc.lowWatermark, tc.highWatermark))
			require.ElementsMatch(t, tc.postFileNames, getDirFiles(t, dir))

		})
	}
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

func getDirFiles(t *testing.T, dir string) []string {
	dirContents, err := os.ReadDir(dir)
	require.NoError(t, err)

	var fileNames []string
	for _, file := range dirContents {
		fileNames = append(fileNames, file.Name())
	}
	return fileNames
}
