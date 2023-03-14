package base

import (
	"bytes"
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

const standardMessage = "foobar"

// RequireLogIs asserts that the logs produced by function f contain string s.
func requireLogIs(t testing.TB, s string, f func()) {
	b := bytes.Buffer{}

	const timestampLength = 30

	// Temporarily override logger output for the given function call
	consoleLogger.logger.SetOutput(&b)
	f()
	var log string
	// Allow time for logs to be printed
	retry := func() (shouldRetry bool, err error, value interface{}) {
		log = b.String()
		if len(log) < timestampLength {
			return false, nil, nil
		}
		log = b.String()[timestampLength:]
		if log == s {
			return false, nil, nil
		}
		return true, nil, nil
	}
	err, _ := RetryLoop("wait for logs", retry, CreateSleeperFunc(10, 1000))
	consoleLogger.logger.SetOutput(os.Stderr)

	require.NoError(t, err, "Console logs did not contain %q, got %q", s, log)
}

func RequireLogMessage(t testing.TB, ctx context.Context, expectedMessage string, logString string) {
	requireLogIs(t, expectedMessage, func() { InfofCtx(ctx, KeyAll, standardMessage) })
}

func TestLogFormat(t *testing.T) {
	testCases := []struct {
		name   string
		ctx    context.Context
		output string
	}{
		{
			name:   "background context",
			ctx:    context.Background(),
			output: "[INF] foobar\n",
		},
		{
			name:   "empty LogContext context",
			ctx:    LogContextWith(context.Background(), &LogContext{}),
			output: "[INF] foobar\n",
		},
		{
			name:   "test context",
			ctx:    TestCtx(t),
			output: "[INF] t:TestLogFormat foobar\n",
		},
		{
			name: "bucket only no database",
			ctx: LogContextWith(context.Background(), &LogContext{
				Bucket: "testBucket",
			}),
			output: "[INF] b:testBucket foobar\n",
		},
		{
			name:   "test and bucket only no database",
			ctx:    bucketNameCtx(TestCtx(t), "testBucket"),
			output: "[INF] t:TestLogFormat b:testBucket foobar\n",
		},

		{
			name: "full keyspace, no database",
			ctx: LogContextWith(context.Background(), &LogContext{
				Bucket:     "testBucket",
				Collection: "testCollection",
				Scope:      "testScope",
			}),
			output: "[INF] b:testBucket.testScope.testCollection foobar\n",
		},
		{
			name: "partial keyspace, no database",
			ctx: LogContextWith(context.Background(), &LogContext{
				Bucket:     "testBucket",
				Collection: "testCollection",
			}),
			output: "[INF] b:testBucket.testCollection foobar\n",
		},
		{
			name: "database only",
			ctx: LogContextWith(context.Background(), &LogContext{
				Database: "dbName",
			}),
			output: "[INF] db:dbName foobar\n",
		},
		{
			name: "database and bucket",
			ctx: LogContextWith(context.Background(), &LogContext{
				Bucket:   "testBucket",
				Database: "dbName",
			}),
			output: "[INF] db:dbName foobar\n",
		},
		{
			name: "database and collection",
			ctx: LogContextWith(context.Background(), &LogContext{
				Database:   "dbName",
				Collection: "testCollection",
			}),
			output: "[INF] db:dbName col:testCollection foobar\n",
		},
		{
			name: "database, scope, and collection",
			ctx: LogContextWith(context.Background(), &LogContext{
				Database:   "dbName",
				Scope:      "scopeName",
				Collection: "testCollection",
			}),
			output: "[INF] db:dbName col:testCollection foobar\n",
		},

		{
			name: "database, scope, collection, and bucket",
			ctx: LogContextWith(context.Background(), &LogContext{
				Bucket:     "testBucket",
				Collection: "testCollection",
				Database:   "dbName",
				Scope:      "ScopeName",
			}),
			output: "[INF] db:dbName col:testCollection foobar\n",
		},
	}
	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {

			RequireLogMessage(t, test.ctx, test.output, standardMessage)
		})
	}
	RequireLogMessage(t, context.Background(), "[INF] foobar\n", standardMessage)
}
