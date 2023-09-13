// Copyright 2023-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package base

import (
	"bytes"
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const standardMessage = "foobar"

// RequireLogIs asserts that the logs produced by function f contain string s.
func requireLogIs(t testing.TB, s string, f func()) {
	b := bytes.Buffer{}

	timestampLength := len(time.Now().Format(ISO8601Format) + " ")

	// Temporarily override logger output for the given function call
	originalColor := consoleLogger.ColorEnabled
	consoleLogger.ColorEnabled = false
	consoleLogger.logger.SetOutput(&b)
	defer func() {
		consoleLogger.ColorEnabled = originalColor
		consoleLogger.logger.SetOutput(os.Stderr)
	}()

	f()

	var log string
	var originalLog string
	// Allow time for logs to be printed
	retry := func() (shouldRetry bool, err error, value interface{}) {
		originalLog = b.String()
		if len(originalLog) < timestampLength {
			return false, nil, nil
		}
		log = originalLog[timestampLength:]
		if log == s {
			return false, nil, nil
		}
		return true, nil, nil
	}
	err, _ := RetryLoop(TestCtx(t), "wait for logs", retry, CreateSleeperFunc(10, 100))

	require.NoError(t, err, "Console logs did not contain %q, got %q", s, originalLog)
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
			// only show collection because bucket and scope are implied by db
			output: "[INF] db:dbName col:testCollection foobar\n",
		},
		{
			name: "test, database, scope, collection, and bucket",
			ctx: LogContextWith(context.Background(), &LogContext{
				TestName:   "aTest",
				Bucket:     "testBucket",
				Collection: "testCollection",
				Database:   "dbName",
				Scope:      "ScopeName",
			}),
			// only show collection because bucket and scope are implied by db
			output: "[INF] t:aTest db:dbName col:testCollection foobar\n",
		},
		{
			name: "test, database, scope, collection, and bucket",
			ctx: LogContextWith(context.Background(), &LogContext{
				TestName:   "aTest",
				Bucket:     "testBucket",
				Collection: "testCollection",
				Database:   "dbName",
				Scope:      "ScopeName",
			}),
			// only show collection because bucket and scope are implied by db
			output: "[INF] t:aTest db:dbName col:testCollection foobar\n",
		},
		{
			name: "test, database, scope, collection, bucket, correlation",
			ctx: LogContextWith(context.Background(), &LogContext{
				TestName:      "aTest",
				Bucket:        "testBucket",
				Collection:    "testCollection",
				Database:      "dbName",
				Scope:         "ScopeName",
				CorrelationID: "correlated",
			}),
			// only show collection because bucket and scope are implied by db
			output: "[INF] t:aTest c:correlated db:dbName col:testCollection foobar\n",
		},
	}
	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {

			RequireLogMessage(t, test.ctx, test.output, standardMessage)
		})
	}
	RequireLogMessage(t, context.Background(), "[INF] foobar\n", standardMessage)
}

func TestTestCtx(t *testing.T) {
	RequireLogMessage(t, TestCtx(t), "[INF] t:TestTestCtx foobar\n", standardMessage)
}

func TestBucketNameCtx(t *testing.T) {
	RequireLogMessage(t, bucketNameCtx(TestCtx(t), "fooBucket"), "[INF] t:TestBucketNameCtx b:fooBucket foobar\n", standardMessage)
}

// Tests the typical request workflow for a database request. Makes sure each level of context does not modify earlier levels.
func TestCollectionLogCtx(t *testing.T) {
	ctx := TestCtx(t)
	collectionCtx := CollectionLogCtx(ctx, "fooCollection")
	RequireLogMessage(t, collectionCtx, "[INF] t:TestCollectionLogCtx col:fooCollection foobar\n", standardMessage)
}

func TestCtxWorkflow(t *testing.T) {
	ctx := TestCtx(t)
	RequireLogMessage(t, ctx, "[INF] t:TestCtxWorkflow foobar\n", standardMessage)

	correlationCtx := CorrelationIDLogCtx(ctx, "correlationID")
	RequireLogMessage(t, correlationCtx, "[INF] t:TestCtxWorkflow c:correlationID foobar\n", standardMessage)
	RequireLogMessage(t, ctx, "[INF] t:TestCtxWorkflow foobar\n", standardMessage)

	bucketCtx := bucketNameCtx(correlationCtx, "fooBucket")
	RequireLogMessage(t, bucketCtx, "[INF] t:TestCtxWorkflow c:correlationID b:fooBucket foobar\n", standardMessage)
	RequireLogMessage(t, correlationCtx, "[INF] t:TestCtxWorkflow c:correlationID foobar\n", standardMessage)
	RequireLogMessage(t, ctx, "[INF] t:TestCtxWorkflow foobar\n", standardMessage)

	keyspaceCtx := KeyspaceLogCtx(bucketCtx, "fooBucket", "fooScope", "fooCollection")
	RequireLogMessage(t, keyspaceCtx, "[INF] t:TestCtxWorkflow c:correlationID b:fooBucket.fooScope.fooCollection foobar\n", standardMessage)
	RequireLogMessage(t, bucketCtx, "[INF] t:TestCtxWorkflow c:correlationID b:fooBucket foobar\n", standardMessage)
	RequireLogMessage(t, correlationCtx, "[INF] t:TestCtxWorkflow c:correlationID foobar\n", standardMessage)
	RequireLogMessage(t, ctx, "[INF] t:TestCtxWorkflow foobar\n", standardMessage)

	databaseCtx := DatabaseLogCtx(keyspaceCtx, "fooDB", nil)
	RequireLogMessage(t, databaseCtx, "[INF] t:TestCtxWorkflow c:correlationID db:fooDB col:fooCollection foobar\n", standardMessage)
	RequireLogMessage(t, keyspaceCtx, "[INF] t:TestCtxWorkflow c:correlationID b:fooBucket.fooScope.fooCollection foobar\n", standardMessage)
	RequireLogMessage(t, bucketCtx, "[INF] t:TestCtxWorkflow c:correlationID b:fooBucket foobar\n", standardMessage)
	RequireLogMessage(t, correlationCtx, "[INF] t:TestCtxWorkflow c:correlationID foobar\n", standardMessage)
	RequireLogMessage(t, ctx, "[INF] t:TestCtxWorkflow foobar\n", standardMessage)

}
