/*
Copyright 2019-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package base

import (
	"context"
	"math/rand"
	"strconv"
	"testing"
)

// LogContextKey is used to key a LogContext value
type LogContextKey struct{}

// LogContext stores values which may be useful to include in logs
type LogContext struct {
	// CorrelationID is a pre-formatted identifier used to correlate logs.
	// E.g: Either blip context ID or HTTP Serial number.
	CorrelationID string

	// TestName can be a unit test name (from t.Name())
	TestName string

	// TestBucketName is the name of a bucket used during a test
	TestBucketName string
}

// addContext returns a string format with additional log context if present.
func (lc *LogContext) addContext(format string) string {
	if lc == nil {
		return ""
	}

	if lc.CorrelationID != "" {
		format = "c:" + lc.CorrelationID + " " + format
	}

	if lc.TestBucketName != "" {
		format = "b:" + lc.TestBucketName + " " + format
	}

	if lc.TestName != "" {
		format = "t:" + lc.TestName + " " + format
	}

	return format
}

func FormatBlipContextID(contextID string) string {
	return "[" + contextID + "]"
}

func NewTaskID(contextID string, taskName string) string {
	return contextID + "-" + taskName + "-" + strconv.Itoa(rand.Intn(65536))
}

// TestCtx creates a log context for the given test.
func TestCtx(t testing.TB) context.Context {
	return context.WithValue(context.Background(), LogContextKey{}, LogContext{TestName: t.Name()})
}

// bucketCtx extends the parent context with a bucket name.
func bucketCtx(parent context.Context, b Bucket) context.Context {
	return bucketNameCtx(parent, b.GetName())
}

// bucketNameCtx extends the parent context with a bucket name.
func bucketNameCtx(parent context.Context, bucketName string) context.Context {
	parentLogCtx, _ := parent.Value(LogContextKey{}).(LogContext)
	newCtx := LogContext{
		TestName:       parentLogCtx.TestName,
		TestBucketName: bucketName,
	}
	return context.WithValue(parent, LogContextKey{}, newCtx)
}
