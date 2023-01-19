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
// type LogContextKey struct{}

// LogContext stores values which may be useful to include in logs
type LogContext struct {
	// CorrelationID is a pre-formatted identifier used to correlate logs.
	// E.g: Either blip context ID or HTTP Serial number.
	CorrelationID string

	// TestName can be a unit test name (from t.Name())
	TestName string

	// TestBucketName is the name of a bucket used during a test
	TestBucketName string

	// TestScopeName is the name of a scope on used during a test
	TestScopeName string

	// TestCollectionName is the name of the collection used during a test
	TestCollectionName string
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
		keyspace := "b:" + lc.TestBucketName
		if lc.TestScopeName != "" {
			keyspace += "." + lc.TestScopeName
		}
		if lc.TestCollectionName != "" {
			keyspace += "." + lc.TestCollectionName
		}

		format = keyspace + " " + format
	}

	if lc.TestName != "" {
		format = "t:" + lc.TestName + " " + format
	}

	return format
}

func (lc *LogContext) getContextKey() LogContextKey {
	return requestContextKey
}

func FormatBlipContextID(contextID string) string {
	return "[" + contextID + "]"
}

func NewTaskID(contextID string, taskName string) string {
	return contextID + "-" + taskName + "-" + strconv.Itoa(rand.Intn(65536))
}

// TestCtx creates a context for the given test which is also cancelled once the test has completed.
func TestCtx(t testing.TB) context.Context {
	ctx, cancelCtx := context.WithCancel(context.Background())
	t.Cleanup(cancelCtx)
	return LogContextWith(ctx, &LogContext{TestName: t.Name()})
}

// bucketCtx extends the parent context with a bucket name.
func bucketCtx(parent context.Context, b Bucket) context.Context {
	return bucketNameCtx(parent, b.GetName())
}

// bucketNameCtx extends the parent context with a bucket name.
func bucketNameCtx(parent context.Context, bucketName string) context.Context {
	parentLogCtx, _ := parent.Value(requestContextKey).(LogContext)
	newCtx := LogContext{
		TestName:       parentLogCtx.TestName,
		TestBucketName: bucketName,
	}
	return LogContextWith(parent, &newCtx)
}

// CollectionCtx extends the parent context with a collection name.
func CollectionCtx(parent context.Context, collectionName string) context.Context {
	newCtx := CollectionLogContext{
		Collection: collectionName,
	}
	return LogContextWith(parent, &newCtx)
}

// testKeyspaceNameCtx extends the parent context with a bucket name.
func testKeyspaceNameCtx(parent context.Context, bucketName, scopeName, collectionName string) context.Context {
	parentLogCtx, _ := parent.Value(requestContextKey).(LogContext)
	newCtx := LogContext{
		TestName:           parentLogCtx.TestName,
		TestBucketName:     bucketName,
		TestScopeName:      scopeName,
		TestCollectionName: collectionName,
	}
	return LogContextWith(parent, &newCtx)
}

// LogContextKey is the type used to store custom data in the go context
type LogContextKey int

const (
	requestContextKey LogContextKey = iota
	serverLogContextKey
	databaseLogContextKey
	keyspaceLogContextKey
)

// ContextAdder interface should be implemented by all custom contexts.
// The custom context should provide its own key to be used with go contexts,
// and be able to add its custom info to the log format msg.
type ContextAdder interface {
	getContextKey() LogContextKey
	addContext(format string) string
}

// allLogContextKeys contains the keys of all custom contexts,
// and is used when writing log prefixes (addPrefixes)
var allLogContextKeys = [...]LogContextKey{requestContextKey, serverLogContextKey, databaseLogContextKey, keyspaceLogContextKey}

// LogContextWith is called to add custom context to the go context.
// All custom contexts should implement ContextAdder interface
func LogContextWith(parent context.Context, adder ContextAdder) context.Context {
	return context.WithValue(parent, adder.getContextKey(), adder)
}

// ServerLogContext stores server context data for logging
type ServerLogContext struct {
	LogContextID string
}

func (c *ServerLogContext) getContextKey() LogContextKey {
	return serverLogContextKey
}

func (c *ServerLogContext) addContext(format string) string {
	if c != nil && c.LogContextID != "" {
		format = "sc:" + c.LogContextID + " " + format
	}
	return format
}

// DatabaseLogContext provides database context data for logging
type DatabaseLogContext struct {
	DatabaseName string
}

func (c *DatabaseLogContext) getContextKey() LogContextKey {
	return databaseLogContextKey
}

func (c *DatabaseLogContext) addContext(format string) string {
	if c != nil && c.DatabaseName != "" {
		format = "db:" + c.DatabaseName + " " + format
	}
	return format
}

// CollectionLogContext provides collection context data for logging
type CollectionLogContext struct {
	Collection string
}

func (c *CollectionLogContext) getContextKey() LogContextKey {
	return keyspaceLogContextKey
}

func (c *CollectionLogContext) addContext(format string) string {
	if c.Collection != "" {
		format = "col:" + c.Collection + " " + format

	}
	return format
}
