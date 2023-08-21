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
	// E.g: Either blip context ID or HTTP Serial number. (see CorrelationIDLogCtx)
	CorrelationID string

	// Database is the name of the sync gateway database (see DatabaseLogCtx)
	Database string

	// DbConsoleLogConfig is database-specific log settings that should be applied (see DatabaseLogCtx)
	DbConsoleLogConfig *DbConsoleLogConfig

	// Bucket is the name of the backing bucket (see KeyspaceLogCtx)
	Bucket string

	// Scope is the name of a scope see (KeyspaceLogCtx)
	Scope string

	// Collection is the name of the collection (see KeyspaceLogCtx)
	Collection string

	// TestName can be a unit test name (see TestCtx)
	TestName string
}

// DbConsoleLogConfig can be used to customise the console logging for logs associated with this database.
type DbConsoleLogConfig struct {
	LogLevel *LogLevel
	LogKeys  *LogKeyMask
}

// addContext returns a string format with additional log context if present.
func (lc *LogContext) addContext(format string) string {
	if lc == nil {
		return ""
	}

	if lc.Bucket != "" {
		if lc.Database != "" {
			if lc.Collection != "" {
				format = "col:" + lc.Collection + " " + format
			}

		} else {
			keyspace := "b:" + lc.Bucket
			if lc.Scope != "" {
				keyspace += "." + lc.Scope
			}
			if lc.Collection != "" {
				keyspace += "." + lc.Collection
			}
			format = keyspace + " " + format
		}
	} else if lc.Collection != "" {
		format = "col:" + lc.Collection + " " + format
	}

	if lc.Database != "" {
		format = "db:" + lc.Database + " " + format
	}
	if lc.CorrelationID != "" {
		format = "c:" + lc.CorrelationID + " " + format
	}

	if lc.TestName != "" {
		format = "t:" + lc.TestName + " " + format
	}

	return format
}

func (lc *LogContext) getContextKey() LogContextKey {
	return requestContextKey
}

func (lc *LogContext) getCopy() LogContext {
	return LogContext{
		CorrelationID: lc.CorrelationID,
		Database:      lc.Database,
		Bucket:        lc.Bucket,
		Scope:         lc.Scope,
		Collection:    lc.Collection,
		TestName:      lc.TestName,
	}
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

// getLogContext returns a log context possibly extending from previous context.
func getLogCtx(ctx context.Context) LogContext {
	parentLogCtx, ok := ctx.Value(requestContextKey).(*LogContext)
	if !ok {
		return LogContext{}
	}
	return parentLogCtx.getCopy()
}

// bucketNameCtx extends the parent context with a bucket name.
func bucketNameCtx(parent context.Context, bucketName string) context.Context {
	newCtx := getLogCtx(parent)
	newCtx.Bucket = bucketName
	return LogContextWith(parent, &newCtx)
}

// CollectionCtx extends the parent context with a collection name.
func CollectionLogCtx(parent context.Context, collectionName string) context.Context {
	newCtx := getLogCtx(parent)
	newCtx.Collection = collectionName
	return LogContextWith(parent, &newCtx)
}

// CorrelationIDCtx extends the parent context with a collection name.
func CorrelationIDLogCtx(parent context.Context, correlationID string) context.Context {
	newCtx := getLogCtx(parent)
	newCtx.CorrelationID = correlationID
	return LogContextWith(parent, &newCtx)
}

// DatabaseLogCtx extends the parent context with a database.
func DatabaseLogCtx(parent context.Context, databaseName string, config *DbConsoleLogConfig) context.Context {
	newCtx := getLogCtx(parent)
	newCtx.Database = databaseName
	newCtx.DbConsoleLogConfig = config
	return LogContextWith(parent, &newCtx)
}

// KeyspaceLogCtx extends the parent context with a bucket name.
func KeyspaceLogCtx(parent context.Context, bucketName, scopeName, collectionName string) context.Context {
	newCtx := getLogCtx(parent)
	newCtx.Bucket = bucketName
	newCtx.Collection = collectionName
	newCtx.Scope = scopeName
	return LogContextWith(parent, &newCtx)
}

// LogContextKey is the type used to store custom data in the go context
type LogContextKey int

const (
	requestContextKey LogContextKey = iota
	serverLogContextKey
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
var allLogContextKeys = [...]LogContextKey{requestContextKey, serverLogContextKey}

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
