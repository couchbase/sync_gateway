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

	// DbLogConfig is database-specific log settings that should be applied (see DatabaseLogCtx)
	DbLogConfig *DbLogConfig

	// Bucket is the name of the backing bucket (see KeyspaceLogCtx)
	Bucket string

	// Scope is the name of a scope see (KeyspaceLogCtx)
	Scope string

	// Collection is the name of the collection (see KeyspaceLogCtx)
	Collection string

	// TestName can be a unit test name (see TestCtx)
	TestName string

	// RequestAdditionalAuditFields is a map of fields to be included in audit logs
	RequestAdditionalAuditFields map[string]any
	// Username is the name of the authenticated user
	Username string
	// UserDomain can be syncgateway or couchbase depending on whether the authenticated user is a sync gateway user or a couchbase RBAC user
	UserDomain string
	// RequestHost is the HTTP Host of the request associated with this log.
	RequestHost string
	// RequestRemoteAddr is the IP and port of the remote client making the request associated with this log
	RequestRemoteAddr string

	// implicitDefaultCollection is set to true when the context represents the default collection, but we want to omit that value from logging to prevent verbosity.
	implicitDefaultCollection bool
}

// DbLogConfig can be used to customise the logging for logs associated with this database.
type DbLogConfig struct {
	Console *DbConsoleLogConfig
	Audit   *DbAuditLogConfig
}

// DbConsoleLogConfig can be used to customise the console logging for logs associated with this database.
type DbConsoleLogConfig struct {
	LogLevel *LogLevel
	LogKeys  *LogKeyMask
}

// DbAuditLogConfig can be used to customise the audit logging for events associated with this database.
type DbAuditLogConfig struct {
	Enabled       bool
	EnabledEvents map[AuditID]struct{}
}

// addContext returns a string format with additional log context if present.
func (lc *LogContext) addContext(format string) string {
	if lc == nil {
		return ""
	}

	if lc.Bucket != "" {
		if lc.Database != "" {
			if !lc.implicitDefaultCollection && lc.Collection != "" {
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
	} else if !lc.implicitDefaultCollection && lc.Collection != "" {
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
		CorrelationID:                lc.CorrelationID,
		Database:                     lc.Database,
		DbLogConfig:                  lc.DbLogConfig,
		Bucket:                       lc.Bucket,
		Scope:                        lc.Scope,
		Collection:                   lc.Collection,
		TestName:                     lc.TestName,
		RequestAdditionalAuditFields: lc.RequestAdditionalAuditFields,
		Username:                     lc.Username,
		UserDomain:                   lc.UserDomain,
		RequestHost:                  lc.RequestHost,
		RequestRemoteAddr:            lc.RequestRemoteAddr,
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

// BucketCtx extends the parent context with a bucket name.
func bucketCtx(parent context.Context, b Bucket) context.Context {
	return BucketNameCtx(parent, b.GetName())
}

// getLogContext returns a log context possibly extending from previous context.
func getLogCtx(ctx context.Context) LogContext {
	parentLogCtx, ok := ctx.Value(requestContextKey).(*LogContext)
	if !ok {
		return LogContext{}
	}
	return parentLogCtx.getCopy()
}

// BucketNameCtx extends the parent context with a bucket name.
func BucketNameCtx(parent context.Context, bucketName string) context.Context {
	newCtx := getLogCtx(parent)
	newCtx.Bucket = bucketName
	return LogContextWith(parent, &newCtx)
}

// CollectionCtx extends the parent context with a collection name.
func CollectionLogCtx(parent context.Context, scopeName, collectionName string) context.Context {
	newCtx := getLogCtx(parent)
	newCtx.Scope = scopeName
	newCtx.Collection = collectionName
	return LogContextWith(parent, &newCtx)
}

// ImplicitDefaultCollectionCtx extends the parent context with _default._default collection. When logging, col:_default will not be shown.
func ImplicitDefaultCollectionLogCtx(parent context.Context) context.Context {
	newCtx := getLogCtx(parent)
	newCtx.implicitDefaultCollection = true
	newCtx.Scope = DefaultScope
	newCtx.Collection = DefaultCollection
	return LogContextWith(parent, &newCtx)
}

// CorrelationIDCtx extends the parent context with a correlation ID (HTTP request ID, BLIP context ID, etc.)
func CorrelationIDLogCtx(parent context.Context, correlationID string) context.Context {
	newCtx := getLogCtx(parent)
	newCtx.CorrelationID = correlationID
	return LogContextWith(parent, &newCtx)
}

// DatabaseLogCtx extends the parent context with a database name.
func DatabaseLogCtx(parent context.Context, databaseName string, config *DbLogConfig) context.Context {
	newCtx := getLogCtx(parent)
	newCtx.Database = databaseName
	newCtx.DbLogConfig = config
	return LogContextWith(parent, &newCtx)
}

// AuditLogCtx extends the parent context with additional audit fields
func AuditLogCtx(parent context.Context, additionalAuditFields map[string]any) context.Context {
	newCtx := getLogCtx(parent)
	newCtx.RequestAdditionalAuditFields = additionalAuditFields
	return LogContextWith(parent, &newCtx)
}

// KeyspaceLogCtx extends the parent context with a fully qualified keyspace (bucket.scope.collection)
func KeyspaceLogCtx(parent context.Context, bucketName, scopeName, collectionName string) context.Context {
	newCtx := getLogCtx(parent)
	newCtx.Bucket = bucketName
	newCtx.Collection = collectionName
	newCtx.Scope = scopeName
	return LogContextWith(parent, &newCtx)
}

func UserLogCtx(parent context.Context, user, domain string) context.Context {
	newCtx := getLogCtx(parent)
	newCtx.Username = user
	newCtx.UserDomain = domain
	return LogContextWith(parent, &newCtx)
}

type RequestData struct {
	RequestHost       string
	RequestRemoteAddr string
}

func RequestLogCtx(parent context.Context, d RequestData) context.Context {
	newCtx := getLogCtx(parent)
	newCtx.RequestHost = d.RequestHost
	newCtx.RequestRemoteAddr = d.RequestRemoteAddr
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
